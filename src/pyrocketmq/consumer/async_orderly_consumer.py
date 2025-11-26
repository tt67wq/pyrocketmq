# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
异步顺序消费者实现

基于AsyncBaseConsumer实现的异步顺序消费者，保证同一消息队列中的消息
按照偏移量顺序进行消费。
"""

import asyncio
import time
from typing import Any

from pyrocketmq.broker import AsyncBrokerClient
from pyrocketmq.consumer.allocate_queue_strategy import AllocateContext
from pyrocketmq.consumer.async_base_consumer import AsyncBaseConsumer
from pyrocketmq.consumer.config import ConsumerConfig
from pyrocketmq.consumer.process_queue import ProcessQueue
from pyrocketmq.logging import get_logger
from pyrocketmq.model import MessageModel, MessageQueue
from pyrocketmq.remote import AsyncConnectionPool


class AsyncOrderlyConsumer(AsyncBaseConsumer):
    """
    异步顺序消费者

    继承AsyncBaseConsumer，专注于顺序消费逻辑。保证同一消息队列中的消息
    按照偏移量严格顺序处理，每个队列同时只能有一个消费任务。
    """

    def __init__(self, config: ConsumerConfig):
        """
        初始化异步顺序消费者

        Args:
            config: 消费者配置
        """

        # 调用父类初始化
        super().__init__(config)

        self.logger = get_logger(__name__)
        # 顺序消费特有字段
        self._queue_locks: dict[str, asyncio.Lock] = {}  # 队列级锁，确保顺序消费
        self._consume_tasks: dict[str, asyncio.Task[None]] = {}  # 队列消费任务
        self._pull_tasks: dict[str, asyncio.Task[None]] = {}  # 队列拉取任务

        self._msg_cache: dict[MessageQueue, ProcessQueue] = {}
        self._cache_lock = asyncio.Lock()  # 用于保护_msg_cache字典

        # 状态管理
        self._assigned_queues: dict[MessageQueue, int] = {}  # queue -> last_offset
        self._assigned_queues_lock = (
            asyncio.Lock()
        )  # 🔐保护_assigned_queues字典的并发访问
        self._last_rebalance_time: float = 0.0

        # 重平衡任务管理
        self._rebalance_task: asyncio.Task[None] | None = None
        self._rebalance_interval: float = 20.0  # 重平衡间隔(秒)

        # 线程同步事件
        self._rebalance_event: asyncio.Event = asyncio.Event()  # 用于重平衡循环的事件

        # 线程停止事件 - 用于优雅关闭拉取和消费循环
        self._pull_stop_events: dict[str, asyncio.Event] = {}
        self._consume_stop_events: dict[str, asyncio.Event] = {}
        self._stop_events_lock = asyncio.Lock()  # 保护停止事件字典

        # 重平衡重入保护
        self._rebalance_lock = asyncio.Lock()  # 重平衡锁，防止重入

        # 远程锁缓存和有效期管理
        # 避免每次消费循环都需要获取远程锁，提升性能
        self._remote_lock_cache: dict[
            MessageQueue, float
        ] = {}  # queue -> lock_expiry_time
        self._remote_lock_cache_lock = asyncio.Lock()  # 保护远程锁缓存
        self._remote_lock_expire_time: float = 30.0  # 远程锁有效期30秒

        # 统计信息扩展
        self._stats.update(
            {
                "queue_lock_wait_count": 0,
                "queue_lock_wait_total_time": 0.0,
                "orderly_consume_success_count": 0,
                "orderly_consume_fail_count": 0,
                "orderly_consume_rt_total": 0.0,
                "orderly_consume_rt_count": 0,
            }
        )

        self.logger.info(
            "AsyncOrderlyConsumer initialized",
            extra={
                "consumer_group": self._config.consumer_group,
                "message_model": self._config.message_model,
                "consume_thread_max": self._config.consume_thread_max,
                "pull_batch_size": self._config.pull_batch_size,
                "remote_lock_expire_time": self._remote_lock_expire_time,
            },
        )

    async def start(self) -> None:
        """异步启动顺序消费者。

        初始化并启动消费者的所有组件，包括：
        - 建立与NameServer和Broker的网络连接
        - 创建异步任务管理器
        - 执行初始队列分配和重平衡
        - 启动心跳和重平衡后台任务

        启动失败时会自动清理已分配的资源。

        Raises:
            ConsumerStartError: 当以下情况发生时抛出：
                - 未注册消息监听器
                - 消息监听器类型不匹配（需要AsyncMessageListener）
                - 网络连接失败
                - 异步任务创建失败
                - 其他初始化错误

        Note:
            此方法是线程安全的，多次调用只会启动一次。
            启动成功后，消费者会自动开始拉取和处理消息。
        """
        async with self._lock:
            if self._is_running:
                self.logger.warning("AsyncOrderlyConsumer is already running")
                return

            try:
                self.logger.info(
                    "Starting AsyncOrderlyConsumer",
                    extra={
                        "consumer_group": self._config.consumer_group,
                        "namesrv_addr": self._config.namesrv_addr,
                    },
                )

                # 启动AsyncBaseConsumer
                await super().start()

                # 执行初始重平衡
                await self._do_rebalance()

                # 启动重平衡任务
                await self._start_rebalance_task()

                self._stats["start_time"] = time.time()

                async with self._assigned_queues_lock:
                    assigned_queues_count = len(self._assigned_queues)

                self.logger.info(
                    "AsyncOrderlyConsumer started successfully",
                    extra={
                        "consumer_group": self._config.consumer_group,
                        "assigned_queues": assigned_queues_count,
                        "rebalance_interval": self._rebalance_interval,
                        "remote_lock_expire_time": self._remote_lock_expire_time,
                    },
                )

            except Exception as e:
                self.logger.error(
                    f"Failed to start AsyncOrderlyConsumer: {e}",
                    extra={
                        "consumer_group": self._config.consumer_group,
                        "error": str(e),
                    },
                    exc_info=True,
                )
                await self._cleanup_on_start_failure()
                from .errors import ConsumerStartError

                raise ConsumerStartError(
                    "Failed to start AsyncOrderlyConsumer",
                    cause=e,
                    context={"consumer_group": self._config.consumer_group},
                ) from e

    async def shutdown(self) -> None:
        """异步优雅停止顺序消费者。

        执行以下关闭流程：
        1. 停止接受新的消息拉取请求
        2. 等待正在处理的消息完成（最多等待30秒）
        3. 持久化所有队列的消费偏移量
        4. 关闭所有异步任务和后台任务
        5. 清理网络连接和资源

        Args:
            None

        Returns:
            None

        Raises:
            ConsumerShutdownError: 当以下情况发生时抛出：
                - 偏移量持久化失败
                - 异步任务关闭超时
                - 网络连接清理失败
                - 其他清理过程中的错误

        Note:
            - 此方法是线程安全的，可以多次调用
            - 会尽力等待正在处理的消息完成，但不会无限期等待
            - 即使关闭过程中发生错误，也会继续执行后续的清理步骤
            - 关闭后的消费者不能重新启动，需要创建新实例
        """
        async with self._lock:
            if not self._is_running:
                self.logger.warning("AsyncOrderlyConsumer is not running")
                return

            try:
                self.logger.info(
                    "Shutting down AsyncOrderlyConsumer",
                    extra={"consumer_group": self._config.consumer_group},
                )

                self._is_running = False

                # 先设置Event以唤醒可能阻塞的异步任务
                self._rebalance_event.set()

                # 停止拉取任务
                await self._stop_pull_tasks()

                # 停止消费任务
                await self._stop_consume_tasks()

                # 等待处理中的消息完成
                await self._wait_for_processing_completion()

                # 持久化偏移量
                try:
                    await self._offset_store.persist_all()
                except Exception as e:
                    self.logger.error(
                        f"Failed to persist offsets during shutdown: {e}",
                        extra={
                            "consumer_group": self._config.consumer_group,
                            "error": str(e),
                        },
                        exc_info=True,
                    )

                # 停止异步任务
                await self._shutdown_async_tasks()

                # 清理资源
                await self._cleanup_resources()

                # 调用父类关闭
                await super().shutdown()

                self.logger.info(
                    "AsyncOrderlyConsumer shutdown completed",
                    extra={
                        "consumer_group": self._config.consumer_group,
                        "final_stats": await self._get_final_stats(),
                    },
                )

            except Exception as e:
                self.logger.error(
                    f"Error during AsyncOrderlyConsumer shutdown: {e}",
                    extra={
                        "consumer_group": self._config.consumer_group,
                        "error": str(e),
                    },
                    exc_info=True,
                )
                from .errors import ConsumerShutdownError

                raise ConsumerShutdownError(
                    "Error during async consumer shutdown",
                    cause=e,
                    context={"consumer_group": self._config.consumer_group},
                ) from e

    async def _pre_rebalance_check(self) -> bool:
        """执行重平衡前置检查。

        检查是否可以执行重平衡操作，包括锁获取和订阅状态检查。

        Returns:
            bool: 如果可以执行重平衡返回True，否则返回False

        Raises:
            None: 此方法不会抛出异常
        """
        # 多个地方都会触发重平衡，加入一个放置重入机制，如果正在执行rebalance，再次触发无效
        # 使用可重入锁保护重平衡操作
        if not await self._rebalance_lock.acquire():
            # 如果无法获取锁，说明正在执行重平衡，跳过本次请求
            self._stats["rebalance_skipped_count"] += 1
            self.logger.debug(
                "Rebalance already in progress, skipping",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "skipped_count": self._stats["rebalance_skipped_count"],
                },
            )
            return False

        # 检查是否有订阅的Topic
        topics: set[str] = self._subscription_manager.get_topics()
        if not topics:
            self.logger.debug("No topics subscribed, skipping rebalance")
            self._rebalance_lock.release()
            return False

        return True

    async def _collect_and_allocate_queues(self) -> list[MessageQueue]:
        """收集所有Topic的可用队列并执行分配。

        遍历所有订阅的Topic，获取每个Topic的可用队列，
        并为每个Topic执行队列分配算法。

        Returns:
            list[MessageQueue]: 分配给当前消费者的所有队列列表

        Raises:
            Exception: 路由信息更新或队列分配失败时抛出异常
        """
        allocated_queues: list[MessageQueue] = []
        topics = self._subscription_manager.get_topics()

        for topic in topics:
            try:
                # 异步更新Topic路由信息
                _ = await self._update_route_info(topic)

                # 获取Topic的所有可用队列
                all_queues: list[MessageQueue] = [
                    x
                    for (x, _) in self._topic_broker_mapping.get_subscribe_queues(topic)
                ]

                if not all_queues:
                    self.logger.debug(
                        "No queues available for subscribed topic",
                        extra={"topic": topic},
                    )
                    continue

                # 异步执行队列分配
                topic_allocated_queues = await self._allocate_queues(topic, all_queues)
                allocated_queues.extend(topic_allocated_queues)

                self.logger.debug(
                    "Topic queue allocation completed",
                    extra={
                        "topic": topic,
                        "total_queues": len(all_queues),
                        "allocated_queues": len(topic_allocated_queues),
                    },
                )

            except Exception as e:
                self.logger.warning(
                    f"Failed to allocate queues for topic {topic}: {e}",
                    extra={"topic": topic, "error": str(e)},
                )
                # 继续处理其他Topic，不中断整个重平衡过程
                continue

        return allocated_queues

    async def _allocate_queues(
        self, topic: str, all_queues: list[MessageQueue]
    ) -> list[MessageQueue]:
        """
        为当前消费者分配队列

        根据消息模式和分配策略，从所有可用队列中选择一部分分配给当前消费者实例。
        这是RocketMQ消费者负载均衡的核心机制，确保多个消费者能够合理地消费同一个Topic下的消息。

        Args:
            topic: 要分配队列的Topic名称
            all_queues: 该Topic下所有可用的消息队列列表

        Returns:
            list[MessageQueue]: 分配给当前消费者的队列列表
        """
        if self._config.message_model == MessageModel.CLUSTERING:
            cids = await self._find_consumer_list(topic)
            if not cids:
                return []

            return self._allocate_strategy.allocate(
                AllocateContext(
                    self._config.consumer_group,
                    self._config.client_id,
                    cids,
                    all_queues,
                    {},
                )
            )
        else:
            return all_queues.copy()

    async def _find_consumer_list(self, topic: str) -> list[str]:
        """
        查找消费者列表

        Args:
            topic: 主题名称

        Returns:
            消费者列表
        """
        addresses: list[str] = await self._nameserver_manager.get_all_broker_addresses(
            topic
        )
        if not addresses:
            self.logger.warning(
                "No broker addresses found for topic", extra={"topic": topic}
            )
            return []

        pool: AsyncConnectionPool = await self._broker_manager.must_connection_pool(
            addresses[0]
        )
        async with pool.get_connection(usage="查找消费者列表") as conn:
            return await AsyncBrokerClient(conn).get_consumers_by_group(
                self._config.consumer_group
            )

    async def _cleanup_on_start_failure(self) -> None:
        """异步启动失败时的资源清理操作。

        当消费者启动过程中发生异常时，调用此方法清理已分配的资源，
        确保消费者状态一致，避免资源泄漏。

        清理流程：
        1. 停止异步任务（拉取任务、消费任务、重平衡任务）
        2. 停止核心组件（NameServer、BrokerManager、偏移量存储）
        3. 清理内存资源和队列

        Args:
            None

        Returns:
            None

        Raises:
            None: 此方法会捕获所有异常并记录日志

        Note:
            此方法是异步的，确保所有IO操作都不阻塞事件循环
        """
        try:
            self.logger.info(
                "Cleaning up resources after startup failure",
                extra={"consumer_group": self._config.consumer_group},
            )

            # 停止异步任务
            await self._shutdown_async_tasks()

            # 停止核心组件
            await self._async_cleanup_resources()

            self.logger.info(
                "Startup failure cleanup completed",
                extra={"consumer_group": self._config.consumer_group},
            )

        except Exception as e:
            self.logger.error(
                f"Error during startup failure cleanup: {e}",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "error": str(e),
                },
                exc_info=True,
            )

    async def _wait_for_processing_completion(self) -> None:
        """异步等待正在处理的消息完成

        等待所有消费任务完成，最多等待30秒。如果超时，会记录警告日志
        但不会阻塞关闭流程。
        """
        try:
            timeout: int = 30  # 30秒超时

            # 收集所有未完成的消费任务
            all_tasks: list[asyncio.Task[None]] = []
            for task in self._consume_tasks.values():
                if task and not task.done():
                    all_tasks.append(task)

            # 等待所有任务完成或超时
            if all_tasks:
                try:
                    await asyncio.wait_for(
                        asyncio.gather(*all_tasks, return_exceptions=True),
                        timeout=timeout,
                    )
                except asyncio.TimeoutError:
                    # 取消未完成的任务
                    for task in all_tasks:
                        if not task.done():
                            task.cancel()

                    # 等待取消完成（短暂等待）
                    try:
                        await asyncio.wait_for(
                            asyncio.gather(*all_tasks, return_exceptions=True),
                            timeout=5.0,
                        )
                    except asyncio.TimeoutError:
                        pass

                    self.logger.warning(
                        "Timeout waiting for consume tasks to complete during shutdown",
                        extra={
                            "consumer_group": self._config.consumer_group,
                            "timeout": timeout,
                            "cancelled_tasks": len(all_tasks),
                        },
                    )

        except Exception as e:
            self.logger.error(
                f"Error waiting for processing completion: {e}",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "error": str(e),
                },
                exc_info=True,
            )

    async def _shutdown_async_tasks(self) -> None:
        """异步关闭所有异步任务"""
        try:
            # 取消重平衡任务
            if self._rebalance_task and not self._rebalance_task.done():
                self._rebalance_task.cancel()
                try:
                    await self._rebalance_task
                except asyncio.CancelledError:
                    pass
                self._rebalance_task = None

            # 取消所有拉取任务
            for task in self._pull_tasks.values():
                if task and not task.done():
                    task.cancel()
            self._pull_tasks.clear()

            # 取消所有消费任务
            for task in self._consume_tasks.values():
                if task and not task.done():
                    task.cancel()
            self._consume_tasks.clear()

            self.logger.info(
                "Async tasks shutdown completed",
                extra={"consumer_group": self._config.consumer_group},
            )

        except Exception as e:
            self.logger.error(
                f"Error shutting down async tasks: {e}",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "error": str(e),
                },
                exc_info=True,
            )

    async def _cleanup_resources(self) -> None:
        """异步清理资源"""
        try:
            # 清理ProcessQueue消息缓存
            for process_queue in self._msg_cache.values():
                _ = process_queue.clear()
            self._msg_cache.clear()

            # 清理停止事件
            async with self._stop_events_lock:
                self._pull_stop_events.clear()
                self._consume_stop_events.clear()

            # 清理状态
            self._pull_tasks.clear()
            self._consume_tasks.clear()

            # 远程解锁所有已分配的队列
            async with self._assigned_queues_lock:
                assigned_queues = list(
                    self._assigned_queues.keys()
                )  # 复制一份避免并发修改

            for queue in assigned_queues:
                try:
                    await self._unlock_remote_queue(queue)
                except Exception as e:
                    self.logger.warning(
                        f"Failed to unlock remote queue {queue} during cleanup: {e}",
                        extra={
                            "consumer_group": self._config.consumer_group,
                            "topic": queue.topic,
                            "queue_id": queue.queue_id,
                            "broker_name": queue.broker_name,
                            "error": str(e),
                        },
                    )

            # 清理队列锁
            self._queue_locks.clear()

            # 清理远程锁缓存
            async with self._remote_lock_cache_lock:
                self._remote_lock_cache.clear()

            # 清理分配的队列
            async with self._assigned_queues_lock:
                self._assigned_queues.clear()

            self.logger.info(
                "Async resources cleanup completed",
                extra={"consumer_group": self._config.consumer_group},
            )

        except Exception as e:
            self.logger.error(
                f"Error during async resource cleanup: {e}",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "error": str(e),
                },
                exc_info=True,
            )

    async def _get_final_stats(self) -> dict[str, Any]:
        """异步获取最终统计信息"""
        stats = {
            "consumer_group": self._config.consumer_group,
            "shutdown_time": time.time(),
        }

        # 合并顺序消费统计信息
        stats.update(self._orderly_stats)

        # 添加队列相关信息
        async with self._assigned_queues_lock:
            stats["assigned_queues_count"] = len(self._assigned_queues)

        return stats
