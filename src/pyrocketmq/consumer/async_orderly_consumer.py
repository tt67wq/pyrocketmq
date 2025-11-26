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

from pyrocketmq.broker import AsyncBrokerClient, MessagePullError
from pyrocketmq.consumer.allocate_queue_strategy import AllocateContext
from pyrocketmq.consumer.async_base_consumer import AsyncBaseConsumer
from pyrocketmq.consumer.config import ConsumerConfig
from pyrocketmq.consumer.errors import MessageConsumeError
from pyrocketmq.consumer.offset_store import ReadOffsetType
from pyrocketmq.consumer.process_queue import ProcessQueue
from pyrocketmq.logging import get_logger
from pyrocketmq.model import (
    MessageExt,
    MessageModel,
    MessageQueue,
    SubscriptionData,
    SubscriptionEntry,
)
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
        self._queue_locks: dict[
            MessageQueue, asyncio.Semaphore
        ] = {}  # 队列级锁信号量，确保顺序消费
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
        if not self._rebalance_lock.acquire():
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

    async def _finalize_rebalance(self, total_topics: int, total_queues: int) -> None:
        """完成重平衡后处理。

        更新重平衡时间戳、统计信息，并记录完成日志。

        Args:
            total_topics: 重平衡处理的Topic总数
            total_queues: 分配到的队列总数

        Raises:
            None: 此方法不会抛出异常
        """
        self._last_rebalance_time = time.time()

        # 更新成功统计
        self._stats["rebalance_success_count"] = (
            self._stats.get("rebalance_success_count", 0) + 1
        )

        self.logger.info(
            "Rebalance completed",
            extra={
                "consumer_group": self._config.consumer_group,
                "total_topics": total_topics,
                "assigned_queues": total_queues,
                "success_count": self._stats["rebalance_success_count"],
            },
        )

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

    async def _get_queue_lock(self, message_queue: MessageQueue) -> asyncio.Semaphore:
        """获取指定消息队列的锁信号量

        使用双重检查锁定模式来避免竞争条件

        Args:
            message_queue: 消息队列

        Returns:
            asyncio.Semaphore: 该队列的异步锁信号量对象（值为1的信号量）
        """
        # 首先进行无锁检查，提高性能
        if message_queue in self._queue_locks:
            return self._queue_locks[message_queue]

        # 由于字典操作本身是原子的，且我们在单线程事件循环中运行，
        # 不需要额外的锁保护
        if message_queue not in self._queue_locks:
            self._queue_locks[message_queue] = asyncio.Semaphore(1)

        return self._queue_locks[message_queue]

    async def _is_locked(self, message_queue: MessageQueue) -> bool:
        """检查指定队列是否已锁定

        Args:
            message_queue: 消息队列

        Returns:
            bool: True如果队列已锁定，False如果队列未锁定
        """
        if message_queue not in self._queue_locks:
            return False

        # asyncio.Semaphore有locked()方法，可以直接检查状态
        return self._queue_locks[message_queue].locked()

    async def _is_remote_lock_valid(self, message_queue: MessageQueue) -> bool:
        """检查指定队列的远程锁是否仍然有效

        Args:
            message_queue: 消息队列

        Returns:
            True如果远程锁仍然有效，False如果已过期或不存在
        """
        async with self._remote_lock_cache_lock:
            expiry_time = self._remote_lock_cache.get(message_queue)
            if expiry_time is None:
                return False

            current_time = time.time()
            return current_time < expiry_time

    async def _set_remote_lock_expiry(self, message_queue: MessageQueue) -> None:
        """设置指定队列的远程锁过期时间

        Args:
            message_queue: 消息队列
        """
        async with self._remote_lock_cache_lock:
            expiry_time = time.time() + self._remote_lock_expire_time
            self._remote_lock_cache[message_queue] = expiry_time

    async def _invalidate_remote_lock(self, message_queue: MessageQueue) -> None:
        """使指定队列的远程锁失效

        Args:
            message_queue: 消息队列
        """
        async with self._remote_lock_cache_lock:
            self._remote_lock_cache.pop(message_queue, None)

    async def _lock_remote_queue(self, message_queue: MessageQueue) -> bool:
        """尝试远程锁定指定队列

        Args:
            message_queue: 消息队列

        Returns:
            True如果锁定成功，False如果锁定失败
        """
        try:
            # 获取队列对应的Broker地址
            broker_address: (
                str | None
            ) = await self._nameserver_manager.get_broker_address(
                message_queue.broker_name
            )
            if not broker_address:
                self.logger.warning(
                    f"Broker address not found for queue: {message_queue}"
                )
                return False

            connection_pool = await self._broker_manager.must_connection_pool(
                broker_address
            )

            # 创建异步broker客户端
            async with connection_pool.get_connection() as conn:
                broker_client = AsyncBrokerClient(conn)

                # 尝试锁定队列
                locked_queues = await broker_client.lock_batch_mq(
                    consumer_group=self._config.consumer_group,
                    client_id=self._config.client_id,
                    mqs=[message_queue],
                )

                # 检查锁定是否成功
                if locked_queues and len(locked_queues) > 0:
                    # 成功获取远程锁，设置过期时间
                    await self._set_remote_lock_expiry(message_queue)
                    self.logger.debug(
                        f"Successfully locked remote queue: {message_queue}",
                        extra={
                            "consumer_group": self._config.consumer_group,
                            "client_id": self._config.client_id,
                            "queue": str(message_queue),
                            "operation": "lock_remote_queue",
                            "expire_seconds": self._remote_lock_expire_time,
                        },
                    )
                    return True
                else:
                    self.logger.warning(
                        f"Failed to lock remote queue: {message_queue}",
                        extra={
                            "consumer_group": self._config.consumer_group,
                            "client_id": self._config.client_id,
                            "queue": str(message_queue),
                            "operation": "lock_remote_queue",
                        },
                    )
                    return False

        except Exception as e:
            self.logger.error(
                f"Exception occurred while locking remote queue {message_queue}: {e}",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "client_id": self._config.client_id,
                    "queue": str(message_queue),
                    "error": str(e),
                    "operation": "lock_remote_queue",
                },
                exc_info=True,
            )
            return False

    async def _unlock_remote_queue(self, message_queue: MessageQueue) -> bool:
        """尝试远程解锁指定队列

        Args:
            message_queue: 消息队列

        Returns:
            True如果解锁成功，False如果解锁失败
        """
        try:
            # 获取队列对应的Broker地址
            broker_address: (
                str | None
            ) = await self._nameserver_manager.get_broker_address(
                message_queue.broker_name
            )
            if not broker_address:
                self.logger.warning(
                    f"Broker address not found for queue: {message_queue}"
                )
                return False

            connection_pool = await self._broker_manager.must_connection_pool(
                broker_address
            )

            async with connection_pool.get_connection() as conn:
                broker_client = AsyncBrokerClient(conn)

                # 尝试解锁队列
                await broker_client.unlock_batch_mq(
                    consumer_group=self._config.consumer_group,
                    client_id=self._config.client_id,
                    mqs=[message_queue],
                )

                # 清除远程锁缓存
                await self._invalidate_remote_lock(message_queue)

                self.logger.debug(
                    f"Successfully unlocked remote queue: {message_queue}",
                    extra={
                        "consumer_group": self._config.consumer_group,
                        "client_id": self._config.client_id,
                        "queue": str(message_queue),
                        "operation": "unlock_remote_queue",
                    },
                )
                return True

        except Exception as e:
            self.logger.error(
                f"Exception occurred while unlocking remote queue {message_queue}: {e}",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "client_id": self._config.client_id,
                    "queue": str(message_queue),
                    "error": str(e),
                    "operation": "unlock_remote_queue",
                },
                exc_info=True,
            )
            return False

    async def _start_pull_tasks_for_queues(self, queues: set[MessageQueue]) -> None:
        """为指定队列启动拉取任务

        Args:
            queues: 要启动拉取任务的队列集合
        """
        for queue in queues:
            if queue not in self._pull_tasks:
                # 为每个队列创建停止事件
                async with self._stop_events_lock:
                    pull_stop_event = asyncio.Event()
                    consume_stop_event = asyncio.Event()
                    self._pull_stop_events[str(queue)] = pull_stop_event
                    self._consume_stop_events[str(queue)] = consume_stop_event

                # 启动拉取任务，传入停止事件
                task = asyncio.create_task(
                    self._pull_messages_loop(queue, pull_stop_event)
                )
                self._pull_tasks[str(queue)] = task

                self.logger.debug(
                    f"Started pull task for queue: {queue}",
                    extra={
                        "consumer_group": self._config.consumer_group,
                        "topic": queue.topic,
                        "queue_id": queue.queue_id,
                    },
                )

    async def _stop_pull_tasks(self) -> None:
        """停止所有消息拉取任务 - 使用停止事件优雅关闭"""
        if not self._pull_tasks:
            return

        # 设置所有停止事件
        async with self._stop_events_lock:
            for queue_key in self._pull_tasks.keys():
                # 设置拉取停止事件
                if queue_key in self._pull_stop_events:
                    self._pull_stop_events[queue_key].set()
                # 设置消费停止事件
                if queue_key in self._consume_stop_events:
                    self._consume_stop_events[queue_key].set()

        # 取消所有异步任务
        for queue_key, task in self._pull_tasks.items():
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    self.logger.debug(
                        f"Pull task cancelled for queue: {queue_key}",
                        extra={"consumer_group": self._config.consumer_group},
                    )

        self._pull_tasks.clear()

        # 清理停止事件
        async with self._stop_events_lock:
            self._pull_stop_events.clear()
            self._consume_stop_events.clear()

        self.logger.debug(
            "All pull tasks stopped",
            extra={"consumer_group": self._config.consumer_group},
        )

    async def _perform_single_pull(
        self, message_queue: MessageQueue, suggest_broker_id: int
    ) -> tuple[list[MessageExt], int, int] | None:
        """执行单次消息拉取操作。

        Args:
            message_queue: 要拉取消息的队列
            suggest_broker_id: 建议的Broker ID

        Returns:
            tuple[list[MessageExt], int, int] | None:
                - messages: 拉取到的消息列表
                - next_begin_offset: 下次拉取的起始偏移量
                - next_suggest_id: 下次建议的Broker ID
            None: 如果没有订阅信息

        Raises:
            MessagePullError: 当拉取请求非法时抛出
        """
        # 获取当前偏移量
        current_offset: int = await self._get_or_initialize_offset(message_queue)

        # 拉取消息
        messages, next_begin_offset, next_suggest_id = await self._pull_messages(
            message_queue,
            current_offset,
            suggest_broker_id,
        )

        # 检查订阅信息
        sub: SubscriptionEntry | None = self._subscription_manager.get_subscription(
            message_queue.topic
        )
        if sub is None:
            # 如果没有订阅信息，则停止消费
            return None

        sub_data: SubscriptionData = sub.subscription_data

        # 根据订阅信息过滤消息
        if sub_data.tags_set:
            messages = self._filter_messages_by_tags(messages, sub_data.tags_set)

        return messages, next_begin_offset, next_suggest_id

    async def _pull_messages(
        self, message_queue: MessageQueue, offset: int, suggest_id: int
    ) -> tuple[list[MessageExt], int, int]:
        """从指定队列拉取消息，支持偏移量管理和Broker选择。

        该方法是顺序消费者的核心拉取逻辑，负责从RocketMQ Broker拉取消息，
        并处理相关的系统标志位和偏移量管理。支持主备Broker的智能选择
        和故障转移机制。

        核心功能:
        - 通过NameServerManager获取最优Broker地址
        - 构建拉取请求的系统标志位
        - 处理commit offset的提交逻辑
        - 支持批量消息拉取以提高效率
        - 完善的错误处理和重试机制

        拉取策略:
        1. 获取目标Broker地址，优先连接master
        2. 读取当前commit offset（如果有）
        3. 构建包含commit标志的系统标志位
        4. 发送PULL_MESSAGE请求到Broker
        5. 解析响应并返回消息列表和下次拉取位置

        返回值说明:
        - list[MessageExt]: 拉取到的消息列表，可能为空
        - int: 下一次拉取的起始偏移量
        - int: 建议下次连接的Broker ID（0=master, 其他=slave）

        Args:
            message_queue (MessageQueue): 目标消息队列，包含topic、broker名称、队列ID等信息
            offset (int): 本次拉取的起始偏移量，从该位置开始拉取消息
            suggest_id (int): 建议的Broker ID，用于连接选择优化，
                            通常为上次拉取时返回的建议ID

        Returns:
            tuple[list[MessageExt], int, int]: 三元组包含：
                                            - 消息列表（可能为空）
                                            - 下次拉取的起始偏移量
                                            - 建议的下次Broker ID

        Raises:
            MessageConsumeError: 当拉取过程中发生错误时抛出，包含详细的错误信息
            ValueError: 当无法找到指定broker的地址时抛出
        """
        try:
            self._stats["pull_requests"] = self._stats.get("pull_requests", 0) + 1

            broker_info: (
                tuple[str, bool] | None
            ) = await self._nameserver_manager.get_broker_address_in_subscription(
                message_queue.broker_name, suggest_id
            )
            if not broker_info:
                raise ValueError(
                    f"Broker address not found for {message_queue.broker_name}"
                )

            commit_offset: int = await self._offset_store.read_offset(
                message_queue, ReadOffsetType.READ_FROM_MEMORY
            )

            broker_address, is_master = broker_info

            pool: AsyncConnectionPool = await self._broker_manager.must_connection_pool(
                broker_address
            )

            # 使用异步连接池拉取消息
            async with pool.get_connection() as conn:
                result = await conn.async_pull_message(
                    consumer_group=self._config.consumer_group,
                    topic=message_queue.topic,
                    queue_id=message_queue.queue_id,
                    offset=offset,
                    max_nums=self._config.pull_batch_size,
                    sys_flag=await self._build_sys_flag(
                        commit_offset=commit_offset > 0 and is_master
                    ),
                    commit_offset=commit_offset,
                    timeout=30,
                )

                if result.messages:
                    self._stats["pull_success"] = self._stats.get("pull_success", 0) + 1
                    return (
                        result.messages,
                        result.next_begin_offset,
                        result.suggest_which_broker_id or 0,
                    )

                self._stats["pull_success"] = self._stats.get("pull_success", 0) + 1
                return [], offset, 0

        except MessagePullError as e:
            self._stats["pull_fail"] = self._stats.get("pull_fail", 0) + 1
            self.logger.warning(
                "The pull request is illegal",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "topic": message_queue.topic,
                    "queue_id": message_queue.queue_id,
                    "offset": offset,
                    "error": str(e),
                },
            )
            raise e

        except Exception as e:
            self._stats["pull_fail"] = self._stats.get("pull_fail", 0) + 1
            self.logger.warning(
                "Failed to pull messages",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "topic": message_queue.topic,
                    "queue_id": message_queue.queue_id,
                    "offset": offset,
                    "error": str(e),
                },
            )
            raise MessageConsumeError(
                message_queue.topic,
                "Failed to pull messages",
                offset,
                cause=e,
            ) from e

    async def _build_sys_flag(self, commit_offset: bool) -> int:
        """构建系统标志位

        根据Go语言实现：
        - bit 0 (0x1): commitOffset 标志
        - bit 1 (0x2): suspend 标志
        - bit 2 (0x4): subscription 标志
        - bit 3 (0x8): classFilter 标志

        Args:
            commit_offset (bool): 是否提交偏移量

        Returns:
            int: 系统标志位
        """
        flag = 0

        if commit_offset:
            flag |= 0x1 << 0  # bit 0: 0x1

        # suspend: always true
        flag |= 0x1 << 1  # bit 1: 0x2

        # subscription: always true
        flag |= 0x1 << 2  # bit 2: 0x4

        # class_filter: always false
        # flag |= 0x1 << 3  # bit 3: 0x8

        return flag

    async def _start_consume_tasks_for_queues(self, queues: set[MessageQueue]) -> None:
        """为指定的队列集合启动消费任务

        Args:
            queues: 需要启动消费任务的队列集合
        """
        for message_queue in queues:
            queue_key = str(message_queue)

            # 获取或创建停止事件
            async with self._stop_events_lock:
                if queue_key not in self._consume_stop_events:
                    self._consume_stop_events[queue_key] = asyncio.Event()
                consume_stop_event = self._consume_stop_events[queue_key]

            # 启动消费任务
            if (
                queue_key not in self._consume_tasks
                or self._consume_tasks[queue_key].done()
            ):
                task = asyncio.create_task(
                    self._consume_messages_loop(message_queue, consume_stop_event)
                )
                self._consume_tasks[queue_key] = task

                self.logger.debug(
                    f"Started consume task for queue: {message_queue}",
                    extra={
                        "consumer_group": self._config.consumer_group,
                        "topic": message_queue.topic,
                        "queue_id": message_queue.queue_id,
                    },
                )

    async def _stop_consume_tasks(self) -> None:
        """停止所有消息消费任务 - 使用停止事件优雅关闭"""
        if not self._consume_tasks:
            return

        # 设置所有停止事件
        async with self._stop_events_lock:
            for queue_key in self._consume_stop_events:
                self._consume_stop_events[queue_key].set()

        # 等待所有任务完成
        tasks_to_cancel = []
        for queue_key, task in self._consume_tasks.items():
            if not task.done():
                # 给任务一些时间来优雅退出
                try:
                    await asyncio.wait_for(task, timeout=1.0)
                except asyncio.TimeoutError:
                    # 超时则取消任务
                    tasks_to_cancel.append(task)

        # 取消超时的任务
        for task in tasks_to_cancel:
            task.cancel()

        # 等待取消完成
        if tasks_to_cancel:
            await asyncio.gather(*tasks_to_cancel, return_exceptions=True)

        self._consume_tasks.clear()

        # 清理停止事件
        async with self._stop_events_lock:
            self._consume_stop_events.clear()

        self.logger.debug(
            "All consume tasks stopped",
            extra={"consumer_group": self._config.consumer_group},
        )

    async def _acquire_consume_lock(
        self, message_queue: MessageQueue, stop_event: asyncio.Event
    ) -> tuple[asyncio.Semaphore, bool]:
        """
        获取消费锁（本地锁 + 远程锁验证）。

        Args:
            message_queue: 要处理的消息队列
            stop_event: 停止事件

        Returns:
            tuple[asyncio.Semaphore, bool]: (队列锁, 是否成功获取锁)
        """
        queue_semaphore = self._get_queue_lock(message_queue)
        lock_acquired = False

        # 尝试非阻塞获取锁，如果失败则等待10ms后重试
        while not lock_acquired and self._is_running and not stop_event.is_set():
            try:
                lock_acquired = queue_semaphore.acquire_nowait()
            except:
                lock_acquired = False

            if not lock_acquired:
                # 等待10ms
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=0.01)
                    break  # 如果收到停止信号，退出循环
                except asyncio.TimeoutError:
                    pass  # 超时是正常的，继续尝试获取锁

        # 如果获取锁失败或消费者停止，则返回
        if not lock_acquired or not self._is_running:
            if lock_acquired:
                queue_semaphore.release()
            return queue_semaphore, False

        # 本地锁持有成功，检查远程锁是否需要重新获取
        # 广播模式下不需要远程锁，每个消费者独立处理所有消息
        if self._config.message_model == MessageModel.BROADCASTING:
            self.logger.debug(
                f"Broadcast mode - skipping remote lock for queue {message_queue}",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "client_id": self._config.client_id,
                    "queue": str(message_queue),
                    "operation": "consume_messages_loop",
                    "message_model": "BROADCASTING",
                },
            )
            return queue_semaphore, True

        # 集群模式下需要远程锁来保证消息的顺序性
        if not await self._is_remote_lock_valid(message_queue):
            # 远程锁已过期或不存在，需要重新获取
            if not await self._lock_remote_queue(message_queue):
                self.logger.debug(
                    f"Failed to acquire remote lock for queue {message_queue}, skipping this round",
                    extra={
                        "consumer_group": self._config.consumer_group,
                        "client_id": self._config.client_id,
                        "queue": str(message_queue),
                        "operation": "consume_messages_loop",
                    },
                )
                # 释放本地锁并继续下一轮循环
                queue_semaphore.release()
                return queue_semaphore, False
        else:
            self.logger.debug(
                f"Using cached remote lock for queue {message_queue}",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "client_id": self._config.client_id,
                    "queue": str(message_queue),
                    "operation": "consume_messages_loop",
                    "lock_cached": True,
                },
            )

        return queue_semaphore, True

    async def _fetch_messages_from_queue(
        self, message_queue: MessageQueue, stop_event: asyncio.Event
    ) -> tuple[ProcessQueue, list[MessageExt]]:
        """
        从处理队列获取消息。

        Args:
            message_queue: 消息队列
            stop_event: 停止事件

        Returns:
            tuple[ProcessQueue, list[MessageExt]]: (处理队列, 消息列表)
        """
        pq: ProcessQueue = await self._get_or_create_process_queue(message_queue)

        # 尝试获取消息
        messages = []

        # 使用异步方式从队列获取消息
        while len(messages) < self._config.consume_message_batch_max_size:
            if stop_event.is_set():
                break

            try:
                # 非阻塞获取消息
                msg = pq.get_message(blocking=False)
                if msg is None:
                    break
                messages.append(msg)
            except:
                break

        return pq, messages

    async def _consume_messages_loop(
        self, message_queue: MessageQueue, stop_event: asyncio.Event
    ) -> None:
        """消费消息的主循环

        Args:
            message_queue: 要消费的队列
            stop_event: 停止事件
        """
        self.logger.debug(
            f"Starting consume messages loop for queue: {message_queue}",
            extra={
                "consumer_group": self._config.consumer_group,
                "topic": message_queue.topic,
                "queue_id": message_queue.queue_id,
            },
        )

        while self._is_running and not stop_event.is_set():
            try:
                # 获取消费锁
                queue_semaphore, lock_acquired = await self._acquire_consume_lock(
                    message_queue, stop_event
                )

                if not lock_acquired:
                    continue

                try:
                    # 从队列获取消息
                    pq, messages = await self._fetch_messages_from_queue(
                        message_queue, stop_event
                    )

                    if messages:
                        # 处理消息
                        await self._process_messages_with_timing(
                            message_queue, pq, messages
                        )
                    else:
                        # 没有消息，短暂休眠
                        try:
                            await asyncio.wait_for(stop_event.wait(), timeout=0.1)
                        except asyncio.TimeoutError:
                            pass

                finally:
                    # 释放锁
                    queue_semaphore.release()

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(
                    f"Error in consume messages loop for {message_queue}: {e}",
                    extra={
                        "consumer_group": self._config.consumer_group,
                        "topic": message_queue.topic,
                        "queue_id": message_queue.queue_id,
                        "error": str(e),
                    },
                    exc_info=True,
                )

                # 错误时短暂等待后继续
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue

        self.logger.debug(
            f"Consume messages loop ended for queue: {message_queue}",
            extra={
                "consumer_group": self._config.consumer_group,
                "topic": message_queue.topic,
                "queue_id": message_queue.queue_id,
            },
        )

    async def _process_messages_with_timing(
        self, message_queue: MessageQueue, pq: ProcessQueue, messages: list[MessageExt]
    ) -> None:
        """处理消息并记录时间

        Args:
            message_queue: 消息队列
            pq: 处理队列
            messages: 要处理的消息列表
        """
        start_time = time.time()

        try:
            # 调用消息处理逻辑
            await self._process_messages_with_retry(message_queue, pq, messages)

            # 更新统计
            process_time = (time.time() - start_time) * 1000  # 转换为毫秒
            self._stats["orderly_consume_rt_total"] = (
                self._stats.get("orderly_consume_rt_total", 0.0) + process_time
            )
            self._stats["orderly_consume_rt_count"] = (
                self._stats.get("orderly_consume_rt_count", 0) + 1
            )
            self._stats["orderly_consume_success_count"] = self._stats.get(
                "orderly_consume_success_count", 0
            ) + len(messages)

        except Exception as e:
            # 更新失败统计
            self._stats["orderly_consume_fail_count"] = self._stats.get(
                "orderly_consume_fail_count", 0
            ) + len(messages)
            raise

    async def _process_messages_with_retry(
        self, message_queue: MessageQueue, pq: ProcessQueue, messages: list[MessageExt]
    ) -> None:
        """带重试机制的消息处理

        Args:
            message_queue: 消息队列
            pq: 处理队列
            messages: 要处理的消息列表
        """
        # 这里应该调用具体的消息处理逻辑
        # 暂时使用简单的日志记录
        self.logger.debug(
            f"Processing {len(messages)} messages from queue: {message_queue}",
            extra={
                "consumer_group": self._config.consumer_group,
                "topic": message_queue.topic,
                "queue_id": message_queue.queue_id,
                "message_count": len(messages),
            },
        )

        # TODO: 实现具体的消息处理逻辑
        # 这里需要调用用户的MessageListener来处理消息

    async def _pull_messages_loop(
        self,
        message_queue: MessageQueue,
        pull_stop_event: asyncio.Event,
    ) -> None:
        """持续拉取指定队列的消息。

        为每个分配的队列创建独立的拉取循环，持续从Broker拉取消息
        并放入处理队列。这是消费者消息拉取的核心执行循环。

        执行流程：
        1. 从队列的当前偏移量开始拉取消息
        2. 如果拉取到消息，更新本地偏移量记录
        3. 将消息添加到处理队列缓存
        4. 根据配置的拉取间隔进行休眠控制

        Args:
            message_queue: 要持续拉取消息的目标队列
            pull_stop_event: 拉取任务停止事件

        Note:
            - 每个队列有独立的拉取任务，避免队列间相互影响
            - 偏移量在本地维护，定期或在消息处理成功后更新到Broker
            - 拉取失败会记录日志并等待重试，不会影响其他队列
            - 消费者停止时此循环会自动退出
            - 支持通过配置控制拉取频率
            - 支持通过停止事件优雅关闭
        """
        suggest_broker_id = 0
        while self._is_running and not pull_stop_event.is_set():
            # 检查是否需要流量控制
            pq = await self._get_or_create_process_queue(message_queue)
            if pq.need_flow_control():
                # 使用可中断的等待，检查停止事件
                try:
                    await asyncio.wait_for(pull_stop_event.wait(), timeout=3.0)
                    break  # 收到停止信号
                except asyncio.TimeoutError:
                    continue  # 超时是正常的，继续检查流量控制

            try:
                # 执行单次拉取操作
                pull_result = await self._perform_single_pull(
                    message_queue, suggest_broker_id
                )

                if pull_result is None:
                    # 如果返回None，说明没有订阅信息，停止消费
                    self.logger.warning(
                        "No subscription found for topic, stopping pull loop",
                        extra={
                            "consumer_group": self._config.consumer_group,
                            "topic": message_queue.topic,
                            "queue_id": message_queue.queue_id,
                        },
                    )
                    break

                messages, next_begin_offset, next_suggest_id = pull_result
                suggest_broker_id = next_suggest_id

                if messages:
                    # 处理拉取到的消息
                    await self._handle_pulled_messages(
                        message_queue, messages, next_begin_offset
                    )
                else:
                    # 没有拉取到消息，只更新请求统计
                    self._stats["pull_requests"] = (
                        self._stats.get("pull_requests", 0) + 1
                    )

                # 控制拉取频率 - 传入是否有消息的标志，使用可中断等待
                await self._apply_pull_interval(
                    has_messages=bool(messages), stop_event=pull_stop_event
                )

            except MessagePullError as e:
                self.logger.warning(
                    "The pull request is illegal",
                    extra={
                        "consumer_group": self._config.consumer_group,
                        "topic": message_queue.topic,
                        "queue_id": message_queue.queue_id,
                        "error": str(e),
                    },
                )
                break
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(
                    f"Error in pull messages loop for {message_queue}: {e}",
                    extra={
                        "consumer_group": self._config.consumer_group,
                        "topic": message_queue.topic,
                        "queue_id": message_queue.queue_id,
                        "error": str(e),
                    },
                    exc_info=True,
                )

                self._stats["pull_fail"] = self._stats.get("pull_fail", 0) + 1

                # 拉取失败时等待一段时间再重试，使用可中断等待
                try:
                    await asyncio.wait_for(pull_stop_event.wait(), timeout=3.0)
                    break  # 收到停止信号
                except asyncio.TimeoutError:
                    continue  # 超时是正常的，继续重试

    async def _get_or_create_process_queue(self, queue: MessageQueue) -> ProcessQueue:
        """获取或创建指定队列的ProcessQueue（消息缓存队列）

        Args:
            queue: 消息队列

        Returns:
            ProcessQueue: 指定队列的处理队列对象
        """
        async with self._cache_lock:
            if queue not in self._msg_cache:
                self._msg_cache[queue] = ProcessQueue(
                    max_cache_count=self._config.max_cache_count_per_queue,
                    max_cache_size_mb=self._config.max_cache_size_per_queue,
                )
            return self._msg_cache[queue]

    async def _get_or_initialize_offset(self, message_queue: MessageQueue) -> int:
        """获取或初始化消费偏移量。

        如果本地缓存的偏移量为0（首次消费），则根据配置的消费策略
        从ConsumeFromWhereManager获取正确的初始偏移量。

        Args:
            message_queue: 要获取偏移量的消息队列

        Returns:
            int: 消费偏移量

        Note:
            - 如果偏移量不为0，直接返回缓存的值
            - 如果偏移量为0，根据consume_from_where策略获取初始偏移量
            - 获取失败时使用默认偏移量0，确保消费流程不中断
        """
        async with self._assigned_queues_lock:  # 🔐保护_assigned_queues访问
            current_offset: int = self._assigned_queues.get(message_queue, 0)

            # 如果current_offset为0（首次消费），则从_consume_from_where_manager中获取正确的初始偏移量
            if current_offset == 0:
                try:
                    # 调用异步版本的偏移量获取
                    current_offset = (
                        await self._consume_from_where_manager.get_consume_offset(
                            message_queue,
                            self._config.consume_from_where,
                            self._config.consume_timestamp
                            if hasattr(self._config, "consume_timestamp")
                            else 0,
                        )
                    )
                    # 更新本地缓存的偏移量
                    self._assigned_queues[message_queue] = current_offset

                    self.logger.info(
                        f"初始化消费偏移量: {current_offset}",
                        extra={
                            "consumer_group": self._config.consumer_group,
                            "topic": message_queue.topic,
                            "queue_id": message_queue.queue_id,
                            "strategy": self._config.consume_from_where,
                            "offset": current_offset,
                        },
                    )

                except Exception as e:
                    self.logger.error(
                        f"获取初始消费偏移量失败，使用默认偏移量0: {e}",
                        extra={
                            "consumer_group": self._config.consumer_group,
                            "topic": message_queue.topic,
                            "queue_id": message_queue.queue_id,
                            "strategy": self._config.consume_from_where,
                            "error": str(e),
                        },
                        exc_info=True,
                    )
                    # 使用默认偏移量0
                    current_offset = 0

        return current_offset

    async def _handle_pulled_messages(
        self,
        message_queue: MessageQueue,
        messages: list[MessageExt],
        next_begin_offset: int,
    ) -> None:
        """处理拉取到的消息。

        包括更新偏移量、缓存消息、统计信息等。

        Args:
            message_queue: 消息队列
            messages: 拉取到的消息列表
            next_begin_offset: 下次拉取的起始偏移量
        """
        # 更新偏移量
        async with self._assigned_queues_lock:  # 🔐保护_assigned_queues访问
            self._assigned_queues[message_queue] = next_begin_offset

        # 将消息添加到缓存中（用于解决并发偏移量问题）
        await self._add_messages_to_cache(message_queue, messages)

        # 更新统计信息
        message_count = len(messages)
        self._stats["pull_success"] = self._stats.get("pull_success", 0) + 1
        self._stats["messages_consumed"] = (
            self._stats.get("messages_consumed", 0) + message_count
        )
        self._stats["pull_requests"] = self._stats.get("pull_requests", 0) + 1

        self.logger.debug(
            f"Pulled {len(messages)} messages from queue: {message_queue}",
            extra={
                "consumer_group": self._config.consumer_group,
                "topic": message_queue.topic,
                "queue_id": message_queue.queue_id,
                "message_count": len(messages),
                "next_begin_offset": next_begin_offset,
            },
        )

    async def _apply_pull_interval(
        self, has_messages: bool = True, stop_event: asyncio.Event | None = None
    ) -> None:
        """应用智能拉取间隔控制。

        根据上次拉取结果智能调整拉取间隔：
        - 如果上次拉取到了消息，立即继续拉取以提高消费速度
        - 如果上次拉取为空，则休眠配置的间隔时间以避免空轮询

        Args:
            has_messages: 上次拉取是否获取到消息，默认为True
            stop_event: 停止事件，用于支持优雅关闭，默认为None
        """
        if self._config.pull_interval > 0:
            if has_messages:
                # 拉取到消息，不休眠继续拉取
                self.logger.debug(
                    "Messages pulled, continuing without interval",
                    extra={
                        "consumer_group": self._config.consumer_group,
                    },
                )
            else:
                # 拉取为空，休眠配置的间隔时间，使用可中断等待
                sleep_time: float = self._config.pull_interval / 1000.0
                self.logger.debug(
                    f"No messages pulled, sleeping for {sleep_time}s",
                    extra={
                        "consumer_group": self._config.consumer_group,
                        "sleep_time": sleep_time,
                    },
                )
                if stop_event:
                    try:
                        await asyncio.wait_for(stop_event.wait(), timeout=sleep_time)
                    except asyncio.TimeoutError:
                        pass  # 超时是正常的，继续拉取
                else:
                    await asyncio.sleep(sleep_time)

    async def _add_messages_to_cache(
        self, queue: MessageQueue, messages: list[MessageExt]
    ) -> None:
        """将消息添加到ProcessQueue缓存中

        此方法用于将从Broker拉取的消息添加到ProcessQueue中，为后续消费做准备。
        ProcessQueue自动保持按queue_offset排序，并提供高效的插入、查询和统计功能。

        Args:
            queue: 目标消息队列
            messages: 要添加的消息列表，消息应包含有效的queue_offset

        Note:
            - 使用ProcessQueue内置的线程安全机制
            - 按queue_offset升序排列，方便后续按序消费
            - 自动过滤空消息列表，避免不必要的操作
            - 自动去重，避免重复缓存相同偏移量的消息
            - 自动检查缓存限制（数量和大小）

        See Also:
            _get_or_create_process_queue: 获取或创建ProcessQueue
        """
        if not messages:
            return

        process_queue: ProcessQueue = await self._get_or_create_process_queue(queue)
        _ = process_queue.add_batch_messages(messages)

        self.logger.debug(
            f"Added {len(messages)} messages to cache for queue: {queue}",
            extra={
                "consumer_group": self._config.consumer_group,
                "topic": queue.topic,
                "queue_id": queue.queue_id,
                "message_count": len(messages),
            },
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
        stats.update(self._stats)

        # 添加队列相关信息
        async with self._assigned_queues_lock:
            stats["assigned_queues_count"] = len(self._assigned_queues)

        return stats
