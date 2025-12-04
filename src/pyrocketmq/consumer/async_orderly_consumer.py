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
from datetime import datetime

from pyrocketmq.broker import AsyncBrokerClient, MessagePullError
from pyrocketmq.consumer.allocate_queue_strategy import AllocateContext
from pyrocketmq.consumer.async_base_consumer import AsyncBaseConsumer
from pyrocketmq.consumer.async_process_queue import AsyncProcessQueue
from pyrocketmq.consumer.config import ConsumerConfig
from pyrocketmq.consumer.errors import MessageConsumeError
from pyrocketmq.consumer.offset_store import ReadOffsetType
from pyrocketmq.logging import get_logger
from pyrocketmq.model import (
    ConsumeMessageDirectlyHeader,
    ConsumeMessageDirectlyResult,
    ConsumeResult,
    MessageExt,
    MessageModel,
    MessageQueue,
    RemotingCommand,
    RemotingCommandBuilder,
    RequestCode,
    ResponseCode,
    SubscriptionData,
    SubscriptionEntry,
)
from pyrocketmq.remote import AsyncConnectionPool


class AsyncOrderlyConsumer(AsyncBaseConsumer):
    """
    异步顺序消费者

    继承AsyncBaseConsumer，专注于顺序消费逻辑。保证同一消息队列中的消息
    按照偏移量严格顺序处理，每个队列同时只能有一个消费任务。

    功能模块组织：
    1. 核心生命周期管理：初始化、启动、关闭
    2. 重平衡管理：队列分配、负载均衡、消费者协调
    3. 队列锁定管理：本地锁和远程锁机制，保证顺序性
    4. 消息拉取模块：从Broker拉取消息，偏移量管理
    5. 消息消费处理：消息处理、重试逻辑、结果处理
    6. 消息缓存管理：ProcessQueue管理，流量控制
    7. 统计与监控：性能统计、状态监控、指标收集
    8. 远程通信处理：处理Broker通知、直接消费请求
    9. 资源清理与错误处理：异常处理、资源管理、优雅关闭
    """

    # ==================== 1. 核心生命周期管理模块 ====================
    #
    # 该模块负责消费者实例的完整生命周期管理，包括：
    # - 初始化配置和状态变量
    # - 启动所有组件和后台任务
    # - 优雅关闭和资源清理
    # - 错误处理和状态维护
    #
    # 相关函数：
    # - __init__: 初始化消费者配置和状态
    # - start: 启动消费者，建立连接并开始消费
    # - shutdown: 优雅关闭消费者，清理所有资源

    def __init__(self, config: ConsumerConfig):
        """初始化异步顺序消费者实例。

        创建一个新的异步顺序消费者，配置所有必要的组件和状态变量。
        顺序消费者保证同一消息队列中的消息按照偏移量严格顺序处理。

        Args:
            config (ConsumerConfig): 消费者配置对象，包含以下关键配置：
                - consumer_group: 消费者组名称
                - message_model: 消息模式（CLUSTERING或BROADCASTING）
                - consume_thread_max: 最大消费线程数
                - pull_batch_size: 拉取消息的批次大小
                - max_reconsume_times: 最大重试次数
                - enable_auto_commit: 是否启用自动提交
                - consume_from_where: 消费起始位置策略
                - pull_interval: 拉取间隔（毫秒）
                - max_cache_count_per_queue: 每队列最大缓存消息数
                - max_cache_size_per_queue: 每队列最大缓存大小（MB）

        Raises:
            TypeError: 当config参数不是ConsumerConfig类型时抛出
            ValueError: 当config中的必要参数缺失或无效时抛出

        Note:
            - 初始化过程中会创建队列锁、任务管理器、消息缓存等核心组件
            - 顺序消费者使用本地锁和远程锁机制确保消息顺序性
            - 远程锁默认有效期为30秒，可通过_remote_lock_expire_time配置
            - 重平衡默认间隔为20秒，可通过_rebalance_interval配置

        Examples:
            >>> from pyrocketmq.consumer.config import ConsumerConfig
            >>> from pyrocketmq.consumer.async_orderly_consumer import AsyncOrderlyConsumer
            >>>
            >>> config = ConsumerConfig(
            ...     consumer_group="test_group",
            ...     namesrv_addr="localhost:9876",
            ...     message_model=MessageModel.CLUSTERING
            ... )
            >>> consumer = AsyncOrderlyConsumer(config)
        """

        # 调用父类初始化
        super().__init__(config)

        # ==================== 基础组件 ====================
        self.logger = get_logger(__name__)

        # ==================== 顺序消费核心组件 ====================
        # 队列锁管理 - 确保单个队列的顺序消费
        self._queue_locks: dict[MessageQueue, asyncio.Semaphore] = {}  # 队列级锁信号量
        self._queue_locks_lock = asyncio.Lock()  # 🔐保护_queue_locks字典的并发访问

        # 任务管理 - 管理拉取和消费的异步任务
        self._consume_tasks: dict[MessageQueue, asyncio.Task[None]] = {}  # 队列消费任务
        self._consume_tasks_lock = asyncio.Lock()  # 🔐保护_consume_tasks字典的并发访问
        self._pull_tasks: dict[MessageQueue, asyncio.Task[None]] = {}  # 队列拉取任务

        # ==================== 状态和队列管理 ====================
        # 分配队列状态 - 当前消费者负责的队列及其偏移量
        self._assigned_queues: dict[MessageQueue, int] = {}  # queue -> last_offset
        self._assigned_queues_lock = (
            asyncio.Lock()
        )  # 🔐保护_assigned_queues字典的并发访问
        self._last_rebalance_time: float = 0.0  # 上次重平衡时间戳

        # ==================== 重平衡管理 ====================
        # 重平衡任务 - 定期执行队列重新分配
        self._rebalance_task: asyncio.Task[None] | None = None  # 重平衡异步任务
        self._rebalance_interval: float = 20.0  # 重平衡间隔(秒)
        self._rebalance_lock = asyncio.Lock()  # 🔐重平衡重入保护锁

        # ==================== 同步和事件管理 ====================
        # 重平衡事件 - 控制重平衡循环
        self._rebalance_event: asyncio.Event = asyncio.Event()  # 重平衡循环控制事件

        # 停止事件 - 用于优雅关闭拉取和消费任务
        self._pull_stop_events: dict[str, asyncio.Event] = {}  # 拉取任务停止事件
        self._consume_stop_events: dict[str, asyncio.Event] = {}  # 消费任务停止事件
        self._stop_events_lock = asyncio.Lock()  # 🔐保护停止事件字典的并发访问

        # ==================== 远程锁优化 ====================
        # 远程锁缓存 - 减少网络请求，提升性能
        self._remote_lock_cache: dict[
            MessageQueue, float
        ] = {}  # queue -> lock_expiry_time
        self._remote_lock_cache_lock = asyncio.Lock()  # 🔐保护远程锁缓存的并发访问
        self._remote_lock_expire_time: float = 30.0  # 远程锁有效期(秒)

        # ==================== 初始化完成日志 ====================
        self.logger.info(
            "AsyncOrderlyConsumer initialized",
            extra={
                "consumer_group": self._config.consumer_group,
                "message_model": self._config.message_model,
                "consume_thread_max": self._config.consume_thread_max,
                "pull_batch_size": self._config.pull_batch_size,
                "rebalance_interval": self._rebalance_interval,
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

    # ==================== 2. 重平衡管理模块 ====================
    #
    # 该模块负责消费者的负载均衡和队列分配管理，是RocketMQ实现消息分发的核心机制。
    # 主要功能包括：
    # - 队列分配算法执行：根据策略在消费者组内分配队列
    # - 消费者协调：发现和管理消费者组成员
    # - 路由信息更新：获取Topic的最新队列信息
    # - 重平衡触发管理：定期重平衡和事件驱动的重平衡
    # - 队列变更处理：新增或回收队列时的任务管理
    #
    # 重平衡触发条件：
    # - 消费者启动/停止时
    # - 新订阅Topic或取消订阅时
    # - 定期重平衡检查（默认20秒间隔）
    # - 收到消费者组变更通知时
    # - Topic路由信息变更时
    #
    # 相关函数：
    # - _do_rebalance: 执行完整的重平衡流程
    # - _pre_rebalance_check: 重平衡前置检查和重入保护
    # - _collect_and_allocate_queues: 收集队列并执行分配
    # - _allocate_queues: 为单个Topic分配队列
    # - _update_assigned_queues: 更新分配的队列集合
    # - _finalize_rebalance: 重平衡完成后的处理
    # - _trigger_rebalance: 触发重平衡执行
    # - _start_rebalance_task: 启动定期重平衡任务
    # - _rebalance_loop: 重平衡循环控制
    # - _find_consumer_list: 查询消费者组成员

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
        if self._rebalance_lock.locked():
            # 如果无法获取锁，说明正在执行重平衡，跳过本次请求
            self.logger.debug(
                "Rebalance already in progress, skipping",
                extra={
                    "consumer_group": self._config.consumer_group,
                },
            )
            return False

        await self._rebalance_lock.acquire()

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

        self.logger.info(
            "Rebalance completed",
            extra={
                "consumer_group": self._config.consumer_group,
                "total_topics": total_topics,
                "assigned_queues": total_queues,
            },
        )

    async def _update_assigned_queues(self, new_queues: list[MessageQueue]) -> None:
        """更新当前消费者的分配队列集合。

        比较新旧队列分配，执行增量更新：
        - 停止被回收队列的拉取任务
        - 启动新分配队列的拉取任务
        - 维护队列偏移量信息
        - 管理每个队列的消费任务

        Args:
            new_queues (list[MessageQueue]): 新分配给当前消费者的队列列表

        Returns:
            None

        Raises:
            None: 此方法会处理所有异常情况

        Note:
            - 队列变更不会中断正在处理的消息
            - 被回收的队列会等待当前消息处理完成后才停止
            - 新队列会立即开始拉取消息
            - 偏移量信息会在队列分配变更时保留
            - 每个队列的消费任务会在队列分配变更时进行管理
        """
        # 使用_assigned_queues_lock保护整个队列更新过程
        async with self._assigned_queues_lock:  # 🔐保护_assigned_queues的完整操作
            old_queues: set[MessageQueue] = set(self._assigned_queues.keys())
            new_queue_set: set[MessageQueue] = set(new_queues)

            removed_queues: set[MessageQueue] = old_queues - new_queue_set
            added_queues: set[MessageQueue] = new_queue_set - old_queues

            # 移除旧队列的偏移量信息
            for q in removed_queues:
                _ = self._assigned_queues.pop(q, None)

            # 添加新队列的偏移量初始化
            for q in added_queues:
                self._assigned_queues[q] = 0  # 初始化偏移量为0，后续会更新

        # 在锁外处理其他资源的清理和创建，避免死锁
        # 停止不再分配的队列的拉取任务和消费任务
        for q in removed_queues:
            if q in self._pull_tasks:
                task: asyncio.Task[None] | None = self._pull_tasks.pop(q)
                if task and not task.done():
                    task.cancel()

            # 停止并移除该队列的消费任务
        async with self._consume_tasks_lock:
            for q in removed_queues:
                if q in self._consume_tasks:
                    task = self._consume_tasks.pop(q)
                    if task and not task.done():
                        task.cancel()

            # 清理队列锁
        async with self._queue_locks_lock:
            for q in removed_queues:
                if q in self._queue_locks:
                    del self._queue_locks[q]

        # 为新分配的队列创建资源
        for q in added_queues:
            # 为新队列创建锁（这里使用_get_queue_lock来确保线程安全）
            await self._get_queue_lock(q)

        # 如果消费者正在运行，启动新队列的拉取任务和消费任务
        if self._is_running and added_queues:
            await self._start_pull_tasks_for_queues(added_queues)
            await self._start_consume_tasks_for_queues(added_queues)

    async def _do_rebalance(self) -> None:
        """执行消费者重平衡操作。

        根据当前订阅的所有Topic，重新计算和分配队列给当前消费者。
        重平衡是RocketMQ实现负载均衡的核心机制，确保消费者组内的队列分配合理。

        执行流程：
        1. 执行重平衡前置检查
        2. 收集所有Topic的可用队列
        3. 执行队列分配算法
        4. 更新分配的队列并启动拉取任务
        5. 完成重平衡后处理

        重平衡触发条件：
        - 消费者启动时
        - 新订阅或取消订阅Topic时
        - 定期重平衡检查（默认20秒间隔）
        - 收到消费者组变更通知时

        Returns:
            None

        Raises:
            None: 此方法会捕获所有异常并记录日志，不会向上抛出

        Note:
            - 重平衡过程中可能会短暂停止消息拉取
            - 新分配的队列会自动开始拉取消息
            - 被回收的队列会停止拉取并等待当前消息处理完成
            - 重平衡失败不会影响已运行的队列，会在下次重试
        """
        # 前置检查
        if not await self._pre_rebalance_check():
            return

        try:
            self.logger.debug(
                "Starting rebalance",
                extra={"consumer_group": self._config.consumer_group},
            )

            # 收集所有可用队列并执行分配
            allocated_queues = await self._collect_and_allocate_queues()

            # 更新分配的队列
            if allocated_queues:
                await self._update_assigned_queues(allocated_queues)

            # 完成重平衡处理
            await self._finalize_rebalance(
                len(self._subscription_manager.get_topics()), len(allocated_queues)
            )

        except Exception as e:
            self.logger.error(
                f"Rebalance failed: {e}",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "error": str(e),
                },
                exc_info=True,
            )
            # 更新失败统计

        finally:
            # 释放重平衡锁
            self._rebalance_lock.release()
            self.logger.debug(
                "Rebalance lock released",
                extra={"consumer_group": self._config.consumer_group},
            )

    async def _trigger_rebalance(self) -> None:
        """触发消费者重平衡操作。

        通过设置重平衡事件来立即触发重平衡流程，而不是等待定期的重平衡间隔。
        这通常在以下情况下调用：
        - 消费者组成员发生变化
        - Topic路由信息更新
        - 收到Broker的重平衡通知

        Returns:
            None

        Note:
            - 只有在消费者正在运行时才会触发重平衡
            - 使用异步事件机制，不会阻塞调用线程
            - 重平衡会在事件循环的下一次迭代中执行
            - 多次调用只会触发一次重平衡，避免重复执行

        Raises:
            None: 此方法不会抛出异常，所有异常都在重平衡循环中处理
        """
        if self._is_running:
            # 唤醒重平衡循环，使其立即执行重平衡
            self._rebalance_event.set()

    async def _start_rebalance_task(self) -> None:
        """启动定期重平衡异步任务。

        创建并启动一个后台异步任务，该任务负责定期执行消费者重平衡操作，
        确保队列分配的动态调整和负载均衡。重平衡任务会持续运行直到消费者关闭。

        Returns:
            None

        Note:
            - 任务名称格式为"{consumer_group}-rebalance-task"
            - 重平衡间隔由_rebalance_interval配置（默认20秒）
            - 任务会自动处理异常并继续运行
            - 只能在消费者启动过程中调用，重复调用会创建多个任务

        Raises:
            RuntimeError: 当在消费者未启动状态下调用时可能抛出
            asyncio.CancelledError: 当消费者关闭时任务会被取消

        See Also:
            _rebalance_loop: 重平衡循环的具体实现
            _do_rebalance: 执行重平衡的核心逻辑
        """
        self._rebalance_task = asyncio.create_task(
            self._rebalance_loop(),
            name=f"{self._config.consumer_group}-rebalance-task",
        )

    async def _rebalance_loop(self) -> None:
        """执行定期重平衡循环。

        这是重平衡任务的核心执行循环，负责定期或在事件触发时执行重平衡操作。
        循环会持续运行直到消费者停止，支持定时重平衡和事件驱动的即时重平衡。

        执行逻辑：
        1. 等待重平衡事件或超时（默认20秒间隔）
        2. 如果事件被触发，检查消费者是否仍在运行
        3. 重置事件状态并执行重平衡操作
        4. 捕获并记录所有异常，确保循环继续运行

        Returns:
            None

        Note:
            - 使用asyncio.wait_for支持可中断的等待
            - 超时是正常情况，会触发定期重平衡
            - 事件触发会立即执行重平衡，跳过等待
            - 所有异常都会被捕获并记录，不会中断循环
            - 消费者停止时会自动退出循环

        Raises:
            None: 此方法会捕获所有异常，不会向上抛出

        See Also:
            _trigger_rebalance: 触发重平衡事件
            _do_rebalance: 执行重平衡的具体逻辑
            _start_rebalance_task: 启动重平衡任务
        """
        while self._is_running:
            try:
                try:
                    await asyncio.wait_for(
                        self._rebalance_event.wait(), timeout=self._rebalance_interval
                    )
                    # Event被触发，检查是否需要退出
                    if not self._is_running:
                        break
                    # 重置事件状态
                    self._rebalance_event.clear()
                except asyncio.TimeoutError:
                    # 超时是正常情况，继续执行重平衡
                    pass

                if self._is_running:
                    await self._do_rebalance()

            except Exception as e:
                self.logger.error(
                    f"Error in rebalance loop: {e}",
                    extra={
                        "consumer_group": self._config.consumer_group,
                        "error": str(e),
                    },
                    exc_info=True,
                )

    async def _find_consumer_list(self, topic: str) -> list[str]:
        """查找指定Topic的消费者组成员列表。

        通过NameServer获取Topic对应的Broker地址，然后向Broker查询
        指定消费者组下的所有活跃消费者客户端ID列表。这是重平衡
        过程中获取消费者组成员信息的关键步骤。

        Args:
            topic (str): 要查询的Topic名称，必须是已订阅的有效Topic

        Returns:
            list[str]: 消费者组成员的客户端ID列表，包含以下情况：
                - 非空列表：成功获取到的消费者客户端ID列表
                - 空列表：无法获取Broker地址或查询失败时返回空列表

        Raises:
            None: 此方法不会抛出异常，所有异常情况都会返回空列表

        Note:
            - 只在集群模式下（MessageModel.CLUSTERING）需要调用此方法
            - 广播模式下每个消费者独立处理所有队列，无需查询其他消费者
            - 查询失败时会记录警告日志并返回空列表
            - 返回的客户端ID列表用于队列分配算法的输入

        Examples:
            >>> consumer_ids = await consumer._find_consumer_list("test_topic")
            >>> if consumer_ids:
            ...     print(f"Found {len(consumer_ids)} consumers")
            ... else:
            ...     print("No consumers found or query failed")
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

    # ==================== 3. 队列锁定管理模块 ====================
    #
    # 该模块是顺序消费的核心，负责管理多层次的锁定机制以确保消息的顺序性。
    # 顺序消费要求同一消息队列中的消息严格按照偏移量顺序进行处理，
    # 这需要本地和远程的协调锁定机制。
    #
    # 锁定机制层次：
    # 1. 本地队列锁 (asyncio.Semaphore)：确保单个消费者内同一队列只有一个消费任务
    # 2. 远程队列锁 (Broker端锁)：在集群模式下确保跨消费者时同一队列只有一个消费者
    # 3. 锁缓存机制：缓存远程锁状态，减少网络请求，提升性能
    #
    # 锁获取流程：
    # 1. 先获取本地队列锁（信号量），确保本地顺序性
    # 2. 检查远程锁缓存，如果有效则直接使用
    # 3. 如果远程锁过期，向Broker申请新的远程锁
    # 4. 锁获取成功后开始消费，失败则跳过本轮
    #
    # 优化特性：
    # - 远程锁缓存：30秒有效期，减少网络开销
    # - 非阻塞锁获取：避免长时间阻塞消费循环
    # - 广播模式优化：广播模式无需远程锁，每个消费者独立处理
    # - 优雅解锁：消费者关闭时自动释放所有远程锁
    #
    # 相关函数：
    # - _get_queue_lock: 获取或创建本地队列锁
    # - _is_locked: 检查本地锁状态
    # - _lock_remote_queue: 向Broker申请远程队列锁
    # - _unlock_remote_queue: 释放远程队列锁
    # - _is_remote_lock_valid: 检查远程锁是否有效
    # - _set_remote_lock_expiry: 设置远程锁过期时间
    # - _invalidate_remote_lock: 使远程锁失效

    async def _get_queue_lock(self, message_queue: MessageQueue) -> asyncio.Semaphore:
        """获取指定消息队列的锁信号量

        使用锁保护来避免并发访问导致的竞争条件

        Args:
            message_queue: 消息队列

        Returns:
            asyncio.Semaphore: 该队列的异步锁信号量对象（值为1的信号量）
        """
        async with self._queue_locks_lock:
            # 检查是否已经存在锁
            if message_queue in self._queue_locks:
                return self._queue_locks[message_queue]

            # 不存在则创建新的锁
            self._queue_locks[message_queue] = asyncio.Semaphore(1)
            return self._queue_locks[message_queue]

    async def _is_locked(self, message_queue: MessageQueue) -> bool:
        """检查指定队列是否已锁定

        Args:
            message_queue: 消息队列

        Returns:
            bool: True如果队列已锁定，False如果队列未锁定
        """
        async with self._queue_locks_lock:
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
            _ = self._remote_lock_cache.pop(message_queue, None)

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
                locked: bool = False
                for q in locked_queues:
                    if q.equal(message_queue):
                        locked = True
                        break

                # 检查锁定是否成功
                if locked:
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

    # ==================== 4. 消息拉取模块 ====================
    #
    # 该模块是消费者的数据源，负责从RocketMQ Broker高效、可靠地拉取消息。
    # 采用多队列并发拉取机制，每个分配的队列都有独立的拉取任务，
    # 确保消息能够及时被拉取到本地进行处理。
    #
    # 核心功能：
    # 1. 多队列并发拉取：每个队列独立的拉取循环，互不干扰
    # 2. 偏移量管理：维护每个队列的消费偏移量，支持多种初始位置策略
    # 3. 流量控制：当ProcessQueue缓存过多消息时暂停拉取，防止内存溢出
    # 4. 智能间隔：根据拉取结果动态调整拉取频率，优化资源使用
    # 5. 故障处理：网络异常、Broker切换等异常情况的处理和恢复
    # 6. 优雅关闭：支持通过停止事件优雅关闭拉取任务
    #
    # 拉取流程：
    # 1. 检查ProcessQueue流量控制状态
    # 2. 从Broker拉取指定偏移量的消息
    # 3. 根据订阅信息过滤消息（Tag过滤）
    # 4. 更新本地偏移量记录
    # 5. 将消息添加到ProcessQueue缓存
    # 6. 应用智能拉取间隔控制
    #
    # 优化特性：
    # - 批量拉取：一次拉取多条消息，提高效率
    # - 主备切换：支持从master或slave拉取消息
    # - 连接池复用：复用TCP连接，减少连接开销
    # - 拉取间隔优化：有消息时立即拉取，无消息时休眠
    # - 可中断等待：所有等待操作都支持停止事件中断
    #
    # 相关函数：
    # - _pull_messages_loop: 单个队列的消息拉取循环
    # - _start_pull_tasks_for_queues: 为指定队列启动拉取任务
    # - _stop_pull_tasks: 停止所有拉取任务
    # - _perform_single_pull: 执行单次消息拉取操作
    # - _pull_messages: 核心拉取逻辑，构建请求并处理响应
    # - _build_sys_flag: 构建系统标志位
    # - _get_or_initialize_offset: 获取或初始化消费偏移量
    # - _handle_pulled_messages: 处理拉取到的消息
    # - _apply_pull_interval: 应用智能拉取间隔

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
                self._pull_tasks[queue] = task

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
            for queue in self._pull_tasks.keys():
                queue_key = str(queue)
                # 设置拉取停止事件
                if queue_key in self._pull_stop_events:
                    self._pull_stop_events[queue_key].set()
                # 设置消费停止事件
                if queue_key in self._consume_stop_events:
                    self._consume_stop_events[queue_key].set()

        # 取消所有异步任务
        for queue, task in self._pull_tasks.items():
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    self.logger.debug(
                        f"Pull task cancelled for queue: {queue}",
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
        pull_start_time = time.time()
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

        # 记录拉取统计
        pull_rt = int((time.time() - pull_start_time) * 1000)  # 转换为毫秒
        message_count = len(messages)

        self._stats_manager.increase_pull_rt(
            self._config.consumer_group, message_queue.topic, pull_rt
        )
        self._stats_manager.increase_pull_tps(
            self._config.consumer_group, message_queue.topic, message_count
        )

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
                result = await AsyncBrokerClient(conn).pull_message(
                    consumer_group=self._config.consumer_group,
                    topic=message_queue.topic,
                    queue_id=message_queue.queue_id,
                    queue_offset=offset,
                    max_msg_nums=self._config.pull_batch_size,
                    sys_flag=await self._build_sys_flag(
                        commit_offset=commit_offset > 0 and is_master
                    ),
                    commit_offset=commit_offset,
                    timeout=30,
                )

                if result.messages:
                    return (
                        result.messages,
                        result.next_begin_offset,
                        result.suggest_which_broker_id or 0,
                    )

                return [], offset, 0

        except MessagePullError as e:
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

    # ==================== 5. 消息消费处理模块 ====================
    #
    # 该模块是顺序消费者的核心处理单元，负责从ProcessQueue中取出消息
    # 并调用用户的MessageListener进行处理，确保消息的严格顺序性。
    #
    # 核心功能：
    # 1. 顺序消费保证：通过队列锁确保同一队列的消息按顺序处理
    # 2. 消费任务管理：为每个队列创建独立的消费任务，并发处理不同队列
    # 3. 锁获取机制：协调本地锁和远程锁的获取，确保消费的顺序性
    # 4. 结果处理：根据用户的消费结果决定消息的提交或重试
    # 5. 重试管理：支持最大重试次数限制，防止无限重试
    # 6. 性能监控：记录消费耗时、成功率等关键指标
    #
    # 消费流程：
    # 1. 获取队列的消费锁（本地锁+远程锁验证）
    # 2. 从ProcessQueue中取出待处理的消息
    # 3. 调用用户的MessageListener处理消息
    # 4. 根据处理结果更新ProcessQueue状态
    # 5. 处理偏移量提交和重试逻辑
    # 6. 更新消费统计信息
    #
    # 消费结果处理：
    # - CONSUME_SUCCESS: 消息处理成功，提交偏移量
    # - RECONSUME_LATER: 消费失败，稍后重试（检查重试次数）
    # - ROLLBACK: 回滚消息（手动提交模式）
    # - COMMIT: 提交消息（手动提交模式）
    #
    # 相关函数：
    # - _consume_messages_loop: 单个队列的消息消费循环
    # - _start_consume_tasks_for_queues: 为指定队列启动消费任务
    # - _stop_consume_tasks: 停止所有消费任务
    # - _acquire_consume_lock: 获取消费锁（本地+远程）
    # - _fetch_messages_from_queue: 从队列获取消息
    # - _process_messages_with_timing: 处理消息并记录时间
    # - _process_messages_with_retry: 带重试机制的消息处理
    # - _handle_auto_commit_result: 处理自动提交模式结果
    # - _handle_manual_commit_result: 处理手动提交模式结果
    # - _check_reconsume_times: 检查消息重试次数

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
            async with self._consume_tasks_lock:
                # 检查是否需要创建新任务
                if (
                    message_queue not in self._consume_tasks
                    or self._consume_tasks[message_queue].done()
                ):
                    task = asyncio.create_task(
                        self._consume_messages_loop(message_queue, consume_stop_event)
                    )
                    self._consume_tasks[message_queue] = task

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
        # 设置所有停止事件
        async with self._stop_events_lock:
            for queue_key in self._consume_stop_events:
                self._consume_stop_events[queue_key].set()

        # 获取所有任务的副本并清空字典
        async with self._consume_tasks_lock:
            if not self._consume_tasks:
                return
            tasks = list(self._consume_tasks.items())
            self._consume_tasks.clear()

        # 等待所有任务完成
        tasks_to_cancel: list[asyncio.Task[None]] = []
        for _queue, task in tasks:
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
        queue_semaphore: asyncio.Semaphore = await self._get_queue_lock(message_queue)
        lock_acquired: bool = False

        # 尝试非阻塞获取锁，如果失败则等待1000ms后重试
        while not lock_acquired and self._is_running and not stop_event.is_set():
            try:
                # 使用带超时的非阻塞获取
                lock_acquired = await asyncio.wait_for(
                    queue_semaphore.acquire(), timeout=1
                )
            except asyncio.TimeoutError:
                # 获取锁超时，继续下一轮尝试
                lock_acquired = False
                continue

            # 如果收到停止信号，退出循环
            if stop_event.is_set():
                break

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
    ) -> tuple[AsyncProcessQueue, list[MessageExt]]:
        """
        从处理队列获取消息。

        Args:
            message_queue: 消息队列
            stop_event: 停止事件

        Returns:
            tuple[ProcessQueue, list[MessageExt]]: (处理队列, 消息列表)
        """
        pq: AsyncProcessQueue = await self._get_or_create_process_queue(message_queue)

        # 使用ProcessQueue的take_messages方法
        messages: list[MessageExt] = await pq.take_messages(
            self._config.consume_batch_size
        )

        if not messages:
            # 没有消息时等待
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=3.0)
            except asyncio.TimeoutError:
                pass
        else:
            # 重置消息的重试次数
            for msg in messages:
                self._reset_retry(msg)

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
            queue_semaphore: asyncio.Semaphore | None = None
            try:
                # 获取消费锁
                queue_semaphore, lock_acquired = await self._acquire_consume_lock(
                    message_queue, stop_event
                )

                if not lock_acquired:
                    continue

                # 从处理队列获取消息
                pq, messages = await self._fetch_messages_from_queue(
                    message_queue, stop_event
                )
                if not messages:
                    continue

                # 处理消息并处理重试逻辑
                await self._process_messages_with_retry(
                    message_queue, pq, messages, stop_event
                )

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

            finally:
                # 释放信号量
                if queue_semaphore is not None:
                    queue_semaphore.release()

        self.logger.debug(
            f"Consume messages loop ended for queue: {message_queue}",
            extra={
                "consumer_group": self._config.consumer_group,
                "topic": message_queue.topic,
                "queue_id": message_queue.queue_id,
            },
        )

    async def _handle_auto_commit_result(
        self,
        pq: AsyncProcessQueue,
        message_queue: MessageQueue,
        messages: list[MessageExt],
        success: bool,
        _result: ConsumeResult,
    ) -> tuple[bool, bool]:
        """
        处理自动提交模式下的消费结果。

        Args:
            pq: 处理队列
            message_queue: 消息队列
            messages: 消息列表
            success: 是否处理成功
            result: 消费结果

        Returns:
            tuple[bool, bool]: (是否继续循环, 是否需要等待)
        """
        if success:
            offset = await pq.commit()
            if offset:
                await self._offset_store.update_offset(message_queue, offset)
            return False, False  # 跳出循环，不等待
        else:
            if await self._check_reconsume_times(message_queue, messages):
                return True, True  # 继续循环，需要等待
            else:
                offset = await pq.commit()
                if offset:
                    await self._offset_store.update_offset(message_queue, offset)
                return False, False  # 跳出循环，不等待

    async def _handle_manual_commit_result(
        self,
        pq: AsyncProcessQueue,
        message_queue: MessageQueue,
        messages: list[MessageExt],
        success: bool,
        result: ConsumeResult,
    ) -> tuple[bool, bool]:
        """
        处理手动提交模式下的消费结果。

        Args:
            pq: 处理队列
            message_queue: 消息队列
            messages: 消息列表
            success: 是否处理成功
            result: 消费结果

        Returns:
            tuple[bool, bool]: (是否继续循环, 是否需要等待)
        """
        if success:
            if result == ConsumeResult.SUCCESS:
                # 啥也不做, 等待下次一起commit
                return False, False  # 跳出循环，不等待
            else:
                # commit
                offset = await pq.commit()
                if offset:
                    await self._offset_store.update_offset(message_queue, offset)
                return False, False  # 跳出循环，不等待
        else:
            if result == ConsumeResult.ROLLBACK:
                _ = await pq.rollback(messages)
                return False, True  # 跳出循环，需要等待
            elif result == ConsumeResult.RECONSUME_LATER:
                if await self._check_reconsume_times(message_queue, messages):
                    return True, True  # 继续循环，需要等待
                else:
                    return False, False  # 跳出循环，不等待

        return False, False

    async def _check_reconsume_times(
        self, _message_queue: MessageQueue, messages: list[MessageExt]
    ) -> bool:
        """检查消息是否还需要重新消费

        Args:
            message_queue: 消息队列
            messages: 消息列表

        Returns:
            bool: 是否需要重新消费
        """
        max_reconsume_times = self._config.max_reconsume_times

        for message in messages:
            reconsume_times = getattr(message, "reconsume_times", 0)
            if reconsume_times >= max_reconsume_times:
                # 超过最大重试次数，不再重试
                return False

        # 所有消息都还可以重试
        return True

    async def _process_messages_with_timing(
        self, messages: list[MessageExt], message_queue: MessageQueue
    ) -> tuple[bool, ConsumeResult]:
        """
        处理消息并计时

        Args:
            messages: 要处理的消息列表
            message_queue: 消息队列

        Returns:
            消费结果，包含成功状态
        """
        start_time: float = time.time()
        success, consume_result = await self._orderly_consume_message(
            messages, message_queue
        )
        duration: float = time.time() - start_time

        # 记录消费统计
        consume_rt = int(duration * 1000)  # 转换为毫秒
        message_count = len(messages)

        self._stats_manager.increase_consume_rt(
            self._config.consumer_group, message_queue.topic, consume_rt
        )

        if success:
            self._stats_manager.increase_consume_ok_tps(
                self._config.consumer_group, message_queue.topic, message_count
            )
        else:
            self._stats_manager.increase_consume_failed_tps(
                self._config.consumer_group, message_queue.topic, message_count
            )

        return success, consume_result

    async def _process_messages_with_retry(
        self,
        message_queue: MessageQueue,
        pq: AsyncProcessQueue,
        messages: list[MessageExt],
        stop_event: asyncio.Event,
    ) -> None:
        """带重试机制的消息处理

        Args:
            message_queue: 消息队列
            pq: 处理队列
            messages: 要处理的消息列表
            stop_event: 停止事件
        """
        while self._is_running and not stop_event.is_set():
            success, result = await self._process_messages_with_timing(
                messages, message_queue
            )

            # 根据提交模式处理结果
            if self._config.enable_auto_commit:
                should_continue, should_wait = await self._handle_auto_commit_result(
                    pq, message_queue, messages, success, result
                )
            else:
                should_continue, should_wait = await self._handle_manual_commit_result(
                    pq, message_queue, messages, success, result
                )

            if should_continue:
                if should_wait:
                    try:
                        await asyncio.wait_for(stop_event.wait(), timeout=1.0)
                    except asyncio.TimeoutError:
                        pass
                continue
            else:
                break

    # ==================== 6. 消息缓存管理模块 ====================
    #
    # 该模块负责管理ProcessQueue消息缓存，是连接消息拉取和消息消费的关键桥梁。
    # ProcessQueue提供了高效的内存缓存机制，支持按偏移量排序的消息管理。
    #
    # 核心功能：
    # 1. 消息缓存：将拉取的消息按偏移量顺序存储在内存中
    # 2. 流量控制：当缓存消息过多时暂停拉取，防止内存溢出
    # 3. 偏移量管理：跟踪已消费和未消费的消息偏移量
    # 4. 消息排序：自动维护消息的偏移量顺序
    # 5. 批量操作：支持批量添加和获取消息，提高效率
    # 6. 内存保护：通过数量和大小限制保护内存使用
    #
    # ProcessQueue特性：
    # - 按queue_offset升序排列，方便顺序消费
    # - 线程安全的并发访问控制
    # - 自动去重，避免重复缓存相同偏移量的消息
    # - 智能流量控制，基于缓存数量和大小
    # - 高效的插入、查询和统计功能
    #
    # 缓存限制：
    # - max_cache_count_per_queue: 单队列最大消息数量限制
    # - max_cache_size_per_queue: 单队列最大内存大小限制
    #
    # 相关函数：
    # - _get_or_create_process_queue: 获取或创建ProcessQueue
    # - _add_messages_to_cache: 将消息添加到缓存
    # - _update_consume_stats: 更新消费统计信息
    #
    # 注意：具体的ProcessQueue实现在process_queue.py中，这里只是使用接口

    # ==================== 7. 统计与监控模块 ====================
    #
    # 该模块负责全面收集和分析消费者的运行时指标，为性能调优和问题诊断
    # 提供数据支持。通过详细的统计信息，可以监控消费者的健康状况和性能表现。
    #
    # 监控指标分类：
    # 1. 消息处理统计：消费数量、成功率、失败率、重试次数
    # 2. 性能指标：消费耗时、TPS、延迟分布、吞吐量
    # 3. 重平衡统计：重平衡次数、成功率、失败原因
    # 4. 拉取统计：拉取次数、成功率、拉取延迟
    # 5. 锁统计：锁等待次数、等待时间、锁竞争情况
    # 6. 队列统计：分配队列数、活跃队列数、队列状态
    # 7. 资源统计：内存使用、连接数、任务数
    #
    # 核心统计项：
    # - messages_consumed: 总消费消息数
    # - messages_success: 成功消费消息数
    # - messages_failed: 失败消费消息数
    # - consume_duration_total: 总消费耗时
    # - pull_requests: 总拉取请求数
    # - pull_success/pull_fail: 拉取成功/失败次数
    # - rebalance_count: 重平衡次数
    # - orderly_consume_success_count: 顺序消费成功数
    # - orderly_consume_fail_count: 顺序消费失败数
    #
    # 相关函数：
    # - _update_consume_stats: 更新消费统计信息
    # - _get_final_stats: 获取最终统计信息
    #
    # 使用场景：
    # - 实时监控：通过JMX或其他监控系统暴露指标
    # - 性能分析：分析消费瓶颈和优化方向
    # - 问题诊断：快速定位消费异常和性能问题
    # - 容量规划：基于历史数据进行资源规划

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
                    pass

                # 控制拉取频率 - 传入是否有消息的标志，使用可中断等待
                await self._apply_pull_interval(
                    has_messages=(len(messages) > 0), stop_event=pull_stop_event
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

                # 拉取失败时等待一段时间再重试，使用可中断等待
                try:
                    await asyncio.wait_for(pull_stop_event.wait(), timeout=3.0)
                    break  # 收到停止信号
                except asyncio.TimeoutError:
                    continue  # 超时是正常的，继续重试

    async def _get_or_create_process_queue(
        self, queue: MessageQueue
    ) -> AsyncProcessQueue:
        """获取或创建指定队列的ProcessQueue（消息缓存队列）

        Args:
            queue: 消息队列

        Returns:
            AsyncProcessQueue: 指定队列的处理队列对象
        """
        async with self._cache_lock:
            if queue not in self._msg_cache:
                self._msg_cache[queue] = AsyncProcessQueue(
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
                else:
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

        process_queue: AsyncProcessQueue = await self._get_or_create_process_queue(
            queue
        )
        _ = await process_queue.add_batch_messages(messages)

        self.logger.debug(
            f"Added {len(messages)} messages to cache for queue: {queue}",
            extra={
                "consumer_group": self._config.consumer_group,
                "topic": queue.topic,
                "queue_id": queue.queue_id,
                "message_count": len(messages),
            },
        )

    # ==================== 9. 资源清理与错误处理模块 ====================
    #
    # 该模块是消费者稳定性的保障，负责处理各种异常情况和优雅的资源清理。
    # 确保消费者在启动、运行和关闭过程中的资源一致性和系统稳定性。
    #
    # 核心职责：
    # 1. 启动失败清理：当消费者启动过程中发生异常时，回滚已分配的资源
    # 2. 优雅关闭：确保所有正在处理的任务完成，避免数据丢失
    # 3. 异步任务管理：统一管理拉取、消费、重平衡等异步任务的生命周期
    # 4. 资源释放：清理内存、连接、锁、缓存等系统资源
    # 5. 异常恢复：处理各种运行时异常，确保系统可继续运行
    #
    # 清理策略：
    # - 分层清理：按照组件依赖关系有序清理
    # - 超时保护：设置合理的清理超时，避免无限等待
    # - 容错处理：清理过程中的异常不中断后续清理
    # - 状态一致性：确保清理后系统状态的一致性
    #
    # 关键场景：
    # 1. 启动失败：网络连接失败、配置错误等场景的清理
    # 2. 正常关闭：用户主动调用shutdown()的清理流程
    # 3. 异常关闭：系统异常、进程终止等场景的紧急清理
    # 4. 重平衡清理：队列重新分配时的资源清理
    # 5. 连接异常：网络中断、Broker故障等场景的恢复
    #
    # 资源类型：
    # - 网络资源：TCP连接、连接池
    # - 内存资源：消息缓存、队列缓存、统计数据
    # - 并发资源：异步任务、锁、信号量、事件
    # - 远程资源：Broker端的远程锁、注册信息
    #
    # 相关函数：
    # - _cleanup_on_start_failure: 启动失败时的资源清理
    # - _wait_for_processing_completion: 等待处理任务完成
    # - _shutdown_async_tasks: 关闭所有异步任务
    # - _cleanup_resources: 清理所有系统资源

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
            async with self._consume_tasks_lock:
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
                _ = await process_queue.clear()
            self._msg_cache.clear()

            # 清理停止事件
            async with self._stop_events_lock:
                self._pull_stop_events.clear()
                self._consume_stop_events.clear()

            # 清理状态
            self._pull_tasks.clear()
            async with self._consume_tasks_lock:
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
            async with self._queue_locks_lock:
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

    # ==================== 8. 远程通信处理模块 ====================
    #
    # 该模块负责处理与RocketMQ Broker的远程通信，包括接收Broker的通知
    # 和处理直接消费请求。这些通信是RocketMQ协调机制的重要组成部分。
    #
    # 核心功能：
    # 1. 通知处理：处理Broker发送的各种管理通知
    # 2. 直接消费：响应Broker的直接消费请求，用于消息验证和调试
    # 3. 消费者协调：处理消费者组成员变更通知
    # 4. 连接管理：注册和注销请求处理器
    # 5. 错误处理：处理通信异常和格式错误
    #
    # 支持的请求类型：
    # - NOTIFY_CONSUMER_IDS_CHANGED: 消费者组成员变更通知
    # - CONSUME_MESSAGE_DIRECTLY: 直接消费消息请求
    #
    # 通知处理流程：
    # 1. 接收Broker发送的通知请求
    # 2. 解析请求头和参数
    # 3. 验证请求的合法性（如客户端ID匹配）
    # 4. 执行相应的处理逻辑
    # 5. 构造响应并返回
    #
    # 直接消费场景：
    # - 消息轨迹跟踪：验证消息是否被正确消费
    # - 问题诊断：检查特定消息的处理情况
    # - 管理界面：支持管理控制台的消息验证功能
    #
    # 相关函数：
    # - _prepare_consumer_remote: 注册远程通信处理器
    # - _on_notify_consume_message_directly: 处理直接消费通知
    # - _on_notify_consume_message_directly_internal: 内部直接消费处理
    # - _on_notify_consumer_ids_changed: 处理消费者ID变更通知
    # - _get_final_stats: 获取最终统计信息（包含通信统计）

    async def _prepare_consumer_remote(self, pool: AsyncConnectionPool) -> None:
        """准备异步消费者远程通信处理器

        Args:
            pool: 异步连接池
        """
        await pool.register_request_processor(
            RequestCode.NOTIFY_CONSUMER_IDS_CHANGED,
            self._on_notify_consumer_ids_changed,
        )
        await pool.register_request_processor(
            RequestCode.CONSUME_MESSAGE_DIRECTLY,
            self._on_notify_consume_message_directly,
        )

    async def _on_notify_consume_message_directly(
        self, command: RemotingCommand, _addr: tuple[str, int]
    ) -> RemotingCommand:
        """处理直接消费消息通知

        Args:
            command: 远程命令
            _addr: 来源地址

        Returns:
            RemotingCommand: 处理结果
        """
        header: ConsumeMessageDirectlyHeader = ConsumeMessageDirectlyHeader.decode(
            command.ext_fields
        )
        if header.client_id == self._config.client_id:
            return await self._on_notify_consume_message_directly_internal(
                header, command
            )
        else:
            return (
                RemotingCommandBuilder(ResponseCode.ERROR)
                .with_remark(f"Can't find client ID {header.client_id}")
                .build()
            )

    async def _on_notify_consume_message_directly_internal(
        self, header: ConsumeMessageDirectlyHeader, command: RemotingCommand
    ) -> RemotingCommand:
        """内部处理直接消费消息

        Args:
            header: 消息头
            command: 远程命令

        Returns:
            RemotingCommand: 处理结果
        """
        if not command.body:
            return (
                RemotingCommandBuilder(ResponseCode.ERROR)
                .with_remark("No message body")
                .build()
            )

        msgs = MessageExt.decode_messages(command.body)
        if len(msgs) == 0:
            return (
                RemotingCommandBuilder(ResponseCode.ERROR)
                .with_remark("No message")
                .build()
            )

        msg: MessageExt = msgs[0]

        q: MessageQueue
        if msg.queue:
            q = MessageQueue(msg.topic, header.broker_name, msg.queue.queue_id)
        else:
            q = MessageQueue(msg.topic, header.broker_name, 0)

        now = datetime.now()

        for msg in msgs:
            self._reset_retry(msg)

        success, _ = await self._orderly_consume_message(msgs, q)
        if success:
            res: ConsumeMessageDirectlyResult = ConsumeMessageDirectlyResult(
                order=False,
                auto_commit=True,
                consume_result=ConsumeResult.SUCCESS,
                remark="Message consumed",
                spent_time_mills=int((datetime.now() - now).total_seconds() * 1000),
            )
            return (
                RemotingCommandBuilder(ResponseCode.SUCCESS)
                .with_remark("Message consumed")
                .with_body(res.encode())
                .build()
            )
        else:
            return (
                RemotingCommandBuilder(ResponseCode.ERROR)
                .with_remark("Failed to consume message")
                .build()
            )

    async def _on_notify_consumer_ids_changed(
        self, _remoting_cmd: RemotingCommand, _remote_addr: tuple[str, int]
    ) -> None:
        """处理消费者ID变更通知

        Args:
            _remoting_cmd: 远程命令
            _remote_addr: 远程地址
        """
        self.logger.info(
            "Received notification of consumer IDs changed",
            extra={
                "consumer_group": self._config.consumer_group,
                "remote_addr": f"{_remote_addr[0]}:{_remote_addr[1]}",
            },
        )
        # 触发重平衡
        await self._trigger_rebalance()

    async def get_stats_manager(self):
        """异步获取消费者的统计管理器实例。

        返回用于收集和管理消费者运行时统计信息的管理器对象。
        统计管理器提供了丰富的性能指标和监控数据，用于性能调优
        和问题诊断。

        Returns:
            StatsManager: 统计管理器实例，提供以下功能：
                - 消费TPS统计：成功和失败的消息处理速率
                - 消费耗时统计：消息处理的响应时间分布
                - 拉取统计：消息拉取的成功率和延迟
                - 重平衡统计：重平衡操作的执行情况
                - 队列状态统计：分配队列的活跃状态

        Note:
            - 统计数据在内存中维护，重启后会清零
            - 支持实时查询历史统计数据
            - 可用于集成外部监控系统
            - 统计数据对性能影响极小

        Examples:
            >>> stats_manager = await consumer.get_stats_manager()
            >>> # 获取消费TPS
            >>> tps = stats_manager.get_consume_tps("consumer_group", "topic")
            >>> # 获取消费耗时
            >>> rt = stats_manager.get_consume_rt("consumer_group", "topic")

        Raises:
            None: 此方法不会抛出异常
        """
        return self._stats_manager

    async def get_consume_status(self, topic: str):
        """异步获取指定Topic的消费状态信息。

        查询并返回指定Topic的消费状态统计数据，包括消费进度、
        性能指标和队列状态等信息。这些数据用于监控消费者的
        运行状况和诊断消费问题。

        Args:
            topic (str): 要查询消费状态的Topic名称，必须是消费者已订阅的Topic

        Returns:
            ConsumeStatus: 消费状态对象，包含以下关键信息：
                - consume_tps: 当前消费TPS（每秒处理消息数）
                - consume_rt: 平均消费响应时间（毫秒）
                - pull_tps: 当前拉取TPS
                - pull_rt: 平均拉取响应时间（毫秒）
                - total_messages: 总处理消息数
                - failed_messages: 失败消息数
                - queue_status: 各队列的消费状态
                - last_consume_time: 最后一次消费时间

        Note:
            - 返回的数据是实时统计，反映当前时刻的消费状态
            - 如果Topic未被订阅，返回的状态数据可能为空或无效
            - 可用于构建监控仪表板和告警系统
            - 统计数据基于内存计算，查询性能很高

        Examples:
            >>> status = await consumer.get_consume_status("test_topic")
            >>> print(f"Consume TPS: {status.consume_tps}")
            >>> print(f"Consume RT: {status.consume_rt}ms")
            >>> print(f"Total Messages: {status.total_messages}")

        Raises:
            None: 此方法不会抛出异常，查询失败时返回默认状态对象

        See Also:
            get_stats_manager: 获取统计管理器以访问更详细的统计数据
        """
        return self._stats_manager.get_consume_status(
            self._config.consumer_group, topic
        )
