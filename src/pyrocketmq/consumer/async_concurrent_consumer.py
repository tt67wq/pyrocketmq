"""
AsyncConcurrentConsumer - 异步并发消费者实现

AsyncConcurrentConsumer是pyrocketmq的异步并发消费者实现，支持高并发异步消息消费。
它使用asyncio和异步任务来并行处理多个队列的消息，提供高吞吐量的异步消息消费能力。

主要特性:
- 异步并发消费，基于asyncio事件循环
- 自动重平衡和队列分配
- 异步偏移量管理
- 完善的错误处理和恢复
- 丰富的监控指标

作者: pyrocketmq开发团队
"""

import asyncio
import time
from datetime import datetime
from typing import Any

# pyrocketmq导入
from pyrocketmq.broker import AsyncBrokerClient, MessagePullError
from pyrocketmq.consumer.allocate_queue_strategy import (
    AllocateContext,
    AllocateQueueStrategyFactory,
)
from pyrocketmq.consumer.async_base_consumer import AsyncBaseConsumer
from pyrocketmq.consumer.async_consume_from_where_manager import (
)
from pyrocketmq.consumer.async_listener import (
    AsyncMessageListener,
    ConsumeResult,
)
from pyrocketmq.consumer.config import ConsumerConfig
from pyrocketmq.consumer.errors import (
    ConsumerShutdownError,
    ConsumerStartError,
    MessageConsumeError,
)
from pyrocketmq.consumer.offset_store import ReadOffsetType
from pyrocketmq.consumer.process_queue import ProcessQueue
from pyrocketmq.logging import get_logger
from pyrocketmq.model import (
    ConsumeMessageDirectlyHeader,
    ConsumeMessageDirectlyResult,
    MessageExt,
    MessageModel,
    MessageQueue,
    MessageSelector,
    PullMessageResult,
    RemotingCommand,
    RemotingCommandBuilder,
    RequestCode,
    ResponseCode,
    SubscriptionData,
    SubscriptionEntry,
    TopicRouteData,
)
from pyrocketmq.remote import AsyncConnectionPool, ConnectionPool

logger = get_logger(__name__)


class AsyncConcurrentConsumer(AsyncBaseConsumer):
    """
    异步并发消费者实现

    支持异步并发消费消息的消费者实现。每个队列都有专门的异步任务负责拉取消息，
    并使用异步任务池来并行处理消息，提供高吞吐量的异步消息消费能力。

    核心特性:
    - 异步并发拉取和处理消息
    - 自动队列重平衡和分配
    - 智能偏移量管理
    - 完善的错误处理和重试机制
    - 丰富的性能监控指标

    使用示例:
        >>> from pyrocketmq.consumer import AsyncConcurrentConsumer, ConsumerConfig
        >>> from pyrocketmq.consumer.async_listener import AsyncMessageListener, ConsumeResult
        >>>
        >>> class MyAsyncListener(AsyncMessageListener):
        ...     async def consume_message(self, messages, context):
        ...         for msg in messages:
        ...             print(f"Processing: {msg.body.decode()}")
        ...         return ConsumeResult.SUCCESS
        >>>
        >>> config = ConsumerConfig(
        ...     consumer_group="my_group",
        ...     namesrv_addr="localhost:9876"
        ... )
        >>> consumer = AsyncConcurrentConsumer(config)
        >>> await consumer.register_message_listener(MyAsyncListener())
        >>> await consumer.subscribe("test_topic", create_tag_selector("*"))
        >>> await consumer.start()
    """

    def __init__(self, config: ConsumerConfig) -> None:
        """
        初始化异步并发消费者

        Args:
            config (ConsumerConfig): 消费者配置参数
        """
        super().__init__(config)

        # 异步任务管理
        self._pull_tasks: dict[MessageQueue, asyncio.Task[None]] = {}
        self._consume_task: asyncio.Task[None] | None = None
        self._rebalance_task: asyncio.Task[None] | None = None

        # 异步信号量控制并发数
        self._consume_semaphore: asyncio.Semaphore = asyncio.Semaphore(
            self._config.consume_thread_max
        )
        self._pull_semaphore: asyncio.Semaphore = asyncio.Semaphore(
            min(self._config.consume_thread_max, 10)
        )

        # 处理队列
        self._process_queue: asyncio.Queue[tuple[list[MessageExt], MessageQueue]] = (
            asyncio.Queue()
        )
        self._msg_cache: dict[MessageQueue, ProcessQueue] = {}
        self._assigned_queues: dict[MessageQueue, int] = {}

        # 异步锁
        self._cache_lock = asyncio.Lock()
        self._stats_lock = asyncio.Lock()
        self._rebalance_lock = asyncio.Lock()

        # 统计信息
        self._stats: dict[str, Any] = {
            "start_time": 0,
            "pull_count": 0,
            "pull_success_count": 0,
            "pull_error_count": 0,
            "consume_count": 0,
            "consume_success_count": 0,
            "consume_error_count": 0,
            "processed_message_count": 0,
            "processed_message_size": 0,
            "rebalance_count": 0,
            "rebalance_success_count": 0,
            "rebalance_failure_count": 0,
            "rebalance_skipped_count": 0,
            "last_pull_time": 0,
            "last_consume_time": 0,
            "last_rebalance_time": 0,
        }

        # 创建队列分配策略
        self._allocate_strategy = AllocateQueueStrategyFactory.create_strategy(
            self._config.allocate_queue_strategy
        )

        # 重平衡事件
        self._rebalance_event = asyncio.Event()

        logger.info(
            "AsyncConcurrentConsumer initialized",
            extra={
                "consumer_group": self._config.consumer_group,
                "message_model": self._config.message_model,
                "consume_thread_max": self._config.consume_thread_max,
                "pull_batch_size": self._config.pull_batch_size,
            },
        )

    async def start(self) -> None:
        """启动异步并发消费者。

        初始化并启动消费者的所有组件，包括：
        - 建立与NameServer和Broker的网络连接
        - 创建异步消息拉取和处理任务
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
            此方法是协程，需要在async上下文中调用。
            启动成功后，消费者会自动开始拉取和处理消息。
        """
        async with self._lock:
            if self._is_running:
                logger.warning("AsyncConsumer is already running")
                return

            try:
                logger.info(
                    "Starting AsyncConcurrentConsumer",
                    extra={
                        "consumer_group": self._config.consumer_group,
                        "namesrv_addr": self._config.namesrv_addr,
                    },
                )

                # 验证必要条件
                if not self._message_listener:
                    raise ConsumerStartError(
                        "No async message listener registered",
                        context={"consumer_group": self._config.consumer_group},
                    )

                if not isinstance(self._message_listener, AsyncMessageListener):
                    raise ConsumerStartError(
                        "Message listener must be AsyncMessageListener for AsyncConcurrentConsumer",
                        context={"consumer_group": self._config.consumer_group},
                    )

                # 启动AsyncBaseConsumer
                await super().start()

                # 执行重平衡
                await self._do_rebalance()

                # 启动重平衡任务
                await self._start_rebalance_task()

                # 启动消息处理任务
                await self._start_consume_tasks()

                self._stats["start_time"] = time.time()

                logger.info(
                    "AsyncConcurrentConsumer started successfully",
                    extra={
                        "consumer_group": self._config.consumer_group,
                        "assigned_queues": len(self._assigned_queues),
                        "consume_concurrency": self._config.consume_thread_max,
                        "pull_concurrency": min(self._config.consume_thread_max, 10),
                    },
                )

            except Exception as e:
                logger.error(
                    f"Failed to start AsyncConcurrentConsumer: {e}",
                    extra={
                        "consumer_group": self._config.consumer_group,
                        "error": str(e),
                    },
                    exc_info=True,
                )
                # 清理资源
                await self._cleanup_on_start_failure()
                raise ConsumerStartError(
                    f"Failed to start AsyncConcurrentConsumer: {e}"
                ) from e

    async def shutdown(self) -> None:
        """关闭异步并发消费者。

        按照以下顺序安全关闭所有组件：
        1. 停止运行状态标记
        2. 停止所有消息拉取任务
        3. 停止消息处理任务
        4. 等待正在处理的消息完成
        5. 持久化偏移量
        6. 关闭网络连接

        此方法会等待所有正在处理的消息完成，确保消息不丢失。

        Raises:
            ConsumerShutdownError: 当关闭过程中发生错误时抛出

        Note:
            此方法是协程，需要在async上下文中调用。
            调用此方法后，消费者实例不可再次使用。
        """
        async with self._lock:
            if not self._is_running:
                logger.warning("AsyncConsumer is not running")
                return

            try:
                logger.info(
                    "Shutting down AsyncConcurrentConsumer",
                    extra={
                        "consumer_group": self._config.consumer_group,
                    },
                )

                # 停止运行状态
                self._is_running = False

                # 停止重平衡事件
                self._rebalance_event.set()

                # 停止所有拉取任务
                await self._stop_pull_tasks()

                # 停止消息处理任务
                await self._stop_consume_tasks()

                # 等待处理完成
                await self._wait_for_processing_completion()

                # 关闭AsyncBaseConsumer
                await super().shutdown()

                # 获取最终统计信息
                final_stats = await self._get_final_stats()
                logger.info(
                    "AsyncConcurrentConsumer shutdown completed",
                    extra={
                        "consumer_group": self._config.consumer_group,
                        "final_stats": final_stats,
                    },
                )

            except Exception as e:
                logger.error(
                    f"Error during AsyncConcurrentConsumer shutdown: {e}",
                    extra={
                        "consumer_group": self._config.consumer_group,
                        "error": str(e),
                    },
                    exc_info=True,
                )
                raise ConsumerShutdownError(
                    f"Failed to shutdown AsyncConcurrentConsumer: {e}"
                ) from e
            finally:
                # 清理资源
                await self._cleanup_resources()

    async def subscribe(self, topic: str, selector: MessageSelector) -> None:
        """订阅指定主题。

        Args:
            topic (str): 要订阅的主题名称
            selector (MessageSelector): 消息选择器，用于过滤消息

        Raises:
            SubscribeError: 当订阅失败时抛出
        """
        await super().subscribe(topic, selector)

        # 如果消费者已启动，触发重平衡
        if self._is_running:
            await self._trigger_rebalance()

    async def unsubscribe(self, topic: str) -> None:
        """取消订阅指定主题。

        Args:
            topic (str): 要取消订阅的主题名称

        Raises:
            UnsubscribeError: 当取消订阅失败时抛出
        """
        await super().unsubscribe(topic)

        # 如果消费者已启动，触发重平衡
        if self._is_running:
            await self._trigger_rebalance()

    # ==================== 内部方法：重平衡机制 ====================

    async def _pre_rebalance_check(self) -> bool:
        """执行重平衡前置检查。

        检查是否可以执行重平衡操作，包括锁获取和订阅状态检查。

        Returns:
            bool: 如果可以执行重平衡返回True，否则返回False

        Raises:
            None: 此方法不会抛出异常
        """
        # 使用异步锁保护重平衡操作，非阻塞尝试获取锁
        try:
            # 使用wait_for实现非阻塞获取锁，超时时间为0
            await asyncio.wait_for(self._rebalance_lock.acquire(), timeout=0)
        except asyncio.TimeoutError:
            # 如果无法获取锁，说明正在执行重平衡，跳过本次请求
            async with self._stats_lock:
                self._stats["rebalance_skipped_count"] += 1

            logger.debug(
                "Rebalance already in progress, skipping",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "skipped_count": self._stats["rebalance_skipped_count"],
                },
            )
            return False

        # 检查是否有订阅的Topic
        topics: set[str] = set(self._subscription_manager.get_topics())
        if not topics:
            logger.debug("No topics subscribed, skipping rebalance")
            self._rebalance_lock.release()
            return False

        return True

    async def _collect_and_allocate_queues(self) -> set[MessageQueue]:
        """收集所有Topic的可用队列并执行分配。

        遍历所有订阅的Topic，获取每个Topic的可用队列，
        并为每个Topic执行队列分配算法。

        Returns:
            set[MessageQueue]: 分配给当前消费者的所有队列集合

        Raises:
            Exception: 路由信息更新或队列分配失败时抛出异常
        """
        allocated_queues: set[MessageQueue] = set()
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
                    logger.debug(
                        "No queues available for subscribed topic",
                        extra={"topic": topic},
                    )
                    continue

                # 为单个Topic执行队列分配（内部会获取该topic的消费者列表）
                topic_allocated_queues = await self._allocate_queues(topic, all_queues)
                allocated_queues.update(topic_allocated_queues)

                logger.debug(
                    "Topic queue allocation completed",
                    extra={
                        "topic": topic,
                        "total_queues": len(all_queues),
                        "allocated_queues": len(topic_allocated_queues),
                    },
                )

            except Exception as e:
                logger.warning(
                    f"Failed to allocate queues for topic {topic}: {e}",
                    extra={"topic": topic, "error": str(e)},
                )
                # 继续处理其他Topic，不中断整个重平衡过程
                continue

        return allocated_queues

    async def _finalize_rebalance(self, new_assigned_queues: set[MessageQueue]) -> None:
        """完成重平衡后处理。

        更新统计信息，并记录完成日志。

        Args:
            new_assigned_queues: 新分配的队列集合

        Raises:
            None: 此方法不会抛出异常
        """
        # 获取所有订阅主题
        topic_set = set(self._subscription_manager.get_topics())

        # 更新统计信息
        async with self._stats_lock:
            self._stats["rebalance_count"] += 1
            self._stats["rebalance_success_count"] += 1
            self._stats["last_rebalance_time"] = time.time()

        logger.info(
            "Rebalance completed",
            extra={
                "consumer_group": self._config.consumer_group,
                "assigned_queue_count": len(new_assigned_queues),
                "topics": list(topic_set),
                "success_count": self._stats["rebalance_success_count"],
            },
        )

    async def _do_rebalance(self) -> None:
        """执行重平衡操作。

        根据当前订阅信息和消费者组中的所有消费者，重新分配队列。

        执行流程：
        1. 执行重平衡前置检查
        2. 查找当前消费者组中的所有消费者
        3. 收集所有Topic的可用队列并执行分配
        4. 更新分配的队列
        5. 完成重平衡后处理
        """
        # 前置检查
        if not await self._pre_rebalance_check():
            return

        try:
            logger.debug(
                "Starting rebalance",
                extra={"consumer_group": self._config.consumer_group},
            )

            # 收集所有可用队列并执行分配
            new_assigned_queues = await self._collect_and_allocate_queues()

            # 更新分配的队列
            await self._update_assigned_queues(new_assigned_queues)

            # 完成重平衡处理
            await self._finalize_rebalance(new_assigned_queues)

        except Exception as e:
            logger.error(
                f"Rebalance failed: {e}",
                extra={"consumer_group": self._config.consumer_group, "error": str(e)},
                exc_info=True,
            )
            async with self._stats_lock:
                self._stats["rebalance_failure_count"] += 1

        finally:
            # 释放重平衡锁
            self._rebalance_lock.release()
            logger.debug(
                "Rebalance lock released",
                extra={"consumer_group": self._config.consumer_group},
            )

    async def _allocate_queues(
        self, topic: str, all_queues: list[MessageQueue]
    ) -> set[MessageQueue]:
        """为单个Topic执行队列分配。

        Args:
            topic: 主题名称
            all_queues: 该主题的所有可用队列列表

        Returns:
            set[MessageQueue]: 分配给当前消费者的该主题队列集合
        """
        if self._config.message_model == MessageModel.CLUSTERING:
            # 异步获取订阅该Topic的所有消费者ID列表
            consumer_ids = await self._find_consumer_list(topic)
            if not consumer_ids:
                return set()

            return set(
                self._allocate_strategy.allocate(
                    AllocateContext(
                        self._config.consumer_group,
                        self._config.client_id,
                        consumer_ids,
                        all_queues,
                        {},
                    )
                )
            )
        else:
            # 广播模式：返回所有队列
            return set(all_queues)

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
            logger.warning(
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

    async def _update_assigned_queues(
        self, new_assigned_queues: set[MessageQueue]
    ) -> None:
        """更新分配的队列。

        Args:
            new_assigned_queues: 新分配的队列集合
        """
        async with self._lock:
            # 记录旧队列集合
            old_queues = set(self._assigned_queues.keys())

            # 停止已移除队列的拉取任务
            removed_queues = old_queues - new_assigned_queues
            for queue in removed_queues:
                task = self._pull_tasks.pop(queue, None)
                if task and not task.done():
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
                self._assigned_queues.pop(queue, None)

            # 初始化新分配队列的偏移量
            added_queues = new_assigned_queues - old_queues
            for queue in added_queues:
                self._assigned_queues[queue] = 0  # 初始化偏移量为0，后续会更新

            # 如果消费者正在运行，启动新队列的拉取任务
            if self._is_running and added_queues:
                await self._start_pull_tasks_for_queues(added_queues)

    async def _trigger_rebalance(self) -> None:
        """触发重平衡"""
        if self._is_running:
            # 唤醒重平衡循环，使其立即执行重平衡
            self._rebalance_event.set()

    # ==================== 内部方法：消息拉取 ====================

    async def _start_pull_tasks_for_queues(self, queues: set[MessageQueue]) -> None:
        """为指定队列启动拉取任务"""

        for queue in queues:
            if queue not in self._pull_tasks:
                task = asyncio.create_task(self._pull_messages_loop(queue))
                self._pull_tasks[queue] = task

    async def _stop_pull_tasks(self) -> None:
        """停止所有消息拉取任务"""
        if not self._pull_tasks:
            return

        # 取消所有拉取任务
        for task in self._pull_tasks.values():
            if not task.done():
                task.cancel()

        # 等待所有任务完成
        if self._pull_tasks:
            await asyncio.gather(*self._pull_tasks.values(), return_exceptions=True)

        self._pull_tasks.clear()

    async def _pull_messages_loop(self, message_queue: MessageQueue) -> None:
        """持续拉取指定队列的消息。

        为每个分配的队列创建独立的拉取循环，持续从Broker拉取消息
        并放入处理队列。这是消费者消息拉取的核心执行循环。

        执行流程：
        1. 从队列的当前偏移量开始拉取消息
        2. 如果拉取到消息，更新本地偏移量记录
        3. 将消息和处理队列信息提交给消费线程池
        4. 根据配置的拉取间隔进行休眠控制

        Args:
            message_queue (MessageQueue): 要持续拉取消息的目标队列

        Returns:
            None

        Raises:
            None: 此方法会捕获所有异常并记录日志，不会中断拉取循环

        Note:
            - 每个队列有独立的拉取任务，避免队列间相互影响
            - 偏移量在本地维护，定期或在消息处理成功后更新到Broker
            - 拉取失败会记录日志并等待重试，不会影响其他队列
            - 消费者停止时此循环会自动退出
            - 支持通过配置控制拉取频率
        """
        suggest_broker_id = 0
        while self._is_running:
            pq: ProcessQueue = await self._get_or_create_process_queue(message_queue)
            if pq.need_flow_control():
                await asyncio.sleep(3.0)
                continue
            try:
                # 执行单次拉取操作
                pull_result: (
                    tuple[list[MessageExt], int, int] | None
                ) = await self._perform_single_pull(message_queue, suggest_broker_id)

                if pull_result is None:
                    # 如果返回None，说明没有订阅信息，停止消费
                    logger.warning(
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
                    await self._handle_pulled_messages(
                        message_queue, messages, next_begin_offset
                    )
                else:
                    async with self._stats_lock:
                        self._stats["pull_requests"] += 1

                # 控制拉取频率 - 传入是否有消息的标志
                await self._apply_pull_interval(len(messages) > 0)

            except MessagePullError as e:
                logger.warning(
                    "The pull request is illegal",
                    extra={
                        "consumer_group": self._config.consumer_group,
                        "topic": message_queue.topic,
                        "queue_id": message_queue.queue_id,
                        "error": str(e),
                    },
                )
                break
            except Exception as e:
                logger.error(
                    f"Error in pull messages loop for {message_queue}: {e}",
                    extra={
                        "consumer_group": self._config.consumer_group,
                        "topic": message_queue.topic,
                        "queue_id": message_queue.queue_id,
                        "error": str(e),
                    },
                    exc_info=True,
                )

                async with self._stats_lock:
                    self._stats["pull_failures"] += 1

                # 拉取失败时等待一段时间再重试
                await asyncio.sleep(3.0)

    async def _perform_single_pull(
        self, message_queue: MessageQueue, suggest_broker_id: int = 0
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
            messages = await self._filter_messages_by_tags(messages, sub_data.tags_set)

        return messages, next_begin_offset, next_suggest_id

    async def _handle_pulled_messages(
        self,
        message_queue: MessageQueue,
        messages: list[MessageExt],
        next_begin_offset: int,
    ) -> None:
        """处理拉取到的消息。

        包括更新偏移量、缓存消息、分批处理等。

        Args:
            message_queue: 消息队列
            messages: 拉取到的消息列表
            next_begin_offset: 下次拉取的起始偏移量
        """
        # 更新偏移量
        async with self._cache_lock:
            self._assigned_queues[message_queue] = next_begin_offset

        # 将消息添加到缓存中（用于解决并发偏移量问题）
        await self._add_messages_to_cache(message_queue, messages)

        # 将消息按批次放入处理队列
        await self._submit_messages_for_processing(message_queue, messages)

        # 更新统计信息
        message_count = len(messages)
        async with self._stats_lock:
            self._stats["pull_successes"] += 1
            self._stats["messages_consumed"] += message_count
            self._stats["pull_requests"] += 1

    async def _filter_messages_by_tags(
        self, messages: list[MessageExt], tags_set: list[str]
    ) -> list[MessageExt]:
        """根据标签过滤消息。

        Args:
            messages: 待过滤的消息列表
            tags_set: 允许的标签集合

        Returns:
            list[MessageExt]: 过滤后的消息列表
        """
        filtered_messages: list[MessageExt] = []
        for message in messages:
            if message.get_tags() in tags_set:
                filtered_messages.append(message)

        return filtered_messages

    async def _submit_messages_for_processing(
        self, message_queue: MessageQueue, messages: list[MessageExt]
    ) -> None:
        """将消息按批次提交给处理队列"""
        batch_size = self._config.consume_batch_size
        message_count = len(messages)
        batch_count = (message_count + batch_size - 1) // batch_size

        logger.debug(
            f"Splitting {message_count} messages into {batch_count} batches",
            extra={
                "consumer_group": self._config.consumer_group,
                "topic": message_queue.topic,
                "queue_id": message_queue.queue_id,
                "message_count": message_count,
                "batch_size": batch_size,
                "batch_count": batch_count,
            },
        )

        # 将消息按批次放入处理队列
        for i in range(0, message_count, batch_size):
            batch_messages = messages[i : i + batch_size]
            await self._process_queue.put((batch_messages, message_queue))

    async def _apply_pull_interval(self, has_messages: bool = True) -> None:
        """应用智能拉取间隔控制"""
        if self._config.pull_interval > 0:
            if has_messages:
                logger.debug(
                    "Messages pulled, continuing without interval",
                    extra={"consumer_group": self._config.consumer_group},
                )
            else:
                sleep_time = self._config.pull_interval / 1000.0
                logger.debug(
                    f"No messages pulled, sleeping for {sleep_time}s",
                    extra={
                        "consumer_group": self._config.consumer_group,
                        "sleep_time": sleep_time,
                    },
                )
                await asyncio.sleep(sleep_time)

    async def _get_or_create_process_queue(self, queue: MessageQueue) -> ProcessQueue:
        """获取或创建指定队列的ProcessQueue"""
        async with self._cache_lock:
            if queue not in self._msg_cache:
                self._msg_cache[queue] = ProcessQueue(
                    max_cache_count=self._config.max_cache_count_per_queue,
                    max_cache_size_mb=self._config.max_cache_size_per_queue,
                )
            return self._msg_cache[queue]

    async def _add_messages_to_cache(
        self, queue: MessageQueue, messages: list[MessageExt]
    ) -> None:
        """将消息添加到缓存"""
        pq = await self._get_or_create_process_queue(queue)
        _ = pq.add_batch_messages(messages)

    async def _get_or_initialize_offset(self, queue: MessageQueue) -> int:
        """获取或初始化队列偏移量

        如果本地缓存的偏移量为0（首次消费），则根据配置的消费策略
        从ConsumeFromWhereManager获取正确的初始偏移量。

        Args:
            queue: 要获取偏移量的消息队列

        Returns:
            int: 消费偏移量
        """
        # 先从_assigned_queues中读取当前偏移量
        async with self._lock:
            current_offset = self._assigned_queues.get(queue, 0)

        # 如果current_offset为0（首次消费），则从_consume_from_where_manager中获取正确的初始偏移量
        if current_offset == 0:
            try:
                current_offset = (
                    await self._consume_from_where_manager.get_consume_offset(
                        queue, self._config.consume_from_where
                    )
                )
                # 更新本地缓存的偏移量
                async with self._lock:
                    self._assigned_queues[queue] = current_offset

                logger.info(
                    f"初始化消费偏移量: {current_offset}",
                    extra={
                        "consumer_group": self._config.consumer_group,
                        "topic": queue.topic,
                        "queue_id": queue.queue_id,
                        "strategy": self._config.consume_from_where,
                        "offset": current_offset,
                    },
                )

            except Exception as e:
                logger.error(
                    f"获取初始消费偏移量失败，使用默认偏移量0: {e}",
                    extra={
                        "consumer_group": self._config.consumer_group,
                        "topic": queue.topic,
                        "queue_id": queue.queue_id,
                        "strategy": self._config.consume_from_where,
                        "error": str(e),
                    },
                    exc_info=True,
                )
                # 使用默认偏移量0
                current_offset = 0
                async with self._lock:
                    self._assigned_queues[queue] = current_offset

        return current_offset

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

    async def _pull_messages(
        self, message_queue: MessageQueue, offset: int, suggest_id: int
    ) -> tuple[list[MessageExt], int, int]:
        """从指定队列拉取消息，支持偏移量管理和Broker选择。

        该方法是并发消费者的核心拉取逻辑，负责从RocketMQ Broker拉取消息，
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

        Example:
            ```python
            # 拉取消息示例
            messages, next_offset, suggested_broker = await consumer._pull_messages(
                message_queue=MessageQueue("test_topic", "broker-a", 0),
                offset=100,
                suggest_id=0
            )

            if messages:
                for msg in messages:
                    print(f"消息内容: {msg.body.decode()}")
                print(f"下次拉取偏移量: {next_offset}")
                if suggested_broker != 0:
                    print(f"建议下次连接slave broker: {suggested_broker}")
            ```

        Note:
            - 该方法会被_pull_messages_loop循环调用，实现持续的消息拉取
            - suggest_id参数用于Broker选择的优化，通常来自上次拉取响应
            - commit_offset只在连接master broker且存在已提交偏移量时使用
            - 拉取失败时会记录详细的错误信息，便于问题诊断
            - 返回的empty消息列表并不意味着队列中没有消息，可能是由于网络延迟
        """
        try:
            async with self._stats_lock:
                self._stats["pull_requests"] += 1

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

            # 使用BrokerManager拉取消息
            pool: AsyncConnectionPool = await self._broker_manager.must_connection_pool(
                broker_address
            )

            # 注册异步请求处理器
            await pool.register_request_processor(
                RequestCode.NOTIFY_CONSUMER_IDS_CHANGED,
                self._on_notify_consumer_ids_changed,
            )
            await pool.register_request_processor(
                RequestCode.CONSUME_MESSAGE_DIRECTLY,
                self._on_notify_consume_message_directly,
            )

            async with pool.get_connection(usage="pull_message") as conn:
                broker_client = AsyncBrokerClient(conn)
                result: PullMessageResult = await broker_client.pull_message(
                    consumer_group=self._config.consumer_group,
                    topic=message_queue.topic,
                    queue_id=message_queue.queue_id,
                    queue_offset=offset,
                    max_msg_nums=self._config.pull_batch_size,
                    sys_flag=await self._build_sys_flag(
                        commit_offset=commit_offset > 0 and is_master
                    ),
                    commit_offset=commit_offset,
                )

                if result.messages:
                    return (
                        result.messages,
                        result.next_begin_offset,
                        result.suggest_which_broker_id or 0,
                    )

                return [], offset, 0

        except MessagePullError as e:
            logger.warning(
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
            logger.warning(
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

    async def _on_notify_consumer_ids_changed(
        self, remoting_cmd: RemotingCommand, remote_addr: tuple[str, int]
    ) -> None:
        """处理消费者ID变更通知"""
        logger.info("Received notification of consumer IDs changed")
        await self._do_rebalance()

    async def _on_notify_consume_message_directly(
        self, command: RemotingCommand, _addr: tuple[str, int]
    ) -> RemotingCommand:
        """处理直接消费消息通知"""
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
        """内部处理直接消费消息的逻辑"""
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

        if await self._concurrent_consume_message(msgs, q):
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

    # ==================== 内部方法：消息处理 ====================

    async def _start_consume_tasks(self) -> None:
        """启动消息处理任务"""
        self._consume_task = asyncio.create_task(self._consume_messages_loop())

    async def _stop_consume_tasks(self) -> None:
        """停止消息处理任务"""
        if self._consume_task and not self._consume_task.done():
            self._consume_task.cancel()
            try:
                await self._consume_task
            except asyncio.CancelledError:
                pass
        self._consume_task = None

    async def _consume_messages_loop(self) -> None:
        """
        持续处理消息的循环
        """
        while self._is_running:
            try:
                # 从处理队列获取消息
                try:
                    messages: list[MessageExt]
                    message_queue: MessageQueue
                    messages, message_queue = await asyncio.wait_for(
                        self._process_queue.get(), timeout=1.0
                    )
                except asyncio.TimeoutError:
                    continue

                # 处理消息
                start_time: float = time.time()
                success: bool = await self._concurrent_consume_message(messages, message_queue)
                duration: float = time.time() - start_time

                async with self._stats_lock:
                    self._stats["consume_duration_total"] += duration

                if not success:
                    self._stats["messages_failed"] += len(messages)

                # 更新偏移量
                if success:
                    try:
                        # 从缓存中移除已处理的消息，并获取当前最小offset
                        min_offset = await self._remove_messages_from_cache(
                            message_queue, messages
                        )

                        # 直接更新最小offset到offset_store，避免重复查询
                        if min_offset is not None:
                            try:
                                await self._offset_store.update_offset(
                                    message_queue, min_offset
                                )
                                logger.debug(
                                    f"Updated offset from cache: {min_offset}",
                                    extra={
                                        "consumer_group": self._config.consumer_group,
                                        "topic": message_queue.topic,
                                        "queue_id": message_queue.queue_id,
                                        "offset": min_offset,
                                        "cache_stats": (
                                            await self._get_or_create_process_queue(
                                                message_queue
                                            )
                                        ).get_stats(),
                                    },
                                )
                            except Exception as e:
                                logger.warning(
                                    f"Failed to update offset from cache: {e}",
                                    extra={
                                        "consumer_group": self._config.consumer_group,
                                        "topic": message_queue.topic,
                                        "queue_id": message_queue.queue_id,
                                        "offset": min_offset,
                                        "error": str(e),
                                    },
                                )
                    except Exception as e:
                        logger.warning(
                            f"Failed to remove messages from cache: {e}",
                            extra={
                                "consumer_group": self._config.consumer_group,
                                "topic": message_queue.topic,
                                "queue_id": message_queue.queue_id,
                                "error": str(e),
                            },
                        )
                else:
                    # TODO: send msg back
                    for _ in messages:
                        # TODO: implement async send_back_message
                        pass

            except Exception as e:
                logger.error(
                    f"Error in consume messages loop: {e}",
                    extra={
                        "consumer_group": self._config.consumer_group,
                        "error": str(e),
                    },
                    exc_info=True,
                )

    async def _update_offset_from_cache(self, queue: MessageQueue, offset: int) -> None:
        """从缓存更新偏移量"""
        async with self._cache_lock:
            self._assigned_queues[queue] = offset
        await self._offset_store.update_offset(queue, offset)

    async def _remove_messages_from_cache(
        self, queue: MessageQueue, messages: list[MessageExt]
    ) -> None:
        """从缓存中移除消息"""
        pq = await self._get_or_create_process_queue(queue)
        _ = pq.remove_batch_messages([m.queue_offset for m in messages if m.queue_offset is not None])

    async def _wait_for_processing_completion(self) -> None:
        """等待正在处理的消息完成"""
        # 等待处理队列为空
        while not self._process_queue.empty():
            await asyncio.sleep(0.1)

        # 等待所有拉取任务完成
        if self._pull_tasks:
            await asyncio.gather(*self._pull_tasks.values(), return_exceptions=True)

        # 等待消费任务完成
        if self._consume_task and not self._consume_task.done():
            await asyncio.wait_for(self._consume_task, timeout=30.0)

    # ==================== 内部方法：重平衡任务 ====================

    async def _start_rebalance_task(self) -> None:
        """启动重平衡任务"""
        self._rebalance_task = asyncio.create_task(self._rebalance_loop())

    async def _rebalance_loop(self) -> None:
        """重平衡循环"""
        while self._is_running:
            try:
                # 等待重平衡事件或超时
                try:
                    await asyncio.wait_for(self._rebalance_event.wait(), timeout=20.0)
                except asyncio.TimeoutError:
                    # 超时也执行重平衡
                    pass

                # 清除事件
                self._rebalance_event.clear()

                # 执行重平衡
                if self._is_running:
                    await self._do_rebalance()

            except asyncio.CancelledError:
                logger.debug("Rebalance loop cancelled")
                break
            except Exception as e:
                logger.error(
                    f"Error in rebalance loop: {e}",
                    extra={"error": str(e)},
                    exc_info=True,
                )
                await asyncio.sleep(5.0)

    # ==================== 内部方法：资源管理和清理 ====================

    async def _cleanup_on_start_failure(self) -> None:
        """启动失败时的清理操作"""
        try:
            # 停止所有任务
            await self._stop_pull_tasks()
            await self._stop_consume_tasks()

            # 清理资源
            await self._cleanup_resources()

        except Exception as e:
            logger.error(
                f"Error during startup failure cleanup: {e}",
                extra={"error": str(e)},
                exc_info=True,
            )

    async def _cleanup_resources(self) -> None:
        """清理资源"""
        try:
            # 清理缓存
            async with self._cache_lock:
                self._msg_cache.clear()
                self._assigned_queues.clear()

            # 清理任务
            self._pull_tasks.clear()

        except Exception as e:
            logger.error(
                f"Error during resource cleanup: {e}",
                extra={"error": str(e)},
                exc_info=True,
            )

    async def _get_final_stats(self) -> dict[str, Any]:
        """获取最终统计信息"""
        async with self._stats_lock:
            stats = self._stats.copy()

        if stats["start_time"] > 0:
            stats["running_time"] = time.time() - stats["start_time"]
        else:
            stats["running_time"] = 0

        return stats

    async def get_stats(self) -> dict[str, Any]:
        """获取消费者统计信息"""
        async with self._stats_lock:
            stats = self._stats.copy()

        if stats["start_time"] > 0:
            stats["running_time"] = time.time() - stats["start_time"]
        else:
            stats["running_time"] = 0

        # 添加缓存统计
        async with self._cache_lock:
            stats["assigned_queue_count"] = len(self._assigned_queues)
            stats["cached_message_count"] = sum(
                pq.get_count() for pq in self._msg_cache.values()
            )
            stats["cached_message_size"] = sum(
                pq.get_total_size() for pq in self._msg_cache.values()
            )

        # 添加任务统计
        stats["pull_task_count"] = len(self._pull_tasks)
        stats["process_queue_size"] = self._process_queue.qsize()

        return stats
