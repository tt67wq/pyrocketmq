"""
ConcurrentConsumer - 并发消费者实现

ConcurrentConsumer是pyrocketmq的核心消费者实现，支持高并发消息消费。
它使用线程池来并行处理多个队列的消息，提供高吞吐量的消息消费能力。

主要特性:
- 多线程并发消费
- 自动重平衡和队列分配
- 偏移量自动管理
- 完善的错误处理和恢复
- 丰富的监控指标

作者: pyrocketmq开发团队
"""

import queue
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from ctypes import memmove
from datetime import datetime
from typing import Any

# pyrocketmq导入
from pyrocketmq.broker import BrokerClient, MessagePullError
from pyrocketmq.consumer.allocate_queue_strategy import (
    AllocateContext,
    AllocateQueueStrategyFactory,
)
from pyrocketmq.consumer.base_consumer import BaseConsumer
from pyrocketmq.consumer.config import ConsumerConfig
from pyrocketmq.consumer.consume_from_where_manager import ConsumeFromWhereManager
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
    ConsumeResult,
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
)
from pyrocketmq.remote import ConnectionPool

logger = get_logger(__name__)


class ConcurrentConsumer(BaseConsumer):
    """
    并发消费者实现

    支持多线程并发消费消息的消费者实现。每个队列都有专门的线程负责拉取消息，
    并使用线程池来并行处理消息，提供高吞吐量的消息消费能力。

    核心特性:
    - 多线程并发拉取和处理消息
    - 自动队列重平衡和分配
    - 智能偏移量管理
    - 完善的错误处理和重试机制
    - 丰富的性能监控指标

    使用示例:
        >>> from pyrocketmq.consumer import ConcurrentConsumer, ConsumerConfig
        >>> from pyrocketmq.consumer.listener import MessageListener, ConsumeResult
        >>>
        >>> class MyListener(MessageListener):
        ...     def consume_message(self, messages, context):
        ...         for msg in messages:
        ...             print(f"Processing: {msg.body.decode()}")
        ...         return ConsumeResult.SUCCESS
        >>>
        >>> config = ConsumerConfig(
        ...     consumer_group="my_group",
        ...     namesrv_addr="localhost:9876"
        ... )
        >>> consumer = ConcurrentConsumer(config)
        >>> consumer.register_message_listener(MyListener())
        >>> consumer.subscribe("test_topic", create_tag_selector("*"))
        >>> consumer.start()
    """

    def __init__(self, config: ConsumerConfig) -> None:
        """
        初始化并发消费者

        Args:
            config: 消费者配置
        """
        super().__init__(config)

        # 创建消费起始位置管理器
        self._consume_from_where_manager: ConsumeFromWhereManager = (
            ConsumeFromWhereManager(
                consume_group=self._config.consumer_group,
                namesrv_manager=self._name_server_manager,
                broker_manager=self._broker_manager,
            )
        )

        # 创建队列分配策略
        self._allocate_strategy = AllocateQueueStrategyFactory.create_strategy(
            self._config.allocate_queue_strategy
        )

        # 线程池和队列管理
        self._consume_executor: ThreadPoolExecutor | None = None
        self._pull_executor: ThreadPoolExecutor | None = None
        self._process_queue: queue.Queue[tuple[list[MessageExt], MessageQueue]] = (
            queue.Queue()
        )

        # 消息缓存管理 - 使用ProcessQueue解决并发消费偏移量问题
        # ProcessQueue支持高效的insert/remove/min/max/count计算
        # 还能统计MessageExt的body总体积，提供更好的性能
        self._msg_cache: dict[MessageQueue, ProcessQueue] = {}
        self._cache_lock = threading.Lock()  # 用于保护_msg_cache字典

        # 状态管理
        self._pull_tasks: dict[MessageQueue, Future[None]] = {}
        self._assigned_queues: dict[MessageQueue, int] = {}  # queue -> last_offset
        self._last_rebalance_time: float = 0.0

        # 重平衡任务管理
        self._rebalance_thread: threading.Thread | None = None
        self._rebalance_interval: float = 20.0  # 重平衡间隔(秒)

        # 线程同步事件
        self._rebalance_event: threading.Event = (
            threading.Event()
        )  # 用于重平衡循环的事件

        # 重平衡重入保护
        self._rebalance_lock: threading.RLock = threading.RLock()  # 重平衡锁，防止重入

        logger.info(
            "ConcurrentConsumer initialized",
            extra={
                "consumer_group": self._config.consumer_group,
                "message_model": self._config.message_model,
                "consume_thread_max": self._config.consume_thread_max,
                "pull_batch_size": self._config.pull_batch_size,
            },
        )

    def start(self) -> None:
        """启动并发消费者。

        初始化并启动消费者的所有组件，包括：
        - 建立与NameServer和Broker的网络连接
        - 创建消息拉取和处理线程池
        - 执行初始队列分配和重平衡
        - 启动心跳和重平衡后台任务

        启动失败时会自动清理已分配的资源。

        Raises:
            ConsumerStartError: 当以下情况发生时抛出：
                - 未注册消息监听器
                - 消息监听器类型不匹配（需要MessageListener）
                - 网络连接失败
                - 线程池创建失败
                - 其他初始化错误

        Note:
            此方法是线程安全的，多次调用只会启动一次。
            启动成功后，消费者会自动开始拉取和处理消息。
        """
        with self._lock:
            if self._is_running:
                logger.warning("Consumer is already running")
                return

            try:
                logger.info(
                    "Starting ConcurrentConsumer",
                    extra={
                        "consumer_group": self._config.consumer_group,
                        "namesrv_addr": self._config.namesrv_addr,
                    },
                )

                # 验证必要条件
                if not self._message_listener:
                    raise ConsumerStartError(
                        "No message listener registered",
                        context={"consumer_group": self._config.consumer_group},
                    )

                # 启动BaseConsumer
                super().start()

                # 创建线程池
                max_workers: int = self._config.consume_thread_max
                pull_workers: int = min(self._config.consume_thread_max, 10)
                self._consume_executor = ThreadPoolExecutor(
                    max_workers=max_workers,
                    thread_name_prefix=f"consume-{self._config.consumer_group}",
                )
                self._pull_executor = ThreadPoolExecutor(
                    max_workers=pull_workers,
                    thread_name_prefix=f"pull-{self._config.consumer_group}",
                )

                self._do_rebalance()

                # 启动重平衡任务
                self._start_rebalance_task()

                # 启动消息处理任务
                self._start_consume_tasks()

                # 启动消息拉取任务
                # self._start_pull_tasks()

                self._stats["start_time"] = time.time()

                logger.info(
                    "ConcurrentConsumer started successfully",
                    extra={
                        "consumer_group": self._config.consumer_group,
                        "assigned_queues": len(self._assigned_queues),
                        "consume_threads": self._config.consume_thread_max,
                        "pull_threads": min(self._config.consume_thread_max, 10),
                    },
                )

            except Exception as e:
                logger.error(
                    f"Failed to start ConcurrentConsumer: {e}",
                    extra={
                        "consumer_group": self._config.consumer_group,
                        "error": str(e),
                    },
                    exc_info=True,
                )
                self._cleanup_on_start_failure()
                raise ConsumerStartError(
                    "Failed to start ConcurrentConsumer",
                    cause=e,
                    context={"consumer_group": self._config.consumer_group},
                ) from e

    def shutdown(self) -> None:
        """优雅停止并发消费者。

        执行以下关闭流程：
        1. 停止接受新的消息拉取请求
        2. 等待正在处理的消息完成（最多等待30秒）
        3. 持久化所有队列的消费偏移量
        4. 关闭所有线程池和后台任务
        5. 清理网络连接和资源

        Args:
            None

        Returns:
            None

        Raises:
            ConsumerShutdownError: 当以下情况发生时抛出：
                - 偏移量持久化失败
                - 线程池关闭超时
                - 网络连接清理失败
                - 其他清理过程中的错误

        Note:
            - 此方法是线程安全的，可以多次调用
            - 会尽力等待正在处理的消息完成，但不会无限期等待
            - 即使关闭过程中发生错误，也会继续执行后续的清理步骤
            - 关闭后的消费者不能重新启动，需要创建新实例
        """
        with self._lock:
            if not self._is_running:
                logger.warning("Consumer is not running")
                return

            try:
                logger.info(
                    "Shutting down ConcurrentConsumer",
                    extra={"consumer_group": self._config.consumer_group},
                )

                self._is_running = False

                # 先设置Event以唤醒可能阻塞的线程
                self._rebalance_event.set()

                # 停止拉取任务
                self._stop_pull_tasks()

                # 等待处理中的消息完成
                self._wait_for_processing_completion()

                # 持久化偏移量
                try:
                    self._offset_store.persist_all()
                except Exception as e:
                    logger.error(
                        f"Failed to persist offsets during shutdown: {e}",
                        extra={
                            "consumer_group": self._config.consumer_group,
                            "error": str(e),
                        },
                        exc_info=True,
                    )

                # 停止线程池
                self._shutdown_thread_pools()

                # 清理资源
                self._cleanup_resources()

                super().shutdown()

                logger.info(
                    "ConcurrentConsumer shutdown completed",
                    extra={
                        "consumer_group": self._config.consumer_group,
                        "final_stats": self._get_final_stats(),
                    },
                )

            except Exception as e:
                logger.error(
                    f"Error during ConcurrentConsumer shutdown: {e}",
                    extra={
                        "consumer_group": self._config.consumer_group,
                        "error": str(e),
                    },
                    exc_info=True,
                )
                raise ConsumerShutdownError(
                    "Error during consumer shutdown",
                    cause=e,
                    context={"consumer_group": self._config.consumer_group},
                ) from e

    def subscribe(self, topic: str, selector: MessageSelector) -> None:
        """订阅指定Topic的消息。

        将消费者注册为指定Topic的订阅者，并设置消息选择器来过滤消息。
        如果消费者已经运行，会自动触发重平衡来分配新的队列。

        Args:
            topic (str): 要订阅的Topic名称，不能为空或None
            selector (MessageSelector): 消息选择器，用于过滤消息
                - TAG选择器：基于消息标签进行过滤
                - SQL选择器：基于消息属性进行复杂过滤

        Returns:
            None

        Raises:
            ValueError: 当topic为空或无效时
            SubscriptionConflict: 当订阅与现有订阅冲突时

        Note:
            - 可以多次调用此方法订阅多个Topic
            - 相同Topic的重复订阅会更新选择器
            - 消费者运行时调用会触发重平衡
            - 订阅会在消费者重启后保持（如果偏移量已持久化）
        """
        super().subscribe(topic, selector)

        # 如果消费者正在运行，触发重平衡
        if self._is_running:
            self._trigger_rebalance()

    def unsubscribe(self, topic: str) -> None:
        """取消订阅指定Topic。

        移除对指定Topic的订阅，停止拉取该Topic的消息，
        并释放相关的队列资源。如果消费者正在运行，会触发重平衡。

        Args:
            topic (str): 要取消订阅的Topic名称

        Returns:
            None

        Raises:
            None: 此方法不会抛出异常，失败的取消订阅会被记录但不会中断执行

        Note:
            - 如果Topic未被订阅，此方法不会产生任何效果
            - 消费者运行时调用会触发重平衡，可能导致其他队列重新分配
            - 取消订阅不会删除已持久化的偏移量，可以通过重新订阅恢复
            - 建议在取消订阅前确保相关业务逻辑已处理完毕
        """
        super().unsubscribe(topic)

        # 如果消费者正在运行，触发重平衡
        if self._is_running:
            self._trigger_rebalance()

    # ==================== 内部方法：重平衡管理 ====================

    def _do_rebalance(self) -> None:
        """执行消费者重平衡操作。

        根据当前订阅的所有Topic，重新计算和分配队列给当前消费者。
        重平衡是RocketMQ实现负载均衡的核心机制，确保消费者组内的队列分配合理。

        执行流程：
        1. 获取所有已订阅的Topic列表
        2. 查询每个Topic的完整路由信息
        3. 为每个Topic执行队列分配算法
        4. 更新当前消费者的分配队列集合
        5. 启动新分配队列的拉取任务

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
        # 多个地方都会触发重平衡，加入一个放置重入机制，如果正在执行rebalance，再次触发无效
        # 使用可重入锁保护重平衡操作
        if not self._rebalance_lock.acquire(blocking=False):
            # 如果无法获取锁，说明正在执行重平衡，跳过本次请求
            self._stats["rebalance_skipped_count"] += 1
            logger.debug(
                "Rebalance already in progress, skipping",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "skipped_count": self._stats["rebalance_skipped_count"],
                },
            )
            return

        try:
            logger.debug(
                "Starting rebalance",
                extra={"consumer_group": self._config.consumer_group},
            )

            # 更新统计信息
            self._stats["rebalance_count"] += 1

            # 获取所有订阅的Topic
            topics: set[str] = self._subscription_manager.get_topics()
            if not topics:
                logger.debug("No topics subscribed, skipping rebalance")
                return

            allocated_queues: list[MessageQueue] = []
            for topic in topics:
                _ = self._update_route_info(topic)
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

                # 执行队列分配（这里简化处理，实际需要获取所有消费者信息）
                allocated_queues.extend(self._allocate_queues(topic, all_queues))

            # 更新分配的队列
            if allocated_queues:
                self._update_assigned_queues(allocated_queues)

            self._last_rebalance_time = time.time()

            # 更新成功统计
            self._stats["rebalance_success_count"] += 1

            logger.info(
                "Rebalance completed",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "total_topics": len(topics),
                    "assigned_queues": len(allocated_queues),
                    "success_count": self._stats["rebalance_success_count"],
                },
            )

        except Exception as e:
            logger.error(
                f"Rebalance failed: {e}",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "error": str(e),
                },
                exc_info=True,
            )
            # 更新失败统计
            self._stats["rebalance_failure_count"] += 1

        finally:
            # 释放重平衡锁
            self._rebalance_lock.release()
            logger.debug(
                "Rebalance lock released",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "rebalance_count": self._stats["rebalance_count"],
                },
            )

    def _allocate_queues(
        self, topic: str, all_queues: list[MessageQueue]
    ) -> list[MessageQueue]:
        """
        为当前消费者分配队列

        根据消息模式和分配策略，从所有可用队列中选择一部分分配给当前消费者实例。
        这是RocketMQ消费者负载均衡的核心机制，确保多个消费者能够合理地消费同一个Topic下的消息。

        ## 分配策略

        ### 集群模式 (CLUSTERING)
        - 在集群模式下，同一个消费者组内的多个消费者会分担队列
        - 每个队列只能被一个消费者消费，避免重复消费
        - 使用分配策略算法（如平均分配）来决定哪个消费者消费哪些队列

        ### 广播模式 (BROADCASTING)
        - 在广播模式下，每个消费者都会消费所有队列
        - 所有消费者都会收到相同的消息，实现广播效果
        - 不需要进行队列分配，直接返回所有队列

        ## 分配流程

        1. **检查消息模式**：
           - 集群模式：执行负载均衡分配
           - 广播模式：返回所有队列

        2. **集群模式分配**：
           - 获取订阅该Topic的所有消费者ID列表
           - 如果没有其他消费者，返回空列表（避免重复消费）
           - 使用配置的分配策略进行队列分配

        3. **分配策略参数**：
           - consumer_group: 消费者组名
           - client_id: 当前消费者客户端ID
           - consumer_ids: 所有消费者ID列表
           - all_queues: 所有可用队列列表
           - message_queues: 队列映射表

        Args:
            topic: 要分配队列的Topic名称
            all_queues: 该Topic下所有可用的消息队列列表

        Returns:
            list[MessageQueue]: 分配给当前消费者的队列列表

        ## 使用场景

        - **消费者启动时**：初次分配队列
        - **重平衡时**：消费者加入或离开后重新分配
        - **路由变更时**：Topic路由信息变化后重新分配

        ## 注意事项

        - 集群模式下确保一个队列只被一个消费者消费
        - 广播模式下每个消费者都能收到所有消息
        - 分配结果会影响消息的并发度和处理性能
        - 分配策略的变更可能导致消息顺序性的变化

        ## 示例

        ```python
        # 假设有3个队列和2个消费者
        all_queues = [queue1, queue2, queue3]

        # 集群模式下，可能分配给当前消费者：[queue1, queue3]
        # 广播模式下，分配给当前消费者：[queue1, queue2, queue3]
        allocated = self._allocate_queues("test_topic", all_queues)
        ```
        """
        if self._config.message_model == MessageModel.CLUSTERING:
            cids = self._find_consumer_list(topic)
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

    def _update_assigned_queues(self, new_queues: list[MessageQueue]) -> None:
        """更新当前消费者的分配队列集合。

        比较新旧队列分配，执行增量更新：
        - 停止被回收队列的拉取任务
        - 启动新分配队列的拉取任务
        - 维护队列偏移量信息

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
        """
        with self._lock:
            old_queues: set[MessageQueue] = set(self._assigned_queues.keys())
            new_queue_set: set[MessageQueue] = set(new_queues)

            # 停止不再分配的队列的拉取任务
            removed_queues: set[MessageQueue] = old_queues - new_queue_set
            for queue in removed_queues:
                if queue in self._pull_tasks:
                    future: Future[None] | None = self._pull_tasks.pop(queue)
                    if future and not future.done():
                        future.cancel()
                _ = self._assigned_queues.pop(queue, None)

            # 启动新分配队列的拉取任务
            added_queues: set[MessageQueue] = new_queue_set - old_queues
            for queue in added_queues:
                self._assigned_queues[queue] = 0  # 初始化偏移量为0，后续会更新

            # 如果消费者正在运行，启动新队列的拉取任务
            if self._is_running and added_queues:
                self._start_pull_tasks_for_queues(added_queues)

    def _trigger_rebalance(self) -> None:
        """
        触发重平衡
        """
        if self._is_running:
            # 唤醒重平衡循环，使其立即执行重平衡
            self._rebalance_event.set()

    # ==================== 内部方法：消息拉取 ====================

    def _start_pull_tasks(self) -> None:
        """
        启动所有队列的消息拉取任务
        """
        if not self._pull_executor or not self._assigned_queues:
            return

        self._start_pull_tasks_for_queues(set(self._assigned_queues.keys()))

    def _start_pull_tasks_for_queues(self, queues: set[MessageQueue]) -> None:
        """
        为指定队列启动拉取任务

        Args:
            queues: 要启动拉取任务的队列集合
        """
        if not self._pull_executor:
            raise ValueError("Pull executor is not initialized")

        for q in queues:
            if q not in self._pull_tasks:
                future: Future[None] = self._pull_executor.submit(
                    self._pull_messages_loop, q
                )
                self._pull_tasks[q] = future

    def _stop_pull_tasks(self) -> None:
        """
        停止所有消息拉取任务
        """
        if not self._pull_tasks:
            return

        for _, future in self._pull_tasks.items():
            if future and not future.done():
                future.cancel()

        self._pull_tasks.clear()

    def _pull_messages_loop(self, message_queue: MessageQueue) -> None:
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
            - 每个队列有独立的拉取线程，避免队列间相互影响
            - 偏移量在本地维护，定期或在消息处理成功后更新到Broker
            - 拉取失败会记录日志并等待重试，不会影响其他队列
            - 消费者停止时此循环会自动退出
            - 支持通过配置控制拉取频率
        """
        suggest_broker_id = 0
        while self._is_running:
            pq: ProcessQueue = self._get_or_create_process_queue(message_queue)
            if pq.need_flow_control():
                time.sleep(3.0)
                continue
            try:
                # 执行单次拉取操作
                pull_result = self._perform_single_pull(
                    message_queue, suggest_broker_id
                )

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
                    # 处理拉取到的消息
                    self._handle_pulled_messages(
                        message_queue, messages, next_begin_offset
                    )
                else:
                    self._stats["pull_requests"] += 1

                # 控制拉取频率 - 传入是否有消息的标志
                self._apply_pull_interval(len(messages) > 0)

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

                self._stats["pull_failures"] += 1

                # 拉取失败时等待一段时间再重试
                time.sleep(3.0)

    def _perform_single_pull(
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
        current_offset: int = self._get_or_initialize_offset(message_queue)

        # 拉取消息
        messages, next_begin_offset, next_suggest_id = self._pull_messages(
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

    def _filter_messages_by_tags(
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

    def _handle_pulled_messages(
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
        self._assigned_queues[message_queue] = next_begin_offset

        # 将消息添加到缓存中（用于解决并发偏移量问题）
        self._add_messages_to_cache(message_queue, messages)

        # 将消息按批次放入处理队列
        self._submit_messages_for_processing(message_queue, messages)

        # 更新统计信息
        message_count = len(messages)
        self._stats["pull_successes"] += 1
        self._stats["messages_consumed"] += message_count
        self._stats["pull_requests"] += 1

    def _submit_messages_for_processing(
        self, message_queue: MessageQueue, messages: list[MessageExt]
    ) -> None:
        """将消息按批次提交给处理队列。

        Args:
            message_queue: 消息队列
            messages: 要处理的消息列表
        """
        # 按照config.consume_batch_size分割消息批次
        batch_size = self._config.consume_batch_size
        message_count = len(messages)

        # 计算批次数量
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
            self._process_queue.put((batch_messages, message_queue))

    def _apply_pull_interval(self, has_messages: bool = True) -> None:
        """应用智能拉取间隔控制。

        根据上次拉取结果智能调整拉取间隔：
        - 如果上次拉取到了消息，立即继续拉取以提高消费速度
        - 如果上次拉取为空，则休眠配置的间隔时间以避免空轮询

        Args:
            has_messages: 上次拉取是否获取到消息，默认为True
        """
        # TODO: 如果msg_cache中消息体积太大，需要调整拉取间隔
        if self._config.pull_interval > 0:
            if has_messages:
                # 拉取到消息，不休眠继续拉取
                logger.debug(
                    "Messages pulled, continuing without interval",
                    extra={
                        "consumer_group": self._config.consumer_group,
                    },
                )
            else:
                # 拉取为空，休眠配置的间隔时间
                sleep_time: float = self._config.pull_interval / 1000.0
                logger.debug(
                    f"No messages pulled, sleeping for {sleep_time}s",
                    extra={
                        "consumer_group": self._config.consumer_group,
                        "sleep_time": sleep_time,
                    },
                )
                time.sleep(sleep_time)

    def _get_or_create_process_queue(self, queue: MessageQueue) -> ProcessQueue:
        """获取或创建指定队列的ProcessQueue"""
        with self._cache_lock:
            if queue not in self._msg_cache:
                self._msg_cache[queue] = ProcessQueue(
                    max_cache_count=self._config.max_cache_count_per_queue,
                    max_cache_size_mb=self._config.max_cache_size_per_queue,
                )
            return self._msg_cache[queue]

    def _add_messages_to_cache(
        self, queue: MessageQueue, messages: list[MessageExt]
    ) -> None:
        """
        将消息添加到ProcessQueue缓存中

        此方法用于将从Broker拉取的消息添加到ProcessQueue中，为后续消费做准备。
        ProcessQueue自动保持按queue_offset排序，并提供高效的插入、查询和统计功能。

        Args:
            queue (MessageQueue): 目标消息队列
            messages (list[MessageExt]): 要添加的消息列表，消息应包含有效的queue_offset

        Note:
            - 使用ProcessQueue内置的线程安全机制
            - 按queue_offset升序排列，方便后续按序消费
            - 自动过滤空消息列表，避免不必要的操作
            - 自动去重，避免重复缓存相同偏移量的消息
            - 自动检查缓存限制（数量和大小）

        Raises:
            无异常抛出，确保消息添加流程的稳定性

        See Also:
            _remove_messages_from_cache: 从缓存中移除已处理的消息
            _get_or_create_process_queue: 获取或创建ProcessQueue
            _is_message_cached: 检查消息是否已在缓存中
        """
        if not messages:
            return

        process_queue: ProcessQueue = self._get_or_create_process_queue(queue)
        _ = process_queue.add_batch_messages(messages)

    def _is_message_cached(self, queue: MessageQueue, queue_offset: int) -> bool:
        """检查指定偏移量的消息是否已在ProcessQueue缓存中。

        使用ProcessQueue的高效查找机制检查消息是否已存在，避免重复缓存。

        Args:
            queue: 消息队列
            queue_offset: 要检查的消息偏移量

        Returns:
            bool: True表示消息已存在，False表示不存在
        """
        with self._cache_lock:
            if queue not in self._msg_cache:
                return False

        # 使用ProcessQueue的contains_message方法
        process_queue = self._msg_cache[queue]
        return process_queue.contains_message(queue_offset)

    def _remove_messages_from_cache(
        self, queue: MessageQueue, messages: list[MessageExt]
    ) -> int | None:
        """
        从ProcessQueue缓存中移除已处理的消息，并返回当前队列的最小offset

        此方法用于从ProcessQueue中移除已经成功处理的消息，释放内存空间。
        ProcessQueue提供高效的移除操作，确保在大量消息缓存中仍能保持良好的性能。
        移除完成后直接返回当前缓存中最小消息的offset，避免额外的查询操作。

        Args:
            queue (MessageQueue): 目标消息队列
            messages (list[MessageExt]): 要移除的消息列表，消息应包含有效的queue_offset

        Returns:
            int | None: 移除完成后缓存中最小消息的offset，如果缓存为空则返回None

        Note:
            - 使用ProcessQueue内置的线程安全机制
            - ProcessQueue提供高效的remove_message操作
            - 只移除完全匹配的消息（queue_offset相同），避免误删
            - 自动过滤空消息列表，减少不必要的操作
            - 如果消息未找到，静默跳过，不影响其他消息的处理
            - 移除完成后直接返回最小offset，提高性能

        Performance:
            - 时间复杂度: O(m * log n)，其中m是要移除的消息数，n是缓存中的消息数
            - 空间复杂度: O(1)，额外空间仅用于临时变量

        Raises:
            无异常抛出，确保消息移除流程的稳定性

        See Also:
            _add_messages_to_cache: 向缓存中添加消息
            _update_offset_from_cache: 更新消费偏移量（独立方法）
            _get_or_create_process_queue: 获取或创建ProcessQueue
        """
        process_queue = self._get_or_create_process_queue(queue)

        if not messages:
            # 如果没有消息要移除，直接返回当前最小offset
            return process_queue.get_min_offset()

        _ = process_queue.remove_batch_messages(
            [x.queue_offset for x in messages if x.queue_offset is not None]
        )

        # 返回移除完成后当前缓存中的最小offset
        return process_queue.get_min_offset()

    def _update_offset_from_cache(self, queue: MessageQueue) -> None:
        """从ProcessQueue缓存中获取最小offset并更新到offset_store"""
        with self._cache_lock:
            if queue not in self._msg_cache:
                # ProcessQueue不存在，不需要更新
                return

        process_queue: ProcessQueue = self._msg_cache[queue]

        # 获取缓存中最小的offset
        min_offset: int | None = process_queue.get_min_offset()
        if min_offset is None:
            # 缓存为空，不需要更新
            return

        # 更新到offset_store
        try:
            self._offset_store.update_offset(queue, min_offset)
            logger.debug(
                f"Updated offset from cache: {min_offset}",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "topic": queue.topic,
                    "queue_id": queue.queue_id,
                    "offset": min_offset,
                    "cache_stats": process_queue.get_stats(),
                },
            )
        except Exception as e:
            logger.warning(
                f"Failed to update offset from cache: {e}",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "topic": queue.topic,
                    "queue_id": queue.queue_id,
                    "error": str(e),
                },
            )

    def _get_or_initialize_offset(self, message_queue: MessageQueue) -> int:
        """获取或初始化消费偏移量。

        如果本地缓存的偏移量为0（首次消费），则根据配置的消费策略
        从ConsumeFromWhereManager获取正确的初始偏移量。

        Args:
            message_queue (MessageQueue): 要获取偏移量的消息队列

        Returns:
            int: 消费偏移量

        Note:
            - 如果偏移量不为0，直接返回缓存的值
            - 如果偏移量为0，根据consume_from_where策略获取初始偏移量
            - 获取失败时使用默认偏移量0，确保消费流程不中断
        """
        current_offset: int = self._assigned_queues.get(message_queue, 0)

        # 如果current_offset为0（首次消费），则从_consume_from_where_manager中获取正确的初始偏移量
        if current_offset == 0:
            try:
                current_offset = self._consume_from_where_manager.get_consume_offset(
                    message_queue,
                    self._config.consume_from_where,
                    self._config.consume_timestamp
                    if hasattr(self._config, "consume_timestamp")
                    else 0,
                )
                # 更新本地缓存的偏移量
                self._assigned_queues[message_queue] = current_offset

                logger.info(
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
                logger.error(
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

    def _pull_messages(
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
            messages, next_offset, suggested_broker = consumer._pull_messages(
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
            self._stats["pull_requests"] += 1

            broker_info: tuple[str, bool] | None = (
                self._name_server_manager.get_broker_address_in_subscription(
                    message_queue.broker_name, suggest_id
                )
            )
            if not broker_info:
                raise ValueError(
                    f"Broker address not found for {message_queue.broker_name}"
                )

            commit_offset: int = self._offset_store.read_offset(
                message_queue, ReadOffsetType.READ_FROM_MEMORY
            )

            broker_address, is_master = broker_info

            # 使用BrokerManager拉取消息
            pool: ConnectionPool = self._broker_manager.must_connection_pool(
                broker_address
            )
            self._prepare_consumer_remote(pool)
            with pool.get_connection(usage="拉取消息") as conn:
                result: PullMessageResult = BrokerClient(conn).pull_message(
                    consumer_group=self._config.consumer_group,
                    topic=message_queue.topic,
                    queue_id=message_queue.queue_id,
                    queue_offset=offset,
                    max_msg_nums=self._config.pull_batch_size,
                    sys_flag=self._build_sys_flag(
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

    def _build_sys_flag(self, commit_offset: bool):
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

    # ==================== 内部方法：消息处理 ====================

    def _start_consume_tasks(self) -> None:
        """
        启动消息处理任务
        """
        if not self._consume_executor:
            return

        # 启动多个消费任务
        for _ in range(self._config.consume_thread_max):
            self._consume_executor.submit(self._consume_messages_loop)

    def _consume_messages_loop(self) -> None:
        """
        持续处理消息的循环
        """
        while self._is_running:
            try:
                # 从处理队列获取消息
                try:
                    messages: list[MessageExt]
                    message_queue: MessageQueue
                    messages, message_queue = self._process_queue.get(timeout=1.0)
                except queue.Empty:
                    continue

                # 处理消息
                start_time: float = time.time()
                success: bool = self._consume_message(messages, message_queue)
                duration: float = time.time() - start_time

                self._stats["consume_duration_total"] += duration

                if not success:
                    self._stats["messages_failed"] += len(messages)

                # 更新偏移量
                if success:
                    try:
                        # 从缓存中移除已处理的消息，并获取当前最小offset
                        min_offset = self._remove_messages_from_cache(
                            message_queue, messages
                        )

                        # 直接更新最小offset到offset_store，避免重复查询
                        if min_offset is not None:
                            try:
                                self._offset_store.update_offset(
                                    message_queue, min_offset
                                )
                                logger.debug(
                                    f"Updated offset from cache: {min_offset}",
                                    extra={
                                        "consumer_group": self._config.consumer_group,
                                        "topic": message_queue.topic,
                                        "queue_id": message_queue.queue_id,
                                        "offset": min_offset,
                                        "cache_stats": self._msg_cache.get(
                                            message_queue, ProcessQueue()
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
                    for message in messages:
                        self._send_back_message(message_queue, message)

            except Exception as e:
                logger.error(
                    f"Error in consume messages loop: {e}",
                    extra={
                        "consumer_group": self._config.consumer_group,
                        "error": str(e),
                    },
                    exc_info=True,
                )

    def _wait_for_processing_completion(self) -> None:
        """
        等待正在处理的消息完成
        """
        if not self._consume_executor:
            return

        try:
            # 等待所有任务完成或超时
            self._consume_executor.shutdown(wait=True)

            # 等待处理队列为空
            timeout: int = 30  # 30秒超时
            start_time: float = time.time()
            while (
                not self._process_queue.empty() and (time.time() - start_time) < timeout
            ):
                time.sleep(0.1)

        except Exception as e:
            logger.warning(
                f"Error waiting for processing completion: {e}",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "error": str(e),
                },
            )

    def _send_back_message(
        self, message_queue: MessageQueue, message: MessageExt
    ) -> None:
        """
        将消息发送回broker
        """
        broker_addr = self._name_server_manager.get_broker_address(
            message_queue.broker_name
        )
        if not broker_addr:
            logger.error(
                "Failed to get broker address ",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "broker_name": message_queue.broker_name,
                },
            )
            return
        pool: ConnectionPool = self._broker_manager.must_connection_pool(broker_addr)
        with pool.get_connection(usage="发送消息回broker") as conn:
            BrokerClient(conn).consumer_send_msg_back(
                message,
                0,
                self._config.consumer_group,
                self._config.max_reconsume_times,
            )

    # ==================== 内部方法：重平衡任务 ====================

    def _start_rebalance_task(self) -> None:
        """
        启动定期重平衡任务
        """
        self._rebalance_thread = threading.Thread(
            target=self._rebalance_loop,
            name=f"{self._config.consumer_group}-rebalance-thread",
            daemon=True,
        )
        self._rebalance_thread.start()

    def _rebalance_loop(self) -> None:
        """
        定期重平衡循环
        """
        while self._is_running:
            try:
                # 使用Event.wait()替代time.sleep()
                if self._rebalance_event.wait(timeout=self._rebalance_interval):
                    # Event被触发，检查是否需要退出
                    if not self._is_running:
                        break
                    # 重置事件状态
                    self._rebalance_event.clear()

                if self._is_running:
                    self._do_rebalance()

            except Exception as e:
                logger.error(
                    f"Error in rebalance loop: {e}",
                    extra={
                        "consumer_group": self._config.consumer_group,
                        "error": str(e),
                    },
                    exc_info=True,
                )

    def _on_notify_consumer_ids_changed(
        self, remoting_cmd: RemotingCommand, remote_addr: tuple[str, int]
    ) -> None:
        logger.info("Received notification of consumer IDs changed")
        self._do_rebalance()

    def _find_consumer_list(self, topic: str) -> list[str]:
        """
        查找消费者列表

        Args:
            topic: 主题名称

        Returns:
            消费者列表
        """

        addresses: list[str] = self._name_server_manager.get_all_broker_addresses(topic)
        if not addresses:
            logger.warning(
                "No broker addresses found for topic", extra={"topic": topic}
            )
            return []

        pool: ConnectionPool = self._broker_manager.must_connection_pool(addresses[0])
        with pool.get_connection(usage="查找消费者列表") as conn:
            return BrokerClient(conn).get_consumers_by_group(
                self._config.consumer_group
            )

    # ==================== 内部方法：资源清理 ====================

    def _cleanup_on_start_failure(self) -> None:
        """启动失败时的资源清理操作。

        当消费者启动过程中发生异常时，调用此方法清理已分配的资源，
        确保消费者状态一致，避免资源泄漏。

        清理流程：
        1. 关闭线程池（拉取线程池、消费线程池）
        2. 停止核心组件（NameServer、BrokerManager、偏移量存储）
        3. 清理内存资源和队列

        Args:
            None

        Returns:
            None

        Raises:
            None: 此方法会捕获所有异常并记录日志

        Note:
            - 仅在启动失败时调用，正常关闭使用shutdown()方法
            - 清理过程中的异常不会中断清理流程
            - 确保消费者处于完全停止状态
            - 所有清理操作都会记录详细日志
        """
        try:
            self._shutdown_thread_pools()
            self._cleanup_resources()
        except Exception as e:
            logger.error(
                f"Error during startup failure cleanup: {e}",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "error": str(e),
                },
                exc_info=True,
            )

    def _shutdown_thread_pools(self) -> None:
        """
        关闭线程池和专用线程
        """
        try:
            # 关闭线程池
            if self._pull_executor:
                self._pull_executor.shutdown(wait=False)
                self._pull_executor = None

            if self._consume_executor:
                self._consume_executor.shutdown(wait=False)
                self._consume_executor = None

            # 等待专用线程结束
            self._rebalance_event.set()  # 唤醒重平衡线程

            # 等待线程结束
            threads_to_join: list[threading.Thread] = []
            if self._rebalance_thread and self._rebalance_thread.is_alive():
                threads_to_join.append(self._rebalance_thread)

            # 并发等待所有线程结束
            for thread in threads_to_join:
                try:
                    thread.join(timeout=5.0)
                    if thread.is_alive():
                        logger.warning(
                            f"Thread did not stop gracefully: {thread.name}",
                            extra={
                                "consumer_group": self._config.consumer_group,
                                "thread_name": thread.name,
                            },
                        )
                except Exception as e:
                    logger.error(
                        f"Error joining thread {thread.name}: {e}",
                        extra={
                            "consumer_group": self._config.consumer_group,
                            "thread_name": thread.name,
                            "error": str(e),
                        },
                    )

        except Exception as e:
            logger.warning(
                f"Error shutting down thread pools and threads: {e}",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "error": str(e),
                },
            )

    def _cleanup_resources(self) -> None:
        """
        清理资源
        """

        # 清理处理队列
        if hasattr(self, "_process_queue"):
            while not self._process_queue.empty():
                try:
                    self._process_queue.get_nowait()
                except queue.Empty:
                    break

        # 清理ProcessQueue消息缓存
        if hasattr(self, "_msg_cache"):
            for process_queue in self._msg_cache.values():
                process_queue.clear()
            self._msg_cache.clear()

        # 清理状态
        self._pull_tasks.clear()
        self._assigned_queues.clear()

    # ==================== 统计和监控方法 ====================

    def _get_final_stats(self) -> dict[str, Any]:
        """
        获取最终统计信息

        Returns:
            dict: 统计信息字典
        """
        uptime: float = time.time() - self._stats.get("start_time", time.time())
        messages_consumed: int = self._stats["messages_consumed"]
        pull_requests: int = self._stats["pull_requests"]
        route_refresh_count: int = self._stats.get("route_refresh_count", 0)

        # 计算路由刷新成功率
        route_refresh_success_rate: float = 0.0
        if route_refresh_count > 0:
            route_refresh_success_rate = (
                self._stats.get("route_refresh_success_count", 0) / route_refresh_count
            )

        return {
            **self._stats,
            "uptime_seconds": uptime,
            "assigned_queues": len(self._assigned_queues),
            "avg_consume_duration": (
                self._stats["consume_duration_total"] / max(messages_consumed, 1)
            ),
            "pull_success_rate": (
                self._stats["pull_successes"] / max(pull_requests, 1)
            ),
            "consume_success_rate": (
                (messages_consumed - self._stats["messages_failed"])
                / max(messages_consumed, 1)
            ),
            # 路由刷新相关统计
            "route_refresh_success_rate": route_refresh_success_rate,
            "route_refresh_avg_interval": (uptime / max(route_refresh_count, 1))
            if route_refresh_count > 0
            else 0.0,
            # 重平衡相关统计
            "rebalance_success_rate": (
                self._stats.get("rebalance_success_count", 0)
                / max(self._stats.get("rebalance_count", 1), 1)
            ),
            "rebalance_avg_interval": (
                uptime / max(self._stats.get("rebalance_count", 1), 1)
            )
            if self._stats.get("rebalance_count", 0) > 0
            else 0.0,
            "rebalance_skipped_rate": (
                self._stats.get("rebalance_skipped_count", 0)
                / max(
                    self._stats.get("rebalance_count", 1)
                    + self._stats.get("rebalance_skipped_count", 1),
                    1,
                )
            ),
        }

    def get_stats(self) -> dict[str, Any]:
        """获取消费者的实时统计信息。

        返回包含消费者运行状态、性能指标和资源使用情况的详细统计数据。

        Returns:
            dict[str, Any]: 统计信息字典，包含以下字段：
                - messages_consumed (int): 已消费的消息总数
                - messages_failed (int): 消费失败的消息总数
                - pull_requests (int): 拉取请求总数
                - pull_successes (int): 拉取成功的次数
                - pull_failures (int): 拉取失败的次数
                - consume_duration_total (float): 总消费耗时（秒）
                - start_time (float): 启动时间戳
                - heartbeat_success_count (int): 心跳成功次数
                - heartbeat_failure_count (int): 心跳失败次数
                - uptime_seconds (float): 运行时长（秒）
                - assigned_queues (int): 当前分配的队列数量
                - avg_consume_duration (float): 平均消费耗时（秒）
                - pull_success_rate (float): 拉取成功率（0-1之间）
                - consume_success_rate (float): 消费成功率（0-1之间）

        Raises:
            None: 此方法不会抛出异常

        Note:
            - 统计数据在消费者生命周期内持续累积
            - 平均值基于实际消费次数计算
            - 成功率基于实际操作次数计算
            - 可用于监控消费者健康状态和性能调优

        Example:
            >>> stats = consumer.get_stats()
            >>> print(f"消费成功率: {stats['consume_success_rate']:.2%}")
            >>> print(f"平均消费耗时: {stats['avg_consume_duration']:.3f}秒")
        """
        return self._get_final_stats()

    # ==================== 其他方法 ====================
    def _prepare_consumer_remote(self, pool: ConnectionPool) -> None:
        pool.register_request_processor(
            RequestCode.NOTIFY_CONSUMER_IDS_CHANGED,
            self._on_notify_consumer_ids_changed,
        )
        pool.register_request_processor(
            RequestCode.CONSUME_MESSAGE_DIRECTLY,
            self._on_notify_consume_message_directly,
        )

    def _on_notify_consume_message_directly(
        self, command: RemotingCommand, _addr: tuple[str, int]
    ) -> RemotingCommand:
        header: ConsumeMessageDirectlyHeader = ConsumeMessageDirectlyHeader.decode(
            command.ext_fields
        )
        if header.client_id == self._config.client_id:
            return self._on_notify_consume_message_directly_internal(header, command)
        else:
            return (
                RemotingCommandBuilder(ResponseCode.ERROR)
                .with_remark(f"Can't find client ID {header.client_id}")
                .build()
            )

    def _on_notify_consume_message_directly_internal(
        self, header: ConsumeMessageDirectlyHeader, command: RemotingCommand
    ) -> RemotingCommand:
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

        if self._consume_message(msgs, q):
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
