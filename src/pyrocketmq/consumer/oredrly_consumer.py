import concurrent.futures
import queue
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor

# pyrocketmq导入
from datetime import datetime
from typing import Any

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


class OrderlyConsumer(BaseConsumer):
    def __init__(self, config: ConsumerConfig) -> None:
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
        # 顺序消息消费者每个message_queue一个消费线程
        self._consume_tasks: dict[MessageQueue, list[Future[None]]] = {}
        self._consume_executor: ThreadPoolExecutor | None = None  # 用于管理消费任务
        self._pull_executor: ThreadPoolExecutor | None = None

        # 消息处理队列管理 - 每个MessageQueue独立处理队列
        # 顺序消费要求每个MessageQueue有独立的处理队列，避免不同队列的消息交叉处理
        self._process_queues: dict[MessageQueue, queue.Queue[list[MessageExt]]] = {}
        self._process_queue_lock = threading.Lock()  # 用于保护_process_queues字典

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

        # 线程停止事件 - 用于优雅关闭拉取和消费循环
        self._pull_stop_events: dict[MessageQueue, threading.Event] = {}
        self._consume_stop_events: dict[MessageQueue, threading.Event] = {}
        self._stop_events_lock = threading.Lock()  # 保护停止事件字典

        # 重平衡重入保护
        self._rebalance_lock: threading.RLock = threading.RLock()  # 重平衡锁，防止重入

        logger.info(
            "OrderlyConsumer initialized",
            extra={
                "consumer_group": self._config.consumer_group,
                "message_model": self._config.message_model,
                "consume_thread_max": self._config.consume_thread_max,
                "pull_batch_size": self._config.pull_batch_size,
            },
        )

    def start(self) -> None:
        """启动顺序消费者。

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
                    "Starting OrderlyConsumer",
                    extra={
                        "consumer_group": self._config.consumer_group,
                        "namesrv_addr": self._config.namesrv_addr,
                    },
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

                # 初始化每个message_queue的锁管理字段
                # 每个queue的锁都有时间限制，支持is_lock_expired方法
                self._queue_locks: dict[MessageQueue, threading.RLock] = {}
                self._queue_lock_timestamps: dict[MessageQueue, float] = {}

                self._stats["start_time"] = time.time()

                logger.info(
                    "OrderlyConsumer started successfully",
                    extra={
                        "consumer_group": self._config.consumer_group,
                        "assigned_queues": len(self._assigned_queues),
                        "consume_threads": self._config.consume_thread_max,
                        "pull_threads": min(self._config.consume_thread_max, 10),
                    },
                )

            except Exception as e:
                logger.error(
                    f"Failed to start OrderlyConsumer: {e}",
                    extra={
                        "consumer_group": self._config.consumer_group,
                        "error": str(e),
                    },
                    exc_info=True,
                )
                self._cleanup_on_start_failure()
                raise ConsumerStartError(
                    "Failed to start OrderlyConsumer",
                    cause=e,
                    context={"consumer_group": self._config.consumer_group},
                ) from e

    def shutdown(self) -> None:
        """优雅停止顺序消费者。

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
                    "Shutting down OrderlyConsumer",
                    extra={"consumer_group": self._config.consumer_group},
                )

                self._is_running = False

                # 先设置Event以唤醒可能阻塞的线程
                self._rebalance_event.set()

                # 停止拉取任务
                self._stop_pull_tasks()

                # 停止消费任务
                self._stop_consume_tasks()

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
                    "OrderlyConsumer shutdown completed",
                    extra={
                        "consumer_group": self._config.consumer_group,
                        "final_stats": self._get_final_stats(),
                    },
                )

            except Exception as e:
                logger.error(
                    f"Error during OrderlyConsumer shutdown: {e}",
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

    # ==================== 内部方法：重平衡管理 ====================

    def _pre_rebalance_check(self) -> bool:
        """执行重平衡前置检查。

        检查是否可以执行重平衡操作，包括锁获取和订阅状态检查。

        Returns:
            bool: 如果可以执行重平衡返回True，否则返回False

        Raises:
            None: 此方法不会抛出异常
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
            return False

        # 检查是否有订阅的Topic
        topics: set[str] = self._subscription_manager.get_topics()
        if not topics:
            logger.debug("No topics subscribed, skipping rebalance")
            self._rebalance_lock.release()
            return False

        return True

    def _collect_and_allocate_queues(self) -> list[MessageQueue]:
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
                # 更新Topic路由信息
                _ = self._update_route_info(topic)

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

                # 执行队列分配
                topic_allocated_queues = self._allocate_queues(topic, all_queues)
                allocated_queues.extend(topic_allocated_queues)

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

    def _finalize_rebalance(self, total_topics: int, total_queues: int) -> None:
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
        self._stats["rebalance_success_count"] += 1

        logger.info(
            "Rebalance completed",
            extra={
                "consumer_group": self._config.consumer_group,
                "total_topics": total_topics,
                "assigned_queues": total_queues,
                "success_count": self._stats["rebalance_success_count"],
            },
        )

    def _do_rebalance(self) -> None:
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
        if not self._pre_rebalance_check():
            return

        try:
            logger.debug(
                "Starting rebalance",
                extra={"consumer_group": self._config.consumer_group},
            )

            # 更新统计信息
            self._stats["rebalance_count"] += 1

            # 收集所有可用队列并执行分配
            allocated_queues = self._collect_and_allocate_queues()

            # 更新分配的队列
            if allocated_queues:
                self._update_assigned_queues(allocated_queues)

            # 完成重平衡处理
            self._finalize_rebalance(
                len(self._subscription_manager.get_topics()), len(allocated_queues)
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
        with self._lock:
            old_queues: set[MessageQueue] = set(self._assigned_queues.keys())
            new_queue_set: set[MessageQueue] = set(new_queues)

            # 停止不再分配的队列的拉取任务和消费任务
            removed_queues: set[MessageQueue] = old_queues - new_queue_set
            for q in removed_queues:
                if q in self._pull_tasks:
                    future: Future[None] | None = self._pull_tasks.pop(q)
                    if future and not future.done():
                        future.cancel()
                _ = self._assigned_queues.pop(q, None)

                # 停止并移除该队列的消费任务
                if q in self._consume_tasks:
                    consume_futures = self._consume_tasks.pop(q)
                    for future in consume_futures:
                        if future and not future.done():
                            future.cancel()

                # 清理队列锁
                if q in self._queue_locks:
                    del self._queue_locks[q]
                if q in self._queue_lock_timestamps:
                    del self._queue_lock_timestamps[q]

            # 启动新分配队列的拉取任务
            added_queues: set[MessageQueue] = new_queue_set - old_queues
            for q in added_queues:
                self._assigned_queues[q] = 0  # 初始化偏移量为0，后续会更新

                # 为新队列创建锁
                self._queue_locks[q] = threading.RLock()
                # 初始化锁时间戳
                self._queue_lock_timestamps = {}
                self._queue_lock_timestamps[q] = time.time() * 1000  # 转换为毫秒

                # 为新队列初始化消费任务列表
                self._consume_tasks[q] = []

            # 如果消费者正在运行，启动新队列的拉取任务和消费任务
            if self._is_running and added_queues:
                self._start_pull_tasks_for_queues(added_queues)
                self._start_consume_tasks_for_queues(added_queues)

    def _get_queue_lock(self, message_queue: MessageQueue) -> threading.RLock:
        """
        获取指定消息队列的锁

        Args:
            message_queue: 消息队列

        Returns:
            该队列的RLock锁对象
        """
        if not hasattr(self, "_queue_locks"):
            self._queue_locks = {}

        if message_queue not in self._queue_locks:
            self._queue_locks[message_queue] = threading.RLock()
            # 初始化锁时间戳
            if not hasattr(self, "_queue_lock_timestamps"):
                self._queue_lock_timestamps = {}
            self._queue_lock_timestamps[message_queue] = (
                time.time() * 1000
            )  # 转换为毫秒

        return self._queue_locks[message_queue]

    def _is_locked(self, message_queue: MessageQueue) -> bool:
        """
        检查指定队列是否已锁定

        Args:
            message_queue: 消息队列

        Returns:
            True如果队列已锁定，False如果队列未锁定
        """
        if message_queue not in self._queue_locks:
            return False

        return self._queue_locks[message_queue].locked()

    def _is_lock_expired(self, message_queue: MessageQueue) -> bool:
        """
        检查指定队列的锁是否已过期

        Args:
            message_queue: 消息队列

        Returns:
            True如果锁已过期，False如果锁仍然有效
        """
        if not hasattr(self, "_queue_lock_timestamps"):
            return True  # 如果没有时间戳记录，认为锁过期

        if message_queue not in self._queue_lock_timestamps:
            return True  # 如果没有时间戳记录，认为锁过期

        current_time = time.time() * 1000  # 转换为毫秒
        lock_time = self._queue_lock_timestamps[message_queue]

        return (current_time - lock_time) > self._config.lock_expire_time

    def _lock_remote_queue(self, message_queue: MessageQueue) -> bool:
        """
        尝试远程锁定指定队列

        Args:
            message_queue: 消息队列

        Returns:
            True如果锁定成功，False如果锁定失败
        """
        try:
            # 获取队列对应的Broker连接
            broker_address: str | None = self._name_server_manager.get_broker_address(
                message_queue.broker_name
            )
            if not broker_address:
                logger.warning(f"Broker address not found for queue: {message_queue}")
                return False

            connection_pool = self._broker_manager.must_connection_pool(broker_address)

            # 创建broker客户端
            with connection_pool.get_connection() as conn:
                broker_client = BrokerClient(conn)

                # 尝试锁定队列
                locked_queues = broker_client.lock_batch_mq(
                    consumer_group=self._config.consumer_group,
                    client_id=self._config.client_id,
                    mqs=[message_queue],
                )

                # 检查锁定是否成功
                if locked_queues and len(locked_queues) > 0:
                    logger.debug(
                        f"Successfully locked remote queue: {message_queue}",
                        extra={
                            "consumer_group": self._config.consumer_group,
                            "client_id": self._config.client_id,
                            "queue": str(message_queue),
                            "operation": "lock_remote_queue",
                        },
                    )
                    return True
                else:
                    logger.warning(
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
            logger.error(
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

    def _unlock_remote_queue(self, message_queue: MessageQueue) -> bool:
        """
        尝试远程解锁指定队列

        Args:
            message_queue: 消息队列

        Returns:
            True如果解锁成功，False如果解锁失败
        """
        try:
            # 获取队列对应的Broker连接
            broker_address: str | None = self._name_server_manager.get_broker_address(
                message_queue.broker_name
            )
            if not broker_address:
                logger.warning(f"Broker address not found for queue: {message_queue}")
                return False

            connection_pool = self._broker_manager.must_connection_pool(broker_address)

            with connection_pool.get_connection() as conn:
                broker_client = BrokerClient(conn)

                # 尝试解锁队列
                broker_client.unlock_batch_mq(
                    consumer_group=self._config.consumer_group,
                    client_id=self._config.client_id,
                    mqs=[message_queue],
                )

                logger.debug(
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
            logger.error(
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

    def _update_lock_timestamp(self, message_queue: MessageQueue) -> None:
        """
        更新指定队列锁的时间戳

        Args:
            message_queue: 消息队列
        """
        if not hasattr(self, "_queue_lock_timestamps"):
            self._queue_lock_timestamps = {}

        self._queue_lock_timestamps[message_queue] = time.time() * 1000  # 转换为毫秒

    def _trigger_rebalance(self) -> None:
        """
        触发重平衡
        """
        if self._is_running:
            # 唤醒重平衡循环，使其立即执行重平衡
            self._rebalance_event.set()

    # ==================== 内部方法：消息拉取 ====================

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
                # 为每个队列创建停止事件
                with self._stop_events_lock:
                    pull_stop_event = threading.Event()
                    consume_stop_event = threading.Event()
                    self._pull_stop_events[q] = pull_stop_event
                    self._consume_stop_events[q] = consume_stop_event

                # 启动拉取任务，传入停止事件
                future: Future[None] = self._pull_executor.submit(
                    self._pull_messages_loop, q, pull_stop_event
                )
                self._pull_tasks[q] = future

    def _stop_pull_tasks(self) -> None:
        """
        停止所有消息拉取任务 - 使用停止事件优雅关闭
        """
        if not self._pull_tasks:
            return

        # 首先设置所有停止事件
        with self._stop_events_lock:
            for message_queue in self._pull_tasks.keys():
                # 设置拉取停止事件
                if message_queue in self._pull_stop_events:
                    self._pull_stop_events[message_queue].set()
                # 设置消费停止事件
                if message_queue in self._consume_stop_events:
                    self._consume_stop_events[message_queue].set()

        # 然后取消Future任务
        for _, future in self._pull_tasks.items():
            if future and not future.done():
                future.cancel()

        self._pull_tasks.clear()

        # 等待一段时间让线程自然退出
        time.sleep(0.1)

    def _stop_consume_tasks(self) -> None:
        """
        停止所有消息消费任务 - 使用停止事件优雅关闭
        """
        if not self._consume_tasks:
            return

        # 首先设置所有停止事件
        with self._stop_events_lock:
            for message_queue in self._consume_tasks.keys():
                if message_queue in self._consume_stop_events:
                    self._consume_stop_events[message_queue].set()

        # 然后取消Future任务
        for message_queue, futures in self._consume_tasks.items():
            for future in futures:
                if future and not future.done():
                    future.cancel()

        self._consume_tasks.clear()

        # 等待一段时间让线程自然退出
        time.sleep(0.1)

    def _pull_messages_loop(
        self,
        message_queue: MessageQueue,
        pull_stop_event: threading.Event,
    ) -> None:
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
            pull_stop_event (threading.Event): 拉取线程停止事件

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
            - 支持通过停止事件优雅关闭
        """
        suggest_broker_id = 0
        while self._is_running and not pull_stop_event.is_set():
            pq: ProcessQueue = self._get_or_create_process_queue(message_queue)
            if pq.need_flow_control():
                # 使用可中断的等待，检查停止事件
                if pull_stop_event.wait(timeout=3.0):
                    break
                continue

            try:
                # 执行单次拉取操作
                pull_result: tuple[list[MessageExt], int, int] | None = (
                    self._perform_single_pull(message_queue, suggest_broker_id)
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

                # 控制拉取频率 - 传入是否有消息的标志，使用可中断等待
                self._apply_pull_interval(len(messages) > 0, pull_stop_event)

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

                # 拉取失败时等待一段时间再重试，使用可中断等待
                if pull_stop_event.wait(timeout=3.0):
                    break

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
        """将消息按批次提交给对应MessageQueue的处理队列。

        Args:
            message_queue: 消息队列
            messages: 要处理的消息列表
        """
        # 获取该MessageQueue专属的处理队列
        consume_queue = self._get_or_create_consume_queue(message_queue)

        # 按照config.consume_batch_size分割消息批次
        batch_size = self._config.consume_batch_size
        message_count = len(messages)

        # 计算批次数量
        batch_count = (message_count + batch_size - 1) // batch_size

        logger.debug(
            f"Splitting {message_count} messages into {batch_count} batches for queue {message_queue}",
            extra={
                "consumer_group": self._config.consumer_group,
                "topic": message_queue.topic,
                "queue_id": message_queue.queue_id,
                "message_count": message_count,
                "batch_size": batch_size,
                "batch_count": batch_count,
            },
        )

        # 将消息按批次放入对应MessageQueue的处理队列
        for i in range(0, message_count, batch_size):
            batch_messages = messages[i : i + batch_size]
            consume_queue.put(batch_messages)

    def _apply_pull_interval(
        self, has_messages: bool = True, stop_event: threading.Event | None = None
    ) -> None:
        """应用智能拉取间隔控制。

        根据上次拉取结果智能调整拉取间隔：
        - 如果上次拉取到了消息，立即继续拉取以提高消费速度
        - 如果上次拉取为空，则休眠配置的间隔时间以避免空轮询

        Args:
            has_messages: 上次拉取是否获取到消息，默认为True
            stop_event: 停止事件，用于支持优雅关闭，默认为None
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
                # 拉取为空，休眠配置的间隔时间，使用可中断等待
                sleep_time: float = self._config.pull_interval / 1000.0
                logger.debug(
                    f"No messages pulled, sleeping for {sleep_time}s",
                    extra={
                        "consumer_group": self._config.consumer_group,
                        "sleep_time": sleep_time,
                    },
                )
                if stop_event:
                    stop_event.wait(timeout=sleep_time)
                else:
                    time.sleep(sleep_time)

    def _get_or_create_process_queue(self, queue: MessageQueue) -> ProcessQueue:
        """获取或创建指定队列的ProcessQueue（消息缓存队列）"""
        with self._cache_lock:
            if queue not in self._msg_cache:
                self._msg_cache[queue] = ProcessQueue(
                    max_cache_count=self._config.max_cache_count_per_queue,
                    max_cache_size_mb=self._config.max_cache_size_per_queue,
                )
            return self._msg_cache[queue]

    def _get_or_create_consume_queue(
        self, mq: MessageQueue
    ) -> queue.Queue[list[MessageExt]]:
        """获取或创建指定队列的消费处理队列"""
        with self._process_queue_lock:
            if mq not in self._process_queues:
                self._process_queues[mq] = queue.Queue()
            return self._process_queues[mq]

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

    def _start_consume_tasks_for_queues(self, queues: set[MessageQueue]) -> None:
        """
        为指定的队列集合启动消费任务

        Args:
            queues: 需要启动消费任务的队列集合
        """
        if not self._consume_executor:
            return

        with self._process_queue_lock:
            for message_queue in queues:
                # 获取该队列的停止事件
                with self._stop_events_lock:
                    if message_queue in self._consume_stop_events:
                        consume_stop_event = self._consume_stop_events[message_queue]
                    else:
                        # 如果事件不存在，创建一个（虽然正常情况下应该已经存在）
                        consume_stop_event = threading.Event()
                        self._consume_stop_events[message_queue] = consume_stop_event

                # 启动消费任务，传入停止事件
                future = self._consume_executor.submit(
                    self._consume_messages_loop, message_queue, consume_stop_event
                )
                if message_queue not in self._consume_tasks:
                    self._consume_tasks[message_queue] = []
                self._consume_tasks[message_queue].append(future)

    def _consume_messages_loop(
        self, message_queue: MessageQueue, stop_event: threading.Event
    ) -> None:
        """
        持续处理指定MessageQueue的消息消费循环。

        这是顺序消费者核心的消息处理循环，每个MessageQueue运行在独立的线程中。
        该循环负责从对应MessageQueue的处理队列中获取消息并调用用户注册的消息监听器进行处理。

        Args:
            message_queue: 要处理的指定MessageQueue
            stop_event: 消费线程停止事件

        主要功能:
            - 从MessageQueue专属的处理队列阻塞式获取消息批次
            - 调用用户消息监听器进行业务处理，保证同一队列内的消息顺序性
            - 根据处理结果执行成功/失败后的处理逻辑
            - 更新消费统计信息
            - 处理重试机制和异常情况

        处理流程:
            1. 从MessageQueue专属处理队列获取消息批次 (_get_messages_from_queue)
            2. 调用消息监听器处理消息 (_process_messages_with_timing)
            3. 根据处理结果执行后续操作：
               - 成功: 更新偏移量，清理处理队列
               - 失败: 发送回broker进行重试
            4. 更新消费统计信息 (_update_consume_stats)
            5. 对发送回broker失败的消息进行延迟重试

        顺序性保证:
            - 每个MessageQueue有独立的处理队列和消费线程
            - 确保同一队列内的消息严格按照偏移量顺序处理
            - 避免不同队列之间的消息交叉处理

        重试机制:
            - 消费失败的消息会尝试发送回broker进行重试
            - 发送回broker失败的消息会在本地延迟5秒后重试
            - 避免因broker连接问题导致的消息丢失

        线程模型:
            - 每个MessageQueue分配一个独立的消费线程
            - 不同队列的消费者并行运行，互不干扰
            - 同一队列内的消息严格串行处理

        异常处理:
            - 捕获所有异常，确保单个消息处理失败不影响整个循环
            - 记录详细的错误日志和异常堆栈信息
            - 保持循环继续运行，确保消费者服务可用

        Note:
            - 该方法是消费者消息处理的核心逻辑，不应被外部调用
            - 循环会在消费者关闭时(_is_running=False)自动退出
            - 支持通过停止事件优雅关闭
            - 消费延迟主要取决于消息处理时间和队列深度
            - 统计信息用于监控消费性能和健康状态
        """
        logger.info(
            f"Starting consume message loop for queue {message_queue}",
            extra={
                "consumer_group": self._config.consumer_group,
                "topic": message_queue.topic,
                "queue_id": message_queue.queue_id,
            },
        )

        # 获取该MessageQueue专属的处理队列
        consume_queue: queue.Queue[list[MessageExt]] = (
            self._get_or_create_consume_queue(message_queue)
        )

        while self._is_running and not stop_event.is_set():
            try:
                # 从MessageQueue专属的处理队列获取消息批次
                messages: list[MessageExt] | None = self._get_messages_from_queue(
                    consume_queue, stop_event
                )
                if messages is None:
                    continue

                while messages and self._is_running and not stop_event.is_set():
                    # 处理消息
                    success, duration = self._process_messages_with_timing(
                        messages, message_queue
                    )

                    # 根据处理结果进行后续处理
                    if success:
                        self._handle_successful_consume(messages, message_queue)
                        messages = []
                    else:
                        messages = self._handle_failed_consume(messages, message_queue)
                        if messages:
                            # 处理失败重试时，使用可中断等待
                            if stop_event.wait(timeout=5.0):
                                break

                    # 更新统计信息
                    self._update_consume_stats(success, duration, len(messages))

            except Exception as e:
                logger.error(
                    f"Error in consume messages loop for queue {message_queue}: {e}",
                    extra={
                        "consumer_group": self._config.consumer_group,
                        "topic": message_queue.topic,
                        "queue_id": message_queue.queue_id,
                        "error": str(e),
                    },
                    exc_info=True,
                )

    def _get_messages_from_queue(
        self,
        consume_queue: queue.Queue[list[MessageExt]],
        stop_event: threading.Event | None = None,
    ) -> list[MessageExt] | None:
        """
        从MessageQueue专属的处理队列获取消息

        Args:
            consume_queue: 指定MessageQueue的消费处理队列
            stop_event: 停止事件，用于支持优雅关闭，默认为None

        Returns:
            消息列表，如果队列为空则返回None
        """
        try:
            if stop_event:
                # 使用可中断的超时获取，支持优雅关闭
                while not stop_event.is_set():
                    try:
                        messages = consume_queue.get(timeout=0.1)
                        return messages
                    except queue.Empty:
                        continue
                # 收到停止信号，返回None
                return None
            else:
                # 传统方式，固定超时
                messages = consume_queue.get(timeout=1.0)
                return messages
        except queue.Empty:
            return None

    def _process_messages_with_timing(
        self, messages: list[MessageExt], message_queue: MessageQueue
    ) -> tuple[bool, float]:
        """
        处理消息并计时

        Args:
            messages: 要处理的消息列表
            message_queue: 消息队列

        Returns:
            消费结果，包含成功状态和耗时
        """
        # 首先尝试远程锁定队列，确保分布式环境下的顺序性
        if not self._lock_remote_queue(message_queue):
            logger.warning(
                f"Failed to acquire remote lock for queue {message_queue}, skipping message processing",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "client_id": self._config.client_id,
                    "queue": str(message_queue),
                    "operation": "process_messages_with_timing",
                },
            )
            return False, 0.0  # 远程锁定失败，不处理消息

        # 获取队列锁，确保同一队列的消息顺序处理
        queue_lock = self._get_queue_lock(message_queue)
        # 更新锁时间戳，表示锁被活跃使用
        self._update_lock_timestamp(message_queue)

        with queue_lock:
            start_time: float = time.time()
            success: bool = self._concurrent_consume_message(messages, message_queue)
            duration: float = time.time() - start_time

            return success, duration

    def _handle_successful_consume(
        self, messages: list[MessageExt], message_queue: MessageQueue
    ) -> None:
        """
        处理成功消费的消息

        Args:
            messages: 成功消费的消息列表
            message_queue: 消息队列
        """
        try:
            # 从缓存中移除已处理的消息，并获取当前最小offset
            min_offset = self._remove_messages_from_cache(message_queue, messages)

            # 直接更新最小offset到offset_store，避免重复查询
            if min_offset is not None:
                self._update_offset_to_store(message_queue, min_offset)

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

    def _update_offset_to_store(
        self, message_queue: MessageQueue, min_offset: int
    ) -> None:
        """
        更新偏移量到存储

        Args:
            message_queue: 消息队列
            min_offset: 最小偏移量
        """
        try:
            self._offset_store.update_offset(message_queue, min_offset)
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

    def _handle_failed_consume(
        self, messages: list[MessageExt], message_queue: MessageQueue
    ) -> list[MessageExt]:
        """
        处理消费失败的消息，根据消息模式采取不同的处理策略。

        当消息监听器返回RECONSUME_LATER或抛出异常时，此方法负责处理失败的消息。
        根据消费者配置的消息模型（集群模式或广播模式），采取不同的重试策略。

        Args:
            messages (list[MessageExt]): 消费失败的消息列表
            message_queue (MessageQueue): 消息所属的队列信息

        Returns:
            list[MessageExt]: 发送回broker失败的消息列表（集群模式）或空列表（广播模式）

        处理策略:
            集群模式 (MessageModel.CLUSTERING):
                - 尝试将失败的消息发送回broker进行重试
                - 只有发送回broker失败的消息才会被返回（需要本地处理）
                - 成功发送回broker的消息由broker负责重试，不在返回列表中
                - 发送失败的消息可能会导致消息丢失，需要业务方关注

            广播模式 (MessageModel.BROADCASTING):
                - 直接丢弃所有失败的消息，返回空列表
                - 广播模式下每个消费者独立消费，不需要重试机制
                - 记录警告日志，便于问题排查

        Examples:
            >>> # 在消息处理循环中使用
            >>> failed_messages = []
            >>> try:
            >>>     result = await self._consume_message(messages, context)
            >>>     if result == ConsumeResult.RECONSUME_LATER:
            >>>         # 返回的是发送到broker失败的消息，需要特殊处理
            >>>         failed_messages = self._handle_failed_consume(messages, queue)
            >>>         if failed_messages:
            >>>             logger.error(f"Failed to send {len(failed_messages)} messages back to broker")
            >>> except Exception as e:
            >>>     failed_messages = self._handle_failed_consume(messages, queue)
            >>>     logger.error(f"Message processing failed: {e}")
            >>>     if failed_messages:
            >>>         # 可能需要记录到死信队列或进行其他补偿处理
            >>>         pass

        Note:
            - 集群模式下，重试次数受max_reconsume_times配置限制
            - 重试消息会被发送到重试主题(%RETRY%{consumer_group})
            - 超过最大重试次数的消息会进入死信队列(%DLQ%{consumer_group})
            - 广播模式的消息丢失风险需要业务方考虑补偿机制
            - 返回的消息列表是发送回broker失败的消息，可能需要特殊的处理逻辑
            - 成功发送回broker的消息将由broker负责重试调度，不需要本地处理
        """
        if self._config.message_model == MessageModel.CLUSTERING:
            return [
                msg
                for msg in messages
                if not self._send_back_message(message_queue, msg)
            ]
        # 广播模式，直接丢掉消息
        logger.warning("Broadcast mode, discard failed messages")
        return []

    def _update_consume_stats(
        self, success: bool, duration: float, message_count: int
    ) -> None:
        """
        更新消费统计信息

        Args:
            consume_result: 消费结果
            message_count: 消息数量
        """
        # 更新总消费时长
        self._stats["consume_duration_total"] += duration

        # 更新失败消息数
        if not success:
            self._stats["messages_failed"] += message_count

    def _wait_for_processing_completion(self) -> None:
        """
        等待正在处理的消息完成
        """
        try:
            # 等待所有消费任务完成
            timeout: int = 30  # 30秒超时

            with self._process_queue_lock:
                # 收集所有未完成的消费任务
                all_futures: list[Future[None]] = []
                for futures in self._consume_tasks.values():
                    for future in futures:
                        if future and not future.done():
                            all_futures.append(future)

            # 等待所有任务完成或超时
            if all_futures and self._consume_executor:
                _, not_done_futures = concurrent.futures.wait(
                    all_futures, timeout=timeout
                )

                if not_done_futures:
                    logger.warning(
                        f"Timeout waiting for {len(not_done_futures)} consume tasks to complete",
                        extra={
                            "consumer_group": self._config.consumer_group,
                            "timeout": timeout,
                            "remaining_tasks": len(not_done_futures),
                        },
                    )

            # 等待所有处理队列为空
            queue_check_start = time.time()
            all_empty: bool = False
            while time.time() - queue_check_start < timeout:
                all_empty = True
                with self._process_queue_lock:
                    for _queue_name, consume_queue in self._process_queues.items():
                        if not consume_queue.empty():
                            all_empty = False
                            break

                if all_empty:
                    break

                time.sleep(0.1)  # 短暂等待

            if not all_empty:
                logger.warning(
                    f"Timeout waiting for processing queues to empty after {timeout}s",
                    extra={
                        "consumer_group": self._config.consumer_group,
                        "timeout": timeout,
                    },
                )

        except Exception as e:
            logger.warning(
                f"Error waiting for processing completion: {e}",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "error": str(e),
                },
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
            # 取消所有消费任务
            for _, futures in self._consume_tasks.items():
                for future in futures:
                    if future and not future.done():
                        future.cancel()
            self._consume_tasks.clear()

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

        # 清理所有MessageQueue的处理队列
        with self._process_queue_lock:
            for _, consume_queue in self._process_queues.items():
                while not consume_queue.empty():
                    try:
                        consume_queue.get_nowait()
                    except queue.Empty:
                        break
            self._process_queues.clear()

        # 清理ProcessQueue消息缓存
        for process_queue in self._msg_cache.values():
            process_queue.clear()
        self._msg_cache.clear()

        # 清理停止事件
        with self._stop_events_lock:
            self._pull_stop_events.clear()
            self._consume_stop_events.clear()

        # 清理状态
        self._pull_tasks.clear()
        self._assigned_queues.clear()

        # 远程解锁所有已分配的队列
        for message_queue in self._assigned_queues.keys():
            try:
                self._unlock_remote_queue(message_queue)
            except Exception as e:
                logger.warning(
                    f"Failed to unlock remote queue {message_queue} during cleanup: {e}",
                    extra={
                        "consumer_group": self._config.consumer_group,
                        "client_id": self._config.client_id,
                        "queue": str(message_queue),
                        "error": str(e),
                        "operation": "cleanup_resources",
                    },
                )

        # 清理队列锁
        if hasattr(self, "_queue_locks"):
            self._queue_locks.clear()
        if hasattr(self, "_queue_lock_timestamps"):
            self._queue_lock_timestamps.clear()

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

        for msg in msgs:
            self._reset_retry(msg)

        if self._concurrent_consume_message(msgs, q):
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

    # ==================== 锁过期测试辅助方法 ====================

    def _get_lock_info(self, message_queue: MessageQueue) -> dict[str, Any]:
        """
        获取指定队列锁的信息（用于调试和监控）

        Args:
            message_queue: 消息队列

        Returns:
            包含锁信息的字典
        """
        if not hasattr(self, "_queue_lock_timestamps"):
            return {"has_lock": False, "has_timestamp": False}

        if message_queue not in self._queue_locks:
            return {"has_lock": False, "has_timestamp": False}

        current_time: float = time.time() * 1000
        lock_time: float = self._queue_lock_timestamps.get(message_queue, current_time)
        age_ms: float = current_time - lock_time
        is_expired: bool = self._is_lock_expired(message_queue)

        return {
            "has_lock": True,
            "has_timestamp": True,
            "lock_age_ms": age_ms,
            "is_expired": is_expired,
            "expire_time_ms": self._config.lock_expire_time,
        }
