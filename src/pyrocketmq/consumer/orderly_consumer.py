import concurrent.futures
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor

# pyrocketmq导入
from datetime import datetime

from pyrocketmq.broker import BrokerClient, MessagePullError
from pyrocketmq.consumer.allocate_queue_strategy import AllocateContext
from pyrocketmq.consumer.base_consumer import BaseConsumer
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

        # 线程池和队列管理
        # 顺序消息消费者每个message_queue一个消费线程
        self._consume_tasks: dict[MessageQueue, list[Future[None]]] = {}
        self._consume_executor: ThreadPoolExecutor | None = None  # 用于管理消费任务
        self._pull_executor: ThreadPoolExecutor | None = None

        # 初始化每个message_queue的锁管理字段
        # 每个queue的锁都有时间限制，支持is_lock_expired方法
        self._queue_locks: dict[MessageQueue, threading.RLock] = {}
        self._queue_lock_management_lock = threading.Lock()  # 🔐保护_queue_locks字典

        # 状态管理
        self._pull_tasks: dict[MessageQueue, Future[None]] = {}
        self._assigned_queues: dict[MessageQueue, int] = {}  # queue -> last_offset
        self._assigned_queues_lock = (
            threading.RLock()
        )  # 🔐保护_assigned_queues字典的并发访问
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

        # 远程锁缓存和有效期管理
        # 避免每次消费循环都需要获取远程锁，提升性能
        self._remote_lock_cache: dict[
            MessageQueue, float
        ] = {}  # queue -> lock_expiry_time
        self._remote_lock_cache_lock = threading.Lock()  # 保护远程锁缓存
        self._remote_lock_expire_time: float = 30.0  # 远程锁有效期30秒

        logger.info(
            "OrderlyConsumer initialized",
            extra={
                "consumer_group": self._config.consumer_group,
                "message_model": self._config.message_model,
                "consume_thread_max": self._config.consume_thread_max,
                "pull_batch_size": self._config.pull_batch_size,
                "remote_lock_expire_time": self._remote_lock_expire_time,
            },
        )

    # ==================== 1. 核心生命周期管理 ====================
    # 功能：管理消费者的启动、停止和状态转换
    # 关联方法：启动相关、停止相关、清理相关

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

                # 初始化处理器
                self._prepare_processors()

                self._do_rebalance()

                # 启动重平衡任务
                self._start_rebalance_task()

                with self._assigned_queues_lock:  # 🔐保护_assigned_queues访问
                    assigned_queues_count = len(self._assigned_queues)

                logger.info(
                    "OrderlyConsumer started successfully",
                    extra={
                        "consumer_group": self._config.consumer_group,
                        "assigned_queues": assigned_queues_count,
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

    # ==================== 2. 重平衡管理模块 ====================
    # 功能：管理消费者队列分配、负载均衡和重平衡流程
    # 关联方法：重平衡检查、队列分配、任务启动、消费者发现

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
            logger.debug(
                "Rebalance already in progress, skipping",
                extra={
                    "consumer_group": self._config.consumer_group,
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

        logger.info(
            "Rebalance completed",
            extra={
                "consumer_group": self._config.consumer_group,
                "total_topics": total_topics,
                "assigned_queues": total_queues,
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

        finally:
            # 释放重平衡锁
            self._rebalance_lock.release()
            logger.debug(
                "Rebalance lock released",
                extra={
                    "consumer_group": self._config.consumer_group,
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

        # 使用_assigned_queues_lock保护整个队列更新过程
        with self._assigned_queues_lock:  # 🔐保护_assigned_queues的完整操作
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
                future: Future[None] | None = self._pull_tasks.pop(q)
                if future and not future.done():
                    future.cancel()

            # 停止并移除该队列的消费任务
            if q in self._consume_tasks:
                consume_futures = self._consume_tasks.pop(q)
                for future in consume_futures:
                    if future and not future.done():
                        future.cancel()

            # 清理队列锁
            if q in self._queue_locks:
                del self._queue_locks[q]

        # 为新分配的队列创建资源
        for q in added_queues:
            # 为新队列创建锁
            self._queue_locks[q] = threading.RLock()

            # 为新队列初始化消费任务列表
            self._consume_tasks[q] = []

        # 如果消费者正在运行，启动新队列的拉取任务和消费任务
        if self._is_running and added_queues:
            self._start_pull_tasks_for_queues(added_queues)
            self._start_consume_tasks_for_queues(added_queues)

    def _trigger_rebalance(self) -> None:
        """触发重平衡"""
        if self._is_running:
            # 唤醒重平衡循环，使其立即执行重平衡
            self._rebalance_event.set()

    def _start_rebalance_task(self) -> None:
        """启动定期重平衡任务"""
        self._rebalance_thread = threading.Thread(
            target=self._rebalance_loop,
            name=f"{self._config.consumer_group}-rebalance-thread",
            daemon=True,
        )
        self._rebalance_thread.start()

    def _rebalance_loop(self) -> None:
        """定期重平衡循环"""
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
        self, _remoting_cmd: RemotingCommand, _remote_addr: tuple[str, int]
    ) -> None:
        """处理消费者ID变更通知"""
        logger.info("Received notification of consumer IDs changed")
        self._do_rebalance()

    def _find_consumer_list(self, topic: str) -> list[str]:
        """查找指定Topic的消费者列表

        Args:
            topic: 主题名称

        Returns:
            list[str]: 消费者ID列表
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

    # ==================== 3. 队列锁管理模块 ====================
    # 功能：管理本地队列锁和远程队列锁，确保顺序消费的线程安全
    # 关联方法：锁获取、锁状态检查、远程锁操作、锁缓存管理

    def _get_queue_lock(self, message_queue: MessageQueue) -> threading.RLock:
        """获取指定消息队列的锁

        Args:
            message_queue: 消息队列

        Returns:
            threading.RLock: 该队列的RLock锁对象
        """
        # 使用双重检查锁定模式来避免竞争条件
        # 首先进行无锁检查，提高性能
        if message_queue in self._queue_locks:
            return self._queue_locks[message_queue]

        # 使用锁保护字典操作，防止竞争条件
        with self._queue_lock_management_lock:
            # 再次检查，防止在等待锁的过程中其他线程已经创建了锁
            if message_queue not in self._queue_locks:
                self._queue_locks[message_queue] = threading.RLock()

            return self._queue_locks[message_queue]

    def _is_locked(self, message_queue: MessageQueue) -> bool:
        """检查指定队列是否已锁定

        Args:
            message_queue: 消息队列

        Returns:
            bool: True如果队列已锁定，False如果队列未锁定
        """
        # 使用锁保护对_queue_locks字典的访问，防止竞争条件
        with self._queue_lock_management_lock:
            if message_queue not in self._queue_locks:
                return False

            return self._queue_locks[message_queue].locked()

    def _is_remote_lock_valid(self, message_queue: MessageQueue) -> bool:
        """检查指定队列的远程锁是否仍然有效

        Args:
            message_queue: 消息队列

        Returns:
            bool: True如果远程锁仍然有效，False如果已过期或不存在
        """
        with self._remote_lock_cache_lock:
            expiry_time = self._remote_lock_cache.get(message_queue)
            if expiry_time is None:
                return False

            current_time = time.time()
            return current_time < expiry_time

    def _set_remote_lock_expiry(self, message_queue: MessageQueue) -> None:
        """设置指定队列的远程锁过期时间

        Args:
            message_queue: 消息队列
        """
        with self._remote_lock_cache_lock:
            expiry_time = time.time() + self._remote_lock_expire_time
            self._remote_lock_cache[message_queue] = expiry_time

    def _invalidate_remote_lock(self, message_queue: MessageQueue) -> None:
        """使指定队列的远程锁失效

        Args:
            message_queue: 消息队列
        """
        with self._remote_lock_cache_lock:
            self._remote_lock_cache.pop(message_queue, None)

    def _lock_remote_queue(self, message_queue: MessageQueue) -> bool:
        """尝试远程锁定指定队列

        Args:
            message_queue: 消息队列

        Returns:
            bool: True如果锁定成功，False如果锁定失败
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
                locked_queues: list[MessageQueue] = broker_client.lock_batch_mq(
                    consumer_group=self._config.consumer_group,
                    client_id=self._config.client_id,
                    mqs=[message_queue],
                )

                locked: bool = False
                for q in locked_queues:
                    if q.equal(message_queue):
                        locked = True
                        break
                if locked:
                    self._set_remote_lock_expiry(message_queue)
                    logger.debug(
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
        """尝试远程解锁指定队列

        Args:
            message_queue: 消息队列

        Returns:
            bool: True如果解锁成功，False如果解锁失败
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

                # 清除远程锁缓存
                self._invalidate_remote_lock(message_queue)

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

    # ==================== 4. 消息拉取模块 ====================
    # 功能：管理消息拉取任务、拉取循环、拉取策略和Broker通信
    # 关联方法：拉取任务管理、拉取循环执行、单次拉取、消息处理

    def _start_pull_tasks_for_queues(self, queues: set[MessageQueue]) -> None:
        """为指定队列启动拉取任务

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
        """停止所有消息拉取任务 - 使用停止事件优雅关闭"""
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
                pq.update_pull_timestamp()

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

        pull_start_time = time.time()
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

        # 记录拉取统计
        pull_rt: int = int((time.time() - pull_start_time) * 1000)  # 转换为毫秒
        message_count = len(messages)

        self._stats_manager.increase_pull_rt(
            self._config.consumer_group, message_queue.topic, pull_rt
        )
        self._stats_manager.increase_pull_tps(
            self._config.consumer_group, message_queue.topic, message_count
        )

        return messages, next_begin_offset, next_suggest_id

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
        with self._assigned_queues_lock:  # 🔐保护_assigned_queues访问
            self._assigned_queues[message_queue] = next_begin_offset

        # 将消息添加到缓存中（用于解决并发偏移量问题）
        self._add_messages_to_cache(message_queue, messages)

        # 更新统计信息
        self._stats_manager.increase_pull_tps(
            self._config.consumer_group, message_queue.topic, len(messages)
        )

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
        with self._assigned_queues_lock:  # 🔐保护_assigned_queues访问
            current_offset: int = self._assigned_queues.get(message_queue, 0)

            # 如果current_offset为0（首次消费），则从_consume_from_where_manager中获取正确的初始偏移量
            if current_offset == 0:
                try:
                    current_offset = (
                        self._consume_from_where_manager.get_consume_offset(
                            message_queue,
                            self._config.consume_from_where,
                            self._config.consume_timestamp
                            if hasattr(self._config, "consume_timestamp")
                            else 0,
                        )
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
        """
        try:
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

    def _build_sys_flag(self, commit_offset: bool) -> int:
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

    # ==================== 5. 消息处理模块 ====================
    # 功能：管理消息消费任务、消息处理循环、消费锁获取和结果处理
    # 关联方法：消费任务管理、消费循环、锁获取、消息处理、结果处理

    def _start_consume_tasks_for_queues(self, queues: set[MessageQueue]) -> None:
        """为指定的队列集合启动消费任务

        Args:
            queues: 需要启动消费任务的队列集合
        """
        if not self._consume_executor:
            return

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

    def _stop_consume_tasks(self) -> None:
        """停止所有消息消费任务 - 使用停止事件优雅关闭"""
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

    def _acquire_consume_lock(
        self, message_queue: MessageQueue, stop_event: threading.Event
    ) -> tuple[threading.RLock | None, bool]:
        """
        获取消费锁（本地锁 + 远程锁验证）。

        Args:
            message_queue: 要处理的消息队列
            stop_event: 停止事件

        Returns:
            tuple[RLock | None, bool]: (队列锁, 是否成功获取锁)
        """
        queue_lock = self._get_queue_lock(message_queue)
        lock_acquired = False

        # 尝试非阻塞获取锁，如果失败则等待10ms后重试
        while not lock_acquired and self._is_running and not stop_event.is_set():
            lock_acquired = queue_lock.acquire(blocking=False)
            if not lock_acquired:
                # 等待10ms
                if stop_event.wait(timeout=0.01):
                    break

        # 如果获取锁失败或消费者停止，则返回
        if not lock_acquired or not self._is_running:
            if lock_acquired:
                queue_lock.release()
            return queue_lock, False

        # 本地锁持有成功，检查远程锁是否需要重新获取
        # 广播模式下不需要远程锁，每个消费者独立处理所有消息
        if self._config.message_model == MessageModel.BROADCASTING:
            logger.debug(
                f"Broadcast mode - skipping remote lock for queue {message_queue}",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "client_id": self._config.client_id,
                    "queue": str(message_queue),
                    "operation": "consume_messages_loop",
                    "message_model": "BROADCASTING",
                },
            )
            return queue_lock, True

        # 集群模式下需要远程锁来保证消息的顺序性
        if not self._is_remote_lock_valid(message_queue):
            # 远程锁已过期或不存在，需要重新获取
            if not self._lock_remote_queue(message_queue):
                logger.debug(
                    f"Failed to acquire remote lock for queue {message_queue}, skipping this round",
                    extra={
                        "consumer_group": self._config.consumer_group,
                        "client_id": self._config.client_id,
                        "queue": str(message_queue),
                        "operation": "consume_messages_loop",
                    },
                )
                # 释放本地锁并继续下一轮循环
                queue_lock.release()
                return queue_lock, False
        else:
            logger.debug(
                f"Using cached remote lock for queue {message_queue}",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "client_id": self._config.client_id,
                    "queue": str(message_queue),
                    "operation": "consume_messages_loop",
                    "lock_cached": True,
                },
            )

        return queue_lock, True

    def _fetch_messages_from_queue(
        self, message_queue: MessageQueue, stop_event: threading.Event
    ) -> tuple[ProcessQueue, list[MessageExt]]:
        """
        从处理队列获取消息。

        Args:
            message_queue: 消息队列
            stop_event: 停止事件

        Returns:
            tuple[ProcessQueue, list[MessageExt]]: (处理队列, 消息列表)
        """
        pq: ProcessQueue = self._get_or_create_process_queue(message_queue)
        messages: list[MessageExt] = pq.take_messages(self._config.consume_batch_size)

        if not messages:
            stop_event.wait(timeout=3.0)
        else:
            # 重置消息的重试次数
            for msg in messages:
                self._reset_retry(msg)

        return pq, messages

    def _handle_auto_commit_result(
        self,
        pq: ProcessQueue,
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
            offset = pq.commit()
            if offset:
                self._offset_store.update_offset(message_queue, offset)
            return False, False  # 跳出循环，不等待
        else:
            if self.check_reconsume_times(message_queue, messages):
                return True, True  # 继续循环，需要等待
            else:
                offset = pq.commit()
                if offset:
                    self._offset_store.update_offset(message_queue, offset)
                return False, False  # 跳出循环，不等待

    def _handle_manual_commit_result(
        self,
        pq: ProcessQueue,
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
                offset = pq.commit()
                if offset:
                    self._offset_store.update_offset(message_queue, offset)
                return False, False  # 跳出循环，不等待
        else:
            if result == ConsumeResult.ROLLBACK:
                _ = pq.rollback(messages)
                return False, True  # 跳出循环，需要等待
            elif result == ConsumeResult.RECONSUME_LATER:
                if self.check_reconsume_times(message_queue, messages):
                    return True, True  # 继续循环，需要等待
                else:
                    return False, False  # 跳出循环，不等待

        return False, False

    def _process_messages_with_retry(
        self,
        pq: ProcessQueue,
        message_queue: MessageQueue,
        messages: list[MessageExt],
        stop_event: threading.Event,
    ) -> None:
        """
        处理消息并处理重试逻辑。

        Args:
            pq: 处理队列
            message_queue: 消息队列
            messages: 消息列表
            stop_event: 停止事件
        """
        while self._is_running and not stop_event.is_set():
            success, result = self._process_messages_with_timing(
                messages, message_queue
            )
            pq.update_consume_timestamp()

            # 根据提交模式处理结果
            if self._config.enable_auto_commit:
                should_continue, should_wait = self._handle_auto_commit_result(
                    pq, message_queue, messages, success, result
                )
            else:
                should_continue, should_wait = self._handle_manual_commit_result(
                    pq, message_queue, messages, success, result
                )

            if should_continue:
                if should_wait:
                    stop_event.wait(timeout=1.0)
                continue
            else:
                break

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
            1. 获取消费锁（本地锁+远程锁验证）(_acquire_consume_lock)
            2. 从处理队列获取消息 (_fetch_messages_from_queue)
            3. 处理消息并处理重试逻辑 (_process_messages_with_retry)
            4. 更新消费统计信息

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

        while self._is_running and not stop_event.is_set():
            queue_lock: threading.RLock | None = None
            try:
                # 获取消费锁
                queue_lock, lock_success = self._acquire_consume_lock(
                    message_queue, stop_event
                )
                if not lock_success:
                    if stop_event.wait(timeout=3.0):
                        break
                    continue

                # 从处理队列获取消息
                pq, messages = self._fetch_messages_from_queue(
                    message_queue, stop_event
                )
                if not messages:
                    continue

                # 处理消息并处理重试逻辑
                self._process_messages_with_retry(
                    pq, message_queue, messages, stop_event
                )

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

            finally:
                # 释放本地锁
                if queue_lock is not None and queue_lock.locked():
                    queue_lock.release()

    def _process_messages_with_timing(
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
        success, consume_result = self._orderly_consume_message(messages, message_queue)
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

    # ==================== 缓存管理模块 ====================

    def _get_or_create_process_queue(self, queue: MessageQueue) -> ProcessQueue:
        """获取或创建指定队列的ProcessQueue（消息缓存队列）"""
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

    def _wait_for_processing_completion(self) -> None:
        """
        等待正在处理的消息完成
        """
        try:
            # 等待所有消费任务完成
            timeout: int = 30  # 30秒超时

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

        except Exception as e:
            logger.warning(
                f"Error waiting for processing completion: {e}",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "error": str(e),
                },
            )

    # ==================== 资源清理模块 ====================

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

        # 清理ProcessQueue消息缓存
        for process_queue in self._msg_cache.values():
            _ = process_queue.clear()
        self._msg_cache.clear()

        # 清理停止事件
        with self._stop_events_lock:
            self._pull_stop_events.clear()
            self._consume_stop_events.clear()

        # 清理状态
        self._pull_tasks.clear()

        # 远程解锁所有已分配的队列
        with self._assigned_queues_lock:  # 🔐保护_assigned_queues访问
            assigned_queues = list(self._assigned_queues.keys())  # 复制一份避免并发修改
            self._assigned_queues.clear()

        for message_queue in assigned_queues:
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
        self._queue_locks.clear()

        # 清理远程锁缓存
        with self._remote_lock_cache_lock:
            self._remote_lock_cache.clear()

    # ==================== 远程通信处理模块 ====================

    def _prepare_processors(self) -> None:
        self._broker_manager.register_pool_processor(
            RequestCode.NOTIFY_CONSUMER_IDS_CHANGED,
            self._on_notify_consumer_ids_changed,
        )
        self._broker_manager.register_pool_processor(
            RequestCode.CONSUME_MESSAGE_DIRECTLY,
            self._on_notify_consume_message_directly,
        )
        self._broker_manager.register_pool_processor(
            RequestCode.GET_CONSUMER_RUNNING_INFO,
            self._on_notify_get_consumer_running_info,
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

        success, _ = self._orderly_consume_message(msgs, q)
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
