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

    # ==================== 构造函数和属性初始化 ====================

    def __init__(self, config: ConsumerConfig) -> None:
        """
        初始化并发消费者

        Args:
            config: 消费者配置
        """
        super().__init__(config)

        # 线程池和队列管理
        self._consume_executor: ThreadPoolExecutor | None = None
        self._pull_executor: ThreadPoolExecutor | None = None
        self._process_queue: queue.Queue[tuple[list[MessageExt], MessageQueue]] = (
            queue.Queue()
        )

        # 状态管理
        self._pull_tasks: dict[MessageQueue, Future[None]] = {}
        self._pull_stop_events: dict[
            MessageQueue, threading.Event
        ] = {}  # 拉取任务停止事件
        self._assigned_queues: dict[MessageQueue, int] = {}  # queue -> last_offset
        self._assigned_queues_lock: threading.RLock = (
            threading.RLock()
        )  # 保护assigned_queues的锁
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

    # ==================== 生命周期管理模块 ====================
    """
    生命周期管理模块负责消费者的启动、停止等核心生命周期操作。
    包括线程池创建、资源初始化、优雅关闭等功能。
    主要方法:
    - start(): 启动消费者，初始化所有组件和任务
    - shutdown(): 优雅关闭消费者，清理资源并持久化状态
    - _cleanup_on_start_failure(): 启动失败时的资源清理
    """

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

                with self._assigned_queues_lock:
                    assigned_queue_count = len(self._assigned_queues)

                logger.info(
                    "ConcurrentConsumer started successfully",
                    extra={
                        "consumer_group": self._config.consumer_group,
                        "assigned_queues": assigned_queue_count,
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

    # ==================== 重平衡管理模块 ====================
    """
    重平衡管理模块负责消费者组内队列的动态分配和调整。
    当消费者加入或离开时，会触发重平衡过程，重新分配队列。
    主要方法:
    - _do_rebalance(): 执行完整的重平衡流程
    - _allocate_queues(): 根据策略分配队列
    - _update_assigned_queues(): 更新分配的队列
    - _start_rebalance_task(): 启动定期重平衡任务
    - _rebalance_loop(): 重平衡循环
    - _find_consumer_list(): 查找消费者组列表
    - _trigger_rebalance(): 触发重平衡
    """

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

    def _finalize_rebalance(self, total_topics: int, _total_queues: int) -> None:
        """完成重平衡后处理。

        更新重平衡时间戳、统计信息，并记录完成日志。

        Args:
            total_topics: 重平衡处理的Topic总数
            total_queues: 分配到的队列总数

        Raises:
            None: 此方法不会抛出异常
        """
        self._last_rebalance_time = time.time()

        with self._assigned_queues_lock:
            assigned_queue_count = len(self._assigned_queues)

        logger.info(
            "Rebalance completed",
            extra={
                "consumer_group": self._config.consumer_group,
                "total_topics": total_topics,
                "assigned_queues": assigned_queue_count,
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
        with self._assigned_queues_lock:
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
        self, _remoting_cmd: RemotingCommand, _remote_addr: tuple[str, int]
    ) -> None:
        """
        处理消费者ID变更通知。

        当消费者组中有消费者加入或退出时，Broker会向所有消费者发送此通知。
        该方法接收通知并触发重平衡操作，以重新分配队列。

        Args:
            _remoting_cmd (RemotingCommand): 通知命令对象
                - 参数名使用下划线前缀表示该参数在当前实现中未被使用
            _remote_addr (tuple[str, int]): 通知来源地址（IP地址和端口）
                - 参数名使用下划线前缀表示该参数在当前实现中未被使用

        Returns:
            None

        处理流程：
            1. 记录接收通知的日志
            2. 调用_do_rebalance()触发重平衡

        触发场景：
            1. 新消费者加入消费者组
            2. 消费者异常退出或正常关闭
            3. Broker检测到消费者心跳超时
            4. 网络分区导致消费者状态变化

        重平衡效果：
            - 重新分配队列给存活的消费者
            - 确保每个队列只被一个消费者消费
            - 保证负载均衡

        Note:
            - 该方法由Netty的I/O线程调用，应该快速返回
            - 实际的重平衡操作在后台线程中执行
            - 频繁的重平衡可能影响消费性能
        """
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

    # ==================== 消息拉取管理模块 ====================
    """
    消息拉取管理模块负责从Broker持续拉取消息并处理。
    每个队列都有独立的拉取线程，支持流量控制和智能间隔调整。
    主要方法:
    - _pull_messages_loop(): 持续拉取消息的核心循环
    - _perform_single_pull(): 执行单次拉取操作
    - _pull_messages(): 实际的Broker拉取请求
    - _handle_pulled_messages(): 处理拉取到的消息
    - _start_pull_tasks(): 启动所有拉取任务
    - _stop_pull_tasks(): 停止拉取任务
    - _apply_pull_interval(): 智能拉取间隔控制
    - _build_sys_flag(): 构建系统标志位
    """

    def _start_pull_tasks(self) -> None:
        """
        启动所有队列的消息拉取任务
        """
        if not self._pull_executor:
            return

        with self._assigned_queues_lock:
            if not self._assigned_queues:
                return
            queue_keys = set(self._assigned_queues.keys())

        self._start_pull_tasks_for_queues(queue_keys)

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
                # 创建停止事件
                stop_event = threading.Event()
                self._pull_stop_events[q] = stop_event

                future: Future[None] = self._pull_executor.submit(
                    self._pull_messages_loop, q, stop_event
                )
                self._pull_tasks[q] = future

    def _stop_pull_tasks(self) -> None:
        """
        停止所有消息拉取任务
        """
        if not self._pull_tasks:
            return

        # 首先设置所有停止事件，通知拉取循环停止
        for _, stop_event in self._pull_stop_events.items():
            stop_event.set()

        # 然后取消Future
        for _, future in self._pull_tasks.items():
            if future and not future.done():
                future.cancel()

        self._pull_stop_events.clear()
        self._pull_tasks.clear()

    def _pull_messages_loop(
        self, message_queue: MessageQueue, stop_event: threading.Event
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
            stop_event (threading.Event): 停止事件，用于中断循环

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
        while self._is_running and not stop_event.is_set():
            pq: ProcessQueue = self._get_or_create_process_queue(message_queue)
            if pq.need_flow_control():
                # 使用可中断的等待代替time.sleep
                if stop_event.wait(timeout=3.0):
                    break
                continue
            try:
                # 执行单次拉取操作
                pull_result: tuple[list[MessageExt], int, int] | None = (
                    self._perform_single_pull(message_queue, suggest_broker_id)
                )
                pq.update_consume_timestamp()

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

                # 控制拉取频率 - 传入是否有消息的标志
                self._apply_pull_interval(len(messages) > 0)

                # 检查是否收到停止信号
                if stop_event.is_set():
                    break

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

                # 拉取失败时等待一段时间再重试
                if stop_event.wait(timeout=3.0):
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
        with self._assigned_queues_lock:
            self._assigned_queues[message_queue] = next_begin_offset

        # 将消息添加到缓存中（用于解决并发偏移量问题）
        self._add_messages_to_cache(message_queue, messages)

        # 将消息按批次放入处理队列
        self._submit_messages_for_processing(message_queue, messages)

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

    # ==================== 偏移量管理模块 ====================
    """
    偏移量管理模块负责消费偏移量的获取、初始化和更新。
    确保消息消费的连续性和正确性，支持不同的消费起始位置策略。
    主要方法:
    - _get_or_initialize_offset(): 获取或初始化消费偏移量
    - _update_offset_from_cache(): 从缓存更新偏移量
    - _update_offset_to_store(): 更新偏移量到存储
    """

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
        with self._assigned_queues_lock:
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
                with self._assigned_queues_lock:
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

    # ==================== 消息缓存管理模块 ====================
    """
    消息缓存管理模块使用ProcessQueue实现高效的消息缓存和并发控制。
    解决了并发消费中的偏移量管理问题，支持流量控制和统计监控。
    主要方法:
    - _get_or_create_process_queue(): 获取或创建ProcessQueue
    - _add_messages_to_cache(): 添加消息到缓存
    - _remove_messages_from_cache(): 从缓存移除消息
    - _is_message_cached(): 检查消息是否已缓存
    """

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
        移除完成后直接返回当前队列中最小消息的offset，避免额外的查询操作。

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
        process_queue: ProcessQueue = self._get_or_create_process_queue(queue)

        if not messages:
            # 如果没有消息要移除，直接返回当前最小offset
            return process_queue.get_min_offset()

        _ = process_queue.remove_batch_messages(
            [x.queue_offset for x in messages if x.queue_offset is not None]
        )

        # 返回移除完成后当前缓存中的最小offset
        return process_queue.get_min_offset()

    # ==================== 核心拉取实现模块 ====================
    """
    核心拉取实现模块包含与Broker通信的底层拉取逻辑。
    处理系统标志位构建、地址解析、实际的拉取请求等核心功能。
    主要方法:
    - _pull_messages(): 核心拉取实现
    - _build_sys_flag(): 构建系统标志位
    """

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
            pull_start_time = time.time()

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
                    timeout=30,
                )

                # 记录拉取统计
                pull_rt = int((time.time() - pull_start_time) * 1000)  # 转换为毫秒
                message_count = len(result.messages) if result.messages else 0

                self._stats_manager.increase_pull_rt(
                    self._config.consumer_group, message_queue.topic, pull_rt
                )
                self._stats_manager.increase_pull_tps(
                    self._config.consumer_group, message_queue.topic, message_count
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

    # ==================== 消息处理模块 ====================
    """
    消息处理模块负责消息的实际消费和业务处理。
    使用线程池进行并发处理，支持成功/失败的不同处理策略。
    主要方法:
    - _start_consume_tasks(): 启动消费任务
    - _consume_messages_loop(): 消费消息的核心循环
    - _process_messages_with_timing(): 带计时的消息处理
    - _handle_successful_consume(): 处理成功消费的消息
    - _handle_failed_consume(): 处理消费失败的消息

    - _wait_for_processing_completion(): 等待处理完成
    """

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
        持续处理消息的消费循环。

        这是并发消费者核心的消息处理循环，运行在独立的消费线程池中。
        该循环负责从消息处理队列中获取消息并调用用户注册的消息监听器进行处理。

        主要功能:
            - 从处理队列阻塞式获取消息批次
            - 调用用户消息监听器进行业务处理
            - 根据处理结果执行成功/失败后的处理逻辑
            - 更新消费统计信息
            - 处理重试机制和异常情况

        处理流程:
            1. 从处理队列获取消息批次 (_get_messages_from_queue)
            2. 调用消息监听器处理消息 (_process_messages_with_timing)
            3. 根据处理结果执行后续操作：
               - 成功: 更新偏移量，清理处理队列
               - 失败: 发送回broker进行重试

            5. 对发送回broker失败的消息进行延迟重试

        重试机制:
            - 消费失败的消息会尝试发送回broker进行重试
            - 发送回broker失败的消息会在本地延迟5秒后重试
            - 避免因broker连接问题导致的消息丢失

        线程模型:
            - 运行在消费线程池中，线程数量由consume_thread_max配置
            - 多个消费线程并行处理不同队列的消息
            - 每个线程独立运行，互不干扰

        异常处理:
            - 捕获所有异常，确保单个消息处理失败不影响整个循环
            - 记录详细的错误日志和异常堆栈信息
            - 保持循环继续运行，确保消费者服务可用

        Note:
            - 该方法是消费者消息处理的核心逻辑，不应被外部调用
            - 循环会在消费者关闭时(_is_running=False)自动退出
            - 消费延迟主要取决于消息处理时间和队列深度
            - 统计信息用于监控消费性能和健康状态
        """
        while self._is_running:
            try:
                # 从处理队列获取消息
                message_data: tuple[list[MessageExt], MessageQueue] | None = (
                    self._get_messages_from_queue()
                )
                if message_data is None:
                    continue

                messages, message_queue = message_data
                pq: ProcessQueue = self._get_or_create_process_queue(message_queue)

                while messages:
                    # 处理消息
                    success = self._process_messages_with_timing(
                        messages, message_queue
                    )
                    pq.update_consume_timestamp()

                    # 根据处理结果进行后续处理
                    if success:
                        self._handle_successful_consume(messages, message_queue)
                        messages = []
                    else:
                        messages: list[MessageExt] = self._handle_failed_consume(
                            messages, message_queue
                        )
                        if messages:
                            time.sleep(5)

            except Exception as e:
                logger.error(
                    f"Error in consume messages loop: {e}",
                    extra={
                        "consumer_group": self._config.consumer_group,
                        "error": str(e),
                    },
                    exc_info=True,
                )

    def _get_messages_from_queue(
        self,
    ) -> tuple[list[MessageExt], MessageQueue] | None:
        """
        从处理队列获取消息

        Returns:
            消息和队列的元组，如果队列为空则返回None
        """
        try:
            messages: list[MessageExt]
            message_queue: MessageQueue
            messages, message_queue = self._process_queue.get(timeout=1.0)
            return messages, message_queue
        except queue.Empty:
            return None

    def _process_messages_with_timing(
        self, messages: list[MessageExt], message_queue: MessageQueue
    ) -> bool:
        """
        处理消息并计时，同时记录统计信息

        Args:
            messages: 要处理的消息列表
            message_queue: 消息队列

        Returns:
            消费结果，True表示成功，False表示失败
        """
        start_time: float = time.time()
        success: bool = self._concurrent_consume_message(messages, message_queue)
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

        return success

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
            min_offset: int | None = self._remove_messages_from_cache(
                message_queue, messages
            )

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

    # ==================== 资源清理模块 ====================
    """
    资源清理模块负责消费者关闭时的资源释放和清理工作。
    确保所有资源都被正确释放，避免内存泄漏和资源浪费。
    主要方法:
    - _cleanup_on_start_failure(): 启动失败时的清理
    - _shutdown_thread_pools(): 关闭线程池
    - _cleanup_resources(): 清理内存资源
    """

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
        with self._assigned_queues_lock:
            self._assigned_queues.clear()

    # ==================== 远程通信模块 ====================
    """
    远程通信模块处理与Broker的双向通信，包括请求接收和响应处理。
    该模块注册了多个请求处理器，用于处理Broker发送的各种控制命令和消费请求。

    功能说明：
    1. 接收并处理来自Broker的远程请求
    2. 提供直接消费消息的调试接口
    3. 响应消费者ID变更通知
    4. 提供消费者运行信息查询接口

    注册的请求处理器：
    - NOTIFY_CONSUMER_IDS_CHANGED: 消费者组ID变更通知
    - CONSUME_MESSAGE_DIRECTLY: 直接消费消息请求（调试用）
    - GET_CONSUMER_RUNNING_INFO: 获取消费者运行信息请求

    方法说明：
    - _prepare_consumer_remote(): 注册所有远程请求处理器
    - _on_notify_consumer_ids_changed(): 处理消费者ID变更通知
    - _on_notify_consume_message_directly(): 处理直接消费请求的入口
    - _on_notify_consume_message_directly_internal(): 实际处理直接消费请求
    - _on_notify_get_consumer_running_info(): 处理运行信息查询（继承自基类）

    使用场景：
    1. 集群重平衡时接收Broker的消费者组变更通知
    2. 运维工具通过直接消费接口调试消息处理逻辑
    3. 监控系统查询消费者运行状态和统计信息
    """

    def _prepare_consumer_remote(self, pool: ConnectionPool) -> None:
        """
        注册消费者远程通信处理器。

        该方法在与Broker建立连接池时被调用，用于注册所有需要处理的远程请求。
        每个请求类型都有对应的处理方法，确保能够正确响应Broker的请求。

        Args:
            pool (ConnectionPool): 连接池，用于注册请求处理器

        注册的请求类型：
            1. NOTIFY_CONSUMER_IDS_CHANGED: 消费者组ID变更通知
               - 触发重平衡操作
               - 处理方法: _on_notify_consumer_ids_changed

            2. CONSUME_MESSAGE_DIRECTLY: 直接消费消息请求
               - 主要用于调试和测试
               - 处理方法: _on_notify_consume_message_directly

            3. GET_CONSUMER_RUNNING_INFO: 获取消费者运行信息
               - 用于监控和诊断
               - 处理方法: _on_notify_get_consumer_running_info（继承自基类）

        Note:
            - 这些处理器会在消费者与Broker建立连接时自动注册
            - 处理器运行在Netty的I/O线程中，应该避免耗时操作
            - 所有响应都通过RemotingCommand返回，保持协议一致性
        """
        pool.register_request_processor(
            RequestCode.NOTIFY_CONSUMER_IDS_CHANGED,
            self._on_notify_consumer_ids_changed,
        )
        pool.register_request_processor(
            RequestCode.CONSUME_MESSAGE_DIRECTLY,
            self._on_notify_consume_message_directly,
        )
        pool.register_request_processor(
            RequestCode.GET_CONSUMER_RUNNING_INFO,
            self._on_notify_get_consumer_running_info,
        )

    def _on_notify_consume_message_directly(
        self, command: RemotingCommand, _addr: tuple[str, int]
    ) -> RemotingCommand:
        """
        处理直接消费消息请求的入口方法。

        该方法是CONSUME_MESSAGE_DIRECTLY请求的处理入口，主要功能是：
        1. 解码请求头，获取消费者客户端ID
        2. 验证请求是否针对当前消费者实例
        3. 路由到内部处理方法或返回错误响应

        Args:
            command (RemotingCommand): 包含直接消费请求的命令对象
                - ext_fields: 包含ConsumeMessageDirectlyHeader的编码数据
                - body: 包含要消费的消息数据
            _addr (tuple[str, int]): 请求来源地址（IP地址和端口）
                - 参数名使用下划线前缀表示该参数在当前实现中未被使用

        Returns:
            RemotingCommand: 包含消费结果的响应命令对象
                - 成功时：返回SUCCESS状态码和消费结果
                - 失败时：返回ERROR状态码和错误信息

        处理流程：
            1. 解码请求头获取客户端ID
            2. 检查客户端ID是否匹配当前消费者
            3. 如果匹配，调用内部处理方法
            4. 如果不匹配，返回错误响应

        使用场景：
            - 运维工具调试消息处理逻辑
            - 消息轨迹跟踪
            - 问题排查和诊断

        Note:
            - 此接口主要用于调试，生产环境应谨慎使用
            - 请求处理是同步的，可能影响其他消息的消费
            - 需要确保client_id匹配以保证请求的安全性
        """
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
        """
        内部方法：实际处理直接消费消息请求。

        该方法负责：
        1. 验证消息体的有效性
        2. 解码消息并构建消息队列对象
        3. 调用消费者的并发消费方法处理消息
        4. 统计处理时间并返回详细结果

        Args:
            header (ConsumeMessageDirectlyHeader): 解码后的请求头
                - broker_name: Broker名称
                - 其他消费相关的元数据
            command (RemotingCommand): 原始命令对象
                - body: 包含要消费的消息的编码数据

        Returns:
            RemotingCommand: 包含详细消费结果的响应命令对象
                - 成功时：返回SUCCESS状态码、消费结果统计和处理时间
                - 失败时：返回ERROR状态码和具体的错误信息

        处理流程：
            1. 验证请求体是否存在（command.body）
            2. 解码消息列表（MessageExt.decode_messages）
            3. 构建MessageQueue对象，用于标识消息来源
            4. 为每条消息重置重试属性（调用_reset_retry）
            5. 调用并发消费方法处理消息（_concurrent_consume_message）
            6. 构建ConsumeMessageDirectlyResult包含处理结果
            7. 计算并记录处理耗时

        返回结果包含：
            - order: 是否顺序消费（并发消费始终为False）
            - auto_commit: 是否自动提交偏移量（始终为True）
            - consume_result: 消费结果（SUCCESS或失败状态）
            - remark: 处理结果的文字描述
            - spent_time_mills: 处理耗时（毫秒）

        使用场景：
            - 调试特定消息的处理逻辑
            - 验证消息监听器的正确性
            - 分析消息处理性能

        Note:
            - 该方法会实际调用注册的消息监听器
            - 处理是同步的，可能阻塞I/O线程
            - 消息会被正常处理，但不会影响正常的消费流程
            - 用于调试时应该控制使用频率，避免影响正常消费
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
