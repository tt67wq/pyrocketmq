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
import time
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, cast

# pyrocketmq导入
from pyrocketmq.broker import BrokerManager
from pyrocketmq.consumer.allocate_queue_strategy import (
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
from pyrocketmq.consumer.listener import MessageListenerConcurrently
from pyrocketmq.consumer.offset_store_factory import OffsetStoreFactory
from pyrocketmq.logging import get_logger
from pyrocketmq.model import MessageModel, TopicRouteData
from pyrocketmq.model.client_data import MessageSelector
from pyrocketmq.model.message_ext import MessageExt
from pyrocketmq.model.message_queue import MessageQueue
from pyrocketmq.nameserver import NameServerManager, create_nameserver_manager
from pyrocketmq.remote import DEFAULT_CONFIG

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
        >>> from pyrocketmq.consumer.listener import MessageListenerConcurrently, ConsumeResult
        >>>
        >>> class MyListener(MessageListenerConcurrently):
        ...     def consume_message_concurrently(self, messages, context):
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

        # 验证消息模型
        if self._config.message_model not in [
            MessageModel.CLUSTERING,
            MessageModel.BROADCASTING,
        ]:
            raise ValueError(f"Unsupported message model: {self._config.message_model}")

        # 初始化核心组件
        self._name_server_manager: NameServerManager = create_nameserver_manager(
            self._config.namesrv_addr
        )
        self._broker_manager: BrokerManager = BrokerManager(DEFAULT_CONFIG)

        # 创建偏移量存储
        self._offset_store = OffsetStoreFactory.create_offset_store(
            consumer_group=self._config.consumer_group,
            message_model=self._config.message_model,
            namesrv_manager=self._name_server_manager,
            broker_manager=self._broker_manager,
        )

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
        self._message_queue: queue.Queue[MessageQueue] = queue.Queue()
        self._process_queue: queue.Queue[tuple[list[MessageExt], MessageQueue]] = (
            queue.Queue()
        )

        # 状态管理
        self._pull_tasks: dict[MessageQueue, Future] = {}
        self._assigned_queues: dict[MessageQueue, int] = {}  # queue -> last_offset
        self._rebalance_interval: float = 20.0  # 重平衡间隔(秒)
        self._last_rebalance_time: float = 0.0

        # 统计信息
        self._stats: dict[str, Any] = {
            "messages_consumed": 0,
            "messages_failed": 0,
            "pull_requests": 0,
            "pull_successes": 0,
            "pull_failures": 0,
            "consume_duration_total": 0.0,
            "start_time": 0.0,
        }

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
        """
        启动并发消费者

        启动消费者的各个组件，包括网络连接、线程池、消息拉取等。

        Raises:
            ConsumerStartError: 启动失败时抛出
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

                if not isinstance(self._message_listener, MessageListenerConcurrently):
                    raise ConsumerStartError(
                        "ConcurrentConsumer requires MessageListenerConcurrently",
                        context={
                            "consumer_group": self._config.consumer_group,
                            "listener_type": type(self._message_listener).__name__,
                        },
                    )

                # 启动核心组件
                self._name_server_manager.start()
                self._broker_manager.start()
                self._offset_store.start()

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

                # 执行初始重平衡
                self._do_rebalance()

                # 启动消息拉取任务
                self._start_pull_tasks()

                # 启动消息处理任务
                self._start_consume_tasks()

                # 启动重平衡任务
                self._start_rebalance_task()

                self._is_running = True
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
        """
        停止并发消费者

        优雅地停止消费者，等待所有正在处理的消息完成，
        持久化偏移量，然后清理所有资源。

        Raises:
            ConsumerShutdownError: 停止过程中发生错误时抛出
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

                # 停止核心组件
                self._shutdown_core_components()

                # 清理资源
                self._cleanup_resources()

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
        """
        订阅Topic

        Args:
            topic: 要订阅的Topic名称
            selector: 消息选择器
        """
        super().subscribe(topic, selector)

        # 如果消费者正在运行，触发重平衡
        if self._is_running:
            self._trigger_rebalance()

    def unsubscribe(self, topic: str) -> None:
        """
        取消订阅Topic

        Args:
            topic: 要取消订阅的Topic名称
        """
        super().unsubscribe(topic)

        # 如果消费者正在运行，触发重平衡
        if self._is_running:
            self._trigger_rebalance()

    # ==================== 内部方法：重平衡管理 ====================

    def _do_rebalance(self) -> None:
        """
        执行重平衡操作

        根据当前订阅信息和集群状态，重新分配队列。
        """
        try:
            logger.debug(
                "Starting rebalance",
                extra={"consumer_group": self._config.consumer_group},
            )

            # 获取所有订阅的Topic
            topics: list[str] = self._subscription_manager.get_topics()
            if not topics:
                logger.debug("No topics subscribed, skipping rebalance")
                return

            # 获取所有Topic的路由信息
            all_queues: list[MessageQueue] = []
            for topic in topics:
                try:
                    route_data: TopicRouteData | None = (
                        self._name_server_manager.get_topic_route(topic)
                    )
                    if (
                        route_data
                        and hasattr(route_data, "message_queues")
                        and route_data.queue_data_list
                    ):
                        all_queues.extend(route_data.message_queues)
                except Exception as e:
                    logger.warning(
                        f"Failed to get route data for topic {topic}: {e}",
                        extra={
                            "consumer_group": self._config.consumer_group,
                            "topic": topic,
                        },
                    )

            if not all_queues:
                logger.debug("No queues available for subscribed topics")
                return

            # 执行队列分配（这里简化处理，实际需要获取所有消费者信息）
            allocated_queues: list[MessageQueue] = self._allocate_queues(all_queues)

            # 更新分配的队列
            self._update_assigned_queues(allocated_queues)

            self._last_rebalance_time = time.time()

            logger.info(
                "Rebalance completed",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "total_topics": len(topics),
                    "total_queues": len(all_queues),
                    "assigned_queues": len(allocated_queues),
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

    def _allocate_queues(self, all_queues: list[MessageQueue]) -> list[MessageQueue]:
        """
        分配队列（简化版本）

        Args:
            all_queues: 所有可用的队列列表

        Returns:
            list[MessageQueue]: 分配给当前消费者的队列列表
        """
        # 这里简化处理，实际应该根据消费者组信息进行分配
        # 暂时返回所有队列，后续可以集成完整的重平衡机制
        return all_queues.copy()

    def _update_assigned_queues(self, new_queues: list[MessageQueue]) -> None:
        """
        更新���配的队列

        Args:
            new_queues: 新分配的队列列表
        """
        with self._lock:
            old_queues: set[MessageQueue] = set(self._assigned_queues.keys())
            new_queue_set: set[MessageQueue] = set(new_queues)

            # 停止不再分配的队列的拉取任务
            removed_queues: set[MessageQueue] = old_queues - new_queue_set
            for queue in removed_queues:
                if queue in self._pull_tasks:
                    future: Future = self._pull_tasks.pop(queue)
                    if future and not future.done():
                        future.cancel()
                self._assigned_queues.pop(queue, None)

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
            # 在后台线程中执行重平衡
            if self._pull_executor:
                self._pull_executor.submit(self._do_rebalance)

    # ==================== 内部方法：消息拉取 ====================

    def _start_pull_tasks(self) -> None:
        """
        启动所有队列的消息拉取任务
        """
        if not self._pull_executor or not self._assigned_queues:
            return

        self._start_pull_tasks_for_queues(self._assigned_queues.keys())

    def _start_pull_tasks_for_queues(self, queues: set[MessageQueue]) -> None:
        """
        为指定队列启动拉取任务

        Args:
            queues: 要启动拉取任务的队列集合
        """
        for queue in queues:
            if queue not in self._pull_tasks:
                future = self._pull_executor.submit(self._pull_messages_loop, queue)
                self._pull_tasks[queue] = future

    def _stop_pull_tasks(self) -> None:
        """
        停止所有消息拉取任务
        """
        if not self._pull_tasks:
            return

        for queue, future in self._pull_tasks.items():
            if future and not future.done():
                future.cancel()

        self._pull_tasks.clear()

    def _pull_messages_loop(self, message_queue: MessageQueue) -> None:
        """
        持续拉取消息的循环

        Args:
            message_queue: 要拉取消息的队列
        """
        while self._is_running:
            try:
                # 获取当前偏移量
                current_offset: int = self._assigned_queues.get(message_queue, 0)

                # 拉取消息
                messages: list[MessageExt] = self._pull_messages(
                    message_queue, current_offset
                )

                if messages:
                    # 更新偏移量
                    last_message: MessageExt = messages[-1]
                    new_offset: int = last_message.queue_offset + len(messages)
                    self._assigned_queues[message_queue] = new_offset

                    # 将消息放入处理队列
                    self._process_queue.put((messages, message_queue))

                    self._stats["pull_successes"] += 1
                    self._stats["messages_consumed"] += len(messages)
                else:
                    self._stats["pull_requests"] += 1

                # 控制拉取频率
                if self._config.pull_interval > 0:
                    sleep_time: float = self._config.pull_interval / 1000.0
                    time.sleep(sleep_time)

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
                time.sleep(1.0)

    def _pull_messages(
        self, message_queue: MessageQueue, offset: int
    ) -> list[MessageExt]:
        """
        从指定队列拉取消息

        Args:
            message_queue: 消息队列
            offset: 起始偏移量

        Returns:
            list[MessageExt]: 拉取到的消息列表
        """
        try:
            self._stats["pull_requests"] += 1

            # 使用BrokerManager拉取消息
            with self._broker_manager.connection(
                message_queue.broker_name
            ) as broker_client:
                from pyrocketmq.broker.broker_client import PullResult

                result: PullResult | None = broker_client.pull_message(
                    consumer_group=self._config.consumer_group,
                    topic=message_queue.topic,
                    queue_id=message_queue.queue_id,
                    offset=offset,
                    max_num=self._config.pull_batch_size,
                )

                if result and result.messages:
                    return result.messages

                return []

        except Exception as e:
            logger.warning(
                f"Failed to pull messages from {message_queue}: {e}",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "topic": message_queue.topic,
                    "queue_id": message_queue.queue_id,
                    "offset": offset,
                    "error": str(e),
                },
            )
            raise MessageConsumeError(
                "Failed to pull messages",
                cause=e,
                context={
                    "consumer_group": self._config.consumer_group,
                    "topic": message_queue.topic,
                    "queue_id": message_queue.queue_id,
                    "offset": offset,
                },
            ) from e

    # ==================== 内部方法：消息处理 ====================

    def _start_consume_tasks(self) -> None:
        """
        启动消息处理任务
        """
        if not self._consume_executor:
            return

        # 启动多个消费任务
        for i in range(self._config.consume_thread_max):
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
                        last_message: MessageExt = messages[-1]
                        new_offset: int = last_message.queue_offset + len(messages)
                        self._offset_store.update_offset(message_queue, new_offset)
                    except Exception as e:
                        logger.warning(
                            f"Failed to update offset: {e}",
                            extra={
                                "consumer_group": self._config.consumer_group,
                                "topic": message_queue.topic,
                                "queue_id": message_queue.queue_id,
                                "error": str(e),
                            },
                        )

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

    # ==================== 内部方法：重平衡任务 ====================

    def _start_rebalance_task(self) -> None:
        """
        启动定期重平衡任务
        """
        if self._pull_executor:
            self._pull_executor.submit(self._rebalance_loop)

    def _rebalance_loop(self) -> None:
        """
        定期重平衡循环
        """
        while self._is_running:
            try:
                time.sleep(self._rebalance_interval)

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

    # ==================== 内部方法：资源清理 ====================

    def _cleanup_on_start_failure(self) -> None:
        """
        启动失败时的清理
        """
        try:
            self._shutdown_thread_pools()
            self._shutdown_core_components()
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
        关闭线程池
        """
        try:
            if self._pull_executor:
                self._pull_executor.shutdown(wait=False)
                self._pull_executor = None

            if self._consume_executor:
                self._consume_executor.shutdown(wait=False)
                self._consume_executor = None

        except Exception as e:
            logger.warning(
                f"Error shutting down thread pools: {e}",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "error": str(e),
                },
            )

    def _shutdown_core_components(self) -> None:
        """
        关闭核心组件
        """
        try:
            if hasattr(self, "_offset_store") and self._offset_store:
                self._offset_store.stop()

            if hasattr(self, "_broker_manager") and self._broker_manager:
                self._broker_manager.stop()

            if hasattr(self, "_name_server_manager") and self._name_server_manager:
                self._name_server_manager.stop()

        except Exception as e:
            logger.warning(
                f"Error shutting down core components: {e}",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "error": str(e),
                },
            )

    def _cleanup_resources(self) -> None:
        """
        清理资源
        """
        super()._cleanup_resources()

        # 清理队列
        if hasattr(self, "_message_queue"):
            while not self._message_queue.empty():
                try:
                    self._message_queue.get_nowait()
                except queue.Empty:
                    break

        if hasattr(self, "_process_queue"):
            while not self._process_queue.empty():
                try:
                    self._process_queue.get_nowait()
                except queue.Empty:
                    break

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
        }

    def get_stats(self) -> dict[str, Any]:
        """
        获取当前统计信息

        Returns:
            dict: 统计信息字典
        """
        return self._get_final_stats()
