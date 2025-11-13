"""
BaseConsumer - 消费者抽象基类

BaseConsumer是pyrocketmq消费者模块的核心抽象基类，定义了所有消费者的
通用接口和基础功能。它提供统一的配置管理、消息监听器注册、订阅管理等
核心功能，为具体消费者实现（如ConcurrentConsumer、OrderlyConsumer等）
提供坚实的基础。

作者: pyrocketmq开发团队
"""

# 标准库导入
import threading
import time
from abc import ABC, abstractmethod
from typing import Any

# pyrocketmq导入
from pyrocketmq.broker import BrokerClient, BrokerManager
from pyrocketmq.consumer.offset_store import OffsetStore
from pyrocketmq.consumer.offset_store_factory import OffsetStoreFactory
from pyrocketmq.consumer.topic_broker_mapping import ConsumerTopicBrokerMapping
from pyrocketmq.logging import get_logger
from pyrocketmq.model import (
    BrokerData,
    ConsumerData,
    ConsumeResult,
    HeartbeatData,
    MessageExt,
    MessageQueue,
    MessageSelector,
)
from pyrocketmq.nameserver import NameServerManager, create_nameserver_manager
from pyrocketmq.remote import DEFAULT_CONFIG

# 本地模块导入
from .config import ConsumerConfig
from .errors import ConsumerError, SubscribeError
from .listener import ConsumeContext, MessageListener
from .subscription_manager import SubscriptionManager

logger = get_logger(__name__)


class BaseConsumer(ABC):
    """
    消费者抽象基类.

    定义了所有RocketMQ消费者的核心接口和基础功能。具体消费者实现
    （如并发消费者、顺序消费者等）需要继承这个类并实现其抽象方法.

    核心功能:
        - 配置管理: 统一的Consumer配置管理
        - 订阅管理: Topic订阅和消息选择器管理
        - 消息监听: 消息处理回调机制
        - 生命周期: 启动、停止等生命周期管理
        - 错误处理: 统一的异常处理和错误恢复

    设计原则:
        - 接口分离: 清晰定义各层职责
        - 可扩展性: 便于添加新的消费者类型
        - 线程安全: 所有公共操作保证线程安全
        - 资源管理: 完善的资源清理机制

    Attributes:
        _config: 消费者配置实例
        _subscription_manager: 订阅关系管理器
        _message_listener: 消息监听器实例
        _is_running: 消费者运行状态标志
        _lock: 线程安全锁

    Example:
        >>> class MyConsumer(BaseConsumer):
        ...     def __init__(self, config):
        ...         super().__init__(config)
        ...         # 初始化特定资源
        ...
        ...     def start(self):
        ...         # 实现具体的启动逻辑
        ...         pass
        ...
        ...     def shutdown(self):
        ...         # 实现具体的停止逻辑
        ...         pass
    """

    def __init__(self, config: ConsumerConfig) -> None:
        """
        初始化BaseConsumer实例.

        创建消费者实例并初始化核心组件，包括订阅管理器、消息监听器存储等。
        验证配置的有效性并进行必要的初始化设置.

        Args:
            config: 消费者配置，包含所有必要的配置参数

        Raises:
            ValueError: 当config为None时抛出

        Note:
            初始化完成后，消费者处于停止状态，需要调用start()方法启动消费。
        """
        if not config:
            raise ValueError("ConsumerConfig cannot be None")

        self._config: ConsumerConfig = config
        self._subscription_manager: SubscriptionManager = SubscriptionManager()
        self._message_listener: MessageListener | None = None
        self._topic_broker_mapping: ConsumerTopicBrokerMapping = (
            ConsumerTopicBrokerMapping()
        )

        # 初始化核心组件
        self._name_server_manager: NameServerManager = create_nameserver_manager(
            self._config.namesrv_addr
        )
        self._broker_manager: BrokerManager = BrokerManager(DEFAULT_CONFIG)
        # 创建偏移量存储
        self._offset_store: OffsetStore = OffsetStoreFactory.create_offset_store(
            consumer_group=self._config.consumer_group,
            message_model=self._config.message_model,
            namesrv_manager=self._name_server_manager,
            broker_manager=self._broker_manager,
        )

        self._is_running: bool = False
        self._lock: threading.RLock = threading.RLock()

        # 路由刷新定时任务
        self._route_refresh_interval: int = 30000  # 30秒刷新一次路由信息
        self._route_refresh_event: threading.Event = (
            threading.Event()
        )  # 用于路由刷新循环的事件
        self._route_refresh_thread: threading.Thread | None = None  # 路由刷新线程

        # 心跳任务管理
        self._heartbeat_interval: float = getattr(
            config, "heartbeat_interval", 30.0
        )  # 心跳间隔(秒)
        self._last_heartbeat_time: float = 0.0
        self._heartbeat_thread: threading.Thread | None = None
        self._heartbeat_event: threading.Event = threading.Event()  # 用于心跳循环的事件

        # 统计信息
        self._stats: dict[str, Any] = {
            "messages_consumed": 0,
            "messages_failed": 0,
            "pull_requests": 0,
            "pull_successes": 0,
            "pull_failures": 0,
            "consume_duration_total": 0.0,
            "start_time": 0.0,
            "heartbeat_success_count": 0,
            "heartbeat_failure_count": 0,
            "route_refresh_count": 0,  # 路由刷新次数统计
            "route_refresh_success_count": 0,  # 路由刷新成功次数
            "route_refresh_failure_count": 0,  # 路由刷新失败次数
            "rebalance_count": 0,  # 重平衡次数统计
            "rebalance_success_count": 0,  # 重平衡成功次数
            "rebalance_failure_count": 0,  # 重平衡失败次数
            "rebalance_skipped_count": 0,  # 跳过重平衡次数统计
        }

        logger.info(
            "Initializing BaseConsumer",
            extra={
                "consumer_group": self._config.consumer_group,
                "client_id": self._config.client_id,
                "namesrv_addr": self._config.namesrv_addr,
            },
        )

    # ==================== 核心生命周期方法 ====================

    @abstractmethod
    def start(self) -> None:
        """
        启动消费者.

        子类必须实现这个方法，用于启动消息消费的各个组件。
        典型实现包括:
            - 启动网络连接
            - 注册到NameServer
            - 启动消费线程
            - 开始拉取消息

        Raises:
            ConsumerError: 启动失败时抛出

        Note:
            启动前需要确保已注册消息监听器和订阅了必要的Topic。
        """
        self._name_server_manager.start()
        self._broker_manager.start()
        self._offset_store.start()

        # 启动路由刷新任务
        self._start_route_refresh_task()

        # 启动心跳任务
        self._start_heartbeat_task()

    @abstractmethod
    def shutdown(self) -> None:
        """
        停止消费者.

        子类必须实现这个方法，用于优雅地停止消费者。
        典型实现包括:
            - 停止拉取消息
            - 等待正在处理的消息完成
            - 持久化偏移量
            - 关闭网络连接
            - 清理资源

        Raises:
            ConsumerError: 停止过程中发生错误时抛出

        Note:
            这是一个阻塞操作，会等待所有正在处理的消息完成。
        """

        self._route_refresh_event.set()
        self._heartbeat_event.set()

        self._shutdown_thread_pools()
        self._cleanup_resources()

    # ==================== 订阅管理方法 ====================

    def subscribe(self, topic: str, selector: MessageSelector) -> None:
        """
        订阅Topic.

        向消费者注册对指定Topic的订阅关系。消费者只会接收到订阅Topic的消息，
        可以通过MessageSelector进行消息过滤。

        Args:
            topic: 要订阅的Topic名称，不能为空字符串
            selector: 消息选择器，用于过滤消息，不能为None

        Raises:
            ConsumerError: 当消费者正在运行或订阅失败时抛出
            ValueError: 当topic为空或selector为None时抛出

        Example:
            >>> # 使用TAG过滤
            >>> from pyrocketmq.model.client_data import create_tag_selector
            >>> consumer.subscribe("order_topic", create_tag_selector("pay||ship"))
            >>>
            >>> # 使用SQL92过滤（如果支持）
            >>> consumer.subscribe("order_topic", create_sql_selector("amount > 100"))
            >>>
            >>> # 订阅所有消息
            >>> consumer.subscribe("log_topic", create_tag_selector("*"))

        Note:
            必须在消费者启动前调用此方法。
        """
        if self._is_running:
            raise ConsumerError(
                "Cannot subscribe while consumer is running. Please shutdown first or subscribe before starting.",
            )

        if not topic:
            raise ValueError("Topic must be a non-empty string")

        if not selector:
            raise ValueError("MessageSelector cannot be None")

        try:
            logger.info(
                f"Subscribing to topic: {topic}",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "topic": topic,
                    "selector_type": selector.type,
                    "selector_expression": selector.expression,
                },
            )

            success: bool = self._subscription_manager.subscribe(topic, selector)
            if success:
                logger.info(
                    f"Successfully subscribed to topic: {topic}",
                    extra={
                        "consumer_group": self._config.consumer_group,
                        "topic": topic,
                        "total_subscriptions": self._subscription_manager.get_subscription_count(),
                    },
                )
            else:
                raise SubscribeError(topic, "Failed to subscribe to topic")

        except Exception as e:
            logger.error(
                f"Failed to subscribe to topic {topic}: {e}",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "topic": topic,
                    "error": str(e),
                },
                exc_info=True,
            )
            raise e

    def unsubscribe(self, topic: str) -> None:
        """
        取消订阅Topic.

        移除对指定Topic的订阅关系。取消订阅后，消费者将不再接收该Topic的消息。

        Args:
            topic: 要取消订阅的Topic名称，不能为空字符串

        Raises:
            ConsumerError: 当消费者正在运行或取消订阅失败时抛出
            ValueError: 当topic为空时抛出

        Example:
            >>> consumer.unsubscribe("order_topic")

        Note:
            必须在消费者停止后调用此方法。如果Topic未订阅，此方法不会抛出异常。
        """
        if self._is_running:
            raise ConsumerError(
                "Cannot unsubscribe while consumer is running. Please shutdown first.",
                context={"consumer_group": self._config.consumer_group},
            )

        if not topic:
            raise ValueError("Topic must be a non-empty string")

        try:
            logger.info(
                f"Unsubscribing from topic: {topic}",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "topic": topic,
                },
            )

            success: bool = self._subscription_manager.unsubscribe(topic)
            if success:
                logger.info(
                    f"Successfully unsubscribed from topic: {topic}",
                    extra={
                        "consumer_group": self._config.consumer_group,
                        "topic": topic,
                        "total_subscriptions": self._subscription_manager.get_subscription_count(),
                    },
                )
            else:
                logger.warning(
                    f"Topic not subscribed: {topic}",
                    extra={
                        "consumer_group": self._config.consumer_group,
                        "topic": topic,
                    },
                )

        except Exception as e:
            logger.error(
                f"Failed to unsubscribe from topic {topic}: {e}",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "topic": topic,
                    "error": str(e),
                },
                exc_info=True,
            )
            raise ConsumerError(
                "Unsubscription failed for topic",
                cause=e,
                context={
                    "consumer_group": self._config.consumer_group,
                    "topic": topic,
                },
            ) from e

    def get_subscribed_topics(self) -> list[str]:
        """
        获取已订阅的Topic列表.

        Returns:
            list[str]: 已订阅的Topic名称列表，如果没有订阅则返回空列表

        Example:
            >>> topics = consumer.get_subscribed_topics()
            >>> print(f"Subscribed topics: {topics}")
        """
        return self._subscription_manager.get_topics()

    def is_subscribed(self, topic: str) -> bool:
        """
        检查是否已订阅指定Topic.

        Args:
            topic: 要检查的Topic名称

        Returns:
            bool: 如果已订阅返回True，否则返回False

        Example:
            >>> if consumer.is_subscribed("order_topic"):
            ...     print("Already subscribed to order_topic")
        """
        return self._subscription_manager.is_subscribed(topic)

    # ==================== 消息监听器管理 ====================

    def register_message_listener(self, listener: MessageListener) -> None:
        """
        注册消息监听器.

        设置用于处理接收到的消息的回调函数。当消费者接收到消息时，
        会调用监听器的consume_message方法进行处理。

        Args:
            listener: 消息监听器实例，必须继承自MessageListener

        Raises:
            ConsumerError: 当消费者正在运行或注册失败时抛出
            ValueError: 当listener为None或不是MessageListener实例时抛出

        Example:
            >>> from pyrocketmq.consumer.listener import MessageListener, ConsumeResult
            >>>
            >>> class MyListener(MessageListener):
            ...     def consume_message(self, messages, context):
            ...         for msg in messages:
            ...             print(f"Processing message: {msg.body.decode()}")
            ...         return ConsumeResult.SUCCESS
            >>>
            >>> consumer.register_message_listener(MyListener())

        Note:
            必须在消费者启动前调用此方法。一个消费者只能注册一个监听器。
        """
        if self._is_running:
            raise ConsumerError(
                "Cannot register message listener while consumer is running. Please shutdown first or register before starting.",
                context={"consumer_group": self._config.consumer_group},
            )

        if not listener:
            raise ValueError("MessageListener cannot be None")

        if not isinstance(listener, MessageListener):
            raise ValueError("Listener must be an instance of MessageListener")

        try:
            self._message_listener = listener
            logger.info(
                "Message listener registered successfully",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "listener_type": type(listener).__name__,
                },
            )
        except Exception as e:
            logger.error(
                f"Failed to register message listener: {e}",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "listener_type": type(listener).__name__,
                    "error": str(e),
                },
                exc_info=True,
            )
            raise ConsumerError(
                "Failed to register message listener",
                context={"consumer_group": self._config.consumer_group},
                cause=e,
            ) from e

    # ==================== 消息处理核心方法 ====================

    def _consume_message(
        self, messages: list[MessageExt], message_queue: MessageQueue
    ) -> bool:
        """
        处理接收到的消息（内部方法）

        这是消息处理的核心方法，由具体的消费者实现调用。
        它负责创建消费上下文，调用注册的消息监听器，并处理消费结果。

        Args:
            messages: 要处理的消息列表
            message_queue: 消息来自的队列

        Returns:
            bool: 消费处理是否成功

        Raises:
            ConsumerError: 消息处理过程中发生严重错误时抛出
        """
        if not self._message_listener:
            logger.error(
                "No message listener registered",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "message_count": len(messages),
                    "topic": message_queue.topic,
                    "queue_id": message_queue.queue_id,
                },
            )
            return False

        if not messages:
            logger.warning(
                "Empty message list received",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "topic": message_queue.topic,
                    "queue_id": message_queue.queue_id,
                },
            )
            return True  # 空消息列表视为处理成功

        # 创建消费上下文
        reconsume_times: int = messages[0].reconsume_times if messages else 0
        context: ConsumeContext = ConsumeContext(
            consumer_group=self._config.consumer_group,
            message_queue=message_queue,
            reconsume_times=reconsume_times,
        )

        try:
            logger.info(
                "Processing messages",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "message_count": len(messages),
                    "topic": context.topic,
                    "queue_id": context.queue_id,
                    "reconsume_times": reconsume_times,
                },
            )

            result: ConsumeResult = self._message_listener.consume_message(
                messages, context
            )
            if result in [
                ConsumeResult.COMMIT,
                ConsumeResult.ROLLBACK,
                ConsumeResult.SUSPEND_CURRENT_QUEUE_A_MOMENT,
            ]:
                logger.error(
                    "Invalid result for concurrent consumer",
                    extra={
                        "consumer_group": self._config.consumer_group,
                        "topic": context.topic,
                        "queue_id": context.queue_id,
                        "result": result.value,
                    },
                )
                return False

            logger.info(
                "Message processing completed",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "message_count": len(messages),
                    "topic": context.topic,
                    "queue_id": context.queue_id,
                    "result": result.value,
                    "duration": context.get_consume_duration(),
                },
            )

            return result == ConsumeResult.SUCCESS

        except Exception as e:
            logger.error(
                f"Failed to process messages: {e}",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "message_count": len(messages),
                    "topic": context.topic,
                    "queue_id": context.queue_id,
                    "error": str(e),
                },
                exc_info=True,
            )

            # 调用异常回调（如果监听器实现了）
            if hasattr(self._message_listener, "on_exception"):
                try:
                    self._message_listener.on_exception(messages, context, e)
                except Exception as callback_error:
                    logger.error(
                        f"Exception callback failed: {callback_error}",
                        extra={
                            "consumer_group": self._config.consumer_group,
                            "error": str(callback_error),
                        },
                        exc_info=True,
                    )

            return False

    # ==================== 状态查询方法 ====================

    def is_running(self) -> bool:
        """
        检查消费者是否正在运行

        Returns:
            bool: 如果消费者正在运行返回True，否则返回False
        """
        return self._is_running

    def get_config(self) -> ConsumerConfig:
        """
        获取消费者配置

        Returns:
            ConsumerConfig: 消费者配置实例
        """
        return self._config

    def get_subscription_manager(self) -> SubscriptionManager:
        """
        获取订阅管理器（主要用于内部使用和测试）

        Returns:
            SubscriptionManager: 订阅管理器实例
        """
        return self._subscription_manager

    def get_message_listener(self) -> MessageListener | None:
        """
        获取当前注册的消息监听器

        Returns:
            MessageListener | None: 当前注册的监听器，如果未注册则返回None
        """
        return self._message_listener

    # ==================== 状态摘要方法 ====================

    def get_status_summary(self) -> dict[str, Any]:
        """
        获取消费者状态摘要

        Returns:
            dict: 包含消费者状态信息的字典
        """
        subscription_status: dict[str, Any] = (
            self._subscription_manager.get_status_summary()
        )

        return {
            "consumer_group": self._config.consumer_group,
            "client_id": self._config.client_id,
            "namesrv_addr": self._config.namesrv_addr,
            "is_running": self._is_running,
            "message_model": self._config.message_model,
            "consume_from_where": self._config.consume_from_where,
            "allocate_queue_strategy": self._config.allocate_queue_strategy,
            "has_listener": self._message_listener is not None,
            "listener_type": type(self._message_listener).__name__
            if self._message_listener
            else None,
            "subscription_status": subscription_status,
        }

    # ==================== 资源清理方法 ====================

    def _shutdown_thread_pools(self) -> None:
        """
        关闭线程池和专用线程
        """
        try:
            # 等待专用线程结束
            self._route_refresh_event.set()  # 唤醒重平衡线程
            self._heartbeat_event.set()  # 唤醒心跳线程

            # 等待线程结束
            threads_to_join: list[threading.Thread] = []

            if self._heartbeat_thread and self._heartbeat_thread.is_alive():
                threads_to_join.append(self._heartbeat_thread)
            if self._route_refresh_thread and self._route_refresh_thread.is_alive():
                threads_to_join.append(self._route_refresh_thread)

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
        清理资源（内部方法）

        由shutdown方法调用，负责清理消费者使用的所有资源。
        子类可以重写这个方法来添加特定的资源清理逻辑。

        注意:
            - 必须调用super()._cleanup_resources()来确保基类资源也被清理
            - 应该处理所有可能的异常，避免资源清理失败
        """
        try:
            logger.info(
                "Cleaning up BaseConsumer resources",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "client_id": self._config.client_id,
                },
            )

            # 清理核心组件
            try:
                # 1. 清理偏移量存储 - 优先清理，确保持久化完成
                if hasattr(self, "_offset_store") and self._offset_store:
                    try:
                        # 尝试持久化未提交的偏移量
                        if hasattr(self._offset_store, "persist_all"):
                            self._offset_store.persist_all()
                        # 关闭偏移量存储
                        if hasattr(self._offset_store, "stop"):
                            self._offset_store.stop()

                        logger.info("OffsetStore cleaned up successfully")
                    except Exception as e:
                        logger.warning(f"Error cleaning up offset_store: {e}")

                # 2. 清理订阅管理器 - 在偏移量存储之后清理
                if (
                    hasattr(self, "_subscription_manager")
                    and self._subscription_manager
                ):
                    try:
                        # 保存订阅状态和清理非活跃订阅
                        if hasattr(
                            self._subscription_manager, "cleanup_inactive_subscriptions"
                        ):
                            self._subscription_manager.cleanup_inactive_subscriptions()

                        # 清理订阅数据
                        if hasattr(self._subscription_manager, "clear_all"):
                            self._subscription_manager.clear_all()

                        logger.info("SubscriptionManager cleaned up successfully")
                    except Exception as e:
                        logger.warning(f"Error cleaning up subscription_manager: {e}")

                # 3. 清理Topic到Broker的映射(Need not to)

                # 4. 清理Broker管理器 - 在依赖组件之后清理
                if hasattr(self, "_broker_manager") and self._broker_manager:
                    try:
                        self._broker_manager.shutdown()
                        logger.info("BrokerManager shutdown successfully")
                    except Exception as e:
                        logger.warning(f"Error shutting down broker_manager: {e}")

                # 5. 清理NameServer管理器 - 最后清理网络连接
                if hasattr(self, "_name_server_manager") and self._name_server_manager:
                    try:
                        self._name_server_manager.stop()
                        logger.info("NameServerManager stopped successfully")
                    except Exception as e:
                        logger.warning(f"Error stopping name_server_manager: {e}")

                # 6. 清理消息监听器引用
                self._message_listener = None

                # 7. 重置运行状态
                self._is_running = False

                logger.info("All core components cleaned up successfully")

            except Exception as cleanup_error:
                logger.error(f"Error during core components cleanup: {cleanup_error}")
                raise

            logger.info(
                "BaseConsumer resources cleaned up successfully",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "client_id": self._config.client_id,
                },
            )

        except Exception as e:
            logger.error(
                f"Error during BaseConsumer resource cleanup: {e}",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "error": str(e),
                },
                exc_info=True,
            )

    # ==================== 内部方法：路由刷新 ====================

    def _start_route_refresh_task(self) -> None:
        """启动路由刷新任务"""
        self._route_refresh_thread = threading.Thread(
            target=self._route_refresh_loop,
            name=f"{self._config.consumer_group}-route-refresh-thread",
            daemon=True,
        )
        self._route_refresh_thread.start()

    def _route_refresh_loop(self) -> None:
        """路由刷新循环

        定期刷新所有订阅Topic的路由信息，确保消费者能够感知到集群拓扑的变化。
        """
        logger.info(
            "Route refresh loop started",
            extra={
                "consumer_group": self._config.consumer_group,
                "refresh_interval": self._route_refresh_interval,
            },
        )

        self._refresh_all_routes()

        while self._is_running:
            try:
                # 等待指定间隔或关闭事件
                if self._route_refresh_event.wait(
                    timeout=self._route_refresh_interval / 1000
                ):
                    # 收到事件信号，退出循环
                    break

                # 检查是否正在关闭
                if self._route_refresh_event.is_set():
                    break

                # 执行路由刷新
                self._refresh_all_routes()
                _ = self._topic_broker_mapping.clear_expired_routes()

                # 更新统计信息
                self._stats["route_refresh_count"] += 1
                self._stats["route_refresh_success_count"] += 1

                logger.debug(
                    "Route refresh completed",
                    extra={
                        "consumer_group": self._config.consumer_group,
                        "refresh_count": self._stats["route_refresh_count"],
                        "topics_count": len(
                            self._topic_broker_mapping.get_all_topics()
                        ),
                    },
                )

            except Exception as e:
                self._stats["route_refresh_failure_count"] += 1
                logger.warning(
                    f"Error in route refresh loop: {e}",
                    extra={
                        "consumer_group": self._config.consumer_group,
                        "error": str(e),
                        "refresh_count": self._stats["route_refresh_count"],
                        "failure_count": self._stats["route_refresh_failure_count"],
                    },
                    exc_info=True,
                )

                # 发生异常时，等待较短时间后重试
                self._route_refresh_event.wait(timeout=5.0)

        logger.info(
            "Route refresh loop stopped",
            extra={
                "consumer_group": self._config.consumer_group,
                "total_refreshes": self._stats["route_refresh_count"],
                "success_count": self._stats["route_refresh_success_count"],
                "failure_count": self._stats["route_refresh_failure_count"],
            },
        )

    def _refresh_all_routes(self) -> None:
        """刷新所有Topic的路由信息"""
        topics = list(self._topic_broker_mapping.get_all_topics())

        for topic in topics:
            try:
                if self._topic_broker_mapping.get_route_info(topic) is None:
                    _ = self._update_route_info(topic)
            except Exception as e:
                logger.debug(
                    "Failed to refresh route",
                    extra={
                        "topic": topic,
                        "error": str(e),
                    },
                )

    def _update_route_info(self, topic: str) -> bool:
        """更新Topic路由信息

        手动触发Topic路由信息的更新。通常情况下，路由信息会自动更新，
        但在某些特殊场景下可能需要手动触发更新。

        Args:
            topic: 要更新的Topic名称

        Returns:
            bool: 更新是否成功
        """
        logger.info(
            "Updating route info for topic",
            extra={
                "topic": topic,
            },
        )
        topic_route_data = self._name_server_manager.get_topic_route(topic)
        if not topic_route_data:
            logger.error(
                "Failed to get topic route data",
                extra={
                    "topic": topic,
                },
            )
            return False

        # 维护broker连接
        for broker_data in topic_route_data.broker_data_list:
            for idx, broker_addr in broker_data.broker_addresses.items():
                logger.info(
                    "Adding broker",
                    extra={
                        "broker_id": idx,
                        "broker_address": broker_addr,
                        "broker_name": broker_data.broker_name,
                    },
                )
                self._broker_manager.add_broker(
                    broker_addr,
                    broker_data.broker_name,
                )

        # 更新本地缓存
        success = self._topic_broker_mapping.update_route_info(topic, topic_route_data)
        if success:
            logger.info(
                "Route info updated for topic",
                extra={
                    "topic": topic,
                    "brokers_count": len(topic_route_data.broker_data_list),
                },
            )
            return True
        else:
            logger.warning(
                "Failed to update route cache for topic",
                extra={"topic": topic},
            )

        # 如果所有NameServer都失败，强制刷新缓存
        return self._topic_broker_mapping.force_refresh(topic)

    # ==================== 内部方法：心跳 ====================

    def _start_heartbeat_task(self) -> None:
        """启动心跳任务"""
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop,
            name=f"{self._config.consumer_group}-heartbeat-thread",
            daemon=True,
        )
        self._heartbeat_thread.start()

    def _heartbeat_loop(self) -> None:
        """消费者心跳发送循环。

        定期向所有相关的Broker发送心跳包，维持消费者与Broker的连接状态。
        心跳机制确保Broker能够感知消费者的在线状态，及时进行重平衡操作。

        执行流程：
        1. 计算距离下次心跳的等待时间
        2. 使用事件等待机制，支持优雅停止
        3. 到达心跳时间时，向所有Broker发送心跳
        4. 记录心跳统计信息

        Args:
            None

        Returns:
            None

        Raises:
            None: 此方法会捕获所有异常并记录日志

        Note:
            - 默认心跳间隔为30秒，可通过配置调整
            - 使用Event.wait()替代time.sleep()，支持快速响应停止请求
            - 心跳失败不会影响消费者的正常运行
            - 统计心跳成功/失败次数用于监控连接健康状态
            - 消费者停止时会立即退出心跳循环
        """
        logger.info("Heartbeat loop started")

        self._send_heartbeat_to_all_brokers()
        while self._is_running:
            try:
                current_time = time.time()

                # 计算到下一次心跳的等待时间
                time_until_next_heartbeat: float = self._heartbeat_interval - (
                    current_time - self._last_heartbeat_time
                )

                # 如果还没到心跳时间，等待一小段时间或直到被唤醒
                if time_until_next_heartbeat > 0:
                    # 使用Event.wait()替代time.sleep()
                    wait_timeout: float = min(
                        time_until_next_heartbeat, 1.0
                    )  # 最多等待1秒
                    if self._heartbeat_event.wait(timeout=wait_timeout):
                        # Event被触发，检查是否需要退出
                        if not self._is_running:
                            break
                        # 重置事件状态
                        self._heartbeat_event.clear()
                        continue  # 重新计算等待时间

                # 检查是否需要发送心跳
                current_time = time.time()  # 重新获取当前时间
                if current_time - self._last_heartbeat_time >= self._heartbeat_interval:
                    self._send_heartbeat_to_all_brokers()
                    self._last_heartbeat_time = current_time

            except Exception as e:
                logger.error(
                    f"Error in heartbeat loop: {e}",
                    extra={
                        "consumer_group": self._config.consumer_group,
                        "error": str(e),
                    },
                    exc_info=True,
                )
                # 等待一段时间再重试，使用Event.wait()
                if self._heartbeat_event.wait(timeout=5.0):
                    if not self._is_running:
                        break
                    self._heartbeat_event.clear()

        logger.info("Heartbeat loop stopped")

    def _send_heartbeat_to_all_brokers(self) -> None:
        """向所有Broker发送心跳"""
        logger.debug("Sending heartbeat to all brokers...")

        try:
            # 获取所有已知的Broker地址
            broker_addrs: set[str] = set()
            all_topics: set[str] = self._topic_broker_mapping.get_all_topics()

            for topic in all_topics:
                brokers: list[BrokerData] = (
                    self._topic_broker_mapping.get_available_brokers(topic)
                )
                for broker_data in brokers:
                    # 获取主从地址
                    if broker_data.broker_addresses:
                        for addr in broker_data.broker_addresses.values():
                            if addr:  # 过滤空地址
                                broker_addrs.add(addr)

            if not broker_addrs:
                logger.debug("No broker addresses found for heartbeat")
                return

            heartbeat_data = HeartbeatData(
                client_id=self._config.client_id,
                consumer_data_set=[
                    ConsumerData(
                        group_name=self._config.consumer_group,
                        consume_type="CONSUME_PASSIVELY",
                        message_model=self._config.message_model,
                        consume_from_where=self._config.consume_from_where,
                        subscription_data=[
                            e.subscription_data
                            for e in self._subscription_manager.get_all_subscriptions()
                        ],
                    )
                ],
            )

            # 向每个Broker发送心跳
            heartbeat_success_count: int = 0
            heartbeat_failure_count: int = 0

            for broker_addr in broker_addrs:
                try:
                    # 创建Broker客户端连接
                    with self._broker_manager.connection(broker_addr) as conn:
                        # 发送心跳请求
                        BrokerClient(conn).send_heartbeat(heartbeat_data)

                except Exception as e:
                    heartbeat_failure_count += 1
                    logger.warning(
                        f"Failed to send heartbeat to broker {broker_addr}: {e}",
                        extra={
                            "broker_addr": broker_addr,
                            "consumer_group": self._config.consumer_group,
                            "error": str(e),
                        },
                    )

            # 更新统计信息
            self._stats["heartbeat_success_count"] = (
                self._stats.get("heartbeat_success_count", 0) + heartbeat_success_count
            )
            self._stats["heartbeat_failure_count"] = (
                self._stats.get("heartbeat_failure_count", 0) + heartbeat_failure_count
            )

            if heartbeat_success_count > 0 or heartbeat_failure_count > 0:
                logger.info(
                    f"Heartbeat summary: {heartbeat_success_count} success, "
                    f"{heartbeat_failure_count} failure",
                    extra={
                        "consumer_group": self._config.consumer_group,
                        "heartbeat_success_count": heartbeat_success_count,
                        "heartbeat_failure_count": heartbeat_failure_count,
                        "total_brokers": len(broker_addrs),
                    },
                )

        except Exception as e:
            logger.error(
                f"Error sending heartbeat to brokers: {e}",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "error": str(e),
                },
                exc_info=True,
            )

    # ==================== 字符串表示方法 ====================

    def __str__(self) -> str:
        """
        返回消费者的字符串表示

        Returns:
            str: 消费者的简洁字符串表示
        """
        return (
            f"BaseConsumer["
            f"group={self._config.consumer_group}, "
            f"running={self._is_running}, "
            f"subscriptions={self._subscription_manager.get_subscription_count()}"
            f"]"
        )

    def __repr__(self) -> str:
        """
        返回消费者的详细字符串表示

        Returns:
            str: 消费者的详细字符串表示
        """
        status = "RUNNING" if self._is_running else "STOPPED"
        return (
            f"BaseConsumer("
            f"group='{self._config.consumer_group}', "
            f"client_id='{self._config.client_id}', "
            f"namesrv='{self._config.namesrv_addr}', "
            f"model='{self._config.message_model}', "
            f"status='{status}', "
            f"subscriptions={self._subscription_manager.get_subscription_count()}, "
            f"listener={'Registered' if self._message_listener else 'None'}"
            f")"
        )
