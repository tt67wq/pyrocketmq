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
from abc import ABC, abstractmethod
from typing import Any

# pyrocketmq导入
from pyrocketmq.logging import get_logger
from pyrocketmq.model import ConsumeResult, MessageExt, MessageQueue, MessageSelector

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
        self._is_running: bool = False
        self._lock: threading.RLock = threading.RLock()

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
        pass

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
        pass

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

            # 清理订阅管理器
            if hasattr(self, "_subscription_manager") and self._subscription_manager:
                # 可以在这里保存订阅状态或其他清理操作
                pass

            # 清理消息监听器引用
            self._message_listener = None

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
