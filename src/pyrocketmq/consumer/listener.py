"""
消息监听器接口模块

定义RocketMQ Consumer的消息处理回调接口，支持顺序消费和并发消费两种模式。
提供灵活的消息处理机制，允许用户自定义消息消费逻辑。

MVP版本专注于核心功能，后续版本会扩展更多监听器类型。
"""

import time
from abc import ABC, abstractmethod
from typing import Any, Callable, override

from ..logging import get_logger
from ..model import ConsumeResult
from ..model.message_ext import MessageExt
from ..model.message_queue import MessageQueue

logger = get_logger(__name__)


class ConsumeContext:
    """
    消费上下文

    包含消息消费过程中的上下文信息，如队列信息、消费者组、
    消费次数等。这些信息可以帮助用户在消息处理时做出更合适的决策。

    MVP版本包含基础上下文信息，后续版本会扩展更多元数据。
    """

    def __init__(
        self,
        consumer_group: str,
        message_queue: MessageQueue,
        reconsume_times: int = 0,
    ) -> None:
        """
        初始化消费上下文

        Args:
            consumer_group: 消费者组名称
            message_queue: 消息队列信息
            reconsume_times: 重新消费次数
        """
        self.consumer_group: str = consumer_group
        self.message_queue: MessageQueue = message_queue
        self.reconsume_times: int = reconsume_times
        self.consume_start_time: float = time.time()
        self._attributes: dict[str, Any] = {}

    @property
    def topic(self) -> str:
        """获取主题名称"""
        return self.message_queue.topic

    @property
    def queue_id(self) -> int:
        """获取队列ID"""
        return self.message_queue.queue_id

    @property
    def broker_name(self) -> str:
        """获取Broker名称"""
        return self.message_queue.broker_name

    def set_attribute(self, key: str, value: Any) -> None:
        """设置自定义属性"""
        self._attributes[key] = value

    def get_attribute(self, key: str, default: Any | None = None) -> Any:
        """获取自定义属性"""
        return self._attributes.get(key, default)

    def get_consume_duration(self) -> float:
        """获取消费持续时间(秒)"""
        return time.time() - self.consume_start_time

    def __str__(self) -> str:
        """字符串表示"""
        return (
            f"ConsumeContext["
            f"group={self.consumer_group}, "
            f"topic={self.topic}, "
            f"queueId={self.queue_id}, "
            f"reconsume={self.reconsume_times}, "
            f"duration={self.get_consume_duration():.3f}s"
            f"]"
        )


class MessageListener(ABC):
    """
    消息监听器抽象基类

    定义消息处理的回调接口。用户需要继承这个类并实现
    consume_message方法来处理接收到的消息。

    设计原则:
    - 简单易用: 提供清晰的接口定义
    - 灵活处理: 支持批量消息处理
    - 异常安全: 完善的错误处理机制
    - 可扩展性: 预留扩展点

    使用示例:
        >>> class MyMessageListener(MessageListener):
        ...     def consume_message(self, messages, context):
        ...         for msg in messages:
        ...             print(f"处理消息: {msg.topic} - {msg.body}")
        ...         return ConsumeResult.SUCCESS
    """

    @abstractmethod
    def consume_message(
        self, messages: list[MessageExt], context: ConsumeContext
    ) -> ConsumeResult:
        """
        消费消息的回调方法

        当Consumer接收到消息时会调用这个方法。用户在这个方法中
        实现具体的消息处理逻辑。

        Args:
            messages: 接收到的消息列表(批量处理)
            context: 消费上下文，包含队列信息、消费组等元数据

        Returns:
            消费结果，决定消息如何被确认或重试

        Raises:
            Exception: 消息处理异常时会被Consumer捕获并处理

        Examples:
            >>> def consume_message(self, messages, context):
            ...     try:
            ...         for msg in messages:
            ...             # 处理业务逻辑
            ...             self.process_message(msg)
            ...         return ConsumeResult.SUCCESS
            ...     except Exception as e:
            ...         logger.error(f"消息处理失败: {e}")
            ...         return ConsumeResult.RECONSUME_LATER
        """
        pass

    def on_success(self, _messages: list[MessageExt], _context: ConsumeContext) -> None:
        """
        消息处理成功回调(可选实现)

        当消息成功处理并被确认后调用，可以用于记录成功日志或
        执行其他后续操作。

        Args:
            messages: 成功处理的消息列表
            context: 消费上下文
        """
        pass

    def on_exception(
        self,
        _messages: list[MessageExt],
        _context: ConsumeContext,
        _exception: Exception,
    ) -> None:
        """
        消息处理异常回调(可选实现)

        当消息处理过程中发生异常时调用，可以用于记录错误日志或
        执行异常恢复操作。

        Args:
            messages: 处理失败的消息列表
            context: 消费上下文
            exception: 发生的异常
        """
        pass


class SimpleMessageListener(MessageListener):
    """
    简单消息监听器

    提供最基础的消息处理功能，适合快速开发和测试场景。
    用户只需要提供一个处理函数即可，无需继承类。

    使用示例:
        >>> def process_message(msg):
        ...     print(f"处理消息: {msg.body.decode()}")
        ...     return ConsumeResult.SUCCESS
        >>>
        >>> listener = SimpleMessageListener(process_message)
    """

    def __init__(
        self, message_handler: Callable[[list[MessageExt]], ConsumeResult]
    ) -> None:
        """
        初始化简单消息监听器

        Args:
            message_handler: 消息处理函数，签名为:
                           function(messages: list[MessageExt]) -> ConsumeResult
        """
        if not callable(message_handler):
            raise ValueError("message_handler 必须是可调用对象")

        self.message_handler: Callable[[list[MessageExt]], ConsumeResult] = (
            message_handler
        )

    @override
    def consume_message(
        self, messages: list[MessageExt], context: ConsumeContext
    ) -> ConsumeResult:
        """
        使用用户提供的处理函数处理消息

        Args:
            messages: 消息列表
            context: 消费上下文

        Returns:
            消费结果
        """
        try:
            return self.message_handler(messages)
        except Exception as e:
            logger.error(
                f"简单消息监听器处理失败: {e}",
                extra={
                    "message_count": len(messages),
                    "topic": context.topic,
                    "error": str(e),
                },
                exc_info=True,
            )
            return ConsumeResult.RECONSUME_LATER


def create_message_listener(
    handler: Callable[[list[MessageExt]], ConsumeResult],
) -> MessageListener:
    """
    创建消息监听器的便利函数

    Args:
        handler: 消息处理函数

    Returns:
        消息监听器实例

    Examples:
        >>> # 创建并发监听器
        >>> def handle_messages(messages):
        ...     for msg in messages:
        ...         print(f"处理: {msg.body}")
        ...     return ConsumeResult.SUCCESS
        >>>
        >>> listener = create_message_listener(handle_messages)
        >>>
        >>> # 创建顺序监听器
        >>> def handle_messages(messages):
        ...     for msg in messages:
        ...         print(f"顺序处理: {msg.body}")
        ...     return ConsumeResult.SUCCESS
        >>>
        >>> listener = create_message_listener(
        ...     handle_messages,
        ... )
    """
    return SimpleMessageListener(handler)
