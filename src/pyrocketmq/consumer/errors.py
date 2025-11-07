"""
Consumer异常定义模块

定义RocketMQ Consumer相关的异常类型，提供精确的错误分类和
详细的错误信息。这些异常类型可以帮助用户快速定位问题
并采取相应的处理措施。

异常设计原则:
- 精确分类: 不同类型的错误使用不同的异常类
- 信息完整: 提供详细的错误上下文信息
- 可处理性: 异常应该包含足够的恢复信息
- MVP优先: 先实现核心异常，后续逐步扩展
"""

from typing import Any


class ConsumerError(Exception):
    """
    Consumer异常基类

    所有Consumer相关异常的基类，提供统一的错误处理接口。
    包含错误代码、错误消息和上下文信息。
    """

    def __init__(
        self,
        message: str,
        error_code: str | None = None,
        context: dict[str, Any] | None = None,
        cause: Exception | None = None,
    ):
        """
        初始化Consumer异常

        Args:
            message: 错误消息
            error_code: 错误代码(可选)
            context: 错误上下文信息(可选)
            cause: 原因异常(可选)
        """
        super().__init__(message)
        self.message: str = message
        self.error_code: str | None = error_code
        self.context: dict[str, Any] = context or {}
        self.cause: Exception | None = cause

    def to_dict(self) -> dict[str, Any]:
        """转换为字典格式"""
        return {
            "error_type": self.__class__.__name__,
            "message": self.message,
            "error_code": self.error_code,
            "context": self.context,
            "cause": str(self.cause) if self.cause else None,
        }

    def __str__(self) -> str:
        """字符串表示"""
        if self.error_code:
            return f"[{self.error_code}] {self.message}"
        return self.message


class ConsumerStartError(ConsumerError):
    """
    Consumer启动异常

    当Consumer启动过程中发生错误时抛出，如配置错误、
    网络连接失败、权限问题等。
    """

    def __init__(self, message: str, cause: Exception | None = None):
        super().__init__(
            message=message, error_code="CONSUMER_START_ERROR", cause=cause
        )


class ConsumerShutdownError(ConsumerError):
    """
    Consumer关闭异常

    当Consumer关闭过程中发生错误时抛出，如资源释放失败、
    网络断开异常等。
    """

    def __init__(self, message: str, cause: Exception | None = None):
        super().__init__(
            message=message, error_code="CONSUMER_SHUTDOWN_ERROR", cause=cause
        )


class ConsumerStateError(ConsumerError):
    """
    Consumer状态异常

    当Consumer处于错误状态时抛出，如重复启动、
    未启动就停止、状态不一致等。
    """

    def __init__(self, message: str, current_state: str | None = None):
        context = {"current_state": current_state} if current_state else {}
        super().__init__(
            message=message, error_code="CONSUMER_STATE_ERROR", context=context
        )


class SubscribeError(ConsumerError):
    """
    订阅异常

    当订阅Topic过程中发生错误时抛出，如Topic不存在、
    权限不足、订阅表达式错误等。
    """

    def __init__(self, topic: str, message: str, cause: Exception | None = None):
        context = {"topic": topic}
        super().__init__(
            message=message, error_code="SUBSCRIBE_ERROR", context=context, cause=cause
        )


class UnsubscribeError(ConsumerError):
    """
    取消订阅异常

    当取消订阅Topic过程中发生错误时抛出。
    """

    def __init__(self, topic: str, message: str, cause: Exception | None = None):
        context = {"topic": topic}
        super().__init__(
            message=message,
            error_code="UNSUBSCRIBE_ERROR",
            context=context,
            cause=cause,
        )


class MessageConsumeError(ConsumerError):
    """
    消息消费异常

    当消息消费过程中发生业务逻辑错误时抛出，
    不包括网络和系统异常。
    """

    def __init__(
        self,
        topic: str,
        message: str,
        message_count: int = 0,
        cause: Exception | None = None,
    ):
        context = {"topic": topic, "message_count": message_count}
        super().__init__(
            message=message,
            error_code="MESSAGE_CONSUME_ERROR",
            context=context,
            cause=cause,
        )


class MessagePullError(ConsumerError):
    """
    消息拉取异常

    当从Broker拉取消息过程中发生错误时抛出，
    包括网络异常、Broker响应异常等。
    """

    def __init__(
        self, topic: str, queue_id: int, message: str, cause: Exception | None = None
    ):
        context = {"topic": topic, "queue_id": queue_id}
        super().__init__(
            message=message,
            error_code="MESSAGE_PULL_ERROR",
            context=context,
            cause=cause,
        )


class OffsetError(ConsumerError):
    """
    偏移量异常

    当操作消息偏移量时发生错误时抛出，如偏移量无效、
    提交失败、读取失败等。
    """

    def __init__(
        self,
        topic: str,
        queue_id: int,
        offset: int,
        message: str,
        cause: Exception | None = None,
    ):
        context = {"topic": topic, "queue_id": queue_id, "offset": offset}
        super().__init__(
            message=message, error_code="OFFSET_ERROR", context=context, cause=cause
        )


class OffsetFetchError(OffsetError):
    """
    获取偏移量失败异常

    当从Broker获取消费偏移量时发生错误时抛出，如网络异常、
    权限不足、队列不存在、Broker响应异常等。
    """

    def __init__(
        self,
        topic: str,
        queue_id: int,
        consumer_group: str,
        message: str,
        cause: Exception | None = None,
    ):
        context = {
            "topic": topic,
            "queue_id": queue_id,
            "consumer_group": consumer_group,
        }
        super().__init__(
            topic=topic,
            queue_id=queue_id,
            offset=-1,  # 获取失败时offset未知，使用-1表示
            message=message,
            cause=cause,
        )
        self.error_code = "OFFSET_FETCH_ERROR"
        self.context = context


class RebalanceError(ConsumerError):
    """
    重平衡异常

    当进行队列重平衡过程中发生错误时抛出，如分配失败、
    队列状态不一致等。
    """

    def __init__(
        self,
        consumer_group: str,
        topic: str,
        message: str,
        cause: Exception | None = None,
    ):
        context = {"consumer_group": consumer_group, "topic": topic}
        super().__init__(
            message=message, error_code="REBALANCE_ERROR", context=context, cause=cause
        )


class BrokerNotAvailableError(ConsumerError):
    """
    Broker不可用异常

    当连接的Broker不可用或响应异常时抛出。
    """

    def __init__(self, broker_name: str, message: str, cause: Exception | None = None):
        context = {"broker_name": broker_name}
        super().__init__(
            message=message,
            error_code="BROKER_NOT_AVAILABLE",
            context=context,
            cause=cause,
        )


class NameServerError(ConsumerError):
    """
    NameServer异常

    当与NameServer通信过程中发生错误时抛出。
    """

    def __init__(self, namesrv_addr: str, message: str, cause: Exception | None = None):
        context = {"namesrv_addr": namesrv_addr}
        super().__init__(
            message=message, error_code="NAMESERVER_ERROR", context=context, cause=cause
        )


class NetworkError(ConsumerError):
    """
    网络异常

    当网络连接、通信过程中发生错误时抛出。
    """

    def __init__(self, address: str, message: str, cause: Exception | None = None):
        context = {"address": address}
        super().__init__(
            message=message, error_code="NETWORK_ERROR", context=context, cause=cause
        )


class TimeoutError(ConsumerError):
    """
    超时异常

    当操作超时时抛出，如网络超时、处理超时等。
    """

    def __init__(
        self,
        operation: str,
        timeout_ms: int,
        message: str,
        cause: Exception | None = None,
    ):
        context = {"operation": operation, "timeout_ms": timeout_ms}
        super().__init__(
            message=message, error_code="TIMEOUT_ERROR", context=context, cause=cause
        )


class ConfigError(ConsumerError):
    """
    配置异常

    当Consumer配置不正确或冲突时抛出。
    """

    def __init__(self, config_key: str, message: str, cause: Exception | None = None):
        context = {"config_key": config_key}
        super().__init__(
            message=message, error_code="CONFIG_ERROR", context=context, cause=cause
        )


class ValidationError(ConsumerError):
    """
    验证异常

    当数据验证失败时抛出，如消息格式错误、参数无效等。
    """

    def __init__(
        self,
        field_name: str,
        field_value: Any,
        message: str,
        cause: Exception | None = None,
    ):
        context = {"field_name": field_name, "field_value": str(field_value)}
        super().__init__(
            message=message, error_code="VALIDATION_ERROR", context=context, cause=cause
        )


# 便利函数


def create_consumer_start_error(
    message: str, cause: Exception | None = None
) -> ConsumerStartError:
    """创建Consumer启动异常的便利函数"""
    return ConsumerStartError(message, cause)


def create_message_consume_error(
    topic: str, message: str, message_count: int = 0, cause: Exception | None = None
) -> MessageConsumeError:
    """创建消息消费异常的便利函数"""
    return MessageConsumeError(topic, message, message_count, cause)


def create_broker_not_available_error(
    broker_name: str, message: str, cause: Exception | None = None
) -> BrokerNotAvailableError:
    """创建Broker不可用异常的便利函数"""
    return BrokerNotAvailableError(broker_name, message, cause)


def create_timeout_error(
    operation: str, timeout_ms: int, message: str, cause: Exception | None = None
) -> TimeoutError:
    """创建超时异常的便利函数"""
    return TimeoutError(operation, timeout_ms, message, cause)


def create_offset_fetch_error(
    topic: str,
    queue_id: int,
    consumer_group: str,
    message: str,
    cause: Exception | None = None,
) -> OffsetFetchError:
    """创建获取偏移量失败异常的便利函数"""
    return OffsetFetchError(topic, queue_id, consumer_group, message, cause)


def is_retriable_error(error: Exception) -> bool:
    """
    判断异常是否可重试

    Args:
        error: 异常实例

    Returns:
        是否可重试

    Examples:
        >>> try:
        ...     # some operation
        ... except Exception as e:
        ...     if is_retriable_error(e):
        ...         # retry logic
        ...         pass
    """
    retriable_errors = (
        NetworkError,
        TimeoutError,
        BrokerNotAvailableError,
        NameServerError,
    )

    return isinstance(error, retriable_errors)


def is_fatal_error(error: Exception) -> bool:
    """
    判断异常是否为致命错误

    Args:
        error: 异常实例

    Returns:
        是否为致命错误
    """
    fatal_errors = (ConfigError, ValidationError, ConsumerStateError)

    return isinstance(error, fatal_errors)
