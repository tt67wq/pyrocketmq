"""
Producer模块异常定义

定义Producer组件相关的异常类型，包括启动异常、消息发送异常、
路由异常等，为Producer的错误处理提供统一的异常体系。

作者: pyrocketmq团队
版本: MVP 1.0
"""

# 导入 queue_helper 模块的异常，以创建别名保持向后兼容
from pyrocketmq.queue_helper.errors import (
    BrokerNotAvailableError as _BrokerNotAvailableError,
)
from pyrocketmq.queue_helper.errors import (
    QueueNotAvailableError as _QueueNotAvailableError,
)
from pyrocketmq.queue_helper.errors import (
    RouteNotFoundError as _RouteNotFoundError,
)


class ProducerError(Exception):
    """Producer基础异常类

    所有Producer相关异常的基类，提供统一的错误处理接口。
    """

    def __init__(
        self,
        message: str,
        error_code: int | None = None,
        cause: Exception | None = None,
    ):
        """
        初始化Producer异常

        Args:
            message: 错误消息
            error_code: 错误代码（可选）
            cause: 原因异常（可选）
        """
        super().__init__(message)
        self.message: str = message
        self.error_code: int | None = error_code
        self.cause: Exception | None = cause

    def __str__(self) -> str:
        """字符串表示"""
        if self.error_code:
            return f"[{self.error_code}] {self.message}"
        return self.message


class ProducerStartError(ProducerError):
    """Producer启动异常

    当Producer启动过程中发生错误时抛出。
    """

    def __init__(
        self,
        message: str = "Failed to start producer",
        cause: Exception | None = None,
    ):
        super().__init__(message, error_code=1001, cause=cause)


class ProducerShutdownError(ProducerError):
    """Producer关闭异常

    当Producer关闭过程中发生错误时抛出。
    """

    def __init__(
        self,
        message: str = "Failed to shutdown producer",
        cause: Exception | None = None,
    ):
        super().__init__(message, error_code=1002, cause=cause)


class ProducerStateError(ProducerError):
    """Producer状态异常

    当Producer处于不正确的状态时抛出。
    """

    def __init__(
        self,
        message: str = "Producer is in invalid state",
        current_state: str | None = None,
        expected_state: str | None = None,
        cause: Exception | None = None,
    ):
        if current_state and expected_state:
            message = f"Producer state error: current={current_state}, expected={expected_state}"
        super().__init__(message, error_code=1003, cause=cause)
        self.current_state: str | None = current_state
        self.expected_state: str | None = expected_state


class MessageSendError(ProducerError):
    """消息发送异常

    当消息发送过程中发生错误时抛出。
    """

    def __init__(
        self,
        message: str = "Failed to send message",
        topic: str | None = None,
        broker: str | None = None,
        cause: Exception | None = None,
    ):
        if topic and broker:
            message = f"Failed to send message to topic {topic} on broker {broker}"
        elif topic:
            message = f"Failed to send message to topic {topic}"

        super().__init__(message, error_code=2001, cause=cause)
        self.topic: str | None = topic
        self.broker: str | None = broker


class MessageValidationError(ProducerError):
    """消息验证异常

    当消息验证失败时抛出。
    """

    def __init__(
        self,
        message: str = "Message validation failed",
        topic: str | None = None,
        validation_error: str | None = None,
    ):
        if validation_error:
            message = f"Message validation failed: {validation_error}"
        elif topic:
            message = f"Message validation failed for topic {topic}"

        super().__init__(message, error_code=2002)
        self.topic: str | None = topic
        self.validation_error: str | None = validation_error


class RouteUpdateError(ProducerError):
    """路由更新异常

    当路由信息更新失败时抛出。
    """

    def __init__(
        self,
        message: str = "Failed to update route info",
        topic: str | None = None,
        cause: Exception | None = None,
    ):
        if topic:
            message = f"Failed to update route info for topic {topic}"

        super().__init__(message, error_code=3004, cause=cause)
        self.topic: str | None = topic


class NameServerError(ProducerError):
    """NameServer异常

    当与NameServer通信发生错误时抛出。
    """

    def __init__(
        self,
        message: str = "NameServer error occurred",
        nameserver_addr: str | None = None,
        cause: Exception | None = None,
    ):
        if nameserver_addr:
            message = f"NameServer error at {nameserver_addr}"

        super().__init__(message, error_code=4001, cause=cause)
        self.nameserver_addr: str | None = nameserver_addr


class ConfigurationError(ProducerError):
    """配置异常

    当Producer配置错误时抛出。
    """

    def __init__(
        self,
        message: str = "Configuration error",
        config_key: str | None = None,
        config_value: str | None = None,
    ):
        if config_key:
            message = f"Invalid configuration: {config_key}={config_value}"

        super().__init__(message, error_code=5001)
        self.config_key: str | None = config_key
        self.config_value: str | None = config_value


class TimeoutError(ProducerError):
    """超时异常

    当操作超时时抛出。
    """

    def __init__(
        self,
        message: str = "Operation timed out",
        operation: str | None = None,
        timeout_ms: int | None = None,
    ):
        if operation and timeout_ms:
            message = f"Operation {operation} timed out after {timeout_ms}ms"
        elif operation:
            message = f"Operation {operation} timed out"

        super().__init__(message, error_code=5002)
        self.operation: str | None = operation
        self.timeout_ms: int | None = timeout_ms


class RetryExhaustedError(ProducerError):
    """重试耗尽异常

    当重试次数耗尽仍然失败时抛出。
    """

    def __init__(
        self,
        message: str = "Retry attempts exhausted",
        attempts: int | None = None,
        last_error: Exception | None = None,
    ):
        if attempts:
            message = f"Failed after {attempts} retry attempts"

        super().__init__(message, error_code=5003, cause=last_error)
        self.attempts: int | None = attempts
        self.last_error: Exception | None = last_error


class CompressionError(ProducerError):
    """压缩异常

    当消息压缩失败时抛出。
    """

    def __init__(
        self,
        message: str = "Message compression failed",
        cause: Exception | None = None,
    ):
        super().__init__(message, error_code=6001, cause=cause)


class SerializationError(ProducerError):
    """序列化异常

    当消息序列化失败时抛出。
    """

    def __init__(
        self,
        message: str = "Message serialization failed",
        cause: Exception | None = None,
    ):
        super().__init__(message, error_code=6002, cause=cause)


# 异常工具函数
def wrap_producer_error(
    error: Exception,
    default_message: str = "Producer operation failed",
    error_code: int | None = None,
) -> ProducerError:
    """
    将普通异常包装为Producer异常

    Args:
        error: 原始异常
        default_message: 默认错误消息
        error_code: 错误代码

    Returns:
        ProducerError: 包装后的Producer异常
    """
    if isinstance(error, ProducerError):
        return error

    message = str(error) or default_message
    return ProducerError(message, error_code, error)


def create_error_context(
    error: ProducerError,
    topic: str | None = None,
    broker: str | None = None,
    operation: str | None = None,
) -> dict[str, str | int | None]:
    """
    创建异常上下文信息

    Args:
        error: Producer异常
        topic: 主题名称
        broker: Broker地址
        operation: 操作名称

    Returns:
        dict: 异常上下文信息
    """
    context = {
        "error_type": type(error).__name__,
        "error_message": error.message,
        "error_code": error.error_code,
    }

    # 添加具体异常的特有属性
    if hasattr(error, "topic") and getattr(error, "topic", None):
        context["topic"] = getattr(error, "topic")
    if hasattr(error, "broker") and getattr(error, "broker", None):
        context["broker"] = getattr(error, "broker")
    if hasattr(error, "broker_name") and getattr(error, "broker_name", None):
        context["broker_name"] = getattr(error, "broker_name")
    if hasattr(error, "queue_id") and getattr(error, "queue_id", None) is not None:
        context["queue_id"] = getattr(error, "queue_id")

    # 添加调用上下文
    if topic:
        context["context_topic"] = topic
    if broker:
        context["context_broker"] = broker
    if operation:
        context["context_operation"] = operation

    return context


# 异常代码常量
class ErrorCodes:
    """错误代码常量"""

    # Producer相关错误 (1000-1999)
    PRODUCER_START_ERROR: int = 1001
    PRODUCER_SHUTDOWN_ERROR: int = 1002
    PRODUCER_STATE_ERROR: int = 1003

    # 消息发送相关错误 (2000-2999)
    MESSAGE_SEND_ERROR: int = 2001
    MESSAGE_VALIDATION_ERROR: int = 2002

    # 路由相关错误 (3000-3999)
    ROUTE_NOT_FOUND: int = 3001
    BROKER_NOT_AVAILABLE: int = 3002
    QUEUE_NOT_AVAILABLE: int = 3003
    ROUTE_UPDATE_ERROR: int = 3004

    # NameServer相关错误 (4000-4999)
    NAMESERVER_ERROR: int = 4001

    # 通用错误 (5000-5999)
    CONFIGURATION_ERROR: int = 5001
    TIMEOUT_ERROR: int = 5002
    RETRY_EXHAUSTED_ERROR: int = 5003

    # 序列化相关错误 (6000-6999)
    COMPRESSION_ERROR: int = 6001
    SERIALIZATION_ERROR: int = 6002


# 创建异常别名，保持向后兼容
# 这些异常现在定义在 queue_helper 模块中，但为了保持向后兼容性，
# 我们在 producer.errors 模块中创建别名
RouteNotFoundError = _RouteNotFoundError
BrokerNotAvailableError = _BrokerNotAvailableError
QueueNotAvailableError = _QueueNotAvailableError

# 清理临时名称
del _RouteNotFoundError
del _BrokerNotAvailableError
del _QueueNotAvailableError
