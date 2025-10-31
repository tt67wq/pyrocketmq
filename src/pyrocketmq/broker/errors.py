"""
Broker 模块异常定义
定义与 RocketMQ Broker 交互时可能出现的各种异常类型。
"""

from ..remote.errors import RemoteError


class BrokerError(RemoteError):
    """Broker 基础异常"""

    error_code: int | None

    def __init__(self, message: str, error_code: int | None = None):
        """初始化Broker异常

        Args:
            message: 错误信息
            error_code: 错误代码（可选）
        """
        super().__init__(message)
        self.error_code = error_code


class BrokerConnectionError(BrokerError):
    """Broker 连接错误"""

    broker_address: str | None

    def __init__(self, message: str, broker_address: str | None = None):
        """初始化连接错误

        Args:
            message: 错误信息
            broker_address: Broker地址（可选）
        """
        super().__init__(message)
        self.broker_address = broker_address


class BrokerTimeoutError(BrokerError):
    """Broker 超时错误"""

    timeout: float | None

    def __init__(self, message: str, timeout: float | None = None):
        """初始化超时错误

        Args:
            message: 错误信息
            timeout: 超时时间（可选）
        """
        super().__init__(message)
        self.timeout = timeout


class BrokerResponseError(BrokerError):
    """Broker 响应错误"""

    response_code: int | None

    def __init__(self, message: str, response_code: int | None = None):
        """初始化响应错误

        Args:
            message: 错误信息
            response_code: 响应代码（可选）
        """
        super().__init__(message, response_code)
        self.response_code = response_code


class BrokerProtocolError(BrokerError):
    """Broker 协议错误"""

    def __init__(self, message: str):
        """初始化协议错误

        Args:
            message: 错误信息
        """
        super().__init__(message)


class AuthorizationError(BrokerError):
    """授权异常"""

    operation: str | None
    resource: str | None

    def __init__(
        self,
        message: str,
        operation: str | None = None,
        resource: str | None = None,
    ):
        """初始化授权错误

        Args:
            message: 错误信息
            operation: 操作类型（可选）
            resource: 资源名称（可选）
        """
        super().__init__(message)
        self.operation = operation
        self.resource = resource


class BrokerBusyError(BrokerError):
    """Broker 繁忙异常"""

    broker_address: str | None

    def __init__(self, message: str, broker_address: str | None = None):
        """初始化Broker繁忙错误

        Args:
            message: 错误信息
            broker_address: Broker地址（可选）
        """
        super().__init__(message)
        self.broker_address = broker_address


class MessagePullError(BrokerError):
    """消息拉取异常"""

    topic: str | None
    queue_id: int | None
    pull_offset: int | None

    def __init__(
        self,
        message: str,
        topic: str | None = None,
        queue_id: int | None = None,
        pull_offset: int | None = None,
    ):
        """初始化消息拉取错误

        Args:
            message: 错误信息
            topic: 主题名称（可选）
            queue_id: 队列ID（可选）
            pull_offset: 拉取偏移量（可选）
        """
        super().__init__(message)
        self.topic = topic
        self.queue_id = queue_id
        self.pull_offset = pull_offset


class OffsetError(BrokerError):
    """偏移量异常"""

    topic: str | None
    queue_id: int | None
    offset: int | None

    def __init__(
        self,
        message: str,
        topic: str | None = None,
        queue_id: int | None = None,
        offset: int | None = None,
    ):
        """初始化偏移量错误

        Args:
            message: 错误信息
            topic: 主题名称（可选）
            queue_id: 队列ID（可选）
            offset: 偏移量（可选）
        """
        super().__init__(message)
        self.topic = topic
        self.queue_id = queue_id
        self.offset = offset


class BrokerSystemError(BrokerError):
    """Broker 系统异常"""

    error_code: int | None

    def __init__(self, message: str, error_code: int | None = None):
        """初始化Broker系统错误

        Args:
            message: 错误信息
            error_code: 错误代码（可选）
        """
        super().__init__(message, error_code)
        self.error_code = error_code
