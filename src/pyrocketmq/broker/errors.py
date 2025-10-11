"""
Broker 模块异常定义
定义与 RocketMQ Broker 交互时可能出现的各种异常类型。
"""

from typing import Optional

from ..remote.errors import RemoteError


class BrokerError(RemoteError):
    """Broker 基础异常"""

    def __init__(self, message: str, error_code: Optional[int] = None):
        """初始化Broker异常

        Args:
            message: 错误信息
            error_code: 错误代码（可选）
        """
        super().__init__(message)
        self.error_code = error_code


class BrokerConnectionError(BrokerError):
    """Broker 连接错误"""

    def __init__(self, message: str, broker_address: Optional[str] = None):
        """初始化连接错误

        Args:
            message: 错误信息
            broker_address: Broker地址（可选）
        """
        super().__init__(message)
        self.broker_address = broker_address


class BrokerTimeoutError(BrokerError):
    """Broker 超时错误"""

    def __init__(self, message: str, timeout: Optional[float] = None):
        """初始化超时错误

        Args:
            message: 错误信息
            timeout: 超时时间（可选）
        """
        super().__init__(message)
        self.timeout = timeout


class BrokerResponseError(BrokerError):
    """Broker 响应错误"""

    def __init__(self, message: str, response_code: Optional[int] = None):
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


class MessageSendError(BrokerError):
    """消息发送异常"""

    def __init__(
        self,
        message: str,
        message_id: Optional[str] = None,
        topic: Optional[str] = None,
    ):
        """初始化消息发送错误

        Args:
            message: 错误信息
            message_id: 消息ID（可选）
            topic: 主题名称（可选）
        """
        super().__init__(message)
        self.message_id = message_id
        self.topic = topic


class MessagePullError(BrokerError):
    """消息拉取异常"""

    def __init__(
        self,
        message: str,
        topic: Optional[str] = None,
        queue_id: Optional[int] = None,
    ):
        """初始化消息拉取错误

        Args:
            message: 错误信息
            topic: 主题名称（可选）
            queue_id: 队列ID（可选）
        """
        super().__init__(message)
        self.topic = topic
        self.queue_id = queue_id


class OffsetError(BrokerError):
    """偏移量操作异常"""

    def __init__(
        self,
        message: str,
        topic: Optional[str] = None,
        queue_id: Optional[int] = None,
        offset: Optional[int] = None,
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


class ClientRegistrationError(BrokerError):
    """客户端注册异常"""

    def __init__(
        self,
        message: str,
        client_id: Optional[str] = None,
        group_name: Optional[str] = None,
    ):
        """初始化客户端注册错误

        Args:
            message: 错误信息
            client_id: 客户端ID（可选）
            group_name: 组名（可选）
        """
        super().__init__(message)
        self.client_id = client_id
        self.group_name = group_name


class HeartbeatError(BrokerError):
    """心跳异常"""

    def __init__(self, message: str, client_id: Optional[str] = None):
        """初始化心跳错误

        Args:
            message: 错误信息
            client_id: 客户端ID（可选）
        """
        super().__init__(message)
        self.client_id = client_id


class BrokerNotAvailableError(BrokerError):
    """Broker 不可用异常"""

    def __init__(self, message: str, broker_address: Optional[str] = None):
        """初始化Broker不可用错误

        Args:
            message: 错误信息
            broker_address: Broker地址（可选）
        """
        super().__init__(message)
        self.broker_address = broker_address


class TopicNotFoundError(BrokerError):
    """主题不存在异常"""

    def __init__(self, topic: str):
        """初始化主题不存在错误

        Args:
            topic: 主题名称
        """
        super().__init__(f"Topic not found: {topic}")
        self.topic = topic


class QueueNotFoundError(BrokerError):
    """队列不存在异常"""

    def __init__(self, topic: str, queue_id: int):
        """初始化队列不存在错误

        Args:
            topic: 主题名称
            queue_id: 队列ID
        """
        super().__init__(f"Queue not found: topic={topic}, queueId={queue_id}")
        self.topic = topic
        self.queue_id = queue_id


class MessageNotFoundError(BrokerError):
    """消息不存在异常"""

    def __init__(
        self, message_id: Optional[str] = None, offset: Optional[int] = None
    ):
        """初始化消息不存在错误

        Args:
            message_id: 消息ID（可选）
            offset: 偏移量（可选）
        """
        if message_id:
            super().__init__(f"Message not found: {message_id}")
        elif offset is not None:
            super().__init__(f"Message not found at offset: {offset}")
        else:
            super().__init__("Message not found")

        self.message_id = message_id
        self.offset = offset


class AuthenticationError(BrokerError):
    """认证异常"""

    def __init__(self, message: str):
        """初始化认证错误

        Args:
            message: 错误信息
        """
        super().__init__(message)


class AuthorizationError(BrokerError):
    """授权异常"""

    def __init__(
        self,
        message: str,
        operation: Optional[str] = None,
        resource: Optional[str] = None,
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

    def __init__(self, message: str, broker_address: Optional[str] = None):
        """初始化Broker繁忙错误

        Args:
            message: 错误信息
            broker_address: Broker地址（可选）
        """
        super().__init__(message)
        self.broker_address = broker_address


class BrokerSystemError(BrokerError):
    """Broker 系统异常"""

    def __init__(self, message: str, error_code: Optional[int] = None):
        """初始化Broker系统错误

        Args:
            message: 错误信息
            error_code: 错误代码（可选）
        """
        super().__init__(message, error_code)
        self.error_code = error_code


class TransactionError(BrokerError):
    """事务异常"""

    def __init__(self, message: str, transaction_id: Optional[str] = None):
        """初始化事务错误

        Args:
            message: 错误信息
            transaction_id: 事务ID（可选）
        """
        super().__init__(message)
        self.transaction_id = transaction_id


class ProducerError(BrokerError):
    """生产者异常"""

    def __init__(self, message: str, producer_group: Optional[str] = None):
        """初始化生产者错误

        Args:
            message: 错误信息
            producer_group: 生产者组名（可选）
        """
        super().__init__(message)
        self.producer_group = producer_group


class ConsumerError(BrokerError):
    """消费者异常"""

    def __init__(self, message: str, consumer_group: Optional[str] = None):
        """初始化消费者错误

        Args:
            message: 错误信息
            consumer_group: 消费者组名（可选）
        """
        super().__init__(message)
        self.consumer_group = consumer_group
