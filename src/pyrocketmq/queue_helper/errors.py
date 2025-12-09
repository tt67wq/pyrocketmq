"""
Queue helper module exceptions.

Defines exceptions related to queue selection and message routing functionality.
"""


class QueueHelperError(Exception):
    """Queue helper基础异常类

    所有queue helper相关异常的基类，提供统一的错误处理接口。
    """

    def __init__(
        self,
        message: str,
        error_code: int | None = None,
        cause: Exception | None = None,
    ):
        """
        初始化QueueHelper异常

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


class RouteNotFoundError(QueueHelperError):
    """路由未找到异常

    当无法找到Topic的路由信息时抛出。
    """

    def __init__(
        self,
        message: str = "Route not found",
        topic: str | None = None,
    ):
        if topic:
            message = f"Route not found for topic {topic}"

        super().__init__(message, error_code=3001)
        self.topic: str | None = topic


class BrokerNotAvailableError(QueueHelperError):
    """Broker不可用异常

    当目标Broker不可用时抛出。
    """

    def __init__(
        self,
        message: str = "Broker is not available",
        broker_name: str | None = None,
        broker_addr: str | None = None,
    ):
        if broker_name:
            message = f"Broker {broker_name} is not available"
        elif broker_addr:
            message = f"Broker at {broker_addr} is not available"

        super().__init__(message, error_code=3002)
        self.broker_name: str | None = broker_name
        self.broker_addr: str | None = broker_addr


class QueueNotAvailableError(QueueHelperError):
    """队列不可用异常

    当目标队列不可用时抛出。
    """

    def __init__(
        self,
        message: str = "Queue is not available",
        topic: str | None = None,
        broker_name: str | None = None,
        queue_id: int | None = None,
    ):
        if topic and broker_name and queue_id is not None:
            message = f"Queue {queue_id} is not available for topic {topic} on broker {broker_name}"
        elif topic:
            message = f"No available queues found for topic {topic}"

        super().__init__(message, error_code=3003)
        self.topic: str | None = topic
        self.broker_name: str | None = broker_name
        self.queue_id: int | None = queue_id
