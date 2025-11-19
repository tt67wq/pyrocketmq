"""
订阅管理器异常定义

本模块定义了SubscriptionManager相关的所有异常类型，
提供了完整的错误处理机制。

作者: pyrocketmq开发团队
"""


class SubscriptionError(Exception):
    """订阅相关异常基类

    所有订阅管理相关的异常都继承自此类。
    """

    def __init__(
        self, message: str, topic: str | None = None, selector: str | None = None
    ) -> None:
        """初始化订阅异常

        Args:
            message: 异常消息
            topic: 相关的Topic名称
            selector: 相关的选择器表达式
        """
        super().__init__(message)
        self.topic: str | None = topic
        self.selector: str | None = selector
        self.message: str = message

    def __str__(self) -> str:
        """返回异常的字符串表示"""
        if self.topic and self.selector:
            return f"{self.message} (topic={self.topic}, selector={self.selector})"
        elif self.topic:
            return f"{self.message} (topic={self.topic})"
        return self.message


class InvalidTopicError(SubscriptionError):
    """无效Topic异常

    当提供的Topic名称不符合RocketMQ规范时抛出。
    """

    def __init__(self, message: str, topic: str | None = None) -> None:
        """初始化无效Topic异常

        Args:
            message: 异常消息
            topic: 无效的Topic名称
        """
        super().__init__(message, topic=topic)


class InvalidSelectorError(SubscriptionError):
    """无效选择器异常

    当提供的消息选择器无效时抛出。
    """

    def __init__(self, message: str, selector: str | None = None) -> None:
        """初始化无效选择器异常

        Args:
            message: 异常消息
            selector: 无效的选择器表达式
        """
        super().__init__(message, selector=selector)


class TopicNotSubscribedError(SubscriptionError):
    """Topic未订阅异常

    当尝试对未订阅的Topic进行操作时抛出。
    """

    def __init__(self, message: str, topic: str | None = None) -> None:
        """初始化Topic未订阅异常

        Args:
            message: 异常消息
            topic: 未订阅的Topic名称
        """
        super().__init__(message, topic=topic)


class SubscriptionConflictError(SubscriptionError):
    """订阅冲突异常

    当新的订阅与现有订阅发生冲突时抛出。
    """

    def __init__(
        self,
        message: str,
        topic: str | None = None,
        existing_selector: str | None = None,
        new_selector: str | None = None,
        conflict_type: str | None = None,
    ) -> None:
        """初始化订阅冲突异常

        Args:
            message: 异常消息
            topic: 冲突的Topic名称
            existing_selector: 现有的选择器
            new_selector: 新的选择器
            conflict_type: 冲突类型
        """
        super().__init__(message, topic=topic)
        self.existing_selector: str | None = existing_selector
        self.new_selector: str | None = new_selector
        self.conflict_type: str | None = conflict_type

    def __str__(self) -> str:
        """返回异常的字符串表示"""
        parts: list[str] = [self.message]
        if self.topic:
            parts.append(f"topic={self.topic}")
        if self.existing_selector:
            parts.append(f"existing={self.existing_selector}")
        if self.new_selector:
            parts.append(f"new={self.new_selector}")
        if self.conflict_type:
            parts.append(f"type={self.conflict_type}")

        if len(parts) > 1:
            return f"{parts[0]} ({', '.join(parts[1:])})"
        return parts[0]


class SubscriptionLimitExceededError(SubscriptionError):
    """订阅数量超限异常

    当订阅数量超过系统限制时抛出。
    """

    def __init__(
        self, message: str, current_count: int | None = None, limit: int | None = None
    ) -> None:
        """初始化订阅数量超限异常

        Args:
            message: 异常消息
            current_count: 当前订阅数量
            limit: 订阅数量限制
        """
        super().__init__(message)
        self.current_count: int | None = current_count
        self.limit: int | None = limit

    def __str__(self) -> str:
        """返回异常的字符串表示"""
        if self.current_count is not None and self.limit is not None:
            return f"{self.message} (current={self.current_count}, limit={self.limit})"
        return self.message


class SubscriptionDataError(SubscriptionError):
    """订阅数据异常

    当订阅数据处理出错时抛出。
    """

    def __init__(
        self, message: str, topic: str | None = None, data_type: str | None = None
    ) -> None:
        """初始化订阅数据异常

        Args:
            message: 异常消息
            topic: 相关的Topic名称
            data_type: 数据类型
        """
        super().__init__(message, topic=topic)
        self.data_type: str | None = data_type

    def __str__(self) -> str:
        """返回异常的字符串表示"""
        parts: list[str] = [self.message]
        if self.topic:
            parts.append(f"topic={self.topic}")
        if self.data_type:
            parts.append(f"type={self.data_type}")

        if len(parts) > 1:
            return f"{parts[0]} ({', '.join(parts[1:])})"
        return parts[0]
