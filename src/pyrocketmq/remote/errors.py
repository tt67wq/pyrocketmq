"""
远程通信模块异常定义
"""


class RemoteError(Exception):
    """远程通信基础异常"""

    pass


class ConnectionError(RemoteError):
    """连接相关错误"""

    pass


class TimeoutError(RemoteError):
    """超时错误"""

    pass


class SerializationError(RemoteError):
    """序列化错误"""

    pass


class ProtocolError(RemoteError):
    """协议错误"""

    pass


class ResourceExhaustedError(RemoteError):
    """资源耗尽错误"""

    pass


class ConfigurationError(RemoteError):
    """配置错误"""

    pass


class WaiterTimeoutError(TimeoutError):
    """等待者超时错误"""

    pass


class MaxWaitersExceededError(ResourceExhaustedError):
    """最大等待者数量超出错误"""

    pass


class InvalidCommandError(ProtocolError):
    """无效命令错误"""

    pass


class ConnectionClosedError(ConnectionError):
    """连接已关闭错误"""

    pass


class TransportError(RemoteError):
    """传输层错误"""

    pass


def is_connection_error(error: Exception) -> bool:
    """检查是否为连接相关错误"""
    return isinstance(error, (ConnectionError, ConnectionClosedError))


def is_timeout_error(error: Exception) -> bool:
    """检查是否为超时错误"""
    return isinstance(error, (TimeoutError, WaiterTimeoutError))


def is_retryable_error(error: Exception) -> bool:
    """检查是否为可重试错误"""
    # 注意：根据设计要求，不实现重试机制，但保留此函数供调用者判断
    return is_connection_error(error) or is_timeout_error(error)


def is_fatal_error(error: Exception) -> bool:
    """检查是否为致命错误"""
    return isinstance(
        error, (ProtocolError, SerializationError, ConfigurationError)
    )
