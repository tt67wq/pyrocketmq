"""Transport模块异常定义"""


class TransportError(Exception):
    """Transport基础异常类"""

    pass


class ConnectionError(TransportError):
    """连接异常"""

    pass


class TimeoutError(TransportError):
    """超时异常"""

    pass


class ConfigurationError(TransportError):
    """配置异常"""

    pass


class InvalidStateTransitionError(TransportError):
    """无效状态转换异常"""

    pass


class ConnectionClosedError(TransportError):
    """连接已关闭异常"""

    pass
