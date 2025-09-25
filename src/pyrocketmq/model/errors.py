"""
RocketMQ模型层异常定义
"""


class RemotingCommandError(Exception):
    """远程命令基础异常"""

    pass


class SerializationError(RemotingCommandError):
    """序列化异常"""

    pass


class DeserializationError(RemotingCommandError):
    """反序列化异常"""

    pass


class ProtocolError(RemotingCommandError):
    """协议错误"""

    pass


class ValidationError(RemotingCommandError):
    """数据验证错误"""

    pass


class MessageTooLargeError(ProtocolError):
    """消息过大错误"""

    def __init__(self, size: int, max_size: int):
        super().__init__(f"消息大小 {size} 超过最大限制 {max_size}")
        self.size = size
        self.max_size = max_size


class HeaderTooLargeError(ProtocolError):
    """Header过大错误"""

    def __init__(self, size: int, max_size: int):
        super().__init__(f"Header大小 {size} 超过最大限制 {max_size}")
        self.size = size
        self.max_size = max_size


class InvalidHeaderError(ProtocolError):
    """无效Header错误"""

    pass


class InvalidMessageError(ProtocolError):
    """无效消息错误"""

    pass


class ConnectionClosedError(RemotingCommandError):
    """连接已关闭错误"""

    pass


class TimeoutError(RemotingCommandError):
    """超时错误"""

    def __init__(self, timeout: float):
        super().__init__(f"操作超时，超时时间: {timeout}秒")
        self.timeout = timeout


class UnsupportedVersionError(ProtocolError):
    """不支持的协议版本错误"""

    def __init__(self, version: int):
        super().__init__(f"不支持的协议版本: {version}")
        self.version = version


class UnsupportedLanguageError(ProtocolError):
    """不支持的语言代码错误"""

    def __init__(self, language: int):
        super().__init__(f"不支持的语言代码: {language}")
        self.language = language


class UnsupportedRequestCodeError(ProtocolError):
    """不支持的请求代码错误"""

    def __init__(self, code: int):
        super().__init__(f"不支持的请求代码: {code}")
        self.code = code
