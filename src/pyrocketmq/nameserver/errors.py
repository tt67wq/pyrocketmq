"""
NameServer 模块异常定义
"""

from pyrocketmq.remote.errors import RemoteError


class NameServerError(RemoteError):
    """NameServer 基础异常"""

    pass


class NameServerConnectionError(NameServerError):
    """NameServer 连接错误"""

    pass


class NameServerTimeoutError(NameServerError):
    """NameServer 超时错误"""

    pass


class NameServerProtocolError(NameServerError):
    """NameServer 协议错误"""

    pass


class NameServerResponseError(NameServerError):
    """NameServer 响应错误"""

    pass


class NameServerDataParseError(NameServerError):
    """NameServer 数据解析错误"""

    pass
