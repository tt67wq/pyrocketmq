"""传输层抽象接口定义模块"""

from abc import ABC, abstractmethod


class Transport(ABC):
    """传输层基础抽象接口"""

    @abstractmethod
    def output(self, msg: bytes) -> None:
        """发送二进制消息

        Args:
            msg: 要发送的二进制消息

        Raises:
            RuntimeError: 连接未建立
            ValueError: 消息无效或超过大小限制
            TransportError: 传输层错误
        """
        pass

    @abstractmethod
    def recv(self, size: int) -> bytes:
        """接收二进制消息

        Args:
            size: 接收缓冲区大小

        Returns:
            接收到的二进制数据，连接关闭时返回空字节

        Raises:
            RuntimeError: 连接未建立
            ValueError: 参数无效
            TimeoutError: 接收超时
            TransportError: 传输层错误
        """
        pass

    @abstractmethod
    def recv_pkg(self) -> bytes:
        """接收完整的数据包（header + body）

        数据包格式:
        - Header: 4字节大端序整数，表示body长度
        - Body: 实际数据内容

        Returns:
            完整的数据包（body部分），连接关闭时返回空字节

        Raises:
            RuntimeError: 连接未建立
            TimeoutError: 接收超时
            TransportError: 传输层错误
            ValueError: 数据包格式错误
        """
        pass

    @abstractmethod
    def connect(self) -> None:
        """建立连接

        Raises:
            ConnectionError: 连接失败
            ConfigurationError: 配置错误
            TransportError: 传输层错误
        """
        pass

    @abstractmethod
    def close(self) -> None:
        """关闭连接

        Raises:
            TransportError: 关闭连接时发生错误
        """
        pass

    @property
    @abstractmethod
    def is_connected(self) -> bool:
        """是否已连接"""
        pass

    @property
    @abstractmethod
    def is_connecting(self) -> bool:
        """是否正在连接"""
        pass

    @property
    @abstractmethod
    def is_disconnected(self) -> bool:
        """是否已断开连接"""
        pass


class SyncTransport(Transport):
    """同步传输接口"""

    def output(self, msg: bytes) -> None:
        """发送二进制消息"""
        return super().output(msg)

    def recv(self, size: int) -> bytes:
        """接收二进制消息"""
        return super().recv(size)

    def recv_pkg(self) -> bytes:
        """接收完整的数据包（header + body）"""
        return super().recv_pkg()

    def connect(self) -> None:
        """建立连接"""
        return super().connect()

    def close(self) -> None:
        """关闭连接"""
        return super().close()


class AsyncTransport(ABC):
    """异步传输接口"""

    @abstractmethod
    async def output(self, msg: bytes) -> None:
        """异步发送二进制消息

        Args:
            msg: 要发送的二进制消息

        Raises:
            RuntimeError: 连接未建立
            ValueError: 消息无效或超过大小限制
            TransportError: 传输层错误
        """
        pass

    @abstractmethod
    async def recv(self, size: int) -> bytes:
        """异步接收二进制消息

        Args:
            size: 接收缓冲区大小，为None时使用默认值

        Returns:
            接收到的二进制数据，连接关闭时返回空字节

        Raises:
            RuntimeError: 连接未建立
            ValueError: 参数无效
            TimeoutError: 接收超时
            TransportError: 传输层错误
        """
        pass

    @abstractmethod
    async def recv_pkg(self) -> bytes:
        """异步接收完整的数据包（header + body）

        数据包格式:
        - Header: 4字节大端序整数，表示body长度
        - Body: 实际数据内容

        Returns:
            完整的数据包（body部分），连接关闭时返回空字节

        Raises:
            RuntimeError: 连接未建立
            TimeoutError: 接收超时
            TransportError: 传输层错误
            ValueError: 数据包格式错误
        """
        pass

    @abstractmethod
    async def connect(self) -> None:
        """异步建立连接

        Raises:
            ConnectionError: 连接失败
            ConfigurationError: 配置错误
            TransportError: 传输层错误
        """
        pass

    @abstractmethod
    async def close(self) -> None:
        """异步关闭连接

        Raises:
            TransportError: 关闭连接时发生错误
        """
        pass

    @property
    @abstractmethod
    def is_connected(self) -> bool:
        """是否已连接"""
        pass

    @property
    @abstractmethod
    def is_connecting(self) -> bool:
        """是否正在连接"""
        pass

    @property
    @abstractmethod
    def is_disconnected(self) -> bool:
        """是否已断开连接"""
        pass


class ConnectionObserver(ABC):
    """连接状态观察者接口"""

    @abstractmethod
    def on_connected(self) -> None:
        """连接建立回调"""
        pass

    @abstractmethod
    def on_disconnected(self) -> None:
        """连接断开回调"""
        pass

    @abstractmethod
    def on_error(self, error: Exception) -> None:
        """连接错误回调"""
        pass


class TransportMetrics(ABC):
    """传输层指标接口"""

    @property
    @abstractmethod
    def bytes_sent(self) -> int:
        """发送字节数"""
        pass

    @property
    @abstractmethod
    def bytes_received(self) -> int:
        """接收字节数"""
        pass

    @property
    @abstractmethod
    def connection_count(self) -> int:
        """连接次数"""
        pass

    @property
    @abstractmethod
    def error_count(self) -> int:
        """错误次数"""
        pass

    @abstractmethod
    def reset_metrics(self) -> None:
        """重置指标"""
        pass
