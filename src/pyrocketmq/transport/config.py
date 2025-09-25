"""传输层配置管理模块"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class TransportConfig:
    """传输层配置类"""

    # 基础连接配置
    host: str = "localhost"
    port: int = 8080

    # 超时配置
    timeout: float = 30.0
    connect_timeout: float = 10.0

    # 重连配置
    retry_interval: float = 5.0
    max_retries: int = -1  # -1表示无限重连

    # 缓冲区配置
    buffer_size: int = 8192
    send_buffer_size: int = 8192
    receive_buffer_size: int = 8192

    # 保活配置
    keep_alive: bool = True
    keep_alive_interval: float = 30.0
    keep_alive_timeout: float = 10.0

    # 其他配置
    enable_nodelay: bool = True
    enable_reuse_address: bool = True

    # 高级配置
    max_message_size: int = 1024 * 1024  # 1MB
    idle_timeout: Optional[float] = None

    def __post_init__(self):
        """配置验证"""
        if self.port < 0 or self.port > 65535:
            raise ValueError("Port must be between 0 and 65535")

        if self.timeout <= 0:
            raise ValueError("Timeout must be positive")

        if self.connect_timeout <= 0:
            raise ValueError("Connect timeout must be positive")

        if self.retry_interval < 0:
            raise ValueError("Retry interval must be non-negative")

        if self.buffer_size <= 0:
            raise ValueError("Buffer size must be positive")

        if self.send_buffer_size <= 0:
            raise ValueError("Send buffer size must be positive")

        if self.receive_buffer_size <= 0:
            raise ValueError("Receive buffer size must be positive")

        if self.keep_alive_interval <= 0:
            raise ValueError("Keep alive interval must be positive")

        if self.keep_alive_timeout <= 0:
            raise ValueError("Keep alive timeout must be positive")

        if self.max_message_size <= 0:
            raise ValueError("Max message size must be positive")

        if self.idle_timeout is not None and self.idle_timeout <= 0:
            raise ValueError("Idle timeout must be positive")

    @property
    def address(self) -> tuple[str, int]:
        """获取地址元组"""
        return (self.host, self.port)

    def copy_with(self, **kwargs) -> "TransportConfig":
        """创建配置副本并更新指定字段"""
        return TransportConfig(
            host=kwargs.get("host", self.host),
            port=kwargs.get("port", self.port),
            timeout=kwargs.get("timeout", self.timeout),
            connect_timeout=kwargs.get("connect_timeout", self.connect_timeout),
            retry_interval=kwargs.get("retry_interval", self.retry_interval),
            max_retries=kwargs.get("max_retries", self.max_retries),
            buffer_size=kwargs.get("buffer_size", self.buffer_size),
            send_buffer_size=kwargs.get(
                "send_buffer_size", self.send_buffer_size
            ),
            receive_buffer_size=kwargs.get(
                "receive_buffer_size", self.receive_buffer_size
            ),
            keep_alive=kwargs.get("keep_alive", self.keep_alive),
            keep_alive_interval=kwargs.get(
                "keep_alive_interval", self.keep_alive_interval
            ),
            keep_alive_timeout=kwargs.get(
                "keep_alive_timeout", self.keep_alive_timeout
            ),
            enable_nodelay=kwargs.get("enable_nodelay", self.enable_nodelay),
            enable_reuse_address=kwargs.get(
                "enable_reuse_address", self.enable_reuse_address
            ),
            max_message_size=kwargs.get(
                "max_message_size", self.max_message_size
            ),
            idle_timeout=kwargs.get("idle_timeout", self.idle_timeout),
        )

    def to_dict(self) -> dict:
        """转换为字典"""
        return {
            "host": self.host,
            "port": self.port,
            "timeout": self.timeout,
            "connect_timeout": self.connect_timeout,
            "retry_interval": self.retry_interval,
            "max_retries": self.max_retries,
            "buffer_size": self.buffer_size,
            "send_buffer_size": self.send_buffer_size,
            "receive_buffer_size": self.receive_buffer_size,
            "keep_alive": self.keep_alive,
            "keep_alive_interval": self.keep_alive_interval,
            "keep_alive_timeout": self.keep_alive_timeout,
            "enable_nodelay": self.enable_nodelay,
            "enable_reuse_address": self.enable_reuse_address,
            "max_message_size": self.max_message_size,
            "idle_timeout": self.idle_timeout,
        }

    @classmethod
    def from_dict(cls, config_dict: dict) -> "TransportConfig":
        """从字典创建配置"""
        return cls(**config_dict)

    def __str__(self) -> str:
        return f"TransportConfig(host={self.host}, port={self.port}, timeout={self.timeout})"

    def __repr__(self) -> str:
        return (
            f"TransportConfig(host={self.host!r}, port={self.port!r}, "
            f"timeout={self.timeout!r}, retry_interval={self.retry_interval!r})"
        )


# 预定义配置
DEFAULT_CONFIG = TransportConfig()

# 高性能配置
HIGH_PERFORMANCE_CONFIG = TransportConfig(
    timeout=10.0,
    connect_timeout=5.0,
    retry_interval=1.0,
    buffer_size=16384,
    send_buffer_size=16384,
    receive_buffer_size=16384,
    keep_alive_interval=15.0,
    keep_alive_timeout=5.0,
    max_message_size=2 * 1024 * 1024,  # 2MB
)

# 调试配置
DEBUG_CONFIG = TransportConfig(
    timeout=60.0,
    connect_timeout=30.0,
    retry_interval=10.0,
    keep_alive_interval=60.0,
    keep_alive_timeout=30.0,
    max_message_size=512 * 1024,  # 512KB
    idle_timeout=300.0,  # 5分钟
)
