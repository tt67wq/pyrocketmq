"""
Transport配置管理模块

提供灵活的配置管理，支持多种配置创建方式和验证。
"""

import os
import warnings
from dataclasses import asdict, dataclass, field, fields
from typing import Any, Dict, Optional


@dataclass
class TransportConfig:
    """Transport配置类"""

    # 基础连接配置
    host: str = "localhost"
    port: int = 8080

    # 超时配置
    connect_timeout: float = 30.0
    read_timeout: float = 30.0
    write_timeout: float = 30.0
    overall_timeout: Optional[float] = None

    # 重连配置
    retry_interval: float = 5.0
    max_retries: int = -1  # -1表示无限重连
    retry_backoff_multiplier: float = 2.0
    max_retry_interval: float = 60.0

    # 缓冲区配置
    buffer_size: int = 8192
    send_buffer_size: int = 8192
    recv_buffer_size: int = 8192
    max_message_size: int = 1024 * 1024  # 1MB

    # 心跳配置
    keep_alive: bool = True
    keep_alive_interval: float = 30.0
    keep_alive_timeout: float = 10.0
    idle_timeout: float = 300.0  # 5分钟

    # TCP特定配置
    tcp_no_delay: bool = True
    so_keep_alive: bool = True
    so_reuse_addr: bool = True
    so_reuse_port: bool = False

    # 高级配置
    connection_pool_size: int = 1
    enable_stats: bool = False
    debug_mode: bool = False

    # 自定义配置
    custom_options: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        """配置验证和后处理"""
        self._validate_config()
        self._normalize_config()

    def _validate_config(self):
        """验证配置参数"""
        if not isinstance(self.host, str) or not self.host.strip():
            raise ValueError("Host must be a non-empty string")

        if not isinstance(self.port, int) or not (1 <= self.port <= 65535):
            raise ValueError("Port must be an integer between 1 and 65535")

        if self.connect_timeout <= 0:
            raise ValueError("Connect timeout must be positive")

        if self.read_timeout <= 0:
            raise ValueError("Read timeout must be positive")

        if self.write_timeout <= 0:
            raise ValueError("Write timeout must be positive")

        if self.retry_interval < 0:
            raise ValueError("Retry interval must be non-negative")

        if self.max_retries < -1:
            raise ValueError(
                "Max retries must be -1 (infinite) or non-negative"
            )

        if self.buffer_size <= 0:
            raise ValueError("Buffer size must be positive")

        if self.max_message_size <= 0:
            raise ValueError("Max message size must be positive")

        if self.keep_alive_interval <= 0:
            raise ValueError("Keep alive interval must be positive")

        if self.connection_pool_size <= 0:
            raise ValueError("Connection pool size must be positive")

    def _normalize_config(self):
        """规范化配置参数"""
        # 确保缓冲区大小合理
        self.buffer_size = max(1024, self.buffer_size)
        self.send_buffer_size = max(1024, self.send_buffer_size)
        self.recv_buffer_size = max(1024, self.recv_buffer_size)

        # 确保超时时间合理
        self.connect_timeout = max(1.0, self.connect_timeout)
        self.read_timeout = max(1.0, self.read_timeout)
        self.write_timeout = max(1.0, self.write_timeout)

        # 确保重试间隔合理
        self.retry_interval = max(0.1, self.retry_interval)
        self.max_retry_interval = max(
            self.retry_interval, self.max_retry_interval
        )

        # 确保心跳间隔合理
        self.keep_alive_interval = max(1.0, self.keep_alive_interval)

    @classmethod
    def create(
        cls, host: str = "localhost", port: int = 8080, **kwargs
    ) -> "TransportConfig":
        """创建配置实例的工厂方法"""
        return cls(host=host, port=port, **kwargs)

    @classmethod
    def from_env(cls, prefix: str = "ROCKETMQ_") -> "TransportConfig":
        """从环境变量创建配置"""
        config = cls()

        # 基础连接配置
        if host := os.getenv(f"{prefix}HOST"):
            config.host = host

        if port_str := os.getenv(f"{prefix}PORT"):
            try:
                config.port = int(port_str)
            except ValueError:
                pass

        # 超时配置
        timeout_mappings = {
            "CONNECT_TIMEOUT": "connect_timeout",
            "READ_TIMEOUT": "read_timeout",
            "WRITE_TIMEOUT": "write_timeout",
            "OVERALL_TIMEOUT": "overall_timeout",
        }

        for env_key, config_key in timeout_mappings.items():
            if timeout_str := os.getenv(f"{prefix}{env_key}"):
                try:
                    if config_key == "overall_timeout":
                        value = float(timeout_str) if timeout_str else None
                    else:
                        value = float(timeout_str)
                    setattr(config, config_key, value)
                except ValueError:
                    pass

        # 重连配置
        retry_mappings = {
            "RETRY_INTERVAL": "retry_interval",
            "MAX_RETRIES": "max_retries",
            "RETRY_BACKOFF_MULTIPLIER": "retry_backoff_multiplier",
            "MAX_RETRY_INTERVAL": "max_retry_interval",
        }

        for env_key, config_key in retry_mappings.items():
            if retry_str := os.getenv(f"{prefix}{env_key}"):
                try:
                    value = (
                        float(retry_str)
                        if "INTERVAL" in env_key or "BACKOFF" in env_key
                        else int(retry_str)
                    )
                    setattr(config, config_key, value)
                except ValueError:
                    pass

        # 缓冲区配置
        buffer_mappings = {
            "BUFFER_SIZE": "buffer_size",
            "SEND_BUFFER_SIZE": "send_buffer_size",
            "RECV_BUFFER_SIZE": "recv_buffer_size",
            "MAX_MESSAGE_SIZE": "max_message_size",
        }

        for env_key, config_key in buffer_mappings.items():
            if buffer_str := os.getenv(f"{prefix}{env_key}"):
                try:
                    setattr(config, config_key, int(buffer_str))
                except ValueError:
                    pass

        # 心跳配置
        heartbeat_mappings = {
            "KEEP_ALIVE": "keep_alive",
            "KEEP_ALIVE_INTERVAL": "keep_alive_interval",
            "KEEP_ALIVE_TIMEOUT": "keep_alive_timeout",
            "IDLE_TIMEOUT": "idle_timeout",
        }

        for env_key, config_key in heartbeat_mappings.items():
            if env_value := os.getenv(f"{prefix}{env_key}"):
                if config_key == "keep_alive":
                    config_value = env_value.lower() in (
                        "true",
                        "1",
                        "yes",
                        "on",
                    )
                else:
                    try:
                        config_value = float(env_value)
                    except ValueError:
                        continue
                setattr(config, config_key, config_value)

        # TCP配置
        tcp_mappings = {
            "TCP_NO_DELAY": "tcp_no_delay",
            "SO_KEEP_ALIVE": "so_keep_alive",
            "SO_REUSE_ADDR": "so_reuse_addr",
            "SO_REUSE_PORT": "so_reuse_port",
        }

        for env_key, config_key in tcp_mappings.items():
            if env_value := os.getenv(f"{prefix}{env_key}"):
                config_value = env_value.lower() in ("true", "1", "yes", "on")
                setattr(config, config_key, config_value)

        # 高级配置
        advanced_mappings = {
            "CONNECTION_POOL_SIZE": "connection_pool_size",
            "ENABLE_STATS": "enable_stats",
            "DEBUG_MODE": "debug_mode",
        }

        for env_key, config_key in advanced_mappings.items():
            if env_value := os.getenv(f"{prefix}{env_key}"):
                if config_key == "connection_pool_size":
                    try:
                        setattr(config, config_key, int(env_value))
                    except ValueError:
                        pass
                else:
                    config_value = env_value.lower() in (
                        "true",
                        "1",
                        "yes",
                        "on",
                    )
                    setattr(config, config_key, config_value)

        return config

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> "TransportConfig":
        """从字典创建配置"""
        # 过滤掉未知的字段
        valid_fields = {f.name for f in fields(cls)}
        filtered_dict = {
            k: v for k, v in config_dict.items() if k in valid_fields
        }
        return cls(**filtered_dict)

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return asdict(self)

    def copy(self) -> "TransportConfig":
        """创建配置副本"""
        return self.from_dict(self.to_dict())

    def update(self, **kwargs) -> "TransportConfig":
        """更新配置并返回新实例"""
        config_dict = self.to_dict()
        config_dict.update(kwargs)
        return self.from_dict(config_dict)

    @classmethod
    def development(cls) -> "TransportConfig":
        """开发环境配置"""
        return cls(
            host="localhost",
            port=8080,
            connect_timeout=10.0,
            read_timeout=30.0,
            debug_mode=True,
            keep_alive=True,
            keep_alive_interval=15.0,
        )

    @classmethod
    def production(cls) -> "TransportConfig":
        """生产环境配置"""
        return cls(
            host="127.0.0.1",
            port=9876,
            connect_timeout=5.0,
            read_timeout=30.0,
            write_timeout=30.0,
            retry_interval=3.0,
            max_retries=-1,  # 无限重连
            keep_alive=True,
            keep_alive_interval=30.0,
            debug_mode=False,
        )

    @classmethod
    def testing(cls) -> "TransportConfig":
        """测试环境配置"""
        return cls(
            host="localhost",
            port=8081,
            connect_timeout=5.0,
            read_timeout=10.0,
            retry_interval=1.0,
            max_retries=3,
            keep_alive=False,
            debug_mode=True,
        )

    @classmethod
    def high_performance(cls) -> "TransportConfig":
        """高性能配置"""
        return cls(
            host="localhost",
            port=8080,
            connect_timeout=1.0,
            read_timeout=10.0,
            write_timeout=10.0,
            buffer_size=65536,
            send_buffer_size=65536,
            recv_buffer_size=65536,
            max_message_size=10 * 1024 * 1024,  # 10MB
            tcp_no_delay=True,
            so_keep_alive=True,
            so_reuse_addr=True,
            so_reuse_port=True,
        )

    def validate_for_server(self):
        """验证作为服务器端的配置"""
        if self.connection_pool_size > 1:
            raise ValueError(
                "Server configuration should not use connection pool"
            )

    def validate_for_client(self):
        """验证作为客户端的配置"""
        if self.so_reuse_port and self.connection_pool_size == 1:
            warnings.warn("SO_REUSEPORT is not needed for single connection")

    def validate_performance(self):
        """验证性能相关配置"""
        if self.buffer_size < 4096:
            warnings.warn("Small buffer size may affect performance")

        if self.tcp_no_delay is False:
            warnings.warn("TCP_NODELAY disabled may increase latency")

    def get_effective_timeout(self, operation: str) -> float:
        """获取操作的有效超时时间"""
        if operation == "connect":
            return self.connect_timeout
        elif operation == "read":
            return self.read_timeout
        elif operation == "write":
            return self.write_timeout
        else:
            return self.overall_timeout or 30.0

    def calculate_retry_delay(self, attempt: int) -> float:
        """计算重试延迟时间"""
        if attempt <= 0:
            return 0.0

        # 无限重连或还有重试次数
        if self.max_retries == -1 or attempt <= self.max_retries:
            delay = self.retry_interval * (
                self.retry_backoff_multiplier ** (attempt - 1)
            )
            return min(delay, self.max_retry_interval)
        else:
            # 超过重试次数
            return -1.0

    def should_retry(self, attempt: int) -> bool:
        """判断是否应该继续重试"""
        return self.max_retries == -1 or attempt <= self.max_retries


class TransportConfigBuilder:
    """Transport配置构建器"""

    def __init__(self):
        self._config = TransportConfig()

    def host(self, host: str) -> "TransportConfigBuilder":
        """设置主机地址"""
        self._config.host = host
        return self

    def port(self, port: int) -> "TransportConfigBuilder":
        """设置端口"""
        self._config.port = port
        return self

    def timeouts(
        self,
        connect: float = 30.0,
        read: float = 30.0,
        write: float = 30.0,
        overall: Optional[float] = None,
    ) -> "TransportConfigBuilder":
        """设置超时时间"""
        self._config.connect_timeout = connect
        self._config.read_timeout = read
        self._config.write_timeout = write
        self._config.overall_timeout = overall
        return self

    def retry(
        self,
        interval: float = 5.0,
        max_retries: int = -1,
        backoff_multiplier: float = 2.0,
        max_interval: float = 60.0,
    ) -> "TransportConfigBuilder":
        """设置重试配置"""
        self._config.retry_interval = interval
        self._config.max_retries = max_retries
        self._config.retry_backoff_multiplier = backoff_multiplier
        self._config.max_retry_interval = max_interval
        return self

    def buffer(
        self,
        size: int = 8192,
        send_size: Optional[int] = None,
        recv_size: Optional[int] = None,
        max_message: int = 1024 * 1024,
    ) -> "TransportConfigBuilder":
        """设置缓冲区配置"""
        self._config.buffer_size = size
        self._config.send_buffer_size = send_size or size
        self._config.recv_buffer_size = recv_size or size
        self._config.max_message_size = max_message
        return self

    def keep_alive(
        self,
        enabled: bool = True,
        interval: float = 30.0,
        timeout: float = 10.0,
    ) -> "TransportConfigBuilder":
        """设置心跳配置"""
        self._config.keep_alive = enabled
        self._config.keep_alive_interval = interval
        self._config.keep_alive_timeout = timeout
        return self

    def tcp_options(
        self,
        no_delay: bool = True,
        keep_alive: bool = True,
        reuse_addr: bool = True,
        reuse_port: bool = False,
    ) -> "TransportConfigBuilder":
        """设置TCP选项"""
        self._config.tcp_no_delay = no_delay
        self._config.so_keep_alive = keep_alive
        self._config.so_reuse_addr = reuse_addr
        self._config.so_reuse_port = reuse_port
        return self

    def connection_pool(self, size: int = 1) -> "TransportConfigBuilder":
        """设置连接池大小"""
        self._config.connection_pool_size = size
        return self

    def debug(self, enabled: bool = True) -> "TransportConfigBuilder":
        """设置调试模式"""
        self._config.debug_mode = enabled
        return self

    def stats(self, enabled: bool = True) -> "TransportConfigBuilder":
        """设置统计功能"""
        self._config.enable_stats = enabled
        return self

    def custom_option(self, key: str, value: Any) -> "TransportConfigBuilder":
        """添加自定义选项"""
        self._config.custom_options[key] = value
        return self

    def build(self) -> TransportConfig:
        """构建配置对象"""
        return self._config.copy()  # 返回副本以确保不可变性


# 向后兼容的别名
Config = TransportConfig
ConfigBuilder = TransportConfigBuilder
