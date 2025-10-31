"""
远程通信模块配置管理
"""

import os
from dataclasses import dataclass


@dataclass
class RemoteConfig:
    """远程通信配置类"""

    # RPC相关配置
    rpc_timeout: float = 30.0  # RPC调用超时时间（秒）
    opaque_start: int = 0  # opaque起始值
    max_waiters: int = 10000  # 最大并发请求数

    # 清理相关配置
    cleanup_interval: float = 60.0  # 清理间隔（秒）
    waiter_timeout: float = 300.0  # 等待者超时时间（秒）

    # 性能监控配置
    enable_metrics: bool = True  # 启用性能指标

    # 连接池配置
    connection_pool_size: int = 1  # 连接池大小
    connection_pool_timeout: float = 10.0  # 连接池获取超时时间

    # 传输层配置
    # transport_config: Optional[TransportConfig] = None

    def __post_init__(self) -> None:
        """后初始化处理"""
        # 验证配置值
        if self.rpc_timeout <= 0:
            raise ValueError("rpc_timeout must be greater than 0")

        if self.max_waiters <= 0:
            raise ValueError("max_waiters must be greater than 0")

        if self.cleanup_interval <= 0:
            raise ValueError("cleanup_interval must be greater than 0")

        if self.waiter_timeout <= 0:
            raise ValueError("waiter_timeout must be greater than 0")

        if self.connection_pool_size <= 0:
            raise ValueError("connection_pool_size must be greater than 0")

        if self.connection_pool_timeout <= 0:
            raise ValueError("connection_pool_timeout must be greater than 0")

        # 创建默认传输层配置
        # if self.transport_config is None:
        #     self.transport_config = TransportConfig()

    @classmethod
    def from_env(cls) -> "RemoteConfig":
        """从环境变量创建配置"""
        config = cls()

        # RPC配置
        if rpc_timeout := os.getenv("PYROCKETMQ_RPC_TIMEOUT"):
            try:
                config.rpc_timeout = float(rpc_timeout)
            except ValueError:
                pass

        # 并发配置
        if max_waiters := os.getenv("PYROCKETMQ_MAX_WAITERS"):
            try:
                config.max_waiters = int(max_waiters)
            except ValueError:
                pass

        # 清理配置
        if cleanup_interval := os.getenv("PYROCKETMQ_CLEANUP_INTERVAL"):
            try:
                config.cleanup_interval = float(cleanup_interval)
            except ValueError:
                pass

        if waiter_timeout := os.getenv("PYROCKETMQ_WAITER_TIMEOUT"):
            try:
                config.waiter_timeout = float(waiter_timeout)
            except ValueError:
                pass

        # 连接池配置
        if pool_size := os.getenv("PYROCKETMQ_CONNECTION_POOL_SIZE"):
            try:
                config.connection_pool_size = int(pool_size)
            except ValueError:
                pass

        if pool_timeout := os.getenv("PYROCKETMQ_CONNECTION_POOL_TIMEOUT"):
            try:
                config.connection_pool_timeout = float(pool_timeout)
            except ValueError:
                pass

        # 性能监控配置
        if enable_metrics := os.getenv("PYROCKETMQ_ENABLE_METRICS"):
            config.enable_metrics = enable_metrics.lower() in (
                "true",
                "1",
                "yes",
                "on",
            )

        return config

    def with_rpc_timeout(self, timeout: float) -> "RemoteConfig":
        """设置RPC超时时间"""
        config = self._copy()
        config.rpc_timeout = timeout
        return config

    def with_max_waiters(self, max_waiters: int) -> "RemoteConfig":
        """设置最大并发请求数"""
        config = self._copy()
        config.max_waiters = max_waiters
        return config

    def with_connection_pool_size(self, pool_size: int) -> "RemoteConfig":
        """设置连接池大小"""
        config = self._copy()
        config.connection_pool_size = pool_size
        return config

    # def with_transport_config(
    #     self, transport_config: TransportConfig
    # ) -> "RemoteConfig":
    #     """设置传输层配置"""
    #     config = self._copy()
    #     config.transport_config = transport_config
    #     return config

    def with_metrics_enabled(self, enabled: bool) -> "RemoteConfig":
        """设置是否启用性能指标"""
        config = self._copy()
        config.enable_metrics = enabled
        return config

    def _copy(self) -> "RemoteConfig":
        """创建配置的拷贝"""
        return RemoteConfig(
            rpc_timeout=self.rpc_timeout,
            opaque_start=self.opaque_start,
            max_waiters=self.max_waiters,
            cleanup_interval=self.cleanup_interval,
            waiter_timeout=self.waiter_timeout,
            enable_metrics=self.enable_metrics,
            connection_pool_size=self.connection_pool_size,
            connection_pool_timeout=self.connection_pool_timeout,
            # transport_config=self.transport_config,
        )

    def validate(self) -> None:
        """验证配置的有效性"""
        if self.rpc_timeout <= 0:
            raise ValueError(f"Invalid rpc_timeout: {self.rpc_timeout}")

        if self.max_waiters <= 0:
            raise ValueError(f"Invalid max_waiters: {self.max_waiters}")

        if self.cleanup_interval <= 0:
            raise ValueError(f"Invalid cleanup_interval: {self.cleanup_interval}")

        if self.waiter_timeout <= 0:
            raise ValueError(f"Invalid waiter_timeout: {self.waiter_timeout}")

        if self.connection_pool_size <= 0:
            raise ValueError(
                f"Invalid connection_pool_size: {self.connection_pool_size}"
            )

        if self.connection_pool_timeout <= 0:
            raise ValueError(
                f"Invalid connection_pool_timeout: {self.connection_pool_timeout}"
            )

    def __str__(self) -> str:
        """字符串表示"""
        return (
            f"RemoteConfig("
            f"rpc_timeout={self.rpc_timeout}, "
            f"max_waiters={self.max_waiters}, "
            f"cleanup_interval={self.cleanup_interval}, "
            f"connection_pool_size={self.connection_pool_size}, "
            f"enable_metrics={self.enable_metrics})"
        )

    def __repr__(self) -> str:
        """调试表示"""
        return self.__str__()


# 预定义配置
DEFAULT_CONFIG = RemoteConfig()
DEVELOPMENT_CONFIG = RemoteConfig(
    rpc_timeout=10.0,
    max_waiters=1000,
    cleanup_interval=30.0,
    waiter_timeout=60.0,
    enable_metrics=True,
    connection_pool_size=1,
)
PRODUCTION_CONFIG = RemoteConfig(
    rpc_timeout=30.0,
    max_waiters=50000,
    cleanup_interval=120.0,
    waiter_timeout=600.0,
    enable_metrics=True,
    connection_pool_size=5,
)
TESTING_CONFIG = RemoteConfig(
    rpc_timeout=5.0,
    max_waiters=100,
    cleanup_interval=10.0,
    waiter_timeout=30.0,
    enable_metrics=False,
    connection_pool_size=1,
)


def get_config(environment: str = "default") -> RemoteConfig:
    """根据环境获取配置"""
    env = environment.lower()

    if env == "development":
        return DEVELOPMENT_CONFIG
    elif env == "production":
        return PRODUCTION_CONFIG
    elif env == "testing":
        return TESTING_CONFIG
    else:
        return DEFAULT_CONFIG
