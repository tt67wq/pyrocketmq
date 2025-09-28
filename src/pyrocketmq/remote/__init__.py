"""
PyRocketMQ 远程通信模块

该模块提供了与RocketMQ服务器进行远程通信的功能，支持同步和异步两种模式。
"""

from .async_remote import AsyncRemote
from .config import (
    DEFAULT_CONFIG,
    DEVELOPMENT_CONFIG,
    PRODUCTION_CONFIG,
    TESTING_CONFIG,
    RemoteConfig,
    get_config,
)
from .errors import (
    ConfigurationError,
    ConnectionClosedError,
    ConnectionError,
    InvalidCommandError,
    MaxWaitersExceededError,
    ProtocolError,
    RemoteError,
    ResourceExhaustedError,
    SerializationError,
    TimeoutError,
    TransportError,
    WaiterTimeoutError,
    is_connection_error,
    is_fatal_error,
    is_retryable_error,
    is_timeout_error,
)
from .factory import (
    RemoteFactory,
    create_async_remote,
    create_async_remote_from_env,
    create_remote_from_env,
    create_sync_remote,
)
from .pool import AsyncConnectionPool, ConnectionPool
from .sync_remote import Remote

__all__ = [
    # 配置相关
    "RemoteConfig",
    "DEFAULT_CONFIG",
    "DEVELOPMENT_CONFIG",
    "PRODUCTION_CONFIG",
    "TESTING_CONFIG",
    "get_config",
    # 异常相关
    "RemoteError",
    "ConnectionError",
    "TimeoutError",
    "SerializationError",
    "ProtocolError",
    "ResourceExhaustedError",
    "ConfigurationError",
    "WaiterTimeoutError",
    "MaxWaitersExceededError",
    "InvalidCommandError",
    "ConnectionClosedError",
    "TransportError",
    "is_connection_error",
    "is_timeout_error",
    "is_retryable_error",
    "is_fatal_error",
    # 远程通信类
    "Remote",
    "AsyncRemote",
    # 工厂函数
    "RemoteFactory",
    "create_sync_remote",
    "create_async_remote",
    "create_remote_from_env",
    "create_async_remote_from_env",
    # 连接池
    "ConnectionPool",
    "AsyncConnectionPool",
]

# 版本信息
__version__ = "1.0.0"
__author__ = "PyRocketMQ Team"
__email__ = "team@pyrocketmq.org"


def __getattr__(name):
    """支持动态导入"""
    if name == "__all__":
        return __all__
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")
