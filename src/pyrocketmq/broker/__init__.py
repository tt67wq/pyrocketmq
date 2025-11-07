"""
Broker 模块
提供 Broker 客户端功能，用于消息发送、拉取、偏移量管理和连接管理。
"""

from .async_broker_manager import (
    AsyncBrokerConnectionPool,
    AsyncBrokerManager,
    create_async_broker_manager,
    create_broker_manager_auto,
    create_simple_async_broker_manager,
)
from .async_client import AsyncBrokerClient, create_async_broker_client
from .broker_manager import (
    BrokerConnectionPool,
    BrokerManager,
    create_broker_manager,
    create_simple_broker_manager,
)
from .client import BrokerClient, create_broker_client
from .connection_info import BrokerConnectionInfo, BrokerState

__all__ = [
    # Sync Client
    "BrokerClient",
    "create_broker_client",
    # Async Client
    "AsyncBrokerClient",
    "create_async_broker_client",
    # Broker Manager - Sync
    "BrokerManager",
    "BrokerConnectionPool",
    "BrokerConnectionInfo",
    "BrokerState",
    # Broker Manager - Async
    "AsyncBrokerManager",
    "AsyncBrokerConnectionPool",
    # Convenience Functions
    "create_broker_manager",
    "create_simple_broker_manager",
    "create_async_broker_manager",
    "create_simple_async_broker_manager",
    "create_broker_manager_auto",
]
