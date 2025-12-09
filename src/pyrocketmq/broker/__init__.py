"""
Broker 模块
提供 Broker 客户端功能，用于消息发送、拉取、偏移量管理和连接管理。
"""

from pyrocketmq.utils import (
    AsyncReaderPreferenceRWLock,
    AsyncReadWriteContext,
    AsyncReadWriteLock,
    AsyncWriterPreferenceRWLock,
    ReadWriteContext,
    ReadWriteLock,
)

from .async_broker_manager import AsyncBrokerManager
from .async_client import AsyncBrokerClient, create_async_broker_client
from .broker_manager import BrokerManager
from .client import BrokerClient, create_broker_client
from .errors import (
    AuthorizationError,
    BrokerBusyError,
    BrokerConnectionError,
    BrokerError,
    BrokerProtocolError,
    BrokerResponseError,
    BrokerSystemError,
    BrokerTimeoutError,
    MessagePullError,
    OffsetError,
)
from .topic_broker_mapping import (
    AsyncRouteInfo,
    AsyncTopicBrokerMapping,
    RouteInfo,
    TopicBrokerMapping,
    async_create_topic_broker_mapping,
    create_async_topic_broker_mapping,
    create_topic_broker_mapping,
)

__all__ = [
    # Sync Client
    "BrokerClient",
    "create_broker_client",
    # Async Client
    "AsyncBrokerClient",
    "create_async_broker_client",
    # Broker Manager - Sync
    "BrokerManager",
    # Broker Manager - Async
    "AsyncBrokerManager",
    # ReadWrite Lock
    "ReadWriteLock",
    "ReadWriteContext",
    "AsyncReadWriteLock",
    "AsyncReadWriteContext",
    "AsyncReaderPreferenceRWLock",
    "AsyncWriterPreferenceRWLock",
    # Topic Broker Mapping
    "RouteInfo",
    "AsyncRouteInfo",
    "TopicBrokerMapping",
    "AsyncTopicBrokerMapping",
    "create_topic_broker_mapping",
    "create_async_topic_broker_mapping",
    "async_create_topic_broker_mapping",
    # Exceptions
    "BrokerError",
    "BrokerConnectionError",
    "BrokerTimeoutError",
    "BrokerResponseError",
    "BrokerProtocolError",
    "AuthorizationError",
    "BrokerBusyError",
    "MessagePullError",
    "OffsetError",
    "BrokerSystemError",
]
