"""
NameServer 模块
提供 NameServer 客户端功能，用于查询 Topic 路由信息和 Broker 集群信息。
"""

from .client import (
    AsyncNameServerClient,
    SyncNameServerClient,
    create_async_client,
    create_sync_client,
)
from .errors import (
    NameServerConnectionError,
    NameServerDataParseError,
    NameServerError,
    NameServerProtocolError,
    NameServerResponseError,
    NameServerTimeoutError,
)
from .models import (
    BrokerClusterInfo,
    BrokerData,
    QueueData,
    TopicRouteData,
)

__all__ = [
    # Models
    "BrokerData",
    "QueueData",
    "TopicRouteData",
    "BrokerClusterInfo",
    # Clients
    "SyncNameServerClient",
    "AsyncNameServerClient",
    "create_sync_client",
    "create_async_client",
    # Errors
    "NameServerError",
    "NameServerConnectionError",
    "NameServerTimeoutError",
    "NameServerProtocolError",
    "NameServerResponseError",
    "NameServerDataParseError",
]
