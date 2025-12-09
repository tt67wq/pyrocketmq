"""
Queue helper module for pyrocketmq.

Provides message routing and queue selection functionality for RocketMQ clients.
Includes both synchronous and asynchronous implementations.
"""

from .errors import (
    BrokerNotAvailableError,
    QueueHelperError,
    QueueNotAvailableError,
    RouteNotFoundError,
)
from .queue_selectors import (
    AsyncMessageHashSelector,
    AsyncQueueSelector,
    AsyncRandomSelector,
    AsyncRoundRobinSelector,
    MessageHashSelector,
    QueueSelector,
    RandomSelector,
    RoundRobinSelector,
)
from .router import (
    MASTER_BROKER_ID,
    AsyncMessageRouter,
    MessageRouter,
    RoutingResult,
    RoutingStrategy,
)

__all__ = [
    # Base Exception
    "QueueHelperError",
    # Exceptions
    "RouteNotFoundError",
    "BrokerNotAvailableError",
    "QueueNotAvailableError",
    # Queue Selectors (Sync)
    "QueueSelector",
    "RoundRobinSelector",
    "RandomSelector",
    "MessageHashSelector",
    # Queue Selectors (Async)
    "AsyncQueueSelector",
    "AsyncRoundRobinSelector",
    "AsyncRandomSelector",
    "AsyncMessageHashSelector",
    # Message Routers
    "MessageRouter",
    "AsyncMessageRouter",
    # Router Enums and Constants
    "RoutingStrategy",
    "MASTER_BROKER_ID",
    # Router Data Classes
    "RoutingResult",
]
