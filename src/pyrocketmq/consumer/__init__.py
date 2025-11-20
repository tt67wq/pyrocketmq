"""
Consumer模块 - 消费者组件

提供完整的RocketMQ消费者功能，包括偏移量管理、订阅管理、消息监听等。

主要组件：
- BaseConsumer: 消费者抽象基类
- Config: 消费者配置管理
- OffsetStore: 偏移量存储功能
- SubscriptionManager: 订阅关系管理
- MessageListener: 消息监听器接口
- AllocateQueueStrategy: 队列分配策略

使用示例：
```python
from pyrocketmq.consumer import (
    create_consumer_config,
    create_offset_store,
    ConsumeResult
)
from pyrocketmq.model import MessageModel

# 创建消费者配置
config = create_consumer_config(
    consumer_group="my_group",
    namesrv_addr="localhost:9876"
)

# 创建偏移量存储
store = await create_offset_store(
    consumer_group="my_group",
    message_model=MessageModel.CLUSTERING,
    broker_manager=broker_manager
)

# 创建消息监听器
```
"""

# ==================== 核心消费者类 ====================
# ==================== 队列分配策略 ====================
from .allocate_queue_strategy import (
    AllocateContext,
    AllocateQueueStrategyBase,
    AllocateQueueStrategyFactory,
    AverageAllocateStrategy,
    HashAllocateStrategy,
    create_average_strategy,
    create_hash_strategy,
)
from .async_base_consumer import AsyncBaseConsumer

# ==================== 消费起始位置管理 ====================
from .async_consume_from_where_manager import AsyncConsumeFromWhereManager

# ==================== 消息监听器 ====================
from .async_listener import (
    AsyncConsumeContext,
    AsyncMessageListener,
    SimpleAsyncMessageListener,
    create_async_message_listener,
)
from .async_offset_store_factory import (
    AsyncOffsetStoreFactory,
)
from .async_offset_store_factory import (
    create_offset_store as create_async_offset_store,
)
from .async_remote_offset_store import AsyncRemoteOffsetStore
from .base_consumer import BaseConsumer
from .concurrent_consumer import ConcurrentConsumer

# ==================== 配置管理 ====================
from .config import ConsumerConfig, create_config, create_consumer_config
from .consume_from_where_manager import ConsumeFromWhereManager

# ==================== Consumer便利工厂函数 ====================
from .consumer_factory import (
    create_concurrent_consumer,
    create_consumer,
    create_consumer_with_config,
)

# ==================== 异常类 ====================
from .errors import (
    BrokerNotAvailableError,
    ConfigError,
    ConsumerError,
    ConsumerShutdownError,
    ConsumerStartError,
    ConsumerStateError,
    MessageConsumeError,
    MessagePullError,
    NameServerError,
    NetworkError,
    OffsetError,
    OffsetFetchError,
    RebalanceError,
    SubscribeError,
    TimeoutError,
    UnsubscribeError,
    ValidationError,
    create_broker_not_available_error,
    create_consumer_start_error,
    create_message_consume_error,
    create_offset_fetch_error,
    create_timeout_error,
)
from .listener import (
    ConsumeContext,
    ConsumeResult,
    MessageListener,
    SimpleMessageListener,
    create_message_listener,
)
from .local_offset_store import LocalOffsetStore

# ==================== 偏移量存储 ====================
from .offset_store import OffsetEntry, OffsetStore, ReadOffsetType
from .offset_store_factory import (
    OffsetStoreFactory,
    create_offset_store,
    validate_offset_store_config,
)
from .remote_offset_store import RemoteOffsetStore
from .subscription_exceptions import (
    InvalidSelectorError,
    InvalidTopicError,
    SubscriptionConflictError,
    SubscriptionDataError,
    SubscriptionError,
    SubscriptionLimitExceededError,
    TopicNotSubscribedError,
)

# ==================== 订阅管理 ====================
from .subscription_manager import (
    SubscriptionConflict,
    SubscriptionEntry,
    SubscriptionManager,
)

# ==================== 公开的API ====================
__all__ = [
    # 核心消费者类
    "BaseConsumer",
    "AsyncBaseConsumer",
    "ConcurrentConsumer",
    # 配置管理
    "ConsumerConfig",
    "create_consumer_config",
    "create_config",
    # Consumer便利工厂函数
    "create_consumer",
    "create_consumer_with_config",
    "create_concurrent_consumer",
    # 偏移量存储
    "OffsetStore",
    "ReadOffsetType",
    "OffsetEntry",
    "LocalOffsetStore",
    "RemoteOffsetStore",
    "AsyncRemoteOffsetStore",
    "OffsetStoreFactory",
    "AsyncOffsetStoreFactory",
    "create_offset_store",
    "create_async_offset_store",
    "validate_offset_store_config",
    # 订阅管理
    "SubscriptionManager",
    "SubscriptionEntry",
    "SubscriptionConflict",
    # 异步消息监听器
    "AsyncMessageListener",
    "AsyncConsumeContext",
    "SimpleAsyncMessageListener",
    "create_async_message_listener",
    # 同步消息监听器
    "MessageListener",
    "SimpleMessageListener",
    "ConsumeResult",
    "ConsumeContext",
    "create_message_listener",
    # 队列分配策略
    "AllocateQueueStrategyBase",
    "AverageAllocateStrategy",
    "HashAllocateStrategy",
    "AllocateQueueStrategyFactory",
    "AllocateContext",
    "create_average_strategy",
    "create_hash_strategy",
    # 消费起始位置管理
    "ConsumeFromWhereManager",
    "AsyncConsumeFromWhereManager",
    # 异常类 - 核心异常
    "ConsumerError",
    "ConsumerStartError",
    "ConsumerShutdownError",
    "ConsumerStateError",
    "SubscribeError",
    "UnsubscribeError",
    "MessageConsumeError",
    "MessagePullError",
    "OffsetError",
    "OffsetFetchError",
    "RebalanceError",
    "BrokerNotAvailableError",
    "NameServerError",
    "NetworkError",
    "TimeoutError",
    "ConfigError",
    "ValidationError",
    # 异常类 - 订阅异常
    "SubscriptionError",
    "InvalidTopicError",
    "InvalidSelectorError",
    "TopicNotSubscribedError",
    "SubscriptionConflictError",
    "SubscriptionLimitExceededError",
    "SubscriptionDataError",
    # 异常创建函数
    "create_consumer_start_error",
    "create_message_consume_error",
    "create_broker_not_available_error",
    "create_timeout_error",
    "create_offset_fetch_error",
]
