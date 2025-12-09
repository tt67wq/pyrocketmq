"""
Consumer模块 - 消费者组件

提供完整的RocketMQ消费者功能，包括偏移量管理、订阅管理、消息监听等。

主要组件：
- BaseConsumer/AsyncBaseConsumer: 消费者抽象基类
- ConcurrentConsumer/AsyncConcurrentConsumer: 并发消费者实现
- Config: 消费者配置管理
- OffsetStore: 偏移量存储功能
- SubscriptionManager: 订阅关系管理
- MessageListener/AsyncMessageListener: 消息监听器接口
- AllocateQueueStrategy: 队列分配策略

使用示例：
```python
from pyrocketmq.consumer import (
    create_consumer_config,
    create_offset_store,
    ConsumeResult,
    AsyncConcurrentConsumer
)
from pyrocketmq.model import MessageModel

# 创建消费者配置
config = create_consumer_config(
    consumer_group="my_group",
    namesrv_addr="localhost:9876"
)

# 创建异步并发消费者
consumer = AsyncConcurrentConsumer(config)
await consumer.register_message_listener(MyAsyncListener())
await consumer.subscribe("test_topic", create_tag_selector("*"))
await consumer.start()

# 创建偏移量存储
store = await create_offset_store(
    consumer_group="my_group",
    message_model=MessageModel.CLUSTERING,
    broker_manager=broker_manager
)
```
"""

# 标准库导入 - 按字母排序
# (当前文件中没有使用标准库中的特定类型)

# 第三方库导入 - 按字母排序
# (当前文件中没有第三方库导入)
# 本地/项目内部模块导入 - 按模块路径排序
# 核心消费者类
from pyrocketmq.consumer.async_base_consumer import AsyncBaseConsumer
from pyrocketmq.consumer.async_concurrent_consumer import AsyncConcurrentConsumer
from pyrocketmq.consumer.async_listener import AsyncMessageListener
from pyrocketmq.consumer.async_orderly_consumer import AsyncOrderlyConsumer
from pyrocketmq.consumer.base_consumer import BaseConsumer
from pyrocketmq.consumer.concurrent_consumer import ConcurrentConsumer
from pyrocketmq.consumer.config import ConsumerConfig
from pyrocketmq.consumer.orderly_consumer import OrderlyConsumer
from pyrocketmq.logging import get_logger

# 队列分配策略
from .allocate_queue_strategy import (
    AllocateContext,
    AllocateQueueStrategyBase,
    AllocateQueueStrategyFactory,
    AverageAllocateStrategy,
    HashAllocateStrategy,
    create_average_strategy,
    create_hash_strategy,
)
from .async_consume_from_where_manager import AsyncConsumeFromWhereManager

# 异步消息监听器
from .async_listener import (
    AsyncConsumeContext,
    SimpleAsyncMessageListener,
    create_async_message_listener,
)

# 偏移量存储
from .async_offset_store_factory import (
    AsyncOffsetStoreFactory,
)
from .async_offset_store_factory import (
    create_offset_store as create_async_offset_store,
)
from .async_remote_offset_store import AsyncRemoteOffsetStore

# 配置管理
from .config import (
    create_config,
    create_consumer_config,
)
from .consume_from_where_manager import ConsumeFromWhereManager

# 异常类 - 核心异常
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

# 工厂函数
from .factory import (
    # 便利启动函数
    create_and_start_async_consumer,
    create_and_start_async_orderly_consumer,
    # 异步消费者
    create_async_concurrent_consumer,
    create_async_consumer_deprecated,
    create_async_consumer_fast,
    create_async_consumer_light,
    # 便利别名
    create_async_consumer_simple,
    create_async_orderly_consumer,
    create_async_orderly_consumer_fast,
    create_async_orderly_consumer_light,
    create_async_orderly_consumer_simple,
    create_async_orderly_consumer_with_subscription,
    create_concurrent_consumer,
    # 同步消费者
    create_consumer,
    # 环境相关函数
    create_environment_based_async_consumer,
    create_environment_based_async_orderly_consumer,
    # 高性能消费者
    create_high_performance_async_consumer,
    create_high_performance_async_orderly_consumer,
    # 内存优化消费者
    create_memory_optimized_async_consumer,
    create_memory_optimized_async_orderly_consumer,
    create_orderly_consumer,
    quick_start_async_consumer,
    quick_start_async_orderly_consumer,
)

# 消息监听器
from .listener import (
    ConsumeContext,
    ConsumeResult,
    MessageListener,
    SimpleMessageListener,
    create_message_listener,
)
from .local_offset_store import LocalOffsetStore
from .offset_store import (
    OffsetEntry,
    OffsetStore,
    ReadOffsetType,
)
from .offset_store_factory import (
    OffsetStoreFactory,
    create_offset_store,
    validate_offset_store_config,
)
from .remote_offset_store import RemoteOffsetStore

# 异常类 - 订阅异常
from .subscription_exceptions import (
    InvalidSelectorError,
    InvalidTopicError,
    SubscriptionConflictError,
    SubscriptionDataError,
    SubscriptionError,
    SubscriptionLimitExceededError,
    TopicNotSubscribedError,
)

# 订阅管理
from .subscription_manager import (
    SubscriptionConflict,
    SubscriptionEntry,
    SubscriptionManager,
)

# 向后兼容别名
create_async_consumer = create_async_consumer_deprecated

logger = get_logger(__name__)


# ==================== 公开的API ====================
__all__ = [
    # 核心消费者类
    "BaseConsumer",
    "AsyncBaseConsumer",
    "AsyncConcurrentConsumer",
    "AsyncOrderlyConsumer",
    "ConcurrentConsumer",
    "OrderlyConsumer",
    # 配置管理
    "ConsumerConfig",
    "create_consumer_config",
    "create_config",
    # Consumer便利工厂函数
    "create_consumer",
    "create_concurrent_consumer",
    "create_orderly_consumer",
    # 异步消费者
    "create_async_concurrent_consumer",
    "create_async_consumer",  # 向后兼容
    "create_async_consumer_deprecated",
    "create_async_orderly_consumer",
    "create_async_orderly_consumer_with_subscription",
    # 高性能消费者
    "create_high_performance_async_consumer",
    "create_high_performance_async_orderly_consumer",
    # 内存优化消费者
    "create_memory_optimized_async_consumer",
    "create_memory_optimized_async_orderly_consumer",
    # 便利启动函数
    "create_and_start_async_consumer",
    "create_and_start_async_orderly_consumer",
    # 环境相关函数
    "create_environment_based_async_consumer",
    "create_environment_based_async_orderly_consumer",
    # 便利别名
    "create_async_consumer_simple",
    "create_async_consumer_fast",
    "create_async_consumer_light",
    "quick_start_async_consumer",
    "create_async_orderly_consumer_simple",
    "create_async_orderly_consumer_fast",
    "create_async_orderly_consumer_light",
    "quick_start_async_orderly_consumer",
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
