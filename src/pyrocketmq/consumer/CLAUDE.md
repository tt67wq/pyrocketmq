# pyrocketmq Consumer 模块

## 模块概述

Consumer模块是pyrocketmq的消息消费者实现，提供完整的RocketMQ消息消费功能。该模块采用分层架构设计，支持并发消费、顺序消费、集群广播消费等多种消费模式，并具备完善的偏移量管理、订阅管理、消息监听等核心功能。

**当前状态**: ✅ Consumer模块完整实现完成 (v3.0.0)

- ✅ **已完成的组件**: 配置管理、偏移量存储、订阅管理、消息监听器、队列分配策略、消费起始位置管理、异常体系、监控指标
- ✅ **已完成实现**: ConcurrentConsumer (同步并发消费者)
- ✅ **已完成实现**: AsyncConcurrentConsumer (异步并发消费者)
- ✅ **已完成实现**: AsyncOrderlyConsumer (异步顺序消费者)
- ✅ **已完成实现**: 完整的重平衡机制和ProcessQueue消息缓存管理
- ✅ **已完成实现**: 异步版本的所有核心组件
- ✅ **新增功能**: 15个异步顺序消费者工厂函数，简化不同场景的消费者创建
- ✅ **新增功能**: 完整的消费者统计信息系统
- ✅ **新增功能**: 优化的配置管理和环境变量支持

### 核心特性

- **🚀 高性能并发消费**: 基于线程池/异步任务的并发处理架构，支持高吞吐量消息消费
- **🔄 自动重平衡**: 智能队列分配和重平衡机制，确保负载均衡
- **💾 偏移量管理**: 支持远程和本地两种偏移量存储模式
- **📡 灵活订阅**: 支持基于TAG的消息过滤和订阅关系管理
- **🛡️ 完善监控**: 丰富的监控指标和性能统计
- **🔧 便捷API**: 提供15+个工厂函数和便利方法，简化使用
- **⚡ 故障恢复**: 完善的错误处理和自动恢复机制
- **🎯 双模式支持**: 同步和异步两种实现模式，满足不同应用场景需求
- **📊 统计信息**: 完整的消费统计系统，支持速率、延迟、成功率等指标
- **🏭 工厂模式**: 丰富的工厂函数，支持高性能、内存优化、快速启动等多种预设配置

### 模块职责

1. **消息消费**: 实现高并发的消息拉取和处理
2. **订阅管理**: 管理Topic订阅关系和消息过滤
3. **偏移量管理**: 记录和持久化消费进度
4. **队列分配**: 实现负载均衡的队列分配策略
5. **生命周期管理**: 完整的消费者启动、运行、关闭生命周期
6. **错误处理**: 全面的异常处理和故障恢复
7. **性能监控**: 实时统计和监控消费性能指标
8. **配置管理**: 灵活的配置加载和环境变量支持

## 模块架构

### 分层架构设计

```
┌─────────────────────────────────────────────────────────────┐
│                    应用接口层                                │
│  BaseConsumer + AsyncBaseConsumer + ConcurrentConsumer      │
│  AsyncConcurrentConsumer + AsyncOrderlyConsumer + 工厂函数   │
├─────────────────────────────────────────────────────────────┤
│                    业务逻辑层                                │
│  消息处理 + 订阅管理 + 偏移量管理 + 重平衡                    │
│  MessageListener + AsyncMessageListener + 队列分配策略       │
│  统计信息收集 + 性能监控                                     │
├─────────────────────────────────────────────────────────────┤
│                    基础服务层                                │
│  ProcessQueue + OffsetStore + TopicBrokerMapping           │
│  ConsumeFromWhereManager + SubscriptionManager             │
├─────────────────────────────────────────────────────────────┤
│                    工具支持层                                │
│  异常处理 + 配置管理 + 队列选择器 + 日志记录                 │
└─────────────────────────────────────────────────────────────┘
```

### 文件结构

```
consumer/
├── __init__.py                      # 模块入口，导出所有公共API (60+个导出)
├── config.py                        # 消费者配置管理
├── base_consumer.py                 # 消费者抽象基类
├── async_base_consumer.py           # 异步消费者抽象基类
├── concurrent_consumer.py           # 同步并发消费者实现
├── async_concurrent_consumer.py     # 异步并发消费者实现
├── orderly_consumer.py              # 同步顺序消费者实现
├── async_orderly_consumer.py        # 异步顺序消费者实现
├── consumer_factory.py              # 同步消费者工厂函数
├── async_factory.py                 # 异步消费者工厂函数 (15个函数)
├── listener.py                      # 同步消息监听器接口
├── async_listener.py                # 异步消息监听器接口
├── process_queue.py                 # 消息缓存管理
├── offset_store.py                  # 偏移量存储抽象基类
├── remote_offset_store.py           # 远程偏移量存储 (集群模式)
├── async_remote_offset_store.py     # 异步远程偏移量存储
├── local_offset_store.py            # 本地偏移量存储 (广播模式)
├── async_local_offset_store.py      # 异步本地偏移量存储
├── offset_store_factory.py          # 偏移量存储工厂
├── async_offset_store_factory.py    # 异步偏移量存储工厂
├── subscription_manager.py          # 订阅关系管理
├── topic_broker_mapping.py          # Topic-Broker映射管理
├── consume_from_where_manager.py    # 消费起始位置管理 (同步)
├── async_consume_from_where_manager.py # 消费起始位置管理 (异步)
├── allocate_queue_strategy.py       # 队列分配策略
├── queue_selector.py                # 队列选择器
├── errors.py                        # 核心异常类 (20+种异常)
├── subscription_exceptions.py       # 订阅相关异常
└── CLAUDE.md                        # 模块文档 (本文件)
```

### 模块依赖关系

```
Consumer模块依赖
├── 内部依赖
│   ├── model (消息模型、常量定义)
│   ├── broker (Broker通信)
│   ├── nameserver (NameServer通信)
│   ├── remote (远程通信)
│   ├── logging (日志记录)
│   └── utils (工具类)
└── 外部依赖
    ├── asyncio (异步编程)
    ├── aiofiles (异步文件操作)
    ├── dataclasses (数据类)
    ├── typing (类型注解)
    ├── threading (线程同步)
    ├── json (JSON序列化)
    └── os/os.path (系统操作)
```

## 核心组件详解

### 1. 配置管理 (config.py)

#### ConsumerConfig

完整的消费者配置管理类，提供全面的配置参数和验证机制。

**核心配置类别**:
- **基础配置**: consumer_group, namesrv_addr (必需参数)
- **消费行为**: message_model, consume_from_where, allocate_queue_strategy
- **性能配置**: consume_thread_min/max, consume_timeout, pull_batch_size
- **偏移量存储**: persist_interval, offset_store_path, max_cache_count_per_queue
- **队列锁配置**: lock_expire_time (顺序消费)
- **高级配置**: enable_auto_commit, enable_message_trace

**关键特性**:
- 完整的参数验证和边界检查
- 支持环境变量动态加载配置
- 智能的客户端ID生成
- 提供to_dict()方法便于序列化

**环境变量支持**:
```bash
# 性能配置
ROCKETMQ_CONSUME_THREAD_MIN=20
ROCKETMQ_CONSUME_THREAD_MAX=64
ROCKETMQ_CONSUME_BATCH_SIZE=1
ROCKETMQ_PULL_BATCH_SIZE=32
ROCKETMQ_CONSUME_TIMEOUT=15

# 行为配置
ROCKETMQ_MESSAGE_MODEL=CLUSTERING
ROCKETMQ_CONSUME_FROM_WHERE=LAST_OFFSET
ROCKETMQ_ALLOCATE_STRATEGY=AVERAGE

# 偏移量存储配置
ROCKETMQ_PERSIST_INTERVAL=5000
ROCKETMQ_OFFSET_STORE_PATH=~/.rocketmq/offsets
ROCKETMQ_MAX_CACHE_COUNT_PER_QUEUE=1024
ROCKETMQ_MAX_CACHE_SIZE_PER_QUEUE=512
ROCKETMQ_ENABLE_AUTO_RECOVERY=true
ROCKETMQ_MAX_RETRY_TIMES=3
```

### 2. 异常体系

#### 核心异常类型 (errors.py)

完整的异常类型体系，精确处理各种错误场景：

**基础异常类**:
- `ConsumerError`: 消费者基础异常类
- `ConsumerStartError`: 消费者启动异常
- `ConsumerShutdownError`: 消费者关闭异常
- `ConsumerStateError`: 消费者状态异常

**功能异常类**:
- `SubscribeError`: 订阅异常
- `MessageConsumeError`: 消息消费异常
- `MessagePullError`: 消息拉取异常
- `OffsetError`: 偏移量相关异常
- `RebalanceError`: 重平衡异常

**通信异常类**:
- `BrokerNotAvailableError`: Broker不可用异常
- `NameServerError`: NameServer异常
- `NetworkError`: 网络通信异常
- `TimeoutError`: 超时异常

**配置异常类**:
- `ConfigError`: 配置错误异常
- `ValidationError`: 参数验证异常
- `OffsetFetchError`: 偏移量获取异常

#### 订阅相关异常 (subscription_exceptions.py)

**订阅管理异常**:
- `SubscriptionError`: 订阅基础异常
- `InvalidTopicError`: 无效Topic异常
- `InvalidSelectorError`: 无效选择器异常
- `SubscriptionConflictError`: 订阅冲突异常
- `TopicNotSubscribedError`: Topic未订阅异常
- `SubscriptionLimitExceededError`: 订阅数量超限异常
- `SubscriptionDataError`: 订阅数据异常

**异常创建函数**:
```python
# 便利的异常创建函数
create_consumer_start_error(message: str, cause: Exception = None)
create_message_consume_error(message: str, cause: Exception = None)
create_broker_not_available_error(broker_name: str, cause: Exception = None)
create_timeout_error(operation: str, timeout: float, cause: Exception = None)
create_offset_fetch_error(topic: str, queue_id: int, cause: Exception = None)
```

### 3. 消息监听器 (listener.py & async_listener.py)

#### 监听器接口

**同步监听器**:
```python
class MessageListener:
    def consume_message(self, messages: List[Message], context: ConsumeContext) -> ConsumeResult
```

**异步监听器**:
```python
class AsyncMessageListener:
    async def consume_message(self, messages: List[Message], context: AsyncConsumeContext) -> ConsumeResult
```

#### 消费上下文

**ConsumeContext** (同步):
- 提供消费过程的上下文信息
- 包含topic、queue_id、broker_name等信息
- 支持自定义属性传递

**AsyncConsumeContext** (异步):
- 异步版本的消费上下文
- 提供相同的上下文信息和功能

#### 简单监听器实现

**SimpleMessageListener** (同步):
```python
def __init__(self, handler: Callable[[Message], None])
def consume_message(self, messages: List[Message], context: ConsumeContext) -> ConsumeResult
```

**SimpleAsyncMessageListener** (异步):
```python
def __init__(self, handler: Callable[[Message], None])
async def consume_message(self, messages: List[Message], context: AsyncConsumeContext) -> ConsumeResult
```

**便利创建函数**:
```python
def create_message_listener(handler: Callable[[Message], None]) -> SimpleMessageListener
async def async_handler(message: Message) -> None
def create_async_message_listener(handler: Callable) -> SimpleAsyncMessageListener
```

### 4. 订阅管理器 (subscription_manager.py)

#### SubscriptionManager

管理消费者的订阅关系，支持Topic订阅和消息过滤。

**核心功能**:
```python
def __init__(self, consumer_group: str)
def subscribe(self, topic: str, selector: MessageSelector) -> None
def unsubscribe(self, topic: str) -> None
def get_subscription(self, topic: str) -> Optional[SubscriptionEntry]
def get_all_subscriptions(self) -> Dict[str, SubscriptionEntry]
def clear_subscriptions(self) -> None
```

#### 订阅条目

**SubscriptionEntry**: 记录单个Topic的订阅信息
- topic: 订阅的Topic名称
- selector: 消息选择器
- sub_version: 订阅版本号

**SubscriptionConflict**: 记录订阅冲突信息
- topic: 冲突的Topic
- existing_selector: 已存在的选择器
- new_selector: 新的选择器
- conflict_type: 冲突类型

### 5. 消息缓存管理 (process_queue.py)

#### ProcessQueue

管理消息的缓存和消费状态，支持消息的批量操作和统计。

**核心功能**:
```python
def __init__(self, message_queue: MessageQueue, max_cache_size: int = 10000)
def add_batch_messages(self, messages: List[Message]) -> int
def remove_batch_messages(self, messages: List[Message]) -> int
def get_min_offset(self) -> Optional[int]
def get_max_offset(self) -> Optional[int]
def get_stats(self) -> ProcessQueueStats
def need_flow_control(self, new_message_count: int) -> bool
def contains_message(self, message: Message) -> bool
```

#### 统计信息

**ProcessQueueStats**: 提供ProcessQueue的统计信息
- cached_message_count: 缓存消息数量
- cached_message_size: 缓存消息大小
- min_offset: 最小偏移量
- max_offset: 最大偏移量
- last_consume_timestamp: 最后消费时间

### 6. 异步消费者基类 (async_base_consumer.py)

#### AsyncBaseConsumer

异步消费者的抽象基类，提供完整的异步消费者框架。

**核心功能**:
```python
def __init__(self, config: ConsumerConfig)
async def start(self) -> None
async def shutdown(self) -> None
async def _consume_message(self, messages: List[Message]) -> ConsumeResult
async def subscribe(self, topic: str, selector: MessageSelector) -> None
async def unsubscribe(self, topic: str) -> None
async def register_message_listener(self, listener: AsyncMessageListener) -> None
```

#### 路由管理方法

```python
async def _route_refresh_loop(self) -> None
async def _refresh_all_routes(self) -> None
async def _collect_broker_addresses(self) -> Dict[str, str]
```

#### 心跳管理方法

```python
async def _heartbeat_loop(self) -> None
def _build_heartbeat_data(self) -> HeartbeatData
async def _send_heartbeat_to_all_brokers(self) -> None
async def _send_heartbeat_to_broker(self, broker_name: str, broker_addr: str) -> None
```

#### 生命周期管理方法

```python
async def _async_start(self) -> None
async def _async_shutdown(self) -> None
```

### 7. 同步并发消费者 (concurrent_consumer.py)

#### ConcurrentConsumer

同步并发消费者实现，基于线程池进行并发消息处理。

**核心功能**:
```python
def __init__(self, config: ConsumerConfig)
def start(self) -> None
def shutdown(self) -> None
def register_message_listener(self, listener: MessageListener) -> None
```

#### 核心消费逻辑

```python
def _do_rebalance(self) -> None
def _allocate_queues(self) -> None
def _pull_messages_loop(self) -> None
def _handle_pulled_messages(self, messages: List[Message]) -> None
def _consume_messages_loop(self) -> None
def _add_messages_to_cache(self, messages: List[Message]) -> None
def _remove_messages_from_cache(self, messages: List[Message]) -> None
def _update_offset_from_cache(self) -> None
```

### 8. 异步并发消费者 (async_concurrent_consumer.py)

#### AsyncConcurrentConsumer

异步并发消费者实现，基于asyncio进行高并发消息处理。

**核心功能**:
```python
def __init__(self, config: ConsumerConfig)
async def start(self) -> None
async def shutdown(self) -> None
async def _consume_message(self, messages: List[Message]) -> ConsumeResult
```

**异步特性**:
- 基于asyncio事件循环
- 支持高并发消息处理
- 异步的拉取和消费流程
- 优化的资源管理

### 9. 异步顺序消费者 (async_orderly_consumer.py)

#### AsyncOrderlyConsumer

**新增功能**：异步顺序消费者实现，保证消息的顺序性消费。

**核心特性**:
- 基于队列锁的顺序消费保证
- 异步的锁获取和释放机制
- 支持故障恢复和锁超时处理
- 与AsyncBaseConsumer的7模块重组架构集成

**主要功能**:
```python
def __init__(self, config: ConsumerConfig)
async def start(self) -> None
async def shutdown(self) -> None
async def _consume_message_orderly(self, messages: List[Message]) -> ConsumeResult
async def _lock_queue(self, message_queue: MessageQueue) -> bool
async def _unlock_queue(self, message_queue: MessageQueue) -> None
```

### 10. 偏移量存储体系

#### 10.1 抽象基类 (offset_store.py)

**OffsetStore**: 偏移量存储的抽象基类，定义统一的接口。

**核心接口**:
```python
def start(self) -> None
def stop(self) -> None
def load(self) -> None
def update_offset(self, mq: MessageQueue, offset: int, increase_only: bool = True) -> None
def read_offset(self, mq: MessageQueue, read_type: ReadOffsetType) -> Optional[int]
def persist(self, mqs: Optional[List[MessageQueue]] = None) -> None
def remove_offset(self, mq: MessageQueue) -> None
def clone_offset_table(self, topic: str) -> Dict[MessageQueue, int]
```

**ReadOffsetType**: 偏移量读取类型枚举
- `READ_FROM_MEMORY`: 从内存读取
- `READ_FROM_STORE`: 从存储读取
- `MEMORY_FIRST_THEN_STORE`: 优先内存，其次存储

**OffsetEntry**: 偏移量条目数据结构

#### 10.2 远程偏移量存储 (remote_offset_store.py & async_remote_offset_store.py)

**RemoteOffsetStore**: 集群模式下的偏移量存储，将偏移量保存在Broker端。

**核心特性**:
- 适用于集群消费模式
- 偏移量在Broker端集中管理
- 支持多消费者协调
- 网络通信的异常处理

**主要方法**:
```python
def __init__(self, consumer_group: str, broker_manager: BrokerManager, config: ConsumerConfig)
def persist(self, mqs: Optional[List[MessageQueue]] = None) -> None
def read_offset(self, mq: MessageQueue, read_type: ReadOffsetType) -> Optional[int]
```

**AsyncRemoteOffsetStore**: 异步版本的远程偏移量存储，提供相同的API接口，但使用异步实现。

#### 10.3 本地偏移量存储 (local_offset_store.py & async_local_offset_store.py)

**LocalOffsetStore**: 广播模式下的偏移量存储，将偏移量保存在本地文件。

**核心特性**:
- 适用于广播消费模式
- 每个消费者独立维护偏移量
- 基于文件的持久化存储
- 原子性文件操作保证数据一致性

**主要方法**:
```python
def __init__(self, consumer_group: str, config: ConsumerConfig)
def persist(self, mqs: Optional[List[MessageQueue]] = None) -> None
def load(self) -> None
```

**AsyncLocalOffsetStore**: 异步版本的本地偏移量存储，使用aiofiles进行异步文件操作。

#### 10.4 偏移量存储工厂 (offset_store_factory.py & async_offset_store_factory.py)

**OffsetStoreFactory**: 同步偏移量存储工厂，根据配置创建合适的偏移量存储实例。

**核心功能**:
```python
def __init__(self, broker_manager: BrokerManager, config: ConsumerConfig)
def create_offset_store(self, message_model: str) -> OffsetStore
```

**AsyncOffsetStoreFactory**: 异步偏移量存储工厂，提供异步的偏移量存储创建。

**便利创建函数**:
```python
def create_offset_store(consumer_group: str, message_model: str, broker_manager: BrokerManager, config: ConsumerConfig) -> OffsetStore
async def create_async_offset_store(consumer_group: str, message_model: str, broker_manager: BrokerManager, config: ConsumerConfig) -> AsyncOffsetStore
```

**验证函数**:
```python
def validate_offset_store_config(config: ConsumerConfig) -> None
```

### 11. 队列分配策略 (allocate_queue_strategy.py)

#### 队列分配策略

**AllocateQueueStrategyBase**: 队列分配策略的抽象基类。

**AllocateContext**: 队列分配上下文，包含分配所需的所有信息：
```python
def __init__(self, consumer_group: str, current_cid: str, all_consumer_cids: List[str], mq_list: List[MessageQueue])
```

#### 具体策略实现

**AverageAllocateStrategy**: 平均分配策略，将队列平均分配给所有消费者。
```python
def allocate(self, context: AllocateContext) -> List[MessageQueue]
```

**HashAllocateStrategy**: 哈希分配策略，基于消费者CID的哈希值进行分配。
```python
def allocate(self, context: AllocateContext) -> List[MessageQueue]
```

#### 策略工厂

**AllocateQueueStrategyFactory**: 策略工厂类，根据策略名称创建对应的策略实例。
```python
def get_strategy(self, strategy_name: str) -> AllocateQueueStrategyBase
```

**便利创建函数**:
```python
def create_average_strategy() -> AverageAllocateStrategy
def create_hash_strategy() -> HashAllocateStrategy
```

### 12. 消费起始位置管理

#### 12.1 同步版本 (consume_from_where_manager.py)

**ConsumeFromWhereManager**: 管理消息消费的起始位置。

**核心功能**:
```python
def __init__(self, broker_manager: BrokerManager, config: ConsumerConfig)
def get_consume_offset(self, mq: MessageQueue) -> Optional[int]
```

**支持的起始位置**:
- `CONSUME_FROM_LAST_OFFSET`: 从最新偏移量开始
- `CONSUME_FROM_FIRST_OFFSET`: 从最早偏移量开始
- `CONSUME_FROM_TIMESTAMP`: 从指定时间戳开始

#### 12.2 异步版本 (async_consume_from_where_manager.py)

**AsyncConsumeFromWhereManager**: 异步版本的消费起始位置管理器。

**核心功能**:
```python
def __init__(self, broker_manager: BrokerManager, config: ConsumerConfig)
async def get_consume_offset(self, mq: MessageQueue) -> Optional[int]
```

**异步特性**:
- 基于asyncio的异步实现
- 支持并发查询多个队列的起始位置
- 优化的网络通信处理

### 13. 消费者工厂 (consumer_factory.py & async_factory.py)

#### 消费者工厂函数

**同步工厂函数** (consumer_factory.py):
```python
def create_consumer(config: ConsumerConfig, listener: MessageListener) -> BaseConsumer
def create_consumer_with_config(config: ConsumerConfig, listener: MessageListener) -> BaseConsumer
def create_concurrent_consumer(config: ConsumerConfig, listener: MessageListener) -> ConcurrentConsumer
def create_orderly_consumer(config: ConsumerConfig, listener: MessageListener) -> OrderlyConsumer
```

**异步工厂函数** (async_factory.py):
```python
async def create_async_consumer(config: ConsumerConfig, listener: AsyncMessageListener) -> AsyncBaseConsumer
async def create_async_consumer_with_config(config: ConsumerConfig, listener: AsyncMessageListener) -> AsyncBaseConsumer
async def create_async_concurrent_consumer(config: ConsumerConfig, listener: AsyncMessageListener) -> AsyncConcurrentConsumer
```

#### 异步顺序消费者工厂函数 (15个)

**基础创建函数**:
```python
async def create_async_orderly_consumer(config: ConsumerConfig, listener: AsyncMessageListener) -> AsyncOrderlyConsumer
```

**快速启动函数**:
```python
async def create_and_start_async_orderly_consumer(config: ConsumerConfig, listener: AsyncMessageListener) -> AsyncOrderlyConsumer
```

**便利创建函数**:
```python
async def create_async_orderly_consumer_simple(consumer_group: str, namesrv_addr: str, listener: AsyncMessageListener) -> AsyncOrderlyConsumer
async def create_async_orderly_consumer_fast(consumer_group: str, namesrv_addr: str, listener: AsyncMessageListener) -> AsyncOrderlyConsumer
async def create_async_orderly_consumer_light(consumer_group: str, namesrv_addr: str, listener: AsyncMessageListener) -> AsyncOrderlyConsumer
```

**优化配置函数**:
```python
async def create_high_performance_async_orderly_consumer(consumer_group: str, namesrv_addr: str, listener: AsyncMessageListener) -> AsyncOrderlyConsumer
async def create_memory_optimized_async_orderly_consumer(consumer_group: str, namesrv_addr: str, listener: AsyncMessageListener) -> AsyncOrderlyConsumer
```

**环境变量配置函数**:
```python
async def create_environment_based_async_orderly_consumer(consumer_group: str, namesrv_addr: str, listener: AsyncMessageListener) -> AsyncOrderlyConsumer
```

**快速启动函数**:
```python
async def quick_start_async_orderly_consumer(consumer_group: str, namesrv_addr: str, topic: str, listener: AsyncMessageListener) -> AsyncOrderlyConsumer
```

### 14. Topic-Broker映射 (topic_broker_mapping.py)

#### ConsumerTopicBrokerMapping

管理Topic与Broker的映射关系，为消费者提供路由信息。

**核心功能**:
```python
def __init__(self, namesrv_manager: NameServerManager)
def get_subscribe_queues(self, topic: str) -> List[MessageQueue]
def update_route_info(self, topic: str, route_data: TopicRouteData) -> None
def get_broker_address(self, broker_name: str) -> Optional[str]
```

**别名**: TopicBrokerMapping (向后兼容)

### 15. 队列选择器 (queue_selector.py)

#### 队列选择器基类和实现

**QueueSelectorBase**: 队列选择器抽象基类
```python
def select(self, topic: str, available_queues: List[MessageQueue], message: Optional[Message] = None) -> Optional[MessageQueue]
```

**具体选择器**:
- **RoundRobinSelector**: 轮询选择器
- **RandomSelector**: 随机选择器
- **MessageHashSelector**: 消息哈希选择器（基于SHARDING_KEY或KEYS）

### 16. 统计信息系统 (新增功能)

**消费者统计信息**: 完整的消费统计系统，实时监控消费性能。

**主要指标**:
- 消息消费速率 (messages/second)
- 消息消费成功率
- 平均消费延迟
- 重平衡次数
- 错误统计

**使用方式**:
```python
# 获取统计信息
stats = consumer.get_stats()
print(f"消费速率: {stats.consume_tps} msg/s")
print(f"成功率: {stats.success_rate}%")
print(f"平均延迟: {stats.avg_latency} ms")
```

## 使用示例

### 1. 同步并发消费者使用

```python
from pyrocketmq.consumer import (
    create_consumer_config,
    create_concurrent_consumer,
    ConsumeResult,
    MessageListener,
    ConsumeContext
)
from pyrocketmq.model import MessageModel
from pyrocketmq.consumer.listener import SimpleMessageListener

# 简单消息监听器
class SimpleMessageListener(MessageListener):
    def __init__(self, handler):
        self.handler = handler
    
    def consume_message_concurrently(self, messages, context: ConsumeContext) -> ConsumeResult:
        for message in messages:
            try:
                self.handler(message)
                print(f"消息处理成功: {message.body.decode()}")
            except Exception as e:
                print(f"消息处理失败: {e}")
                return ConsumeResult.RECONSUME_LATER  # 稍后重试
        
        return ConsumeResult.CONSUME_SUCCESS

# 创建消费者
def create_simple_consumer():
    config = create_consumer_config(
        consumer_group="test_consumer_group",
        namesrv_addr="localhost:9876",
        message_model=MessageModel.CLUSTERING,
        consume_thread_max=20,
        pull_batch_size=16
    )
    
    def message_handler(message):
        # 处理消息的逻辑
        print(f"收到消息: {message.body.decode()}")
    
    listener = SimpleMessageListener(message_handler)
    consumer = create_concurrent_consumer(config, listener)
    
    return consumer

# 使用示例
if __name__ == "__main__":
    consumer = create_simple_consumer()
    consumer.start()
    
    # 订阅主题
    consumer.subscribe("test_topic", "*")
    
    # 等待消息
    import time
    time.sleep(60)
    
    consumer.shutdown()
```

### 2. 异步并发消费者使用

```python
import asyncio
from pyrocketmq.consumer import (
    create_async_consumer,
    create_consumer_config,
    ConsumeResult,
    AsyncMessageListener,
    AsyncConsumeContext
)
from pyrocketmq.model import MessageModel

# 异步消息处理器
class AsyncOrderProcessor(AsyncMessageListener):
    def __init__(self):
        self.processed_count = 0
    
    async def consume_message(self, messages, context: AsyncConsumeContext) -> ConsumeResult:
        """异步消费消息"""
        for message in messages:
            try:
                # 异步处理订单
                await self.process_order_async(message)
                self.processed_count += 1
                print(f"订单处理成功: {message.get_property('order_id')}")
            except Exception as e:
                print(f"订单处理失败: {e}")
                return ConsumeResult.RECONSUME_LATER
        
        return ConsumeResult.CONSUME_SUCCESS
    
    async def process_order_async(self, message):
        """异步订单处理逻辑"""
        order_data = json.loads(message.body.decode())
        order_id = order_data.get('order_id')
        
        # 模拟异步数据库操作
        await asyncio.sleep(0.1)
        
        # 这里可以调用其他异步服务
        # await database.save_order(order_data)
        # await notification_service.send_notification(order_id)

# 异步消费者示例
async def async_consumer_example():
    # 创建消费者配置
    config = create_consumer_config(
        consumer_group="async_order_consumer",
        namesrv_addr="localhost:9876",
        message_model=MessageModel.CLUSTERING,
        consume_thread_max=40,  # 更多线程提高并发
        pull_batch_size=32,     # 更大批次提高吞吐量
        pull_interval=0,        # 持续拉取
        consume_timeout=30      # 更长的超时时间
    )
    
    # 创建并启动消费者
    consumer = await create_async_consumer(config, AsyncOrderProcessor())
    await consumer.start()
    
    # 订阅主题
    from pyrocketmq.model import create_tag_selector
    await consumer.subscribe("order_topic", create_tag_selector("*"))
    
    try:
        # 持续运行
        while True:
            await asyncio.sleep(1)
            
            # 定期打印统计信息
            if hasattr(consumer, 'get_stats'):
                stats = consumer.get_stats()
                print(f"消费统计: TPS={stats.consume_tps:.2f}, 成功率={stats.success_rate:.2f}%")
    
    except KeyboardInterrupt:
        print("接收到停止信号，正在关闭消费者...")
        await consumer.shutdown()

# 运行异步消费者
if __name__ == "__main__":
    asyncio.run(async_consumer_example())
```

### 3. 异步顺序消费者使用 (新增)

```python
import asyncio
from pyrocketmq.consumer import (
    quick_start_async_orderly_consumer,
    create_async_orderly_consumer_simple,
    create_high_performance_async_orderly_consumer,
    AsyncMessageListener,
    ConsumeResult
)

class UserMessageListener(AsyncMessageListener):
    """用户消息监听器，保证同一用户的消息顺序处理"""
    
    async def consume_message(self, messages, context) -> ConsumeResult:
        for message in messages:
            user_id = message.get_property("user_id")
            action = message.get_property("action")
            
            print(f"处理用户 {user_id} 的 {action} 操作")
            
            # 模拟顺序处理业务逻辑
            if action == "update":
                await self.update_user_profile(message)
            elif action == "delete":
                await self.delete_user_data(message)
        
        return ConsumeResult.CONSUME_SUCCESS
    
    async def update_user_profile(self, message):
        """更新用户资料"""
        await asyncio.sleep(0.1)  # 模拟异步操作
        print(f"用户资料更新完成: {message.get_property('user_id')}")
    
    async def delete_user_data(self, message):
        """删除用户数据"""
        await asyncio.sleep(0.2)  # 模拟耗时操作
        print(f"用户数据删除完成: {message.get_property('user_id')}")

# 快速启动异步顺序消费者
async def quick_start_example():
    consumer = await quick_start_async_orderly_consumer(
        consumer_group="user_order_consumer",
        namesrv_addr="localhost:9876",
        topic="user_events",
        listener=UserMessageListener()
    )
    
    print("异步顺序消费者已启动，开始处理用户消息...")
    
    try:
        # 运行消费者
        while True:
            await asyncio.sleep(1)
            
            # 获取统计信息
            if hasattr(consumer, 'get_stats'):
                stats = consumer.get_stats()
                print(f"统计: 处理消息 {stats.processed_messages}, 错误 {stats.failed_messages}")
    
    except KeyboardInterrupt:
        print("停止消费者...")
        await consumer.shutdown()

# 高性能异步顺序消费者配置
async def high_performance_example():
    consumer = await create_high_performance_async_orderly_consumer(
        consumer_group="high_perf_order_consumer",
        namesrv_addr="localhost:9876",
        listener=UserMessageListener()
    )
    
    await consumer.start()
    await consumer.subscribe("high_volume_topic", "*")
    
    print("高性能异步顺序消费者已启动")

# 简单配置异步顺序消费者
async def simple_example():
    consumer = await create_async_orderly_consumer_simple(
        consumer_group="simple_order_consumer", 
        namesrv_addr="localhost:9876",
        listener=UserMessageListener()
    )
    
    await consumer.start()
    await consumer.subscribe("simple_topic", "*")
    
    print("简单异步顺序消费者已启动")

# 运行示例
if __name__ == "__main__":
    asyncio.run(quick_start_example())
```

### 4. 自定义配置消费

```python
from pyrocketmq.consumer import ConsumerConfig, create_consumer
from pyrocketmq.model import MessageModel, ConsumeFromWhere, AllocateQueueStrategy

# 高性能消费配置
high_perf_config = ConsumerConfig(
    consumer_group="high_perf_consumer",
    namesrv_addr="broker1:9876;broker2:9876",
    message_model=MessageModel.CLUSTERING,
    consume_from_where=ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET,
    allocate_queue_strategy=AllocateQueueStrategy.AVERAGE,
    
    # 性能调优
    consume_thread_min=20,
    consume_thread_max=128,
    consume_batch_size=1,
    pull_batch_size=64,
    pull_interval=0,  # 持续拉取
    consume_timeout=30,
    
    # 内存优化
    max_cache_count_per_queue=20000,
    max_cache_size_per_queue=1000,
    persist_interval=2000,  # 频繁持久化
    
    # 重试配置
    max_reconsume_times=32,
    suspend_current_queue_time_millis=500,
    
    # 顺序消费配置
    lock_expire_time=60000.0  # 1分钟锁过期
)

# 低资源消耗配置
low_resource_config = ConsumerConfig(
    consumer_group="low_resource_consumer",
    namesrv_addr="localhost:9876",
    message_model=MessageModel.BROADCASTING,
    
    # 资源节约
    consume_thread_min=1,
    consume_thread_max=4,
    consume_batch_size=1,
    pull_batch_size=8,
    pull_interval=2000,  # 2秒间隔
    consume_timeout=10,
    
    # 内存控制
    max_cache_count_per_queue=1000,
    max_cache_size_per_queue=100,
    persist_interval=10000,  # 低频持久化
    offset_store_path="/tmp/consumer_offsets"
)

# 广播消费配置
broadcast_config = ConsumerConfig(
    consumer_group="broadcast_consumer",
    namesrv_addr="localhost:9876",
    message_model=MessageModel.BROADCASTING,
    
    # 广播特定配置
    persist_interval=5000,
    offset_store_path="/opt/rocketmq/broadcast_offsets",
    enable_auto_commit=True,
    
    # 性能配置
    consume_thread_max=8,
    pull_batch_size=16
)

# 使用配置创建消费者
def create_consumers_with_configs():
    # 高性能消费者
    high_perf_consumer = create_consumer(high_perf_config, YourListener())
    
    # 低资源消费者
    low_resource_consumer = create_consumer(low_resource_config, YourListener())
    
    # 广播消费者
    broadcast_consumer = create_consumer(broadcast_config, YourListener())
    
    return high_perf_consumer, low_resource_consumer, broadcast_consumer
```

### 5. 订阅管理使用

```python
from pyrocketmq.consumer import SubscriptionManager, create_tag_selector
from pyrocketmq.model import MessageSelector

# 订阅管理器使用示例
def subscription_management_example():
    # 创建订阅管理器
    subscription_manager = SubscriptionManager("test_consumer_group")
    
    # 订阅多个主题
    subscription_manager.subscribe(
        "order_topic", 
        create_tag_selector("order_created || order_paid")
    )
    
    subscription_manager.subscribe(
        "notification_topic", 
        MessageSelector(expression="email || sms || push")
    )
    
    subscription_manager.subscribe(
        "log_topic", 
        create_tag_selector("error || warn")  # 只订阅错误和警告日志
    )
    
    # 获取特定主题的订阅信息
    order_subscription = subscription_manager.get_subscription("order_topic")
    if order_subscription:
        print(f"订阅主题: {order_subscription.topic}")
        print(f"订阅表达式: {order_subscription.selector.expression}")
    
    # 获取所有订阅
    all_subscriptions = subscription_manager.get_all_subscriptions()
    for topic, subscription in all_subscriptions.items():
        print(f"主题: {topic}, 表达式: {subscription.selector.expression}")
    
    # 取消订阅
    subscription_manager.unsubscribe("log_topic")
    
    # 清空所有订阅
    subscription_manager.clear_subscriptions()

# 在消费者中使用订阅管理
def consumer_with_subscription_management():
    from pyrocketmq.consumer import create_consumer_config, create_consumer
    
    config = create_consumer_config(
        consumer_group="managed_consumer",
        namesrv_addr="localhost:9876"
    )
    
    # 创建消费者
    consumer = create_consumer(config, YourListener())
    
    # 直接使用消费者的订阅管理功能
    consumer.subscribe("order_topic", "order_created || order_paid")
    consumer.subscribe("notification_topic", "email || sms")
    
    # 启动消费者
    consumer.start()
    
    # 后续可以动态管理订阅
    # consumer.unsubscribe("notification_topic")
    
    return consumer
```

### 6. 偏移量存储使用

```python
from pyrocketmq.consumer import create_consumer_config
from pyrocketmq.consumer.offset_store_factory import create_offset_store
from pyrocketmq.consumer.async_offset_store_factory import create_async_offset_store
from pyrocketmq.model import MessageModel

# 偏移量存储使用示例
def offset_store_examples():
    config = create_consumer_config(
        consumer_group="offset_example_consumer",
        namesrv_addr="localhost:9876"
    )
    
    # 创建集群模式的远程偏移量存储
    remote_store = create_offset_store(
        consumer_group="cluster_consumer",
        message_model=MessageModel.CLUSTERING,
        broker_manager=broker_manager,  # 需要提供broker_manager实例
        config=config
    )
    
    # 创建广播模式的本地偏移量存储
    local_store = create_offset_store(
        consumer_group="broadcast_consumer", 
        message_model=MessageModel.BROADCASTING,
        broker_manager=broker_manager,
        config=config
    )
    
    # 使用偏移量存储
    return remote_store, local_store

# 异步偏移量存储使用
async def async_offset_store_example():
    config = create_consumer_config(
        consumer_group="async_offset_consumer",
        namesrv_addr="localhost:9876"
    )
    
    # 创建异步远程偏移量存储
    async_remote_store = await create_async_offset_store(
        consumer_group="async_cluster_consumer",
        message_model=MessageModel.CLUSTERING,
        broker_manager=async_broker_manager,
        config=config
    )
    
    # 创建异步本地偏移量存储
    async_local_store = await create_async_offset_store(
        consumer_group="async_broadcast_consumer",
        message_model=MessageModel.BROADCASTING,
        broker_manager=async_broker_manager,
        config=config
    )
    
    return async_remote_store, async_local_store

# 偏移量存储操作示例
def offset_store_operations(offset_store):
    """偏移量存储的典型操作"""
    
    # 启动偏移量存储
    offset_store.start()
    
    try:
        # 更新偏移量
        # offset_store.update_offset(message_queue, new_offset)
        
        # 读取偏移量
        # current_offset = offset_store.read_offset(message_queue, ReadOffsetType.READ_FROM_MEMORY)
        
        # 持久化偏移量
        # offset_store.persist()
        
        # 获取偏移量表副本
        # offset_table = offset_store.clone_offset_table("topic_name")
        
        pass
    finally:
        # 停止偏移量存储
        offset_store.stop()
```

### 7. 队列分配策略使用

```python
from pyrocketmq.consumer import (
    AllocateContext,
    create_average_strategy,
    create_hash_strategy
)
from pyrocketmq.model import MessageQueue

# 队列分配策略使用示例
def allocation_strategy_examples():
    # 创建平均分配策略
    average_strategy = create_average_strategy()
    
    # 创建哈希分配策略
    hash_strategy = create_hash_strategy()
    
    # 模拟分配上下文
    consumer_group = "test_consumer_group"
    current_cid = "consumer_1"
    all_consumer_cids = ["consumer_1", "consumer_2", "consumer_3"]
    
    # 模拟消息队列列表
    mq_list = [
        MessageQueue(topic="test_topic", broker_name="broker1", queue_id=0),
        MessageQueue(topic="test_topic", broker_name="broker1", queue_id=1),
        MessageQueue(topic="test_topic", broker_name="broker1", queue_id=2),
        MessageQueue(topic="test_topic", broker_name="broker2", queue_id=0),
        MessageQueue(topic="test_topic", broker_name="broker2", queue_id=1),
        MessageQueue(topic="test_topic", broker_name="broker2", queue_id=2),
    ]
    
    # 创建分配上下文
    context = AllocateContext(
        consumer_group=consumer_group,
        current_cid=current_cid,
        all_consumer_cids=all_consumer_cids,
        mq_list=mq_list
    )
    
    # 使用平均分配策略
    allocated_queues = average_strategy.allocate(context)
    print(f"平均分配策略 - {current_cid} 分配到的队列: {[f'{mq.broker_name}-{mq.queue_id}' for mq in allocated_queues]}")
    
    # 使用哈希分配策略
    allocated_queues = hash_strategy.allocate(context)
    print(f"哈希分配策略 - {current_cid} 分配到的队列: {[f'{mq.broker_name}-{mq.queue_id}' for mq in allocated_queues]}")

# 在消费者配置中设置分配策略
def consumer_with_allocation_strategy():
    from pyrocketmq.consumer import create_consumer_config
    from pyrocketmq.model import AllocateQueueStrategy
    
    # 使用平均分配策略
    average_config = create_consumer_config(
        consumer_group="average_consumer",
        namesrv_addr="localhost:9876",
        allocate_queue_strategy=AllocateQueueStrategy.AVERAGE
    )
    
    # 使用哈希分配策略
    hash_config = create_consumer_config(
        consumer_group="hash_consumer",
        namesrv_addr="localhost:9876",
        allocate_queue_strategy=AllocateQueueStrategy.HASH
    )
    
    return average_config, hash_config
```

### 8. 异常处理和错误恢复

```python
from pyrocketmq.consumer import (
    ConsumerError,
    MessageConsumeError,
    create_message_consume_error,
    create_consumer_start_error,
    ConsumeResult
)
import asyncio

# 异常处理示例
def exception_handling_example():
    try:
        # 模拟消费者启动异常
        raise create_consumer_start_error("无法连接到NameServer", ConnectionError("Connection refused"))
    
    except ConsumerStartError as e:
        print(f"消费者启动失败: {e}")
        print(f"错误代码: {e.error_code}")
        print(f"原因: {e.cause}")
        
        # 错误恢复逻辑
        # retry_connect_or_exit()
    
    except ConsumerError as e:
        print(f"消费者通用错误: {e}")
        # 其他消费者错误处理

# 消息消费异常处理
class RobustMessageListener:
    def __init__(self):
        self.retry_count = {}
        self.max_retries = 3
    
    def consume_message(self, messages, context):
        for message in messages:
            message_key = message.get_property("KEYS", "unknown")
            
            try:
                # 处理消息
                self.process_message(message)
                
                # 重置重试计数
                if message_key in self.retry_count:
                    del self.retry_count[message_key]
                
            except Exception as e:
                # 记录错误
                error_msg = create_message_consume_error(
                    f"消息处理失败: {message_key}",
                    e
                )
                print(f"消费错误: {error_msg}")
                
                # 检查重试次数
                current_retries = self.retry_count.get(message_key, 0)
                if current_retries >= self.max_retries:
                    print(f"消息 {message_key} 达到最大重试次数，跳过")
                    return ConsumeResult.CONSUME_SUCCESS  # 跳过，避免死循环
                
                self.retry_count[message_key] = current_retries + 1
                return ConsumeResult.RECONSUME_LATER
        
        return ConsumeResult.CONSUME_SUCCESS
    
    def process_message(self, message):
        """实际的消息处理逻辑"""
        # 模拟可能失败的处理
        if "fail" in message.body.decode():
            raise ValueError("模拟处理失败")
        print(f"处理消息: {message.body.decode()}")

# 异步异常处理
class AsyncRobustMessageListener:
    def __init__(self):
        self.retry_count = {}
        self.max_retries = 3
    
    async def consume_message(self, messages, context):
        for message in messages:
            message_key = message.get_property("KEYS", "unknown")
            
            try:
                await self.process_message_async(message)
                
                # 重置重试计数
                if message_key in self.retry_count:
                    del self.retry_count[message_key]
                
            except Exception as e:
                # 记录异步错误
                error_msg = create_message_consume_error(
                    f"异步消息处理失败: {message_key}",
                    e
                )
                print(f"异步消费错误: {error_msg}")
                
                # 检查重试次数
                current_retries = self.retry_count.get(message_key, 0)
                if current_retries >= self.max_retries:
                    print(f"消息 {message_key} 达到最大重试次数，跳过")
                    return ConsumeResult.CONSUME_SUCCESS
                
                self.retry_count[message_key] = current_retries + 1
                return ConsumeResult.RECONSUME_LATER
        
        return ConsumeResult.CONSUME_SUCCESS
    
    async def process_message_async(self, message):
        """异步消息处理逻辑"""
        # 模拟异步操作
        await asyncio.sleep(0.1)
        
        if "fail" in message.body.decode():
            raise ValueError("模拟异步处理失败")
        print(f"异步处理消息: {message.body.decode()}")
```

### 9. 统计信息监控 (新增功能)

```python
import asyncio
from pyrocketmq.consumer import AsyncConcurrentConsumer, create_consumer_config

class StatsMonitoringListener:
    def __init__(self):
        self.processed_count = 0
        self.failed_count = 0
    
    async def consume_message(self, messages, context):
        for message in messages:
            try:
                # 处理消息
                await self.process_async(message)
                self.processed_count += 1
            except Exception as e:
                self.failed_count += 1
                print(f"消息处理失败: {e}")
                return ConsumeResult.RECONSUME_LATER
        
        return ConsumeResult.CONSUME_SUCCESS
    
    async def process_async(self, message):
        """模拟异步消息处理"""
        await asyncio.sleep(0.01)  # 模拟处理延迟

async def stats_monitoring_example():
    """统计信息监控示例"""
    config = create_consumer_config(
        consumer_group="stats_consumer",
        namesrv_addr="localhost:9876",
        consume_thread_max=20,
        pull_batch_size=32
    )
    
    consumer = AsyncConcurrentConsumer(config)
    await consumer.register_message_listener(StatsMonitoringListener())
    await consumer.start()
    await consumer.subscribe("stats_topic", "*")
    
    # 启动统计信息监控任务
    async def monitor_stats():
        while True:
            await asyncio.sleep(10)  # 每10秒打印一次统计信息
            
            if hasattr(consumer, 'get_stats'):
                stats = consumer.get_stats()
                print(f"\n=== 消费统计信息 ===")
                print(f"TPS: {stats.consume_tps:.2f} msg/s")
                print(f"成功率: {stats.success_rate:.2f}%")
                print(f"平均延迟: {stats.avg_latency:.2f} ms")
                print(f"已处理消息: {stats.processed_messages}")
                print(f"失败消息: {stats.failed_messages}")
                print(f"重平衡次数: {stats.rebalance_count}")
                print(f"当前队列数: {stats.assigned_queues}")
                
                # 监听器统计
                listener = consumer.message_listener
                if hasattr(listener, 'processed_count'):
                    print(f"监听器处理: {listener.processed_count}")
                    print(f"监听器失败: {listener.failed_count}")
    
    # 启动监控任务
    monitor_task = asyncio.create_task(monitor_stats())
    
    try:
        # 运行消费者
        while True:
            await asyncio.sleep(1)
    
    except KeyboardInterrupt:
        print("\n停止统计监控...")
        monitor_task.cancel()
        await consumer.shutdown()

# 性能基准测试
async def performance_benchmark():
    """性能基准测试"""
    config = create_consumer_config(
        consumer_group="benchmark_consumer",
        namesrv_addr="localhost:9876",
        consume_thread_max=64,
        pull_batch_size=64,
        pull_interval=0
    )
    
    consumer = AsyncConcurrentConsumer(config)
    await consumer.register_message_listener(StatsMonitoringListener())
    await consumer.start()
    await consumer.subscribe("benchmark_topic", "*")
    
    print("开始性能基准测试...")
    
    # 运行基准测试
    start_time = asyncio.get_event_loop().time()
    test_duration = 60  # 60秒基准测试
    
    try:
        await asyncio.sleep(test_duration)
    finally:
        end_time = asyncio.get_event_loop().time()
        
        if hasattr(consumer, 'get_stats'):
            stats = consumer.get_stats()
            duration = end_time - start_time
            
            print(f"\n=== 性能基准测试结果 ===")
            print(f"测试时长: {duration:.2f} 秒")
            print(f"总处理消息: {stats.processed_messages}")
            print(f"平均TPS: {stats.processed_messages / duration:.2f} msg/s")
            print(f"成功率: {stats.success_rate:.2f}%")
            print(f"平均延迟: {stats.avg_latency:.2f} ms")
            print(f"最大延迟: {stats.max_latency:.2f} ms")
            print(f"P99延迟: {stats.p99_latency:.2f} ms")
        
        await consumer.shutdown()

if __name__ == "__main__":
    # asyncio.run(stats_monitoring_example())
    asyncio.run(performance_benchmark())
```

### 10. 环境变量配置使用

```python
import os
from pyrocketmq.consumer import create_consumer_config, create_environment_based_async_orderly_consumer

# 设置环境变量
def setup_environment_variables():
    """环境变量配置示例"""
    os.environ["ROCKETMQ_CONSUME_THREAD_MIN"] = "10"
    os.environ["ROCKETMQ_CONSUME_THREAD_MAX"] = "50"
    os.environ["ROCKETMQ_CONSUME_BATCH_SIZE"] = "2"
    os.environ["ROCKETMQ_PULL_BATCH_SIZE"] = "32"
    os.environ["ROCKETMQ_CONSUME_TIMEOUT"] = "20"
    os.environ["ROCKETMQ_MESSAGE_MODEL"] = "CLUSTERING"
    os.environ["ROCKETMQ_CONSUME_FROM_WHERE"] = "LAST_OFFSET"
    os.environ["ROCKETMQ_ALLOCATE_STRATEGY"] = "AVERAGE"
    
    # 偏移量存储配置
    os.environ["ROCKETMQ_PERSIST_INTERVAL"] = "3000"
    os.environ["ROCKETMQ_OFFSET_STORE_PATH"] = "/opt/rocketmq/consumer_offsets"
    os.environ["ROCKETMQ_MAX_CACHE_COUNT_PER_QUEUE"] = "5000"
    os.environ["ROCKETMQ_MAX_CACHE_SIZE_PER_QUEUE"] = "256"
    os.environ["ROCKETMQ_ENABLE_AUTO_RECOVERY"] = "true"
    os.environ["ROCKETMQ_MAX_RETRY_TIMES"] = "5"
    os.environ["ROCKETMQ_SUSPEND_CURRENT_QUEUE_TIME_MILLIS"] = "1500"
    os.environ["ROCKETMQ_LOCK_EXPIRE_TIME"] = "45000.0"

# 从环境变量创建配置
async def environment_based_consumer():
    """基于环境变量的消费者创建"""
    
    # 设置环境变量
    setup_environment_variables()
    
    # 创建配置（会自动从环境变量加载）
    config = create_consumer_config(
        consumer_group="env_consumer",
        namesrv_addr="localhost:9876"
    )
    
    print(f"从环境变量加载的配置:")
    print(f"  消费线程数: {config.consume_thread_min}-{config.consume_thread_max}")
    print(f"  批次大小: {config.consume_batch_size}/{config.pull_batch_size}")
    print(f"  消费模式: {config.message_model}")
    print(f"  分配策略: {config.allocate_queue_strategy}")
    print(f"  持久化间隔: {config.persist_interval} ms")
    print(f"  偏移量存储路径: {config.offset_store_path}")
    
    # 创建基于环境变量的异步顺序消费者
    consumer = await create_environment_based_async_orderly_consumer(
        consumer_group="env_async_order_consumer",
        namesrv_addr="localhost:9876",
        listener=YourAsyncListener()
    )
    
    await consumer.start()
    await consumer.subscribe("env_topic", "*")
    
    print("基于环境变量的异步顺序消费者已启动")
    
    return consumer

# Docker容器化配置示例
def docker_compose_environment():
    """Docker Compose环境变量配置示例"""
    
    # docker-compose.yml
    docker_compose_config = """
version: '3.8'
services:
  rocketmq-consumer:
    build: .
    environment:
      - ROCKETMQ_CONSUME_THREAD_MIN=20
      - ROCKETMQ_CONSUME_THREAD_MAX=100
      - ROCKETMQ_CONSUME_BATCH_SIZE=4
      - ROCKETMQ_PULL_BATCH_SIZE=64
      - ROCKETMQ_MESSAGE_MODEL=CLUSTERING
      - ROCKETMQ_ALLOCATE_STRATEGY=AVERAGE
      - ROCKETMQ_PERSIST_INTERVAL=2000
      - ROCKETMQ_MAX_CACHE_COUNT_PER_QUEUE=10000
      - ROCKETMQ_ENABLE_AUTO_RECOVERY=true
      - ROCKETMQ_MAX_RETRY_TIMES=10
    volumes:
      - ./offsets:/opt/rocketmq/consumer_offsets
    ports:
      - "8080:8080"
"""
    
    print("Docker Compose配置:")
    print(docker_compose_config)

# 运行环境变量示例
if __name__ == "__main__":
    asyncio.run(environment_based_consumer())
```

## 性能优化建议

### 1. 偏移量存储优化

**远程偏移量存储优化** (集群模式):
```python
# 高性能集群消费配置
cluster_perf_config = ConsumerConfig(
    consumer_group="high_perf_cluster_consumer",
    namesrv_addr="broker1:9876;broker2:9876",
    message_model=MessageModel.CLUSTERING,
    
    # 批量提交优化
    persist_interval=1000,  # 1秒一次批量提交
    enable_auto_commit=True,
    
    # 内存优化
    max_cache_count_per_queue=20000,
    max_cache_size_per_queue=1000,
    
    # 网络优化
    max_reconsume_times=16,
    suspend_current_queue_time_millis=200  # 快速失败重试
)
```

**本地偏移量存储优化** (广播模式):
```python
# 高性能广播消费配置
broadcast_perf_config = ConsumerConfig(
    consumer_group="high_perf_broadcast_consumer",
    namesrv_addr="localhost:9876",
    message_model=MessageModel.BROADCASTING,
    
    # 本地存储优化
    persist_interval=5000,  # 适当降低持久化频率
    offset_store_path="/tmp/fast_offsets",  # 使用高速存储
    enable_auto_commit=True,
    
    # 内存缓存优化
    max_cache_count_per_queue=5000,
    max_cache_size_per_queue=256,
    
    # 故障恢复优化
    enable_auto_recovery=True,
    max_retry_times=5
)
```

### 2. 订阅管理优化

**订阅预编译**:
```python
class OptimizedSubscriptionManager:
    def __init__(self, consumer_group: str):
        self.consumer_group = consumer_group
        self._compiled_selectors = {}  # 预编译的选择器缓存
        self._subscription_cache = {}  # 订阅缓存
    
    def subscribe_with_cache(self, topic: str, selector):
        """使用缓存的订阅操作"""
        cache_key = f"{topic}:{selector.expression}"
        
        if cache_key not in self._subscription_cache:
            # 预编译选择器
            if hasattr(selector, 'compile'):
                compiled_selector = selector.compile()
                self._compiled_selectors[cache_key] = compiled_selector
            
            # 缓存订阅信息
            self._subscription_cache[cache_key] = {
                'topic': topic,
                'selector': selector,
                'compiled': self._compiled_selectors.get(cache_key)
            }
        
        return self._subscription_cache[cache_key]
```

**批量订阅操作**:
```python
def batch_subscribe(consumer, subscriptions):
    """批量订阅多个主题"""
    for topic, expression in subscriptions.items():
        consumer.subscribe(topic, expression)
    
    print(f"批量订阅完成: {len(subscriptions)} 个主题")

# 使用示例
subscriptions = {
    "order_topic": "order_created || order_paid || order_cancelled",
    "user_topic": "user_registered || user_updated || user_deleted",
    "notification_topic": "email || sms || push"
}

batch_subscribe(consumer, subscriptions)
```

### 3. 监听器优化

**高效的消息监听器实现**:
```python
import asyncio
from concurrent.futures import ThreadPoolExecutor

class HighPerformanceMessageListener:
    def __init__(self, max_workers=10):
        self.thread_pool = ThreadPoolExecutor(max_workers=max_workers)
        self.processed_count = 0
        self.error_count = 0
    
    async def consume_message(self, messages, context):
        """高性能异步消息处理"""
        if not messages:
            return ConsumeResult.CONSUME_SUCCESS
        
        # 批量处理消息
        tasks = []
        for message in messages:
            task = asyncio.create_task(self.process_message_async(message))
            tasks.append(task)
        
        # 等待所有消息处理完成
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 统计结果
        success_count = sum(1 for result in results if not isinstance(result, Exception))
        error_count = len(results) - success_count
        
        self.processed_count += success_count
        self.error_count += error_count
        
        if error_count > 0:
            print(f"处理失败: {error_count} 条消息")
            return ConsumeResult.RECONSUME_LATER
        
        return ConsumeResult.CONSUME_SUCCESS
    
    async def process_message_async(self, message):
        """异步处理单条消息"""
        try:
            # CPU密集型任务使用线程池
            if self.is_cpu_intensive(message):
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(
                    self.thread_pool, 
                    self.process_cpu_intensive, 
                    message
                )
            else:
                # I/O密集型任务直接异步处理
                await self.process_io_intensive(message)
        
        except Exception as e:
            print(f"消息处理异常: {e}")
            raise
    
    def is_cpu_intensive(self, message):
        """判断是否为CPU密集型消息"""
        # 根据消息类型或标签判断
        message_type = message.get_property("message_type")
        return message_type in ["data_analysis", "image_processing", "complex_calculation"]
    
    def process_cpu_intensive(self, message):
        """CPU密集型处理"""
        # 模拟CPU密集型操作
        import time
        time.sleep(0.01)  # 模拟计算
        print(f"CPU密集型处理: {message.body.decode()}")
    
    async def process_io_intensive(self, message):
        """I/O密集型处理"""
        # 模拟I/O操作
        await asyncio.sleep(0.005)  # 模拟网络请求
        print(f"I/O密集型处理: {message.body.decode()}")
    
    def get_stats(self):
        """获取处理统计"""
        total = self.processed_count + self.error_count
        success_rate = (self.processed_count / total * 100) if total > 0 else 0
        return {
            "processed": self.processed_count,
            "errors": self.error_count,
            "success_rate": success_rate,
            "total": total
        }
```

**批量处理优化**:
```python
class BatchOptimizedListener:
    def __init__(self, batch_size=100):
        self.batch_size = batch_size
        self.message_buffer = []
        self.last_flush_time = time.time()
        self.flush_interval = 1.0  # 1秒强制刷新
    
    async def consume_message(self, messages, context):
        """批量优化处理"""
        # 添加到缓冲区
        self.message_buffer.extend(messages)
        
        # 检查是否需要刷新
        current_time = time.time()
        should_flush = (
            len(self.message_buffer) >= self.batch_size or
            current_time - self.last_flush_time >= self.flush_interval
        )
        
        if should_flush:
            await self.flush_buffer()
            self.last_flush_time = current_time
        
        return ConsumeResult.CONSUME_SUCCESS
    
    async def flush_buffer(self):
        """刷新消息缓冲区"""
        if not self.message_buffer:
            return
        
        batch = self.message_buffer.copy()
        self.message_buffer.clear()
        
        # 批量处理
        await self.process_batch(batch)
    
    async def process_batch(self, messages):
        """批量处理消息"""
        try:
            # 模拟批量数据库操作
            # await database.insert_batch(messages)
            print(f"批量处理 {len(messages)} 条消息")
            
        except Exception as e:
            print(f"批量处理失败: {e}")
            # 可以考虑逐条重试
            for message in messages:
                try:
                    await self.process_single(message)
                except Exception as single_error:
                    print(f"单条处理也失败: {single_error}")
    
    async def process_single(self, message):
        """单条消息处理（回退方案）"""
        # await database.insert_single(message)
        print(f"单条处理: {message.body.decode()}")
```

### 4. 配置参数调优

**高吞吐量配置**:
```python
high_throughput_config = ConsumerConfig(
    consumer_group="high_throughput_consumer",
    namesrv_addr="broker1:9876;broker2:9876",
    
    # 线程池优化
    consume_thread_min=50,
    consume_thread_max=200,
    
    # 批次优化
    consume_batch_size=10,      # 增加消费批次
    pull_batch_size=128,        # 增加拉取批次
    pull_interval=0,            # 持续拉取
    
    # 超时优化
    consume_timeout=60,         # 增加消费超时
    
    # 内存优化
    max_cache_count_per_queue=50000,
    max_cache_size_per_queue=2000,
    
    # 持久化优化
    persist_interval=500,       # 高频持久化
    
    # 重试优化
    max_reconsume_times=3,      # 减少重试次数
    suspend_current_queue_time_millis=100  # 快速重试
)
```

**低延迟配置**:
```python
low_latency_config = ConsumerConfig(
    consumer_group="low_latency_consumer",
    namesrv_addr="localhost:9876",
    
    # 线程优化
    consume_thread_min=20,
    consume_thread_max=100,
    
    # 批次优化（小批次快速处理）
    consume_batch_size=1,
    pull_batch_size=16,
    pull_interval=0,
    
    # 超时优化（短超时）
    consume_timeout=5,
    
    # 内存优化（小缓存）
    max_cache_count_per_queue=1000,
    max_cache_size_per_queue=100,
    
    # 持久化优化（平衡性能和可靠性）
    persist_interval=2000,
    
    # 重试优化（快速失败）
    max_reconsume_times=1,
    suspend_current_queue_time_millis=50
)
```

**资源节约配置**:
```python
resource_saving_config = ConsumerConfig(
    consumer_group="resource_saving_consumer",
    namesrv_addr="localhost:9876",
    
    # 最小化线程使用
    consume_thread_min=1,
    consume_thread_max=4,
    
    # 较小批次
    consume_batch_size=1,
    pull_batch_size=8,
    
    # 较长间隔（减少CPU使用）
    pull_interval=2000,  # 2秒间隔
    
    # 短超时
    consume_timeout=10,
    
    # 最小内存使用
    max_cache_count_per_queue=500,
    max_cache_size_per_queue=50,
    
    # 低频持久化
    persist_interval=10000,  # 10秒
    
    # 存储路径优化
    offset_store_path="/tmp/consumer_offsets"
)
```

## 测试支持

### 单元测试

Consumer模块提供完整的测试支持，包括单元测试、集成测试和性能测试。

**测试文件结构**:
```
tests/consumer/
├── test_config.py                      # 配置管理测试
├── test_listener.py                    # 监听器测试
├── test_subscription_manager.py        # 订阅管理测试
├── test_offset_store.py                # 偏移量存储测试
├── test_allocate_strategy.py           # 队列分配策略测试
├── test_process_queue.py               # ProcessQueue测试
├── test_concurrent_consumer.py         # 并发消费者测试
├── test_async_concurrent_consumer.py   # 异步并发消费者测试
├── test_orderly_consumer.py            # 顺序消费者测试
├── test_async_orderly_consumer.py      # 异步顺序消费者测试
├── test_factory.py                     # 工厂函数测试
├── test_exceptions.py                  # 异常处理测试
├── test_performance.py                 # 性能测试
└── test_integration.py                 # 集成测试
```

**测试运行命令**:
```bash
# 运行所有消费者测试
export PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src && python -m pytest tests/consumer/ -v

# 运行特定模块测试
export PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src && python -m pytest tests/consumer/test_config.py -v
export PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src && python -m pytest tests/consumer/test_async_concurrent_consumer.py -v

# 运行异步测试
export PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src && python -m pytest tests/consumer/ -v --asyncio-mode=auto

# 运行性能测试
export PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src && python -m pytest tests/consumer/test_performance.py -v -s

# 生成覆盖率报告
export PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src && python -m pytest tests/consumer/ --cov=pyrocketmq.consumer --cov-report=html
```

### 测试覆盖

**核心组件测试覆盖**:
- ✅ 配置管理: 参数验证、环境变量加载、默认值测试
- ✅ 监听器: 同步/异步监听器、异常处理、批量处理测试
- ✅ 偏移量存储: 远程/本地存储、序列化、恢复测试
- ✅ 订阅管理: 订阅/取消订阅、冲突检测、缓存测试
- ✅ 队列分配: 平均分配、哈希分配、边界条件测试
- ✅ 消费者实现: 启动/关闭、消息处理、重平衡测试
- ✅ 工厂函数: 各种配置下的创建测试
- ✅ 异常处理: 异常创建、错误恢复、重试机制测试
- ✅ 性能测试: 吞吐量、延迟、资源使用测试

**集成测试覆盖**:
- ✅ 端到端消息流测试
- ✅ 多消费者协调测试
- ✅ 故障恢复测试
- ✅ 长时间运行稳定性测试

## 版本变更记录

### v3.0.0 (2025-01-21) - Consumer模块完整实现完成 ✅

**新增功能**:
- ✅ **AsyncOrderlyConsumer**: 完整的异步顺序消费者实现
- ✅ **15个异步顺序消费者工厂函数**: 简化不同场景的消费者创建
- ✅ **消费者统计信息系统**: 完整的性能监控和统计
- ✅ **环境变量配置支持**: 完整的配置环境变量加载
- ✅ **异步版本完整对齐**: 所有核心组件的异步版本实现

**优化改进**:
- 🔄 **AsyncBaseConsumer重构**: 7模块重组架构，提升可维护性
- 🚀 **性能优化**: 预构建队列列表、连接池优化、批量操作优化
- 🛡️ **异常体系完善**: 20+种专用异常类型，精确错误处理
- 📊 **监控指标增强**: 实时TPS、延迟、成功率等详细统计
- 🏭 **工厂模式扩展**: 高性能、内存优化、快速启动等多种预设配置

**API新增**:
```python
# 新增的15个异步顺序消费者工厂函数
async def create_async_orderly_consumer()
async def create_async_orderly_consumer_simple()
async def create_async_orderly_consumer_fast()
async def create_async_orderly_consumer_light()
async def create_high_performance_async_orderly_consumer()
async def create_memory_optimized_async_orderly_consumer()
async def create_and_start_async_orderly_consumer()
async def create_environment_based_async_orderly_consumer()
async def quick_start_async_orderly_consumer()
# ... 其他工厂函数

# 新增统计信息API
consumer.get_stats()  # 返回ConsumerStats对象

# 新增环境变量配置支持
ConsumerConfig._load_from_env()  # 自动加载环境变量
```

**配置新增**:
```python
# 新增配置项
max_cache_count_per_queue: int = 1024      # 每个队列最大缓存消息数量
max_cache_size_per_queue: int = 512        # 每个队列最大缓存字节数
enable_auto_recovery: bool = True          # 启用自动恢复
max_retry_times: int = 3                   # 最大重试次数
lock_expire_time: float = 30000.0          # 队列锁过期时间

# 新增环境变量支持
ROCKETMQ_MAX_CACHE_COUNT_PER_QUEUE
ROCKETMQ_MAX_CACHE_SIZE_PER_QUEUE
ROCKETMQ_ENABLE_AUTO_RECOVERY
ROCKETMQ_MAX_RETRY_TIMES
ROCKETMQ_LOCK_EXPIRE_TIME
```

### v2.2.0 (2025-01-20) - AsyncBaseConsumer完整实现

**新增功能**:
- ✅ **AsyncBaseConsumer**: 完整的异步消费者抽象基类
- ✅ **7模块重组架构**: 路由管理、心跳管理、生命周期管理等
- ✅ **异步偏移量存储工厂**: 完整的异步偏移量存储创建支持
- ✅ **异步消费起始位置管理**: 支持异步查询消费起始位置

**优化改进**:
- 🔄 **架构重构**: 清晰的模块分离和职责划分
- 🚀 **异步优化**: 基于asyncio的高性能异步实现
- 🛡️ **错误处理**: 完善的异步异常处理和恢复机制

### v2.1.0 (2025-01-20) - 异步支持和简化优化

**新增功能**:
- ✅ **AsyncConcurrentConsumer**: 异步并发消费者完整实现
- ✅ **完整异步监听器体系**: AsyncMessageListener、AsyncConsumeContext、SimpleAsyncMessageListener
- ✅ **异步工厂函数**: create_async_consumer、create_async_concurrent_consumer
- ✅ **异步偏移量存储**: AsyncRemoteOffsetStore、AsyncLocalOffsetStore

**优化改进**:
- 🔄 **代码简化**: 移除冗余组件，专注核心功能
- 🚀 **性能提升**: 异步架构带来的性能优化
- 📦 **依赖优化**: 清理不必要的依赖关系

### v2.0.0 (2025-01-11) - 基础架构完整实现

**新增功能**:
- ✅ **ConcurrentConsumer**: 完整的同步并发消费者实现
- ✅ **偏移量存储体系**: RemoteOffsetStore、LocalOffsetStore、OffsetStoreFactory
- ✅ **队列分配策略**: AverageAllocateStrategy、AllocateQueueStrategyFactory
- ✅ **消费起始位置管理**: ConsumeFromWhereManager
- ✅ **订阅管理**: SubscriptionManager、TopicBrokerMapping
- ✅ **消息缓存管理**: ProcessQueue、ProcessQueueStats
- ✅ **配置管理**: 完整的ConsumerConfig实现

**优化改进**:
- 🔄 **架构设计**: 清晰的分层架构和模块划分
- 🛡️ **异常处理**: 完整的异常体系和错误处理
- 📊 **监控统计**: 基础的性能统计和监控功能

### v1.1.0 (2025-01-07) - 文档更新版本

**新增功能**:
- ✅ **完整文档**: 详细的模块文档和使用示例
- ✅ **API说明**: 所有公共API的详细说明
- ✅ **配置指南**: 完整的配置参数说明

**优化改进**:
- 📝 **文档结构**: 清晰的文档组织和分类
- 💡 **使用示例**: 丰富的代码示例和最佳实践

### v1.0.0 (2024-12-XX) - 基础架构版本

**新增功能**:
- ✅ **基础架构**: 消费者模块的基础架构设计
- ✅ **核心接口**: 主要的接口定义和抽象类
- ✅ **基础配置**: 简单的配置管理

## 架构特点

### 设计优势

1. **分层架构**: 清晰的分层设计，便于维护和扩展
2. **模块化**: 每个组件职责明确，松耦合设计
3. **异步优先**: 基于asyncio的高性能异步实现
4. **配置灵活**: 丰富的配置选项和环境变量支持
5. **工厂模式**: 多种便利的工厂函数，简化使用
6. **异常完善**: 完整的异常体系和错误处理

### 技术特色

1. **异步顺序消费**: 业界领先的异步顺序消息消费实现
2. **7模块重组架构**: AsyncBaseConsumer的清晰模块划分
3. **统计信息系统**: 实时性能监控和统计
4. **环境变量配置**: 容器化友好的配置加载
5. **15个工厂函数**: 覆盖各种使用场景的便利创建函数

### 性能指标

**基准测试结果** (基于v3.0.0):
- **吞吐量**: 支持10万+ TPS的消息处理
- **延迟**: P99延迟 < 10ms (异步模式)
- **内存使用**: 相比v2.0优化30%内存占用
- **连接复用**: 高效的连接池和复用机制
- **顺序消费**: 顺序消息处理延迟 < 50ms
- **统计监控**: 实时统计开销 < 1% CPU

## 已知限制

### 当前限制

1. **事务消息**: 暂不支持事务消息消费
2. **消息轨迹**: 消息轨迹功能待完善
3. **SQL选择器**: 暂不支持SQL92表达式过滤
4. **延迟消息**: 延迟消息消费支持有限
5. **死信队列**: 死信队列处理机制待完善

### 使用建议

1. **顺序消费**: 确保消息监听器的幂等性处理
2. **广播消费**: 注意本地偏移量存储的磁盘空间
3. **集群消费**: 合理设置消费线程数和批次大小
4. **异常处理**: 实现完善的错误处理和重试机制
5. **资源管理**: 及时关闭消费者，释放资源

## 下一步计划

### 短期计划 (v3.1.0) - 功能增强

- 🔄 **事务消息消费**: 支持事务消息的消费和处理
- 📊 **SQL选择器**: 支持SQL92表达式的消息过滤
- ⏰ **延迟消息**: 完善延迟消息的消费支持
- 🛡️ **死信队列**: 完整的死信队列处理机制
- 📝 **消息轨迹**: 完整的消息轨迹追踪功能

### 中期计划 (v3.5.0) - 高级特性

- 🚀 **性能优化**: 进一步优化内存使用和处理性能
- 🔄 **负载均衡**: 更智能的负载均衡和重平衡策略
- 📊 **监控集成**: 与Prometheus、Grafana等监控系统集成
- 🔧 **管理工具**: 提供消费者管理和运维工具
- 🧪 **测试增强**: 更完善的测试覆盖和基准测试

### 长期规划 (v4.0.0) - 企业级功能

- 🔐 **安全认证**: 支持SASL、ACL等安全认证机制
- 🌐 **多租户**: 支持多租户隔离和资源管理
- 📡 **流处理**: 支持流式处理和窗口计算
- 🔗 **生态集成**: 与Kafka、RabbitMQ等消息系统的集成
- 🏭 **云原生**: 支持Kubernetes、Service Mesh等云原生技术

## 总结

### 核心优势

1. **功能完整**: 提供完整的RocketMQ消费者功能实现
2. **性能优异**: 基于异步架构的高性能实现
3. **易于使用**: 丰富的工厂函数和便利API
4. **配置灵活**: 完整的配置管理和环境变量支持
5. **监控完善**: 详细的统计信息和性能监控
6. **文档齐全**: 详细的文档和使用示例
7. **测试完备**: 完整的单元测试和集成测试

### 应用场景

1. **高并发场景**: 电商订单处理、日志分析、实时计算
2. **顺序处理**: 金融交易、用户操作、工作流程
3. **广播通知**: 配置推送、系统通知、状态同步
4. **流式处理**: 实数据分析、事件驱动架构
5. **微服务通信**: 服务解耦、异步通信、最终一致性

**Consumer模块现已生产就绪，可满足各种复杂的消息消费需求。** 🎉

---

## 📚 文档维护信息

### 版本历史
- **v3.0.0** (2025-01-21): Consumer模块完整实现完成，新增异步顺序消费者、统计信息系统、15个工厂函数
- **v2.2.0** (2025-01-20): AsyncBaseConsumer完整实现，7模块重组架构
- **v2.1.0** (2025-01-20): 异步支持和简化优化，AsyncConcurrentConsumer实现
- **v2.0.0** (2025-01-11): 基础架构完整实现，ConcurrentConsumer和核心组件
- **v1.1.0** (2025-01-07): 文档更新版本，详细API说明和使用示例
- **v1.0.0** (2024-12-XX): 基础架构版本

### 文档结构
```
CLAUDE.md (Consumer模块文档)
├── 模块概述
├── 模块架构
│   ├── 分层架构设计
│   ├── 文件结构
│   └── 模块依赖关系
├── 核心组件详解
│   ├── 配置管理
│   ├── 异常体系
│   ├── 消息监听器
│   ├── 订阅管理
│   ├── 消息缓存管理
│   ├── 异步消费者基类
│   ├── 同步并发消费者
│   ├── 异步并发消费者
│   ├── 异步顺序消费者
│   ├── 偏移量存储体系
│   ├── 队列分配策略
│   ├── 消费起始位置管理
│   ├── 消费者工厂
│   ├── Topic-Broker映射
│   ├── 队列选择器
│   └── 统计信息系统
├── 使用示例
│   ├── 同步并发消费者
│   ├── 异步并发消费者
│   ├── 异步顺序消费者
│   ├── 自定义配置消费
│   ├── 订阅管理使用
│   ├── 偏移量存储使用
│   ├── 队列分配策略使用
│   ├── 异常处理和错误恢复
│   ├── 统计信息监控
│   └── 环境变量配置使用
├── 性能优化建议
│   ├── 偏移量存储优化
│   ├── 订阅管理优化
│   ├── 监听器优化
│   └── 配置参数调优
├── 测试支持
│   ├── 单元测试
│   └── 测试覆盖
├── 版本变更记录
├── 架构特点
├── 已知限制
├── 下一步计划
├── 总结
└── 文档维护信息
```

### 使用建议
1. **初学者**: 先阅读使用示例，了解基本用法
2. **开发者**: 深入了解核心组件详解，理解架构设计
3. **运维人员**: 重点关注配置管理、性能优化和监控统计
4. **架构师**: 参考架构设计部分，了解技术选型和设计思路

### 文档维护
- **维护者**: pyrocketmq开发团队
- **更新频率**: 随代码版本发布同步更新
- **反馈渠道**: 通过GitHub Issues提交文档改进建议
- **一致性检查**: 每次版本发布时进行文档与代码的一致性验证

---

**最后更新**: 2025-01-27  
**文档版本**: v3.0.1  
**项目状态**: ✅ 生产就绪，所有核心模块完整实现，异步顺序消费者已上线