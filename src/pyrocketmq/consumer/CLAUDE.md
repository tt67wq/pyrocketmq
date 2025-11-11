# pyrocketmq Consumer 模块

## 模块概述

pyrocketmq Consumer 模块是RocketMQ消息队列的完整Python消费者实现，提供生产级、高性能、可靠的消息消费功能。该模块采用分层架构设计，基于多线程并发处理，支持集群消费和广播消费两种模式，具备完整的偏移量管理、订阅管理、消息监听、自动重平衡等核心功能。

**🎯 生产就绪**: ConcurrentConsumer已完整实现，可直接用于生产环境部署
**⚡ 高性能**: 多线程并发架构，支持高吞吐量消息消费
**🔄 智能管理**: 自动重平衡、智能偏移量管理、完善的错误恢复机制
**📊 全面监控**: 丰富的性能指标和运行状态监控
**🛡️ 企业级**: 完整的异常体系、资源管理、优雅关闭机制

### 核心特性
- **完整的消费模式支持**: 集群消费（多实例协同）和广播消费（独立消费）
- **高性能偏移量存储**: 支持本地文件和远程Broker两种存储方式
- **灵活的订阅管理**: 支持主题订阅、选择器订阅、动态订阅管理
- **完整的监听器体系**: 顺序消息监听器和并发消息监听器
- **企业级异常处理**: 完整的异常体系和错误恢复机制
- **丰富的监控指标**: 全面的性能监控和运行状态监控
- **线程安全设计**: 所有核心组件都支持多线程并发访问

## 模块架构

### 分层架构设计
```
Consumer Module
├── 配置层 (config.py)
│   └── ConsumerConfig - 消费者配置管理
├── 接口层 (listener.py)
│   └── MessageListener - 消息监听器接口
├── 管理层 (subscription_manager.py)
│   └── SubscriptionManager - 订阅关系管理
├── 存储层 (offset_store/)
│   ├── OffsetStore - 偏移量存储抽象基类
│   ├── RemoteOffsetStore - 远程偏移量存储（集群模式）
│   ├── LocalOffsetStore - 本地偏移量存储（广播模式）
│   └── OffsetStoreFactory - 偏移量存储工厂
├── 异常层 (errors.py, subscription_exceptions.py)
│   └── ConsumerError, SubscriptionError - 异常体系
└── 依赖层
    ├── model/ - 消息模型和数据结构
    ├── broker/ - Broker通信客户端
    ├── nameserver/ - NameServer客户端
    └── remote/ - 远程通信层
```

## 核心组件详解

### 1. 配置管理 (config.py)

#### ConsumerConfig
消费者配置管理类，提供完整的配置参数控制。

**主要配置类别:**
- **基础配置**: 消费者组名称、NameServer地址、实例名称
- **消费行为配置**: 消息模式、消费起始位置、队列分配策略
- **性能配置**: 消费线程数、批量拉取大小、各类超时时间
- **流量控制配置**: 消息消费阈值、网络流量控制
- **OffsetStore配置**: 持久化间隔、缓存大小、故障恢复配置
- **高级配置**: 自动提交、消息追踪、压缩等

**使用示例:**
```python
from pyrocketmq.consumer import create_consumer_config, MessageModel

# 创建基础配置
config = create_consumer_config(
    consumer_group="test_group",
    namesrv_addr="localhost:9876",
    message_model=MessageModel.CLUSTERING
)

# 自定义配置
config.consume_thread_num = 20
config.consume_message_batch_max_size = 1
config.pull_batch_size = 32
config.auto_commit_interval = 5000
```

### 2. 异常体系

#### 异常层次结构
```
ConsumerError (基类)
├── ConsumerStartError - 消费者启动失败
├── ConsumerShutdownError - 消费者关闭失败
├── ConsumerStateError - 消费者状态错误
├── SubscribeError - 订阅操作失败
├── UnsubscribeError - 取消订阅失败
├── MessageConsumeError - 消息消费失败
├── MessagePullError - 消息拉取失败
├── OffsetError - 偏移量操作失败
├── RebalanceError - 队列重平衡失败
├── BrokerNotAvailableError - Broker不可用
├── NameServerError - NameServer错误
├── NetworkError - 网络通信错误
├── TimeoutError - 操作超时
├── ConfigError - 配置错误
└── ValidationError - 参数验证错误

SubscriptionError (订阅专用异常)
├── InvalidTopicError - 无效主题
├── InvalidSelectorError - 无效选择器
├── TopicNotSubscribedError - 主题未订阅
├── SubscriptionConflictError - 订阅冲突
├── SubscriptionLimitExceededError - 订阅数量超限
└── SubscriptionDataError - 订阅数据错误
```

**异常处理示例:**
```python
from pyrocketmq.consumer.errors import ConsumerError, BrokerNotAvailableError
from pyrocketmq.consumer.subscription_exceptions import SubscriptionError

try:
    # 消费者操作
    pass
except BrokerNotAvailableError as e:
    print(f"Broker不可用，等待重试: {e}")
    # 实现重试逻辑
except SubscriptionError as e:
    print(f"订阅配置错误: {e}")
    # 处理订阅问题
except ConsumerError as e:
    print(f"消费者错误: {e}")
    # 通用错误处理
```

### 3. 消息监听器 (listener.py)

#### 监听器接口

**MessageListener (基础接口)**
```python
class MessageListener(ABC):
    """基础消息监听器接口"""

    @abstractmethod
    def consume_message(
        self,
        messages: list[MessageExt],
        context: ConsumeContext
    ) -> ConsumeResult:
        """消费消息的抽象方法"""
        pass
```

**MessageListenerOrderly (顺序消息监听器)**
```python
class MessageListenerOrderly(MessageListener):
    """顺序消息监听器

    保证同一队列中的消息按顺序消费，适用于需要严格顺序的业务场景。
    """

    def consume_message(
        self,
        messages: list[MessageExt],
        context: ConsumeContext
    ) -> ConsumeResult:
        # 实现顺序消息处理逻辑
        for message in messages:
            # 处理消息
            pass
        return ConsumeResult.SUCCESS
```

**MessageListenerConcurrently (并发消息监听器)**
```python
class MessageListenerConcurrently(MessageListener):
    """并发消息监听器

    支持多线程并发消费消息，提供更高的吞吐量。
    """

    def consume_message(
        self,
        messages: list[MessageExt],
        context: ConsumeContext
    ) -> ConsumeResult:
        # 实现并发消息处理逻辑
        for message in messages:
            # 处理消息
            pass
        return ConsumeResult.SUCCESS
```

**消费结果枚举**
```python
class ConsumeResult(Enum):
    SUCCESS = "CONSUME_SUCCESS"                    # 消费成功
    RECONSUME_LATER = "RECONSUME_LATER"            # 稍后重新消费
    COMMIT = "COMMIT"                              # 提交消息
    ROLLBACK = "ROLLBACK"                          # 回滚消息
    SUSPEND_CURRENT_QUEUE_A_MOMENT = "SUSPEND_CURRENT_QUEUE_A_MOMENT"  # 暂停当前队列
```

**监听器使用示例:**
```python
from pyrocketmq.consumer.listener import (
    MessageListenerConcurrently,
    MessageListenerOrderly,
    ConsumeResult,
    ConsumeContext
)
from pyrocketmq.model.message_ext import MessageExt

# 并发消息监听器
class MyConcurrentListener(MessageListenerConcurrently):
    def consume_message(self, messages: list[MessageExt], context: ConsumeContext):
        for message in messages:
            try:
                # 处理消息
                print(f"处理消息: {message.body.decode()}")
                return ConsumeResult.SUCCESS
            except Exception as e:
                print(f"消息处理失败: {e}")
                return ConsumeResult.RECONSUME_LATER

# 顺序消息监听器
class MyOrderlyListener(MessageListenerOrderly):
    def consume_message(self, messages: list[MessageExt], context: ConsumeContext):
        for message in messages:
            try:
                # 顺序处理消息
                print(f"顺序处理消息: {message.body.decode()}")
                return ConsumeResult.SUCCESS
            except Exception as e:
                print(f"顺序消息处理失败: {e}")
                return ConsumeResult.RECONSUME_LATER
```

### 4. 订阅管理器 (subscription_manager.py)

#### SubscriptionManager
管理消息订阅关系，支持主题订阅、选择器订阅等。

**核心功能:**
- **订阅管理**: subscribe()、unsubscribe()、update_selector()
- **查询操作**: get_subscription()、is_subscribed()、get_topics()
- **数据转换**: to_subscription_data_list()、from_subscription_data_list()
- **导入导出**: export_subscriptions()、import_subscriptions()
- **冲突检测**: detect_conflicts()、get_conflicts()

**订阅操作示例:**
```python
from pyrocketmq.consumer.subscription_manager import SubscriptionManager
from pyrocketmq.model.selector import MessageSelector

# 创建订阅管理器
subscription_manager = SubscriptionManager()

# 订阅主题（全部消息）
subscription_manager.subscribe("test_topic")

# 订阅主题（带选择器）
selector = MessageSelector("tag", "order_*")
subscription_manager.subscribe("order_topic", selector)

# 更新选择器
new_selector = MessageSelector("sql", "amount > 100")
subscription_manager.update_selector("order_topic", new_selector)

# 查询订阅信息
is_subscribed = subscription_manager.is_subscribed("test_topic")
subscription = subscription_manager.get_subscription("test_topic")
all_topics = subscription_manager.get_topics()

# 检测订阅冲突
conflicts = subscription_manager.detect_conflicts()
if conflicts:
    print(f"发现订阅冲突: {conflicts}")

# 取消订阅
subscription_manager.unsubscribe("test_topic")

# 导出导入订阅配置
export_data = subscription_manager.export_subscriptions()
subscription_manager.import_subscriptions(export_data)
```

### 5. 偏移量存储体系

#### 5.1 抽象基类 (offset_store.py)

**OffsetStore - 偏移量存储抽象基类**
```python
class OffsetStore(ABC):
    """偏移量存储抽象基类"""

    @abstractmethod
    def start(self) -> None:
        """启动偏移量存储服务"""
        pass

    @abstractmethod
    def stop(self) -> None:
        """停止偏移量存储服务"""
        pass

    @abstractmethod
    def load(self) -> None:
        """加载偏移量数据"""
        pass

    @abstractmethod
    def update_offset(self, queue: MessageQueue, offset: int) -> None:
        """更新偏移量到本地缓存"""
        pass

    @abstractmethod
    def persist(self, queue: MessageQueue) -> None:
        """持久化单个队列的偏移量"""
        pass

    @abstractmethod
    def persist_all(self) -> None:
        """持久化所有偏移量"""
        pass
```

**ReadOffsetType - 读取类型枚举**
```python
class ReadOffsetType(Enum):
    MEMORY_FIRST_THEN_STORE = "MEMORY_FIRST_THEN_STORE"  # 内存优先
    READ_FROM_MEMORY = "READ_FROM_MEMORY"                # 仅内存
    READ_FROM_STORE = "READ_FROM_STORE"                  # 仅存储
```

#### 5.2 远程偏移量存储 (remote_offset_store.py)

**RemoteOffsetStore - 集群模式偏移量存储**

**特点:**
- **远程存储**: 偏移量存储在Broker服务器上
- **多实例协同**: 支持多Consumer实例协同消费
- **本地缓存**: remote_offset_cache减少网络请求
- **批量提交**: 批量提交提升性能
- **Broker地址缓存**: 避免频繁查询NameServer
- **完整监控**: 丰富的性能指标收集

**使用示例:**
```python
from pyrocketmq.consumer.offset_store_factory import create_offset_store
from pyrocketmq.consumer.config import MessageModel

# 创建集群模式偏移量存储
remote_store = create_offset_store(
    consumer_group="cluster_group",
    namesrv_addr="localhost:9876",
    message_model=MessageModel.CLUSTERING,
    broker_manager=broker_manager,
    persist_interval=5000,
    persist_batch_size=10
)

# 使用偏移量存储
remote_store.start()
remote_store.update_offset(message_queue, 100)
remote_store.persist_all()

# 读取偏移量
offset = remote_store.read_offset(
    message_queue,
    ReadOffsetType.MEMORY_FIRST_THEN_STORE
)

# 获取性能指标
metrics = remote_store.get_metrics()
print(f"持久化成功率: {metrics['persist_success_rate']:.1%}")

# 关闭存储
remote_store.stop()
```

#### 5.3 本地偏移量存储 (local_offset_store.py)

**LocalOffsetStore - 广播模式偏移量存储**

**特点:**
- **本地存储**: 偏移量存储在本地文件中
- **独立消费**: 每个Consumer实例独立消费所有消息
- **JSON格式**: 使用JSON格式存储，易于调试
- **原子操作**: 文件原子性写入保证数据一致性
- **线程安全**: 文件读写操作完全线程安全

**使用示例:**
```python
# 创建广播模式偏移量存储
local_store = create_offset_store(
    consumer_group="broadcast_group",
    namesrv_addr="localhost:9876",
    message_model=MessageModel.BROADCASTING,
    store_path="/tmp/rocketmq_offsets",
    persist_interval=3000
)

# 使用偏移量存储
local_store.start()
local_store.update_offset(message_queue, 50)
local_store.persist_all()

# 读取偏移量
offset = local_store.read_offset(
    message_queue,
    ReadOffsetType.READ_FROM_STORE
)

# 关闭存储
local_store.stop()
```

#### 5.4 偏移量存储工厂 (offset_store_factory.py)

**OffsetStoreFactory - 偏移量存储工厂**
```python
# 工厂方法创建存储实例
offset_store = OffsetStoreFactory.create_offset_store(
    consumer_group="test_group",
    namesrv_addr="localhost:9876",
    message_model=MessageModel.CLUSTERING,
    broker_manager=broker_manager,
    auto_start=True
)
```

**OffsetStoreManager - 偏移量存储管理器**
```python
from pyrocketmq.consumer.offset_store_factory import get_offset_store_manager

# 获取全局管理器
manager = get_offset_store_manager()

# 获取或创建存储实例
store = manager.get_or_create_offset_store(
    consumer_group="test_group",
    namesrv_addr="localhost:9876",
    message_model=MessageModel.CLUSTERING,
    broker_manager=broker_manager
)

# 查询管理的存储信息
info = manager.get_managed_stores_info()
metrics = manager.get_all_metrics()

# 关闭指定存储
manager.close_offset_store("test_group", MessageModel.CLUSTERING)

# 关闭所有存储
manager.close_all()
```

### 6. 队列分配策略 (allocate_queue_strategy.py)

#### 队列分配策略
提供在消费者重平衡时将消息队列分配给不同消费者的策略。

**核心策略:**
- **AverageAllocateStrategy**: 平均分配策略，将队列平均分配给所有消费者
- **HashAllocateStrategy**: 哈希分配策略，基于一致性哈希算法分配队列

**使用示例:**
```python
from pyrocketmq.consumer.allocate_queue_strategy import (
    AllocateContext,
    AverageAllocateStrategy,
    HashAllocateStrategy,
    create_average_strategy,
    create_hash_strategy
)
from pyrocketmq.model import MessageQueue

# 创建分配上下文
context = AllocateContext(
    consumer_group="test_group",
    consumer_id="consumer_1",
    current_cid_all=["consumer_1", "consumer_2", "consumer_3"],
    mq_all=[
        MessageQueue("test_topic", "broker-a", 0),
        MessageQueue("test_topic", "broker-a", 1),
        MessageQueue("test_topic", "broker-b", 0),
        MessageQueue("test_topic", "broker-b", 1),
    ],
    mq_divide={}
)

# 使用平均分配策略
avg_strategy = create_average_strategy()
avg_result = avg_strategy.allocate(context)

# 使用哈希分配策略
hash_strategy = create_hash_strategy()
hash_result = hash_strategy.allocate(context)

print(f"平均分配结果: {avg_result}")
print(f"哈希分配结果: {hash_result}")
```

### 7. 消费起始位置管理 (consume_from_where_manager.py)

#### ConsumeFromWhereManager
管理消费者的消费起始位置策略，支持从不同位置开始消费。

**支持的策略:**
- **CONSUME_FROM_LAST_OFFSET**: 从最大偏移量开始消费（默认）
- **CONSUME_FROM_FIRST_OFFSET**: 从最小偏移量开始消费
- **CONSUME_FROM_TIMESTAMP**: 从指定时间戳对应的偏移量开始消费

**使用示例:**
```python
from pyrocketmq.consumer.consume_from_where_manager import ConsumeFromWhereManager
from pyrocketmq.model import ConsumeFromWhere
from pyrocketmq.model.message_queue import MessageQueue

# 创建管理器
consume_manager = ConsumeFromWhereManager(
    consume_group="test_group",
    namesrv_manager=namesrv_manager,
    broker_manager=broker_manager
)

# 创建消息队列
message_queue = MessageQueue("test_topic", "broker-a", 0)

# 从最新偏移量开始消费
offset1 = consume_manager.get_consume_offset(
    message_queue, 
    ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET
)

# 从最早偏移量开始消费  
offset2 = consume_manager.get_consume_offset(
    message_queue,
    ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET
)

# 从指定时间戳开始消费
offset3 = consume_manager.get_consume_offset(
    message_queue,
    ConsumeFromWhere.CONSUME_FROM_TIMESTAMP,
    timestamp=1640995200000  # 2022-01-01 00:00:00
)

print(f"最新偏移量: {offset1}")
print(f"最早偏移量: {offset2}")
print(f"时间戳偏移量: {offset3}")
```

### 8. 并发消费者 (concurrent_consumer.py)

#### ConcurrentConsumer - 完整的并发消息消费者实现

**概述**
ConcurrentConsumer是Consumer模块的完整实现，提供高性能、可靠的消息消费功能。支持多线程并发拉取和处理消息，具备完整的生命周期管理、队列重平衡、偏移量管理等功能。

**核心特性**
- **多线程并发**: 每个队列独立的拉取线程 + 消费线程池并行处理
- **自动重平衡**: 支持消费者组动态扩缩容，自动队列重新分配
- **智能偏移量管理**: 支持集群模式（Broker存储）和广播模式（本地存储）
- **完整的监控指标**: 丰富的性能指标和运行状态监控
- **企业级异常处理**: 完善的错误处理、重试机制和恢复策略
- **生命周期管理**: 完整的启动、运行、关闭流程管理

**架构设计**
```
ConcurrentConsumer
├── 拉取线程池 (Pull Thread Pool)
│   ├── 每个队列独立的拉取线程
│   ├── _pull_messages_loop: 持续拉取消息
│   └── _get_or_initialize_offset: 智能偏移量初始化
├── 消费线程池 (Consume Thread Pool)
│   ├── 并发消息处理
│   ├── _consume_messages_loop: 消费处理循环
│   └── MessageListener: 用户消息处理逻辑
├── 重平衡管理 (Rebalance Manager)
│   ├── _rebalance_loop: 重平衡检查
│   ├── _allocate_queues: 队列分配策略
│   └── 队列分配通知机制
├── 偏移量管理 (Offset Management)
│   ├── OffsetStore: 偏移量存储抽象
│   ├── RemoteOffsetStore: 集群模式
│   └── LocalOffsetStore: 广播模式
└── 监控指标 (Metrics)
    ├── 拉取统计: 成功率、数量、延迟
    ├── 消费统计: 成功率、数量、并发度
    ├── 重平衡统计: 次数、耗时、队列变化
    └── 状态监控: 线程状态、队列状态、连接状态
```

**主要方法**

##### 生命周期方法
```python
def start(self) -> None:
    """启动消费者实例
    
    启动步骤：
    1. 初始化NameServer和Broker连接
    2. 加载历史订阅和偏移量信息
    3. 启动重平衡监控线程
    4. 启动心跳监控线程
    5. 为分配的队列启动拉取线程
    6. 启动消息处理线程池
    """

def shutdown(self) -> None:
    """关闭消费者实例
    
    优雅关闭步骤：
    1. 停止接收新消息
    2. 等待正在处理的消息完成
    3. 持久化最后偏移量
    4. 停止所有线程池
    5. 关闭网络连接
    6. 清理资源
    """
```

##### 订阅管理方法
```python
def subscribe(self, topic: str, selector: MessageSelector) -> None:
    """订阅主题
    
    Args:
        topic: 主题名称
        selector: 消息选择器（标签选择器或SQL选择器）
    """

def unsubscribe(self, topic: str) -> None:
    """取消订阅主题
    """
```

##### 核心处理方法
```python
def _pull_messages_loop(self, message_queue: MessageQueue) -> None:
    """消息拉取循环（每个队列独立线程）
    
    核心逻辑：
    1. 获取或初始化消费偏移量
    2. 从Broker拉取批量消息
    3. 更新本地偏移量缓存
    4. 将消息放入处理队列
    5. 控制拉取频率
    """

def _get_or_initialize_offset(self, message_queue: MessageQueue) -> int:
    """获取或初始化消费偏移量
    
    智能偏移量策略：
    - 如果本地偏移量不为0，直接使用
    - 如果为0（首次消费），根据consume_from_where策略获取：
      * CONSUME_FROM_LAST_OFFSET: 从最新偏移量开始
      * CONSUME_FROM_FIRST_OFFSET: 从最早偏移量开始  
      * CONSUME_FROM_TIMESTAMP: 从指定时间戳开始
    """
```

**使用示例**

##### 基础并发消费
```python
from pyrocketmq.consumer import create_consumer, MessageListenerConcurrently, ConsumeResult
from pyrocketmq.consumer.selector import create_tag_selector

class OrderProcessor(MessageListenerConcurrently):
    def consume_message_concurrently(self, messages, context):
        for message in messages:
            try:
                order_data = json.loads(message.body.decode())
                print(f"处理订单: {order_data['order_id']}")
                # 业务处理逻辑
                process_order(order_data)
                return ConsumeResult.SUCCESS
            except Exception as e:
                print(f"订单处理失败: {e}")
                return ConsumeResult.RECONSUME_LATER

# 创建消费者
consumer = create_consumer(
    consumer_group="order_processor_group",
    namesrv_addr="localhost:9876",
    message_listener=OrderProcessor()
)

# 订阅主题并启动
consumer.subscribe("order_topic", create_tag_selector("*"))
consumer.start()

try:
    # 保持消费者运行
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print(" shutting down consumer...")
    consumer.shutdown()
```

##### 高级配置消费
```python
from pyrocketmq.consumer import create_consumer_with_config, ConsumerConfig
from pyrocketmq.consumer.config import MessageModel, ConsumeFromWhere

# 生产环境配置
config = ConsumerConfig(
    consumer_group="prod_consumer_group",
    namesrv_addr="broker1:9876;broker2:9876",
    message_model=MessageModel.CLUSTERING,
    consume_from_where=ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET,
    
    # 性能配置
    consume_thread_max=50,          # 最大消费线程数
    pull_batch_size=64,             # 批量拉取大小
    consume_message_batch_max_size=10, # 批量消费大小
    pull_interval=0,                # 拉取间隔(毫秒)
    
    # 流量控制
    pull_threshold_for_all=50000,   # 总消息拉取阈值
    pull_threshold_size_for_all=100, # 总消息大小阈值(MB)
    consume_concurrently_max_span=2000, # 并发消费最大跨度
    
    # 超时配置
    consumer_timeout_millis_when_suspend=1000,
    consumer_timeout_millis_send_message=3000,
    consumer_timeout_millis_of_request=20000,
    
    # 偏移量管理
    auto_commit_interval=5000,
    persist_consumer_offset_interval=10000,
    offset_store_recovery_threshold=0.8
)

# 广播模式消费配置（每个消费者独立消费所有消息）
broadcast_config = ConsumerConfig(
    consumer_group="notification_group",
    namesrv_addr="localhost:9876",
    message_model=MessageModel.BROADCASTING,
    consume_from_where=ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET,
    store_path="/tmp/notification_offsets"
)

# 创建消费者
consumer = create_consumer_with_config(config, NotificationListener())
consumer.subscribe("notification_topic", create_tag_selector("alert_*"))
consumer.start()
```

##### 顺序消息消费
```python
from pyrocketmq.consumer import MessageListenerOrderly

class UserMessageProcessor(MessageListenerOrderly):
    def consume_message_orderly(self, messages, context):
        for message in messages:
            user_id = message.get_property("user_id")
            action = json.loads(message.body.decode())
            
            # 保证同一用户的消息顺序处理
            if action['type'] == 'create':
                create_user_account(action['data'])
            elif action['type'] == 'update':
                update_user_profile(action['data'])
            elif action['type'] == 'delete':
                delete_user_account(action['data'])
        
        return ConsumeResult.SUCCESS

# 使用同一队列选择器确保顺序性
from pyrocketmq.producer import MessageHashSelector
# 在Producer端使用相同分片键确保消息在同一队列
```

**监控和调试**
```python
# 获取消费者统计信息
stats = consumer.get_stats()
print(f"拉取成功次数: {stats['pull_successes']}")
print(f"拉取失败次数: {stats['pull_failures']}")
print(f"消费消息总数: {stats['messages_consumed']}")
print(f"当前队列数: {stats['assigned_queues_count']}")
print(f"运行时间(秒): {stats['uptime_seconds']}")

# 检查消费者状态
if consumer.is_running():
    print("消费者正在运行")
else:
    print("消费者已停止")

# 获取分配的队列信息
assigned_queues = consumer.get_assigned_queues()
for queue in assigned_queues:
    print(f"分配队列: {queue}")
```

### 9. 便捷API (全局函数)

#### Consumer创建函数
```python
from pyrocketmq.consumer import (
    create_consumer,
    create_consumer_with_config,
    create_concurrent_consumer
)

# 快速创建消费者
consumer = create_consumer(
    consumer_group="quick_group",
    namesrv_addr="localhost:9876",
    consume_thread_max=20,
    pull_batch_size=32
)

# 使用自定义配置创建
consumer = create_consumer_with_config(custom_config, message_listener)

# 创建并注册监听器
consumer = create_consumer(
    consumer_group="my_group", 
    namesrv_addr="localhost:9876",
    message_listener=MyListener()
)
```

#### 偏移量存储便捷函数
```python
from pyrocketmq.consumer.offset_store_factory import (
    create_offset_store,
    close_offset_store,
    close_all_offset_stores,
    get_offset_store_metrics,
    validate_offset_store_config
)

# 快速创建偏移量存储
store = create_offset_store(
    consumer_group="quick_group",
    namesrv_addr="localhost:9876",
    message_model=MessageModel.CLUSTERING,
    broker_manager=broker_manager
)

# 验证配置
is_valid = validate_offset_store_config(
    consumer_group="test_group",
    message_model=MessageModel.CLUSTERING,
    store_path="/tmp/offsets"
)

# 获取指标
metrics = get_offset_store_metrics()

# 清理资源
close_offset_store("quick_group", MessageModel.CLUSTERING)
close_all_offset_stores()
```

## 完整使用示例

### 1. 集群消费示例
```python
from pyrocketmq.consumer import (
    create_consumer_config,
    SubscriptionManager,
    MessageListenerConcurrently,
    create_offset_store
)
from pyrocketmq.consumer.config import MessageModel
from pyrocketmq.consumer.listener import ConsumeResult, ConsumeContext
from pyrocketmq.model.message_ext import MessageExt

# 创建配置
config = create_consumer_config(
    consumer_group="order_consumer_group",
    namesrv_addr="localhost:9876",
    message_model=MessageModel.CLUSTERING
)

# 创建消息监听器
class OrderMessageListener(MessageListenerConcurrently):
    def consume_message(self, messages: list[MessageExt], context: ConsumeContext):
        for message in messages:
            try:
                # 处理订单消息
                order_data = json.loads(message.body.decode())
                print(f"处理订单: {order_data['order_id']}")

                # 更新偏移量
                context.offset_store.update_offset(
                    context.message_queue,
                    message.queue_offset + 1
                )

                return ConsumeResult.SUCCESS
            except Exception as e:
                print(f"订单处理失败: {e}")
                return ConsumeResult.RECONSUME_LATER

# 创建偏移量存储
offset_store = create_offset_store(
    consumer_group="order_consumer_group",
    namesrv_addr="localhost:9876",
    message_model=MessageModel.CLUSTERING,
    broker_manager=broker_manager
)

# 创建订阅管理器
subscription_manager = SubscriptionManager()
subscription_manager.subscribe("order_topic")

# 注意：BaseConsumer是抽象类，实际使用需要实现具体的Consumer类
# 以下示例展示如何使用各个组件
print("Consumer组件配置完成")
print(f"配置: {config}")
print(f"偏移量存储: {offset_store}")
print(f"订阅管理: {subscription_manager}")
print(f"消息监听器: {OrderMessageListener()}")
```

### 2. 广播消费示例
```python
from pyrocketmq.consumer import (
    create_consumer_config,
    MessageListenerOrderly,
    create_offset_store
)
from pyrocketmq.consumer.config import MessageModel

# 创建广播消费配置
config = create_consumer_config(
    consumer_group="notification_consumer",
    namesrv_addr="localhost:9876",
    message_model=MessageModel.BROADCASTING,
    store_path="/tmp/notification_offsets"
)

# 创建顺序消息监听器
class NotificationListener(MessageListenerOrderly):
    def consume_message(self, messages, context):
        for message in messages:
            try:
                # 处理通知消息
                notification = json.loads(message.body.decode())
                print(f"处理通知: {notification['title']}")

                return ConsumeResult.SUCCESS
            except Exception as e:
                print(f"通知处理失败: {e}")
                return ConsumeResult.RECONSUME_LATER

# 创建本地偏移量存储
offset_store = create_offset_store(
    consumer_group="notification_consumer",
    namesrv_addr="localhost:9876",
    message_model=MessageModel.BROADCASTING,
    store_path="/tmp/notification_offsets"
)

# 使用消费者...
```

### 3. 高级配置示例
```python
# 创建生产环境配置
config = create_consumer_config(
    consumer_group="prod_consumer_group",
    namesrv_addr="broker1:9876;broker2:9876",
    message_model=MessageModel.CLUSTERING
)

# 性能调优配置
config.consume_thread_num = 50
config.consume_message_batch_max_size = 5
config.pull_batch_size = 64
config.pull_threshold_for_all = 50000
config.pull_threshold_size_for_all = 100
config.pull_threshold_for_topic = 5000
config.pull_threshold_size_for_topic = 100

# 网络配置优化
config.consumer_timeout_millis_when_suspend = 1000
config.consumer_timeout_millis_send_message = 3000
config.consumer_timeout_millis_of_request = 20000

# 流量控制配置
config.consume_concurrently_max_span = 2000
config.flow_control_max_diff = 10000
config.flow_control_interval = 20

# OffsetStore配置
config.auto_commit_interval = 1000
config.persist_consumer_offset_interval = 5000
config.max_offset_store_size = 10000
config.offset_store_retry_times = 3
config.offset_store_recovery_threshold = 0.8

# 高级功能配置
config.enable_message_trace = True
config.enable_message_compression = True
config.compress_msg_body_over_howmuch = 4096
config.retry_times_when_send_async_failed = 2
```

## 性能优化建议

### 1. 偏移量存储优化
- **批量提交**: 增大 `persist_batch_size` 以减少网络请求
- **调整持久化间隔**: 根据业务需求调整 `persist_interval`
- **监控缓存命中率**: 保持高缓存命中率以提升性能
- **Broker地址缓存**: 利用Broker地址缓存减少NameServer查询

### 2. 订阅管理优化
- **限制订阅数量**: 避免订阅过多主题影响性能
- **合理使用选择器**: 选择器过于复杂会影响匹配性能
- **定期清理**: 清理不再需要的订阅关系

### 3. 监听器优化
- **异步处理**: 对于耗时操作，考虑异步处理
- **批量处理**: 充分利用 `consume_message_batch_max_size`
- **错误处理**: 实现合理的重试和降级策略

### 4. 监控指标监控
```python
# 获取偏移量存储指标
metrics = offset_store.get_metrics()

# 关键指标监控
print(f"持久化成功率: {metrics['persist_success_rate']:.2%}")
print(f"缓存命中率: {metrics['cache_hit_rate']:.2%}")
print(f"平均持久化延迟: {metrics['avg_persist_latency']:.2f}ms")

# 全局指标监控
all_metrics = get_offset_store_metrics()
for store_key, store_metrics in all_metrics.items():
    if 'error' not in store_metrics:
        print(f"{store_key}: {store_metrics['persist_success_count']} 次成功持久化")
```

## 依赖项列表

### 内部依赖
- **pyrocketmq.model**: 消息模型和数据结构
- **pyrocketmq.broker**: Broker客户端通信
- **pyrocketmq.nameserver**: NameServer客户端
- **pyrocketmq.remote**: 远程通信层
- **pyrocketmq.logging**: 日志系统

### 外部依赖
- **threading**: 线程安全和并发控制
- **pathlib**: 路径处理（本地存储）
- **dataclasses**: 配置类定义
- **enum**: 枚举类型定义
- **typing**: 类型注解支持
- **json**: JSON序列化（本地存储）
- **asyncio**: 异步操作支持（某些实现）

### 可选依赖
- **time**: 时间处理（指标收集）
- **os**: 操作系统接口（文件操作）
- **shutil**: 文件操作（本地存储备份）

## 版本变更记录

### v2.0.0 (2025-01-11) - ConcurrentConsumer完整实现版本
**重大更新**: Consumer模块核心实现完成，提供完整可用的消息消费者功能

**新增功能**:
- ✅ **ConcurrentConsumer完整实现**: 高性能并发消息消费者
  - 多线程架构: 每队列独立拉取线程 + 消费线程池
  - 智能偏移量管理: `_get_or_initialize_offset()` 支持三种消费起始策略
  - 自动重平衡机制: 动态队列分配和消费者组管理
  - 完整生命周期管理: 优雅启动和关闭流程
  - 丰富监控指标: 拉取、消费、重平衡等全方位监控

- ✅ **消息拉取循环优化**: 
  - 重构 `_pull_messages_loop()` 方法，抽取偏移量初始化逻辑
  - 支持CONSUME_FROM_LAST_OFFSET、CONSUME_FROM_FIRST_OFFSET、CONSUME_FROM_TIMESTAMP策略
  - 完善的异常处理和恢复机制
  - 拉取频率控制和流量管理

- ✅ **ConsumerFactory便利函数**:
  - `create_consumer()`: 快速创建并发消费者
  - `create_consumer_with_config()`: 使用自定义配置创建消费者
  - `create_concurrent_consumer()`: 明确命名的并发消费者创建函数

- ✅ **完整的监听器支持**:
  - MessageListenerConcurrently: 并发消息监听器
  - MessageListenerOrderly: 顺序消息监听器
  - 完整的ConsumeResult返回值支持

- ✅ **企业级监控和调试**:
  - `get_stats()`: 详细的运行统计信息
  - `get_assigned_queues()`: 当前分配的队列信息
  - `is_running()`: 消费者运行状态检查

**技术改进**:
- **多线程安全**: 所有核心操作都经过线程安全设计
- **资源管理**: 完善的资源清理和内存管理
- **性能优化**: 批量操作、连接池、缓存机制
- **错误处理**: 分层异常处理和智能重试
- **配置灵活性**: 支持所有RocketMQ Consumer配置参数

**使用场景支持**:
- **集群消费**: 多Consumer实例协同消费，支持负载均衡
- **广播消费**: 每个Consumer独立消费所有消息
- **顺序消息**: 保证同一队列消息的顺序处理
- **高吞吐量**: 多线程并发处理，适合大规模消息消费

**向后兼容性**:
- 完全兼容现有的配置管理、监听器接口、偏移量存储等组件
- 所有现有的便利函数和工具类保持不变
- API设计遵循RocketMQ标准，便于迁移和学习

### v1.1.0 (2025-01-07) - 文档更新版本
**更新内容**:
- ✅ 修正监听器接口的参数类型，使用正确的 `MessageExt` 和 `ConsumeContext`
- ✅ 更新监听器使用示例，添加完整的类型注解
- ✅ 修正订阅管理器示例中的 `MessageSelector` 导入路径
- ✅ 添加队列分配策略详细说明和使用示例
- ✅ 添加消费起始位置管理器详细说明和使用示例
- ✅ 修正集群消费示例，移除不存在的 `Consumer` 类，使用组件展示
- ✅ 更新文档结构，将新增组件整合到合适的位置
- ✅ 完善导入路径和使用说明，确保与代码实现一致

**技术改进**:
- 所有示例代码现在使用正确的类型注解
- 修正了所有组件的导入路径
- 添加了队列分配策略和消费起始位置管理的完整文档
- 所有API使用示例现在与实际代码完全一致

### v1.0.0 (当前版本)
**发布时间**: 2025-01-XX

**主要特性**:
- ✅ 完整的偏移量存储体系实现
  - RemoteOffsetStore: 集群模式远程存储
  - LocalOffsetStore: 广播模式本地存储
  - OffsetStoreFactory: 工厂模式创建
  - OffsetStoreManager: 全局实例管理

- ✅ 灵活的订阅管理系统
  - SubscriptionManager: 订阅关系管理
  - 支持主题订阅和选择器订阅
  - 订阅冲突检测和限制管理
  - 导入导出功能

- ✅ 完整的监听器体系
  - MessageListener: 基础监听器接口
  - MessageListenerOrderly: 顺序消息监听器
  - MessageListenerConcurrently: 并发消息监听器
  - SimpleMessageListener: 简单实现

- ✅ 企业级异常体系
  - ConsumerError: 消费者异常基类
  - 20+ 种专用异常类型
  - SubscriptionError: 订阅专用异常
  - 完整的错误上下文和恢复建议

- ✅ 完整的配置管理
  - ConsumerConfig: 全面的配置参数
  - 环境变量支持
  - 参数验证机制
  - 便利创建函数

- ✅ 丰富的监控指标
  - OffsetStoreMetrics: 性能指标收集
  - 持久化统计、缓存命中率、延迟监控
  - 全局管理器指标聚合
  - 实时运行状态监控

### 架构特点
- **分层设计**: 清晰的职责分离和模块化
- **设计模式**: 工厂模式、单例模式、策略模式应用
- **线程安全**: 所有核心组件支持多线程并发
- **高性能**: 缓存机制、批量操作、并发处理
- **企业级**: 完整的异常处理、监控、配置管理

### 已知限制
- 消费者核心实现（Consumer类）尚未完成
- 消息拉取和重平衡机制待实现
- 与Producer的事务消息集成待完善
- 某些高级配置项的功能待验证

### 下一步计划
- 实现完整的Consumer类
- 添加消息拉取和重平衡逻辑
- 完善事务消息消费支持
- 添加更多性能监控指标
- 优化网络通信和序列化性能

---

## 总结

pyrocketmq Consumer模块是一个功能完整、架构优秀、性能卓越的消息消费者实现。该模块具备以下核心优势：

1. **功能完整性**: 涵盖偏移量管理、订阅管理、消息处理等所有核心功能
2. **架构优秀**: 采用分层架构和多种设计模式，代码结构清晰
3. **高性能**: 通过缓存、批量操作、并发处理等优化手段提供卓越性能
4. **企业级**: 完整的异常体系、监控指标、配置管理，满足生产环境需求
5. **易用性**: 丰富的便捷API和完整的使用示例，降低学习成本

该模块已具备生产环境使用的基础条件，可以作为RocketMQ Python消费者的核心实现。随着后续功能的完善（如完整的Consumer类实现），将为用户提供更加完整和强大的消息消费能力。
