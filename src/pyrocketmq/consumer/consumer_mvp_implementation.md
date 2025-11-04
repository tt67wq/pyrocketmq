# Consumer MVP 实施方案

## 项目概述

基于现有的pyrocketmq基础设施，实现一个最小可行产品(MVP)版本的RocketMQ Consumer。该Consumer将实现基本的消息消费功能，为后续扩展奠定基础。

## 技术选型

### 1. 架构模式
- **MVP优先**: 从最简实现开始，逐步迭代
- **分层设计**: 复用现有的分层架构模式
- **同步优先**: 专注同步消费实现，异步版本后续补充
- **模块化设计**: 并发消费和顺序消费独立实现

### 2. 核心技术栈
- **网络通信**: 复用现有的Transport/Remote层
- **协议解析**: 复用现有的Model层
- **路由发现**: 复用现有的NameServer/Broker客户端
- **并发模型**: 使用threading + queue实现基本并发
- **消息处理**: 采用callback模式处理消息

### 3. 设计原则
- **MVP优先**: 专注同步版本实现，后续补充异步版本
- **简化状态管理**: 使用布尔状态而非复杂状态机
- **模块化设计**: 清晰的模块边界和职责分工
- **可扩展性**: 预留扩展点，便于后续功能增强
- **错误处理**: 完善的异常处理和恢复机制

## 功能拆分

### 功能模块组成
1. **ConsumerConfig** - Consumer配置管理
2. **MessageListener** - 消息处理回调接口
3. **MessageSelector** - 消息选择器数据结构(与Go语言兼容)
4. **SubscriptionInfo** - 订阅关系数据结构

### 核心组件实现
1. **ConcurrentConsumer** - 并发消费Consumer (同步版本)
   - 生命周期管理(start/shutdown)
   - 订阅管理(subscribe/unsubscribe)
   - 基本的消息拉取循环
   - 多线程消息处理
2. **MessageProcessor** - 消息处理器
   - 消息反序列化
   - 回调调用
   - 消费结果处理

### 路由和队列管理
1. **RebalanceImpl** - 重平衡实现
   - 队列分配策略(支持AVG/HASH/CONFIGURATION/MACHINE_ROOM)
   - 消费者发现和注册
   - 队列重新分配触发机制
2. **AllocateQueueStrategy** - 负载均衡策略实现
   - **AverageAllocateStrategy**: 平均分配策略(MVP默认)
   - **HashAllocateStrategy**: 基于Consumer ID的哈希分配
   - **ConfigAllocateStrategy**: 基于配置文件的分配
   - **MachineRoomAllocateStrategy**: 机房优先分配策略
3. **PullTask** - 拉取任务
   - 定时拉取和流量控制
   - 偏移量管理和持久化
   - 批量处理和大小限制

### 高级功能 (后续扩展)
1. **OrderlyConsumer** - 顺序消费 (独立实现)
2. **AsyncConcurrentConsumer** - 异步并发Consumer
3. **AsyncOrderlyConsumer** - 异步顺序Consumer
4. **TransactionConsumer** - 事务消费
5. **BroadcastConsumer** - 广播消费

## Consumer实现架构设计：并发 vs 顺序

### 设计原则：分离并发和顺序消费

基于RocketMQ Go实现的观察，顺序消费(OrderlyConsumer)和并发消费(ConcurrentlyConsumer)在核心架构上存在根本性差异，建议拆分为两个独立的实现，避免单一实现过于复杂。

### 核心差异分析

#### 1. 消息处理模式差异
- **并发消费**: 多线程并行处理，追求吞吐量
- **顺序消费**: 单队列锁定，保证消息顺序，牺牲并发性

#### 2. 偏移量管理差异
- **并发消费**: 异步提交偏移量，允许消息丢失
- **顺序消费**: 同步提交偏移量，严格保证不丢失

#### 3. 队列管理差异
- **并发消费**: 动态队列分配，支持重平衡
- **顺序消费**: 队列锁定机制，避免重平衡导致的顺序错乱

#### 4. 错误处理差异
- **并发消费**: 消息失败进入重试队列，不阻塞其他消息
- **顺序消费**: 消息失败阻塞队列，需要人工干预

### 实现架构设计

#### 基础抽象层
```python
class BaseConsumer(ABC):
    """Consumer基类，定义通用接口"""

    def __init__(self, config: ConsumerConfig):
        self.config = config
        self.subscription_manager = SubscriptionManager()
        self.message_selector = MessageSelector()

    @abstractmethod
    def start(self) -> None:
        """启动Consumer"""
        pass

    @abstractmethod
    def shutdown(self) -> None:
        """关闭Consumer"""
        pass

    @abstractmethod
    subscribe(self, topic: str, selector: MessageSelector) -> None:
        """订阅Topic"""
        pass
```

#### 并发消费实现 (MVP核心)
```python
class ConcurrentConsumer(BaseConsumer):
    """并发消费Consumer - MVP核心实现 (同步版本)

    支持多线程并发处理消息，追求高吞吐量。
    这是RocketMQ的标准消费模式，适用于大多数业务场景。
    MVP版本专注同步实现，简化复杂度。
    """

    def __init__(self, config: ConsumerConfig):
        super().__init__(config)
        self.thread_pool = ConsumeThreadPool(
            min_threads=config.consume_thread_min,
            max_threads=config.consume_thread_max
        )
        self.message_queue = queue.Queue()
        self.offset_store = RemoteOffsetStore(...)
        self.rebalance_impl = RebalanceImpl(...)

        # 同步版本状态管理
        self.running = False
        self.pull_tasks: List[PullTask] = []

    def start(self) -> None:
        """启动Consumer (同步版本)"""
        if self.running:
            return

        self.running = True

        # 启动消费线程池
        self.thread_pool.start()

        # 启动拉取任务
        for queue in self.assigned_queues:
            task = PullTask(queue, self)
            self.pull_tasks.append(task)
            task.start()

    def shutdown(self) -> None:
        """关闭Consumer (同步版本)"""
        if not self.running:
            return

        self.running = False

        # 停止拉取任务
        for task in self.pull_tasks:
            task.stop()

        # 停止线程池
        self.thread_pool.shutdown()

        # 持久化偏移量
        try:
            asyncio.run(self.offset_store.persist_all())
        except Exception as e:
            logger.error(f"Failed to persist offsets on shutdown: {e}")

    def consume_message(self, messages: List[MessageExt]) -> ConsumeResult:
        """同步消费消息处理"""
        # 提交到线程池并发处理
        return self.thread_pool.submit(messages)
```

#### 异步版本 (后续扩展)
```python
class AsyncConcurrentConsumer(BaseConsumer):
    """异步并发消费Consumer - 后续扩展版本

    基于asyncio实现的全异步Consumer，提供更高的并发性能。
    适用于高并发场景和异步应用架构。
    """

    def __init__(self, config: ConsumerConfig):
        super().__init__(config)
        self.consume_semaphore = asyncio.Semaphore(config.consume_thread_max)
        self.message_queue = asyncio.Queue()
        self.offset_store = RemoteOffsetStore(...)
        self.rebalance_impl = RebalanceImpl(...)

        # 异步版本状态管理
        self.running = False
        self.pull_tasks: List[AsyncPullTask] = []

    async def start(self) -> None:
        """启动Consumer (异步版本)"""
        if self.running:
            return

        self.running = True

        # 启动拉取任务
        for queue in self.assigned_queues:
            task = AsyncPullTask(queue, self)
            self.pull_tasks.append(task)
            await task.start()

    async def shutdown(self) -> None:
        """关闭Consumer (异步版本)"""
        if not self.running:
            return

        self.running = False

        # 停止拉取任务
        for task in self.pull_tasks:
            await task.stop()

        # 持久化偏移量
        await self.offset_store.persist_all()

    async def consume_message(self, messages: List[MessageExt]) -> ConsumeResult:
        """异步消费消息处理"""
        async with self.consume_semaphore:
            return await self._process_messages_async(messages)
```

#### 顺序消费实现 (独立模块)
```python
class OrderlyConsumer(BaseConsumer):
    """顺序消费Consumer - 独立实现

    保证每个队列内的消息严格按顺序处理。
    通过队列锁定机制避免重平衡导致的顺序问题。
    """

    def __init__(self, config: ConsumerConfig):
        super().__init__(config)
        self.queue_locks = QueueLockManager()  # 队列锁管理
        self.consume_processor = OrderlyConsumeProcessor()
        self.offset_store = OrderlyOffsetStore(...)  # 顺序消费专用OffsetStore
        self.rebalance_impl = OrderlyRebalanceImpl(...)  # 顺序消费专用重平衡

    def start(self) -> None:
        # 锁定当前队列，避免重平衡
        await self.queue_locks.lock_all_queues(self.assigned_queues)

        # 启动单线程消费处理器
        self.consume_processor.start()

        # 启动拉取任务（单线程）
        for queue in self.assigned_queues:
            task = OrderlyPullTask(queue, self)
            await task.start()

    def consume_message(self, messages: List[MessageExt]) -> ConsumeResult:
        # 同步处理，确保顺序
        try:
            result = await self._process_orderly(messages)
            if result == ConsumeResult.SUCCESS:
                # 同步提交偏移量
                await self.offset_store.sync_commit()
            return result
        except Exception as e:
            # 顺序消费失败，需要暂停队列
            await self._pause_queue_processing(e)
            return ConsumeResult.RECONSUME_LATER
```

### 共享组件设计

#### 1. 配置管理 (统一)
```python
@dataclass
class ConsumerConfig:
    # 基础配置 (共享)
    consumer_group: str
    namesrv_addr: str
    message_model: MessageModel

    # 并发消费配置 (ConcurrentConsumer使用)
    consume_thread_min: int = 20
    consume_thread_max: int = 64

    # 顺序消费配置 (OrderlyConsumer使用)
    orderly_max_reconsume_times: int = 16
    orderly_consume_timeout: int = 30
    orderly_suspend_current_queue_time_millis: int = 1000
```

#### 2. 通用工具类 (共享)
```python
class MessageSelector:
    """消息选择器 - 两种Consumer共享"""

class SubscriptionManager:
    """订阅关系管理 - 两种Consumer共享"""

class OffsetStore:
    """偏移量存储 - 基类，两种Consumer继承实现"""
```



### API设计统一

#### 统一的使用接口
```python
# 同步并发消费使用 (MVP)
consumer = ConcurrentConsumer(config)
consumer.subscribe("order_topic", MessageSelector.by_tag("*"))
consumer.register_message_listener(OrderConcurrentlyListener())
consumer.start()  # 同步启动

# 同步顺序消费使用
consumer = OrderlyConsumer(config)
consumer.subscribe("order_topic", MessageSelector.by_tag("*"))
consumer.register_message_listener(OrderOrderlyListener())
consumer.start()  # 同步启动

# 异步并发消费使用 (后续扩展)
consumer = AsyncConcurrentConsumer(config)
consumer.subscribe("order_topic", MessageSelector.by_tag("*"))
consumer.register_message_listener(AsyncOrderConcurrentlyListener())
await consumer.start()  # 异步启动

# 异步顺序消费使用 (后续扩展)
consumer = AsyncOrderlyConsumer(config)
consumer.subscribe("order_topic", MessageSelector.by_tag("*"))
consumer.register_message_listener(AsyncOrderOrderlyListener())
await consumer.start()  # 异步启动
```

#### 差异化配置
```python
# 并发消费配置
concurrent_config = ConsumerConfig(
    consumer_group="concurrent_group",
    namesrv_addr="localhost:9876",
    consume_thread_max=64,
    enable_auto_commit=True
)

# 顺序消费配置
orderly_config = ConsumerConfig(
    consumer_group="orderly_group",
    namesrv_addr="localhost:9876",
    orderly_max_reconsume_times=16,
    orderly_consume_timeout=30
)
```

### 优势分析

#### 1. 代码清晰性
- 每个Consumer专注于自己的核心逻辑
- 避免复杂的条件判断和状态管理
- 更容易理解和维护

#### 2. 性能优化
- 并发Consumer可以专注优化吞吐量
- 顺序Consumer可以专注优化顺序保证
- 避免不必要的抽象开销

#### 3. 扩展性
- 可以独立添加新功能
- 不会相互影响
- 更好的测试覆盖率

#### 4. 符合RocketMQ设计理念
- 与Go实现保持一致
- 符合RocketMQ的官方设计
- 便于后续功能升级

## 实现内容

### 核心组件
1. **ConsumerConfig** - Consumer配置管理
2. **MessageListener** - 消息处理回调接口
3. **BaseConsumer** - Consumer基类，定义通用接口
4. **ConcurrentConsumer** - 并发消费Consumer (同步版本)
5. **OrderlyConsumer** - 顺序消费Consumer (独立实现)

### 关键模块
1. **RebalanceImpl** - 重平衡实现
2. **AllocateQueueStrategy** - 负载均衡策略实现
3. **PullTask** - 拉取任务
4. **OffsetStore** - 偏移量存储
5. **MessageSelector** - 消息选择器

### 支持功能
- 多种负载均衡策略
- 队列重平衡机制
- 偏移量管理和持久化
- 消息过滤和选择
- 错误处理和恢复
- 性能监控和统计

## 核心组件设计

### 1. ConsumerConfig
```python
from enum import Enum

class ConsumeFromWhere(Enum):
    """消费起始位置策略"""
    LAST_OFFSET = "CONSUME_FROM_LAST_OFFSET"     # 从最后偏移量开始消费
    FIRST_OFFSET = "CONSUME_FROM_FIRST_OFFSET"   # 从第一个偏移量开始消费
    TIMESTAMP = "CONSUME_FROM_TIMESTAMP"         # 从指定时间戳开始消费
    MIN_OFFSET = "CONSUME_FROM_MIN_OFFSET"       # 从最小偏移量开始消费
    MAX_OFFSET = "CONSUME_FROM_MAX_OFFSET"       # 从最大偏移量开始消费

class AllocateQueueStrategy(Enum):
    """队列负载均衡策略"""
    AVERAGE = "AVG"                              # 平均分配策略
    HASH = "HASH"                                # 哈希分配策略
    CONFIGURATION = "CONFIGURATION"              # 配置指定策略
    MACHINE_ROOM = "MACHINE_ROOM"                # 机房优先策略

@dataclass
class ConsumerConfig:
    consumer_group: str
    namesrv_addr: str
    consume_thread_min: int = 20
    consume_thread_max: int = 64
    pull_batch_size: int = 32
    consume_timeout: int = 15
    max_reconsume_times: int = 16
    message_model: MessageModel = MessageModel.CLUSTERING

    # 消费起始位置和负载均衡配置
    consume_from_where: ConsumeFromWhere = ConsumeFromWhere.LAST_OFFSET  # 消费起始位置
    consume_timestamp: int = 0  # 当consume_from_where=TIMESTAMP时使用
    allocate_queue_strategy: AllocateQueueStrategy = AllocateQueueStrategy.AVERAGE  # 负载均衡策略

    # 拉取控制配置
    pull_interval: int = 0     # 拉取间隔(毫秒)，0表示持续拉取
    pull_threshold_for_all: int = 50000  # 所有队列消息数阈值
    pull_threshold_for_topic: int = 10000  # 单个topic消息数阈值
    pull_threshold_size_for_topic: int = 100  # 单个topic消息大小阈值(MB)
    pull_threshold_of_queue: int = 1000  # 单个队列消息数阈值
    pull_threshold_size_of_queue: int = 100  # 单个队列消息大小阈值(MB)

    # OffsetStore偏移量存储配置
    # 持久化间隔配置
    persist_interval: int = 5000  # 毫秒，定期持久化间隔
    persist_batch_size: int = 10  # 批量提交大小

    # 本地存储路径(广播模式使用)
    offset_store_path: str = "~/.rocketmq/offsets"

    # 内存缓存配置
    max_cache_size: int = 10000  # 最大缓存条目数

    # 故障恢复配置
    enable_auto_recovery: bool = True  # 启用自动恢复
    max_retry_times: int = 3  # 最大重试次数
```

### 2. MessageListener
```python
class MessageListener:
    def consume_message(self, messages: List[MessageExt]) -> ConsumeResult:
        pass

class ConsumeResult(Enum):
    SUCCESS = "CONSUME_SUCCESS"
    RECONSUME_LATER = "RECONSUME_LATER"
```

### 3. MessageSelector数据结构
已在model中实现

### 4. 基础抽象类 (MVP)
```python
from abc import ABC, abstractmethod

class BaseConsumer(ABC):
    """Consumer基类，定义通用接口 (同步版本)"""

    def __init__(self, config: ConsumerConfig):
        self.config = config
        self.subscription_manager = SubscriptionManager()
        self.running = False

    @abstractmethod
    def start(self) -> None:
        """启动Consumer"""
        pass

    @abstractmethod
    def shutdown(self) -> None:
        """关闭Consumer"""
        pass

    @abstractmethod
    def subscribe(self, topic: str, selector: MessageSelector) -> None:
        """订阅Topic"""
        pass

    @abstractmethod
    def register_message_listener(self, listener: MessageListener) -> None:
        """注册消息监听器"""
        pass

class ConcurrentConsumer(BaseConsumer):
    """并发消费Consumer - MVP核心实现 (同步版本)"""
    # 详细实现见上方架构设计部分

class OrderlyConsumer(BaseConsumer):
    """顺序消费Consumer - 独立实现 (同步版本)"""
    # 详细实现见上方架构设计部分
```

## 依赖关系

### 复用现有组件
- **Model层**: Message, MessageExt, MessageQueue等数据结构
- **Transport层**: TCP连接和状态管理
- **Remote层**: 远程通信和连接池
- **NameServer**: 路由查询和Broker发现
- **Broker**: 消息拉取和偏移量管理
- **Logging**: 统一日志记录

### 新增依赖
- **threading**: 消费线程池管理
- **queue**: 消息队列和任务调度
- **time**: 定时任务和超时控制
- **collections**: 消费状态统计

## 负载均衡策略详细设计

### 1. AverageAllocateStrategy (平均分配策略) - MVP默认
```python
class AverageAllocateStrategy:
    """平均分配队列策略

    将所有队列尽可能平均分配给所有消费者，确保每个消费者分配到的队列数量相差不超过1。
    这是最常用和最简单的负载均衡策略。
    """
    def allocate(self, consumer_group: str, current_cid: str,
                 mq_all: List[MessageQueue], cid_all: List[str]) -> List[MessageQueue]:
        # 算法: 队列列表排序后按消费者数量平均分配
```

### 2. HashAllocateStrategy (哈希分配策略)
```python
class HashAllocateStrategy:
    """基于Consumer ID哈希的分配策略

    对Consumer ID进行哈希计算，确保相同的Consumer总是分配到相同的队列集合。
    适用于需要稳定分配关系的场景。
    """
    def allocate(self, consumer_group: str, current_cid: str,
                 mq_all: List[MessageQueue], cid_all: List[str]) -> List[MessageQueue]:
        # 算法: 基于Consumer ID哈希值确定分配起始位置
```

### 3. ConfigAllocateStrategy (配置指定策略)
```python
class ConfigAllocateStrategy:
    """基于配置文件的分配策略

    允许通过配置文件指定Consumer和队列的映射关系，提供最精确的控制。
    适用于有特定业务需求或运维要求的场景。
    """
    def allocate(self, consumer_group: str, current_cid: str,
                 mq_all: List[MessageQueue], cid_all: List[str]) -> List[MessageQueue]:
        # 算法: 读取配置文件中的映射关系
```

### 4. MachineRoomAllocateStrategy (机房优先策略)
```python
class MachineRoomAllocateStrategy:
    """机房优先分配策略

    优先将Consumer分配到同机房的Broker队列，减少跨机房网络开销。
    适用于多机房部署的场景。
    """
    def allocate(self, consumer_group: str, current_cid: str,
                 mq_all: List[MessageQueue], cid_all: List[str]) -> List[MessageQueue]:
        # 算法: 优先分配同机房队列，剩余队列按平均策略分配
```

## 消费起始位置管理

### ConsumeFromWhere策略实现
```python
class ConsumeFromWhereManager:
    """管理消费起始位置的具体实现"""

    def get_consume_offset(self, queue: MessageQueue, strategy: ConsumeFromWhere,
                          timestamp: int = 0) -> int:
        """根据策略获取消费起始偏移量"""
        if strategy == ConsumeFromWhere.LAST_OFFSET:
            return self.get_max_offset(queue)
        elif strategy == ConsumeFromWhere.FIRST_OFFSET:
            return self.get_min_offset(queue)
        elif strategy == ConsumeFromWhere.TIMESTAMP:
            return self.get_offset_by_timestamp(queue, timestamp)
        elif strategy == ConsumeFromWhere.MIN_OFFSET:
            return self.get_min_offset(queue)
        elif strategy == ConsumeFromWhere.MAX_OFFSET:
            return self.get_max_offset(queue)
```

## 订阅关系与消息过滤

### MessageSelector设计详解

与Go语言RocketMQ客户端保持完全兼容的消息选择器实现：

```python
# Go语言原结构
type MessageSelector struct {
    Type       ExpressionType
    Expression string
}

type ExpressionType string

const (
    SQL92 = ExpressionType("SQL92") // deprecated
    TAG   = ExpressionType("TAG")
)

# Python对应实现
@dataclass
class MessageSelector:
    type: ExpressionType
    expression: str
```

### 使用示例

#### TAG类型订阅(推荐)
```python
from pyrocketmq.consumer import Consumer, MessageSelector

# 订阅所有消息
selector = MessageSelector.by_tag("*")
consumer.subscribe("order_topic", selector)

# 订阅特定标签
selector = MessageSelector.by_tag("order || payment")
consumer.subscribe("order_topic", selector)
```

#### SQL92类型订阅(已废弃)
```python
# 不推荐使用，仅保持兼容性
selector = MessageSelector.by_sql("color = 'red' AND price > 100")
consumer.subscribe("product_topic", selector)
```

#### 便利函数支持
```python
# 向后兼容的字符串接口
consumer.subscribe("order_topic", "*")  # 自动转换为TAG选择器
consumer.subscribe("order_topic", "tag1 || tag2")  # 自动转换为TAG选择器

# 判断表达式类型函数(与Go语言IsTagType对应)
def is_tag_type(expression: str) -> bool:
    """判断表达式是否为TAG类型"""
    return not expression or expression.upper() == "TAG"
```

## OffsetStore 偏移量存储方案设计

### 1. OffsetStore概述

OffsetStore是RocketMQ Consumer的核心组件，负责管理和持久化消息消费偏移量。它确保Consumer能够记录每个队列的消费进度，并在重启后恢复消费位置。

### 2. 设计目标

- **数据持久化**: 确保偏移量数据不丢失
- **高性能**: 支持高并发场景下的频繁读写
- **容错性**: 支持Broker故障时的数据恢复
- **兼容性**: 与RocketMQ协议完全兼容
- **灵活性**: 支持不同消息模式的存储策略

### 3. 存储策略

#### 3.1 集群模式(Clustering) - 远程存储
```python
class RemoteOffsetStore:
    """集群模式下的远程偏移量存储

    偏移量存储在Broker服务器上，支持多Consumer实例协同消费。
    这是RocketMQ的标准实现方式。
    """

    def __init__(self, consumer_group: str, broker_manager: BrokerManager):
        self.consumer_group = consumer_group
        self.broker_manager = broker_manager
        self.local_cache = {}  # 本地缓存，减少网络请求

    async def load(self) -> None:
        """从Broker加载偏移量到本地缓存"""

    async def update_offset(self, queue: MessageQueue, offset: int) -> None:
        """更新偏移量到本地缓存"""

    async def persist(self, queue: MessageQueue) -> None:
        """将本地缓存的偏移量持久化到Broker"""

    async def read_offset(self, queue: MessageQueue, read_type: ReadOffsetType) -> int:
        """读取偏移量，支持多种读取策略"""

    async def persist_all(self) -> None:
        """持久化所有本地缓存的偏移量"""
```

#### 3.2 广播模式(Broadcasting) - 本地存储
```python
class LocalOffsetStore:
    """广播模式下的本地偏移量存储

    偏移量存储在本地文件中，每个Consumer实例独立消费所有消息。
    """

    def __init__(self, consumer_group: str, store_path: str):
        self.consumer_group = consumer_group
        self.store_path = store_path
        self.offset_table = {}
        self.file_path = os.path.join(store_path, f"{consumer_group}.offset")

    async def load(self) -> None:
        """从本地文件加载偏移量"""

    async def update_offset(self, queue: MessageQueue, offset: int) -> None:
        """更新内存中的偏移量"""

    async def persist(self, queue: MessageQueue) -> None:
        """持久化偏移量到本地文件"""

    async def read_offset(self, queue: MessageQueue, read_type: ReadOffsetType) -> int:
        """从内存读取偏移量"""

    async def persist_all(self) -> None:
        """持久化所有偏移量到本地文件"""
```

### 4. 偏移量读取策略

```python
from enum import Enum

class ReadOffsetType(Enum):
    """偏移量读取类型"""
    MEMORY_FIRST_THEN_STORE = "MEMORY_FIRST_THEN_STORE"  # 优先从内存读取，失败则从存储读取
    READ_FROM_MEMORY = "READ_FROM_MEMORY"                 # 仅从内存读取
    READ_FROM_STORE = "READ_FROM_STORE"                   # 仅从存储读取
```

### 5. 核心接口设计

```python
from abc import ABC, abstractmethod
from typing import Dict, Optional
from dataclasses import dataclass

@dataclass
class OffsetEntry:
    """偏移量条目"""
    queue: MessageQueue
    offset: int
    last_update_timestamp: int

class OffsetStore(ABC):
    """偏移量存储抽象基类"""

    @abstractmethod
    async def load(self) -> None:
        """加载偏移量数据"""
        pass

    @abstractmethod
    async def update_offset(self, queue: MessageQueue, offset: int) -> None:
        """更新偏移量"""
        pass

    @abstractmethod
    async def persist(self, queue: MessageQueue) -> None:
        """持久化单个队列的偏移量"""
        pass

    @abstractmethod
    async def persist_all(self) -> None:
        """持久化所有偏移量"""
        pass

    @abstractmethod
    async def read_offset(self, queue: MessageQueue, read_type: ReadOffsetType) -> int:
        """读取偏移量"""
        pass

    @abstractmethod
    async def remove_offset(self, queue: MessageQueue) -> None:
        """移除队列的偏移量"""
        pass

    @abstractmethod
    def clone_offset_table(self, topic: str) -> Dict[MessageQueue, int]:
        """克隆指定Topic的偏移量表"""
        pass
```

### 6. 实现细节

#### 6.1 RemoteOffsetStore实现要点
```python
class RemoteOffsetStore(OffsetStore):
    """远程偏移量存储实现"""

    def __init__(self, consumer_group: str, broker_manager: BrokerManager):
        self.consumer_group = consumer_group
        self.broker_manager = broker_manager
        self.offset_table: Dict[MessageQueue, int] = {}
        self.lock = asyncio.RLock()

    async def update_offset(self, queue: MessageQueue, offset: int) -> None:
        """更新偏移量到本地缓存"""
        async with self.lock:
            self.offset_table[queue] = offset

    async def persist(self, queue: MessageQueue) -> None:
        """持久化单个队列的偏移量到Broker"""
        async with self.lock:
            if queue in self.offset_table:
                offset = self.offset_table[queue]
                # 构建更新偏移量请求
                request = self._build_update_request(queue, offset)

                # 发送到对应的Broker
                broker_client = self.broker_manager.get_client(queue.broker_name)
                await broker_client.update_consumer_offset(
                    self.consumer_group, queue, offset
                )

    async def persist_all(self) -> None:
        """批量持久化所有偏移量"""
        # 按Broker分组，批量提交
        broker_offsets = self._group_by_broker()

        tasks = []
        for broker_name, offsets in broker_offsets.items():
            task = self._persist_to_broker(broker_name, offsets)
            tasks.append(task)

        await asyncio.gather(*tasks)
```

#### 6.2 LocalOffsetStore实现要点
```python
class LocalOffsetStore(OffsetStore):
    """本地偏移量存储实现"""

    def __init__(self, consumer_group: str, store_path: str):
        self.consumer_group = consumer_group
        self.store_path = store_path
        self.offset_table: Dict[MessageQueue, int] = {}
        self.file_path = os.path.join(store_path, f"{consumer_group}.offset")
        self.lock = threading.RLock()

    async def load(self) -> None:
        """从本地文件加载偏移量"""
        with self.lock:
            if not os.path.exists(self.file_path):
                return

            try:
                with open(self.file_path, 'r') as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue

                        # 解析格式: topic@brokerId:queueId=offset
                        try:
                            queue_key, offset_str = line.split('=')
                            offset = int(offset_str)
                            queue = self._parse_queue_key(queue_key)
                            self.offset_table[queue] = offset
                        except (ValueError, IndexError):
                            continue

            except IOError as e:
                logger.warning(f"Failed to load offset file: {e}")

    async def persist_all(self) -> None:
        """持久化所有偏移量到本地文件"""
        with self.lock:
            try:
                # 确保目录存在
                os.makedirs(os.path.dirname(self.file_path), exist_ok=True)

                with open(self.file_path, 'w') as f:
                    for queue, offset in self.offset_table.items():
                        queue_key = self._format_queue_key(queue)
                        f.write(f"{queue_key}={offset}\n")

            except IOError as e:
                logger.error(f"Failed to persist offset file: {e}")
```

### 7. 配置管理

OffsetStore相关配置已整合到ConsumerConfig中，包含以下配置项：

- **persist_interval**: 定期持久化间隔(毫秒)
- **persist_batch_size**: 批量提交大小
- **offset_store_path**: 本地存储路径(广播模式使用)
- **max_cache_size**: 最大缓存条目数
- **enable_auto_recovery**: 启用自动恢复
- **max_retry_times**: 最大重试次数

### 8. 容错和恢复机制

```python
class OffsetStoreErrorHandler:
    """偏移量存储错误处理器"""

    async def handle_persist_failure(self, queue: MessageQueue,
                                   offset: int, error: Exception) -> None:
        """处理持久化失败"""
        if isinstance(error, BrokerNotAvailableError):
            # Broker不可用，加入重试队列
            await self._add_to_retry_queue(queue, offset)
        elif isinstance(error, NetworkTimeoutError):
            # 网络超时，延长重试间隔
            await self._schedule_retry_with_backoff(queue, offset)
        else:
            # 其他错误，记录日志并跳过
            logger.error(f"Failed to persist offset for {queue}: {error}")

    async def handle_load_failure(self, error: Exception) -> None:
        """处理加载失败"""
        if isinstance(error, FileNotFoundError):
            # 文件不存在，使用默认偏移量
            logger.info("Offset file not found, using default offset")
        else:
            logger.error(f"Failed to load offsets: {error}")
```

### 10. 性能优化

#### 10.1 批量操作优化
- **批量提交**: 将多个偏移量更新合并为一次请求
- **异步写入**: 使用异步IO避免阻塞消费线程
- **本地缓存**: 减少网络请求频率

#### 10.2 内存管理优化
- **LRU缓存**: 限制内存中缓存的数据量
- **延迟写入**: 非关键路径的写入操作延迟执行
- **压缩存储**: 对本地文件使用压缩格式

#### 10.3 网络优化
- **连接复用**: 复用Broker连接减少握手开销
- **请求合并**: 合并同一Broker的多个请求
- **超时控制**: 合理设置网络超时时间

### 11. 监控和指标

```python
@dataclass
class OffsetStoreMetrics:
    """偏移量存储指标"""
    persist_success_count: int = 0      # 成功持久化次数
    persist_failure_count: int = 0      # 失败持久化次数
    load_success_count: int = 0         # 成功加载次数
    load_failure_count: int = 0         # 失败加载次数
    cache_hit_count: int = 0            # 缓存命中次数
    cache_miss_count: int = 0           # 缓存未命中次数
    avg_persist_latency: float = 0.0    # 平均持久化延迟
```

### OffsetStore设计要点总结

1. **双模式支持**: 集群模式使用远程存储，广播模式使用本地存储
2. **高性能设计**: 本地缓存 + 批量持久化 + 异步IO
3. **容错机制**: 完善的错误处理和自动恢复
4. **配置灵活**: 支持多种性能调优选项
5. **监控完善**: 提供详细的性能指标和统计
6. **测试充分**: 覆盖各种场景的测试策略

### 测试策略
- 每个里程碑完成后进行全面测试
- 使用现有的Producer进行端到端验证
- 重点关注边界条件和异常场景

### 文档要求
- 每个模块都需要完整的API文档
- 提供详细的使用示例和最佳实践
- 维护更新日志和版本说明

---

**创建时间**: 2025-01-04
**版本**: MVP 1.0
**负责人**: pyrocketmq团队
**预计完成时间**: 4周
