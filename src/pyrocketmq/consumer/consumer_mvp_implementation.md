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

## 功能规划

### MVP核心功能 (第1-2周)
1. **ConsumerConfig** - Consumer配置管理
2. **MessageListener** - 消息处理回调接口
3. **ConcurrentConsumer** - 并发消费Consumer (同步版本)
4. **SubscriptionManager** - 订阅关系管理
5. **OffsetStore** - 偏移量存储 (集群模式)

### 扩展功能 (第3-4周)
1. **OrderlyConsumer** - 顺序消费 (独立实现)
2. **负载均衡策略** - 完整的队列分配算法
3. **重平衡机制** - 动态队列分配
4. **本地偏移量存储** - 广播模式支持

### 高级功能 (后续版本)
1. **AsyncConcurrentConsumer** - 异步并发Consumer
2. **AsyncOrderlyConsumer** - 异步顺序Consumer
3. **事务消费** - TransactionConsumer
4. **监控和指标** - 完善的统计功能

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

#### 2. 共享组件

#### MessageSelector ✅ **已实现**
消息选择器已在 `src/pyrocketmq/model/client_data.py` 中完整实现，支持与Go语言RocketMQ客户端完全兼容的功能。

#### SubscriptionManager
订阅关系管理组件，负责管理Topic订阅关系和消息选择器。详见下方 SubscriptionManager 详细设计章节。

#### OffsetStore
偏移量存储组件，提供集群模式和广播模式的偏移量管理。详见下方 OffsetStore 详细设计章节。



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

### 设计优势

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

## 实施计划

### 第1周：基础架构
- BaseConsumer抽象基类
- ConsumerConfig配置管理
- MessageListener接口设计
- 基础异常体系

### 第2周：并发消费核心
- ConcurrentConsumer实现
- SubscriptionManager订阅管理
- 基础消息拉取逻辑
- 偏移量存储接口

### 第3周：负载均衡与重平衡
- AllocateQueueStrategy实现
- RebalanceImpl重平衡机制
- OffsetStore远程存储
- 集群模式支持

### 第4周：顺序消费与优化
- OrderlyConsumer实现
- 本地偏移量存储
- 广播模式支持
- 性能优化与测试

## 核心组件设计

### 1. ConsumerConfig - 配置管理

```python
from enum import Enum
from dataclasses import dataclass
from typing import Optional


# 消费起始位置枚举
class ConsumeFromWhere:
    """消费起始位置"""

    CONSUME_FROM_LAST_OFFSET: str = "CONSUME_FROM_LAST_OFFSET"  # 从最后偏移量开始消费
    CONSUME_FROM_FIRST_OFFSET: str = (
        "CONSUME_FROM_FIRST_OFFSET"  # 从第一个偏移量开始消费
    )
    CONSUME_FROM_TIMESTAMP: str = "CONSUME_FROM_TIMESTAMP"  # 从指定时间戳开始消费

class AllocateQueueStrategy(Enum):
    """队列负载均衡策略"""
    AVERAGE = "AVG"
    HASH = "HASH"
    CONFIGURATION = "CONFIGURATION"
    MACHINE_ROOM = "MACHINE_ROOM"

@dataclass
class ConsumerConfig:
    """Consumer配置管理"""
    # 基础配置
    consumer_group: str
    namesrv_addr: str
    message_model: MessageModel = MessageModel.CLUSTERING

    # 线程配置
    consume_thread_min: int = 20
    consume_thread_max: int = 64

    # 拉取配置
    pull_batch_size: int = 32
    pull_interval: int = 0  # 0表示持续拉取
    consume_timeout: int = 15
    max_reconsume_times: int = 16

    # 消费起始位置
    consume_from_where: ConsumeFromWhere = ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET
    consume_timestamp: int = 0

    # 负载均衡
    allocate_queue_strategy: AllocateQueueStrategy = AllocateQueueStrategy.AVERAGE

    # OffsetStore配置
    persist_interval: int = 5000  # 毫秒
    persist_batch_size: int = 10
    offset_store_path: str = "~/.rocketmq/offsets"
    max_cache_size: int = 10000
    enable_auto_recovery: bool = True
    max_retry_times: int = 3
```

### 2. MessageListener - 消息处理接口

```python
from enum import Enum
from typing import List
from pyrocketmq.model.message import MessageExt

class ConsumeResult(Enum):
    """消费结果"""
    SUCCESS = "CONSUME_SUCCESS"
    RECONSUME_LATER = "RECONSUME_LATER"
    COMMIT = "COMMIT"
    ROLLBACK = "ROLLBACK"

class MessageListener(ABC):
    """消息监听器接口"""

    @abstractmethod
    def consume_message(self, messages: List[MessageExt]) -> ConsumeResult:
        """消费消息"""
        pass
```

### 3. BaseConsumer - 抽象基类

```python
from abc import ABC, abstractmethod

class BaseConsumer(ABC):
    """Consumer基类，定义通用接口"""

    def __init__(self, config: ConsumerConfig):
        self.config = config
        self.subscription_manager = SubscriptionManager()
        self.running = False
        self.message_listener: Optional[MessageListener] = None

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

    def register_message_listener(self, listener: MessageListener) -> None:
        """注册消息监听器"""
        self.message_listener = listener
```

## 依赖关系

### 复用现有组件
- **Model层**: Message, MessageExt, MessageQueue, MessageSelector
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
- **asyncio**: 异步操作支持

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

## 消费起始位置管理

### ConsumeFromWhere策略实现
```python
class ConsumeFromWhereManager:
    """管理消费起始位置的具体实现"""

    def get_consume_offset(self, queue: MessageQueue, strategy: ConsumeFromWhere,
                          timestamp: int = 0) -> int:
        """根据策略获取消费起始偏移量"""
        if strategy == ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET:
            return self.get_max_offset(queue)
        elif strategy == ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET:
            return self.get_min_offset(queue)
        elif strategy == ConsumeFromWhere.CONSUME_FROM_TIMESTAMP:
            return self.get_offset_by_timestamp(queue, timestamp)
```

## 订阅关系与消息过滤

### MessageSelector设计详解 ✅ **已实现**

MessageSelector已经在 `src/pyrocketmq/model/client_data.py` 中完整实现，支持与Go语言RocketMQ客户端完全兼容的消息选择器功能：

**实现位置**: `src/pyrocketmq/model/client_data.py:32-227`

**核心功能**:
- 支持TAG和SQL92两种表达式类型（TAG推荐，SQL92已废弃）
- 提供便利工厂方法：`by_tag()`, `by_sql()`
- 完整的验证和序列化支持
- 类型安全的枚举定义

**使用示例**:

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

#### 便利函数支持 ✅ **已实现**
```python
# 向后兼容的字符串接口
consumer.subscribe("order_topic", "*")  # 自动转换为TAG选择器
consumer.subscribe("order_topic", "tag1 || tag2")  # 自动转换为TAG选择器

# 全局便利函数 (client_data.py:230-298)
def is_tag_type(expression: str) -> bool:
    """判断表达式是否为TAG类型"""
    return not expression or expression.upper() == "TAG"

def create_tag_selector(expression: str) -> MessageSelector:
    """创建TAG类型选择器"""
    return MessageSelector.by_tag(expression)

def create_sql_selector(expression: str) -> MessageSelector:
    """创建SQL类型选择器"""
    return MessageSelector.by_sql(expression)
```

## SubscriptionManager 订阅关系管理

### 概述

SubscriptionManager是RocketMQ Consumer的核心组件，负责管理Topic订阅关系、消息选择器和订阅数据，提供完整的订阅生命周期管理。

### 核心数据结构

#### SubscriptionEntry - 订阅条目
```python
@dataclass
class SubscriptionEntry:
    """订阅条目数据结构"""
    topic: str
    selector: MessageSelector
    subscription_data: SubscriptionData
    created_at: datetime
    updated_at: datetime
    is_active: bool = True

    def update_timestamp(self) -> None:
        self.updated_at = datetime.now()
```

#### SubscriptionConflict - 订阅冲突
```python
@dataclass
class SubscriptionConflict:
    """订阅冲突信息"""
    topic: str
    existing_selector: MessageSelector
    new_selector: MessageSelector
    conflict_type: str
    timestamp: datetime
```

### SubscriptionManager 核心实现
```python
class SubscriptionManager:
    """订阅关系管理器"""

    def __init__(self, max_subscriptions: int = 1000):
        """
        初始化订阅管理器

        Args:
            max_subscriptions: 最大订阅数量限制
        """
        self._subscriptions: Dict[str, SubscriptionEntry] = {}
        self._lock = RLock()                          # 可重入锁，保证线程安全
        self._max_subscriptions = max_subscriptions
        self._conflict_history: List[SubscriptionConflict] = []
        self._metrics = SubscriptionMetrics()

    # ==================== 核心订阅操作 ====================

    def subscribe(self, topic: str, selector: MessageSelector) -> bool:
        """
        订阅Topic

        Args:
            topic: Topic名称
            selector: 消息选择器

        Returns:
            bool: 订阅是否成功

        Raises:
            SubscriptionError: 订阅失败时抛出
            SubscriptionLimitExceededError: 超过订阅限制时抛出
        """
        with self._lock:
            # 验证参数
            if not self._validate_topic(topic):
                raise InvalidTopicError(f"Invalid topic: {topic}")

            if not self._validate_selector(selector):
                raise InvalidSelectorError(f"Invalid selector: {selector}")

            # 检查订阅数量限制
            if len(self._subscriptions) >= self._max_subscriptions:
                raise SubscriptionLimitExceededError(
                    f"Subscription limit exceeded: {self._max_subscriptions}"
                )

            # 检查是否存在冲突
            conflict = self._check_subscription_conflict(topic, selector)
            if conflict:
                self._conflict_history.append(conflict)
                self._metrics.record_conflict(conflict.conflict_type)
                raise SubscriptionConflictError(f"Subscription conflict for topic {topic}: {conflict}")

            # 创建或更新订阅
            now = datetime.now()
            subscription_data = SubscriptionData.from_selector(topic, selector)

            if topic in self._subscriptions:
                # 更新现有订阅
                existing = self._subscriptions[topic]
                existing.selector = selector
                existing.subscription_data = subscription_data
                existing.updated_at = now
                existing.is_active = True
                self._metrics.record_subscription_updated(topic)
            else:
                # 创建新订阅
                entry = SubscriptionEntry(
                    topic=topic,
                    selector=selector,
                    subscription_data=subscription_data,
                    created_at=now,
                    updated_at=now
                )
                self._subscriptions[topic] = entry
                self._metrics.record_subscription_added(topic)

            return True

    def unsubscribe(self, topic: str) -> bool:
        """
        取消订阅Topic

        Args:
            topic: Topic名称

        Returns:
            bool: 取消订阅是否成功
        """
        with self._lock:
            if topic in self._subscriptions:
                del self._subscriptions[topic]
                self._metrics.record_subscription_removed(topic)
                return True
            return False

    def update_selector(self, topic: str, new_selector: MessageSelector) -> bool:
        """
        更新消息选择器

        Args:
            topic: Topic名称
            new_selector: 新的消息选择器

        Returns:
            bool: 更新是否成功
        """
        with self._lock:
            if topic not in self._subscriptions:
                raise TopicNotSubscribedError(f"Topic not subscribed: {topic}")

            # 检查冲突
            conflict = self._check_subscription_conflict(topic, new_selector)
            if conflict:
                self._conflict_history.append(conflict)
                raise SubscriptionConflictError(f"Selector update conflict for topic {topic}: {conflict}")

            # 更新选择器
            entry = self._subscriptions[topic]
            entry.selector = new_selector
            entry.subscription_data = SubscriptionData.from_selector(topic, new_selector)
            entry.update_timestamp()

            self._metrics.record_selector_updated(topic)
            return True

    # ==================== 查询操作 ====================

    def get_subscription(self, topic: str) -> Optional[SubscriptionEntry]:
        """
        获取Topic订阅信息

        Args:
            topic: Topic名称

        Returns:
            Optional[SubscriptionEntry]: 订阅条目，如果不存在则返回None
        """
        with self._lock:
            return self._subscriptions.get(topic)

    def get_all_subscriptions(self) -> List[SubscriptionEntry]:
        """
        获取所有订阅信息

        Returns:
            List[SubscriptionEntry]: 所有订阅条目的列表
        """
        with self._lock:
            return list(self._subscriptions.values())

    def get_active_subscriptions(self) -> List[SubscriptionEntry]:
        """
        获取所有活跃订阅

        Returns:
            List[SubscriptionEntry]: 活跃订阅条目的列表
        """
        with self._lock:
            return [entry for entry in self._subscriptions.values() if entry.is_active]

    def is_subscribed(self, topic: str) -> bool:
        """
        检查是否已订阅Topic

        Args:
            topic: Topic名称

        Returns:
            bool: 是否已订阅
        """
        with self._lock:
            return topic in self._subscriptions

    def get_subscription_count(self) -> int:
        """
        获取订阅数量

        Returns:
            int: 当前订阅数量
        """
        with self._lock:
            return len(self._subscriptions)

    # ==================== 数据转换操作 ====================

    def to_subscription_data_list(self) -> List[SubscriptionData]:
        """
        转换为SubscriptionData列表

        Returns:
            List[SubscriptionData]: 订阅数据列表
        """
        with self._lock:
            return [entry.subscription_data for entry in self._subscriptions.values()]

    def from_subscription_data_list(self, subscription_data_list: List[SubscriptionData]) -> None:
        """
        从SubscriptionData列表恢复订阅关系

        Args:
            subscription_data_list: 订阅数据列表
        """
        with self._lock:
            self.clear_all()

            for sub_data in subscription_data_list:
                selector = MessageSelector.from_dict({
                    'type': sub_data.expression_type,
                    'expression': sub_data.sub_string
                })

                entry = SubscriptionEntry(
                    topic=sub_data.topic,
                    selector=selector,
                    subscription_data=sub_data,
                    created_at=datetime.now(),
                    updated_at=datetime.now()
                )

                self._subscriptions[sub_data.topic] = entry

    def export_subscriptions(self) -> Dict[str, Any]:
        """
        导出订阅关系

        Returns:
            Dict[str, Any]: 导出的订阅数据
        """
        with self._lock:
            return {
                'subscriptions': [entry.to_dict() for entry in self._subscriptions.values()],
                'conflict_history': [conflict.__dict__ for conflict in self._conflict_history],
                'export_time': datetime.now().isoformat(),
                'metrics': self._metrics.to_dict()
            }

    def import_subscriptions(self, data: Dict[str, Any]) -> None:
        """
        导入订阅关系

        Args:
            data: 导入的订阅数据
        """
        with self._lock:
            self.clear_all()

            for sub_data in data.get('subscriptions', []):
                selector = MessageSelector.from_dict(sub_data['selector'])
                subscription_data = SubscriptionData.from_dict(sub_data['subscription_data'])

                entry = SubscriptionEntry(
                    topic=sub_data['topic'],
                    selector=selector,
                    subscription_data=subscription_data,
                    created_at=datetime.fromisoformat(sub_data['created_at']),
                    updated_at=datetime.fromisoformat(sub_data['updated_at']),
                    is_active=sub_data.get('is_active', True)
                )

                self._subscriptions[entry.topic] = entry

            # 导入冲突历史
            for conflict_data in data.get('conflict_history', []):
                conflict = SubscriptionConflict(
                    topic=conflict_data['topic'],
                    existing_selector=MessageSelector.from_dict(conflict_data['existing_selector']),
                    new_selector=MessageSelector.from_dict(conflict_data['new_selector']),
                    conflict_type=conflict_data['conflict_type'],
                    timestamp=datetime.fromisoformat(conflict_data['timestamp'])
                )
                self._conflict_history.append(conflict)

    # ==================== 管理操作 ====================

    def clear_all(self) -> None:
        """清除所有订阅关系"""
        with self._lock:
            self._subscriptions.clear()
            self._metrics.record_all_cleared()

    def deactivate_subscription(self, topic: str) -> bool:
        """
        停用订阅（不删除，只是标记为非活跃）

        Args:
            topic: Topic名称

        Returns:
            bool: 操作是否成功
        """
        with self._lock:
            if topic in self._subscriptions:
                self._subscriptions[topic].is_active = False
                self._metrics.record_subscription_deactivated(topic)
                return True
            return False

    def activate_subscription(self, topic: str) -> bool:
        """
        激活订阅

        Args:
            topic: Topic名称

        Returns:
            bool: 操作是否成功
        """
        with self._lock:
            if topic in self._subscriptions:
                self._subscriptions[topic].is_active = True
                self._metrics.record_subscription_activated(topic)
                return True
            return False

    def cleanup_inactive_subscriptions(self, inactive_threshold: timedelta = timedelta(hours=24)) -> int:
        """
        清理长时间非活跃的订阅

        Args:
            inactive_threshold: 非活跃时间阈值

        Returns:
            int: 清理的订阅数量
        """
        with self._lock:
            cutoff_time = datetime.now() - inactive_threshold
            to_remove = []

            for topic, entry in self._subscriptions.items():
                if not entry.is_active and entry.updated_at < cutoff_time:
                    to_remove.append(topic)

            for topic in to_remove:
                del self._subscriptions[topic]

            if to_remove:
                self._metrics.record_subscriptions_cleaned(len(to_remove))

            return len(to_remove)

    # ==================== 验证和冲突检测 ====================

    def validate_subscription(self, topic: str, selector: MessageSelector) -> bool:
        """
        验证订阅是否有效

        Args:
            topic: Topic名称
            selector: 消息选择器

        Returns:
            bool: 验证是否通过
        """
        return self._validate_topic(topic) and self._validate_selector(selector)

    def get_conflict_history(self, limit: int = 100) -> List[SubscriptionConflict]:
        """
        获取冲突历史

        Args:
            limit: 返回的冲突数量限制

        Returns:
            List[SubscriptionConflict]: 冲突历史列表
        """
        with self._lock:
            return self._conflict_history[-limit:]

    def clear_conflict_history(self) -> None:
        """清除冲突历史"""
        with self._lock:
            self._conflict_history.clear()

    # ==================== 私有方法 ====================

    def _validate_topic(self, topic: str) -> bool:
        """验证Topic名称"""
        if not topic or not isinstance(topic, str):
            return False

        # Topic名称验证规则
        return (len(topic) > 0 and
                len(topic) <= 127 and
                topic not in ('', 'DEFAULT_TOPIC') and
                not topic.startswith('SYS_'))

    def _validate_selector(self, selector: MessageSelector) -> bool:
        """验证消息选择器"""
        if not isinstance(selector, MessageSelector):
            return False

        try:
            selector.validate()
            return True
        except Exception:
            return False

    def _check_subscription_conflict(self, topic: str, new_selector: MessageSelector) -> Optional[SubscriptionConflict]:
        """检查订阅冲突"""
        if topic not in self._subscriptions:
            return None

        existing = self._subscriptions[topic]

        # 检查选择器类型冲突
        if existing.selector.type != new_selector.type:
            return SubscriptionConflict(
                topic=topic,
                existing_selector=existing.selector,
                new_selector=new_selector,
                conflict_type="SELECTOR_TYPE_MISMATCH",
                timestamp=datetime.now()
            )

        # 检查表达式冲突（仅对相同类型进行）
        if (existing.selector.type == new_selector.type and
            existing.selector.expression != new_selector.expression):
            return SubscriptionConflict(
                topic=topic,
                existing_selector=existing.selector,
                new_selector=new_selector,
                conflict_type="EXPRESSION_MISMATCH",
                timestamp=datetime.now()
            )

        return None

    @property
    def metrics(self) -> 'SubscriptionMetrics':
        """获取订阅指标"""
        return self._metrics
```

### 5. 监控和指标

#### 5.1 SubscriptionMetrics 订阅指标
```python
@dataclass
class SubscriptionMetrics:
    """订阅管理指标"""

    # 计数器
    total_subscriptions: int = 0
    active_subscriptions: int = 0
    added_subscriptions: int = 0
    removed_subscriptions: int = 0
    updated_subscriptions: int = 0
    conflicts: int = 0
    deactivated_subscriptions: int = 0
    activated_subscriptions: int = 0
    cleaned_subscriptions: int = 0

    # 统计信息
    conflict_types: Dict[str, int] = field(default_factory=dict)
    topic_subscription_counts: Dict[str, int] = field(default_factory=dict)
    selector_type_counts: Dict[str, int] = field(default_factory=dict)

    def record_subscription_added(self, topic: str) -> None:
        """记录订阅添加"""
        self.added_subscriptions += 1
        self._update_topic_count(topic)

    def record_subscription_removed(self, topic: str) -> None:
        """记录订阅移除"""
        self.removed_subscriptions += 1
        self._update_topic_count(topic, -1)

    def record_subscription_updated(self, topic: str) -> None:
        """记录订阅更新"""
        self.updated_subscriptions += 1

    def record_conflict(self, conflict_type: str) -> None:
        """记录冲突"""
        self.conflicts += 1
        self.conflict_types[conflict_type] = self.conflict_types.get(conflict_type, 0) + 1

    def record_selector_updated(self, topic: str) -> None:
        """记录选择器更新"""
        self.updated_subscriptions += 1

    def record_subscription_deactivated(self, topic: str) -> None:
        """记录订阅停用"""
        self.deactivated_subscriptions += 1

    def record_subscription_activated(self, topic: str) -> None:
        """记录订阅激活"""
        self.activated_subscriptions += 1

    def record_subscriptions_cleaned(self, count: int) -> None:
        """记录订阅清理"""
        self.cleaned_subscriptions += count

    def record_all_cleared(self) -> None:
        """记录全部清除"""
        self.topic_subscription_counts.clear()
        self.selector_type_counts.clear()

    def _update_topic_count(self, topic: str, delta: int = 1) -> None:
        """更新Topic订阅计数"""
        self.topic_subscription_counts[topic] = self.topic_subscription_counts.get(topic, 0) + delta
        if self.topic_subscription_counts[topic] <= 0:
            del self.topic_subscription_counts[topic]

    def update_current_stats(self, subscriptions: List[SubscriptionEntry]) -> None:
        """更新当前统计信息"""
        self.total_subscriptions = len(subscriptions)
        self.active_subscriptions = len([s for s in subscriptions if s.is_active])

        # 统计选择器类型
        self.selector_type_counts.clear()
        for entry in subscriptions:
            selector_type = entry.selector.type.value
            self.selector_type_counts[selector_type] = self.selector_type_counts.get(selector_type, 0) + 1

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            'total_subscriptions': self.total_subscriptions,
            'active_subscriptions': self.active_subscriptions,
            'added_subscriptions': self.added_subscriptions,
            'removed_subscriptions': self.removed_subscriptions,
            'updated_subscriptions': self.updated_subscriptions,
            'conflicts': self.conflicts,
            'deactivated_subscriptions': self.deactivated_subscriptions,
            'activated_subscriptions': self.activated_subscriptions,
            'cleaned_subscriptions': self.cleaned_subscriptions,
            'conflict_types': self.conflict_types.copy(),
            'topic_subscription_counts': self.topic_subscription_counts.copy(),
            'selector_type_counts': self.selector_type_counts.copy()
        }
```

### 6. 异常定义

```python
class SubscriptionError(Exception):
    """订阅相关异常基类"""
    pass

class InvalidTopicError(SubscriptionError):
    """无效Topic异常"""
    pass

class InvalidSelectorError(SubscriptionError):
    """无效选择器异常"""
    pass

class TopicNotSubscribedError(SubscriptionError):
    """Topic未订阅异常"""
    pass

class SubscriptionConflictError(SubscriptionError):
    """订阅冲突异常"""
    pass

class SubscriptionLimitExceededError(SubscriptionError):
    """订阅数量超限异常"""
    pass
```

### 7. 使用示例

```python
# 创建订阅管理器
subscription_manager = SubscriptionManager(max_subscriptions=500)

# 订阅Topic
try:
    selector = MessageSelector.by_tag("order || payment")
    subscription_manager.subscribe("order_topic", selector)
    print("订阅成功")
except SubscriptionError as e:
    print(f"订阅失败: {e}")

# 查询订阅
if subscription_manager.is_subscribed("order_topic"):
    entry = subscription_manager.get_subscription("order_topic")
    print(f"Topic: {entry.topic}, Selector: {entry.selector}")

# 更新选择器
new_selector = MessageSelector.by_tag("order || payment || refund")
subscription_manager.update_selector("order_topic", new_selector)

# 获取所有订阅
all_subs = subscription_manager.get_all_subscriptions()
print(f"当前订阅数量: {len(all_subs)}")

# 导出订阅关系
exported_data = subscription_manager.export_subscriptions()

# 获取指标
metrics = subscription_manager.metrics
print(f"总订阅数: {metrics.total_subscriptions}")
print(f"活跃订阅数: {metrics.active_subscriptions}")
print(f"冲突数: {metrics.conflicts}")
```

### 8. SubscriptionManager设计要点总结

1. **线程安全**: 使用RLock确保多线程环境下的安全操作
2. **冲突检测**: 自动检测并记录订阅冲突，防止数据不一致
3. **指标监控**: 提供详细的订阅指标和统计信息
4. **数据持久化**: 支持订阅关系的导出和导入
5. **生命周期管理**: 支持订阅的激活、停用和清理
6. **异常处理**: 完整的异常体系，便于错误处理和调试
7. **性能优化**: 合理的数据结构和缓存策略
8. **扩展性**: 模块化设计，便于功能扩展

### 9. 测试策略

- **单元测试**: 测试所有核心方法的正确性
- **并发测试**: 验证多线程环境下的线程安全性
- **冲突测试**: 验证冲突检测和处理逻辑
- **性能测试**: 测试大量订阅场景下的性能表现
- **持久化测试**: 验证数据导出导入的正确性

### 10. 文档要求

- 完整的API文档和使用示例
- 配置参数说明和最佳实践
- 异常处理指南和故障排查
- 性能调优建议和监控指标

## OffsetStore 偏移量存储

### 概述

OffsetStore负责管理和持久化消息消费偏移量，确保Consumer能够记录每个队列的消费进度，并在重启后恢复消费位置。

### 存储策略

#### ReadOffsetType - 读取策略
```python
from enum import Enum

class ReadOffsetType(Enum):
    """偏移量读取类型"""
    MEMORY_FIRST_THEN_STORE = "MEMORY_FIRST_THEN_STORE"  # 优先从内存读取
    READ_FROM_MEMORY = "READ_FROM_MEMORY"                 # 仅从内存读取
    READ_FROM_STORE = "READ_FROM_STORE"                   # 仅从存储读取
```

#### OffsetEntry - 偏移量条目
```python
@dataclass
class OffsetEntry:
    """偏移量条目"""
    queue: MessageQueue
    offset: int
    last_update_timestamp: int
```

#### OffsetStore - 抽象基类
```python
from abc import ABC, abstractmethod

class OffsetStore(ABC):
    """偏移量存储抽象基类"""

    @abstractmethod
    def load(self) -> None:
        """加载偏移量数据"""
        pass

    @abstractmethod
    def update_offset(self, queue: MessageQueue, offset: int) -> None:
        """更新偏移量"""
        pass

    @abstractmethod
    def persist(self, queue: MessageQueue) -> None:
        """持久化单个队列的偏移量"""
        pass

    @abstractmethod
    def persist_all(self) -> None:
        """持久化所有偏移量"""
        pass

    @abstractmethod
    def read_offset(self, queue: MessageQueue, read_type: ReadOffsetType) -> int:
        """读取偏移量"""
        pass
```

### 实现模式

#### RemoteOffsetStore - 集群模式
- 偏移量存储在Broker服务器上
- 支持多Consumer实例协同消费
- 使用本地缓存减少网络请求
- 批量提交提升性能

#### LocalOffsetStore - 广播模式
- 偏移量存储在本地文件中
- 每个Consumer实例独立消费所有消息
- 文件格式：topic@brokerId:queueId=offset
- 线程安全的文件读写操作

### 配置管理

OffsetStore相关配置已整合到ConsumerConfig中：
- **persist_interval**: 定期持久化间隔(毫秒)
- **persist_batch_size**: 批量提交大小
- **offset_store_path**: 本地存储路径
- **max_cache_size**: 最大缓存条目数
- **enable_auto_recovery**: 启用自动恢复
- **max_retry_times**: 最大重试次数

### 性能优化

1. **批量操作**: 多个偏移量更新合并为一次请求
2. **异步写入**: 使用异步IO避免阻塞消费线程
3. **本地缓存**: 减少网络请求频率
4. **连接复用**: 复用Broker连接减少握手开销

---

## 总结

本文档详细规划了pyrocketmq Consumer MVP的实施方案，采用模块化设计，分离并发和顺序消费实现，确保代码清晰性和可维护性。

### 关键特性

- **MVP优先**: 专注核心功能，逐步迭代
- **分离设计**: 并发和顺序消费独立实现
- **复用架构**: 充分利用现有基础设施
- **完善配置**: 灵活的配置管理和性能调优

### 实施计划

4周分阶段实施，从基础架构到完整功能，确保每个阶段都有可验证的交付物。

---

**文档版本**: MVP 1.0
**创建时间**: 2025-01-04
**预计完成时间**: 4周
