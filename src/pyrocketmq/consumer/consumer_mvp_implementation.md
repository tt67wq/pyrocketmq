# Consumer MVP 实施方案

## 项目概述

基于现有的pyrocketmq基础设施，实现一个最小可行产品(MVP)版本的RocketMQ Consumer。该Consumer将实现基本的消息消费功能，为后续扩展奠定基础。

## 技术选型

### 1. 架构模式
- **MVP优先**: 从最简实现开始，逐步迭代
- **分层设计**: 复用现有的分层架构模式
- **同步优先**: 先实现同步消费，后续扩展异步

### 2. 核心技术栈
- **网络通信**: 复用现有的Transport/Remote层
- **协议解析**: 复用现有的Model层
- **路由发现**: 复用现有的NameServer/Broker客户端
- **并发模型**: 使用threading + queue实现基本并发
- **消息处理**: 采用callback模式处理消息

### 3. 设计原则
- **简化状态管理**: 使用布尔状态而非复杂状态机
- **模块化设计**: 清晰的模块边界和职责分工
- **可扩展性**: 预留扩展点，便于后续功能增强
- **错误处理**: 完善的异常处理和恢复机制

## 功能拆分

### Phase 1: 核心数据结构 (MVP必需)
1. **ConsumerConfig** - Consumer配置管理
2. **MessageListener** - 消息处理回调接口
3. **MessageSelector** - 消息选择器数据结构(与Go语言兼容)
4. **SubscriptionInfo** - 订阅关系数据结构

### Phase 2: 基础Consumer实现 (MVP核心)
1. **Consumer** - 主Consumer类
   - 生命周期管理(start/shutdown)
   - 订阅管理(subscribe/unsubscribe)
   - 基本的消息拉取循环
2. **MessageProcessor** - 消息处理器
   - 消息反序列化
   - 回调调用
   - 消费结果处理

### Phase 3: 路由和队列管理 (MVP核心)
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

### Phase 4: 高级功能 (后续扩展)
1. **AsyncConsumer** - 异步Consumer
2. **OrderlyConsumer** - 顺序消费
3. **TransactionConsumer** - 事务消费
4. **BroadcastConsumer** - 广播消费

## 实现里程碑

### 🎯 Milestone 1: MVP Consumer框架 (Week 1)
**目标**: 建立基本的Consumer框架和配置管理

**交付物**:
- [ ] `src/pyrocketmq/consumer/config.py` - Consumer配置类
- [ ] `src/pyrocketmq/consumer/listener.py` - 消息监听器接口
- [ ] `src/pyrocketmq/consumer/subscription.py` - 消息选择器和订阅关系数据结构
- [ ] `src/pyrocketmq/consumer/errors.py` - Consumer异常定义
- [ ] `src/pyrocketmq/consumer/consumer.py` - 基础Consumer类框架
- [ ] `src/pyrocketmq/consumer/__init__.py` - 模块初始化

**验收标准**:
- Consumer能够创建和启动
- 支持基本的配置管理
- 具备完善的错误处理体系

### 🎯 Milestone 2: 消息拉取与处理 (Week 2)
**目标**: 实现基本的消息拉取和处理流程

**交付物**:
- [ ] 消息拉取循环实现
- [ ] 消息处理器和回调机制
- [ ] 基本的偏移量管理
- [ ] 与Broker客户端的集成

**验收标准**:
- 能够从指定Topic拉取消息
- 消息能够正确反序列化并调用回调
- 支持手动和自动确认机制

### 🎯 Milestone 3: 重平衡与队列管理 (Week 3)
**目标**: 实现智能队列分配和重平衡机制

**交付物**:
- [ ] 重平衡实现器(RebalanceImpl)
- [ ] 多种队列分配策略实现(AVG/HASH/CONFIGURATION/MACHINE_ROOM)
- [ ] 消费者发现和注册机制
- [ ] 心跳和状态上报
- [ ] 消费起始位置管理(ConsumeFromWhere)

**验收标准**:
- 支持多消费者场景下的队列自动分配
- 消费者加入/退出时能够自动重平衡
- 支持Broker主从切换的处理
- 支持灵活的消费起始位置配置
- 支持多种负载均衡策略选择

### 🎯 Milestone 4: 测试与文档 (Week 4)
**目标**: 完善测试覆盖和使用文档

**交付物**:
- [ ] 单元测试套件
- [ ] 集成测试示例
- [ ] 使用文档和示例代码
- [ ] 性能基准测试

**验收标准**:
- 测试覆盖率达到80%以上
- 提供完整的使用示例
- 性能满足基本要求

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
    
    # 新增配置项
    consume_from_where: ConsumeFromWhere = ConsumeFromWhere.LAST_OFFSET  # 消费起始位置
    consume_timestamp: int = 0  # 当consume_from_where=TIMESTAMP时使用
    allocate_queue_strategy: AllocateQueueStrategy = AllocateQueueStrategy.AVERAGE  # 负载均衡策略
    pull_interval: int = 0     # 拉取间隔(毫秒)，0表示持续拉取
    pull_threshold_for_all: int = 50000  # 所有队列消息数阈值
    pull_threshold_for_topic: int = 10000  # 单个topic消息数阈值
    pull_threshold_size_for_topic: int = 100  # 单个topic消息大小阈值(MB)
    pull_threshold_of_queue: int = 1000  # 单个队列消息数阈值
    pull_threshold_size_of_queue: int = 100  # 单个队列消息大小阈值(MB)
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
```python
from enum import Enum
from dataclasses import dataclass

class ExpressionType(Enum):
    """表达式类型枚举 - 与Go语言实现保持一致"""
    TAG = "TAG"          # 标签过滤表达式
    SQL92 = "SQL92"      # SQL92过滤表达式(已废弃)

@dataclass
class MessageSelector:
    """消息选择器 - 与Go语言MessageSelector完全对应"""
    type: ExpressionType  # 表达式类型
    expression: str       # 过滤表达式
    
    @classmethod
    def by_tag(cls, tag_expression: str) -> "MessageSelector":
        """创建基于标签的消息选择器"""
        
    @classmethod
    def by_sql(cls, sql_expression: str) -> "MessageSelector":
        """创建基于SQL92的消息选择器(已废弃)"""
```

### 4. Consumer核心类
```python
class Consumer:
    def __init__(self, config: ConsumerConfig) -> None
    def start(self) -> None
    def shutdown(self) -> None
    def subscribe(self, topic: str, selector: MessageSelector) -> None
    def unsubscribe(self, topic: str) -> None
    def register_message_listener(self, listener: MessageListener) -> None
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

## 风险评估与缓解

### 技术风险
1. **复杂度风险**: Consumer逻辑复杂，可能过度设计
   - **缓解**: 严格遵循MVP原则，从简到繁
   
2. **并发风险**: 多线程消费可能导致竞态条件
   - **缓解**: 使用线程安全的数据结构和锁机制

3. **状态管理风险**: 消费状态管理复杂
   - **缓解**: 采用简化的状态模型，避免过度抽象

### 业务风险
1. **兼容性风险**: 与RocketMQ协议不兼容
   - **缓解**: 基于现有Producer验证过的协议实现

2. **性能风险**: MVP版本性能可能不满足要求
   - **缓解**: 预留性能优化点，逐步改进

## 质量保证

### 代码质量
- 遵循现有代码风格和命名规范
- 完整的类型注解和文档字符串
- 单一职责原则和模块化设计

### 测试策略
- 单元测试: 覆盖核心逻辑
- 集成测试: 验证与现有组件的协作
- 端到端测试: 完整消费流程验证

### 性能指标
- 消息拉取延迟: < 100ms
- 消息处理吞吐: > 1000 msg/s
- 内存使用: < 100MB

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

## 后续扩展计划

### 短期扩展 (1-2个月)
- 异步Consumer实现
- 顺序消费支持
- 广播消费模式
- 高级消息过滤功能(正则表达式等)
- 更多的负载均衡策略(轮询、权重等)

### 中期扩展 (3-6个月)
- 事务消息消费
- 延时消息消费
- 死信队列处理
- 监控和指标收集

### 长期扩展 (6个月+)
- 分布式协调
- 故障恢复机制
- 性能优化
- 企业级特性

---

## 实施建议

### 开发顺序
1. 先实现配置、异常和订阅关系数据结构
2. 再实现基础的Consumer框架
3. 然后添加消息拉取和处理
4. 最后实现重平衡和高级功能

### 订阅关系设计要点
- **Go语言兼容**: MessageSelector结构与Go实现完全一致
- **类型安全**: 使用强类型替代字符串，提供更好的IDE支持
- **向后兼容**: 支持字符串重载版本，降低使用门槛
- **便利函数**: 提供TAG和SQL92选择器的创建函数
- **默认行为**: 空字符串或"*"默认为TAG类型，订阅所有消息

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