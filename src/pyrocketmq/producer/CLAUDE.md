# Producer模块

## 模块概述

Producer模块是pyrocketmq的消息生产者实现，提供完整的消息发送、路由管理、故障处理和性能监控功能。该模块采用分层架构设计，从配置管理到消息路由，为上层应用提供简单易用的消息发送接口。

### 核心功能
- **智能路由**: 支持多种路由策略（轮询、随机、消息哈希）
- **故障感知**: 自动检测和规避故障Broker
- **性能监控**: 实时统计发送延迟和成功率
- **灵活配置**: 支持多种环境配置模板
- **异步支持**: 提供完整的异步操作接口

## 模块结构

```
producer/
├── config.py              # 配置管理
├── topic_broker_mapping.py # Topic-Broker映射管理
├── queue_selectors.py     # 队列选择策略
├── router.py              # 消息路由器
├── errors.py              # 异常定义
└── CLAUDE.md              # 本文档
```

## 核心数据结构

### 1. RouteInfo
路由信息包装类，包含Topic路由数据和预构建的队列列表。

```python
@dataclass
class RouteInfo:
    topic_route_data: TopicRouteData
    last_update_time: float
    available_queues: List[Tuple[MessageQueue, BrokerData]]  # 预构建队列列表
```

**设计亮点**:
- 预构建队列列表，避免每次路由时重新计算，提升性能
- 内置过期时间管理，支持路由信息自动刷新

### 2. BrokerHealthInfo
Broker健康状态管理，记录每个Broker的性能指标和故障信息。

```python
@dataclass
class BrokerHealthInfo:
    broker_data: BrokerData
    status: BrokerState
    success_count: int
    failure_count: int
    avg_latency: float
    consecutive_failures: int
```

**关键特性**:
- 自动故障检测：连续失败3次降级，5次标记不健康
- 自动恢复机制：连续成功5次恢复健康状态
- 性能监控：跟踪延迟、成功率等关键指标

### 3. RoutingResult
路由决策结果，包含选中的队列、Broker地址等信息。

```python
@dataclass
class RoutingResult:
    success: bool
    message_queue: Optional[MessageQueue]
    broker_data: Optional[BrokerData]
    broker_address: Optional[str]
    error: Optional[Exception]
    routing_strategy: Optional[RoutingStrategy]
```

## 核心组件

### 1. TopicBrokerMapping
Topic-Broker映射管理器，负责缓存和管理路由信息。

**核心职责**:
- 缓存Topic路由信息，避免频繁查询NameServer
- 预构建队列列表，提升路由性能
- 路由信息过期管理和自动清理

**关键方法**:
```python
def get_available_queues(self, topic: str) -> List[Tuple[MessageQueue, BrokerData]]
def update_route_info(self, topic: str, topic_route_data: TopicRouteData) -> bool
def clear_expired_routes(self, timeout: Optional[float] = None) -> int
```

**性能优化**:
- 使用`threading.RLock()`保证线程安全
- 预构建队列列表，避免运行时计算开销
- 路由信息过期时间默认30秒，可配置

### 2. MessageRouter
消息路由器，提供高级的路由决策功能。

**路由策略**:
- **ROUND_ROBIN**: 轮询策略，默认选择，保证负载均衡
- **RANDOM**: 随机策略，适合无状态负载均衡
- **MESSAGE_HASH**: 消息哈希策略，基于`SHARDING_KEY`或`KEYS`确保消息顺序性

**故障处理**:
- 基于健康状态的Broker选择
- 自动故障规避和恢复
- 延迟感知的路由优化

**核心方法**:
```python
def route_message(
    self,
    topic: str,
    message: Optional[Message] = None,
    strategy: Optional[RoutingStrategy] = None
) -> RoutingResult

def report_routing_result(self, result: RoutingResult, latency_ms: Optional[float] = None)
def report_routing_failure(self, broker_name: str, error: Exception)
```

### 3. QueueSelector族
队列选择器策略模式实现。

**同步选择器**:
- `RoundRobinSelector`: 维护每个Topic的计数器，实现轮询
- `RandomSelector`: 使用`random.choice()`随机选择
- `MessageHashSelector`: 优先使用`SHARDING_KEY`，其次使用`KEYS`的第一个值

**异步选择器**:
- 对应的异步版本，使用`asyncio.Lock()`和`run_in_executor()`

**消息属性优先级**:
1. `SHARDING_KEY`: 分片键，用于顺序性保证
2. `KEYS`: 消息键，多个键用空格分隔
3. 随机选择：当都没有时回退到随机选择

### 4. ProducerConfig
完整的Producer配置管理，支持环境变量和预定义模板。

**配置分类**:
- **基础配置**: producer_group, client_id, namesrv_addr
- **消息配置**: send_msg_timeout, retry_times, max_message_size
- **路由配置**: poll_name_server_interval, update_topic_route_info_interval
- **性能配置**: batch_size, async_send_semaphore, send_latency_enable

**预定义模板**:
```python
DEFAULT_CONFIG       # 默认配置
DEVELOPMENT_CONFIG   # 开发环境：启用调试和跟踪
PRODUCTION_CONFIG    # 生产环境：注重性能和稳定性
HIGH_PERFORMANCE_CONFIG # 高性能配置：优化吞吐量
TESTING_CONFIG       # 测试环境：简化配置
```

## 核心流程

### 1. 消息路由流程
```
MessageRouter.route_message()
    ↓
获取可用队列 (TopicBrokerMapping.get_available_queues)
    ↓
根据策略选择队列 (QueueSelector.select)
    ↓
选择Broker地址 (优先Master)
    ↓
返回RoutingResult
```

### 2. 故障处理流程
```
报告路由结果/失败
    ↓
更新BrokerHealthInfo
    ↓
判断故障状态:
- 连续失败3次 → DEGRADED
- 连续失败5次 → UNHEALTHY
- 连续成功5次 → HEALTHY
    ↓
影响后续路由选择
```

### 3. 路由信息更新流程
```
检查路由缓存 (TopicBrokerMapping.get_route_info)
    ↓
如果过期或不存在 → 触发更新
    ↓
更新路由信息 (TopicBrokerMapping.update_route_info)
    ↓
预构建队列列表 (RouteInfo.create_with_queues)
    ↓
更新缓存
```

## 设计模式

### 1. 策略模式 (Strategy Pattern)
**QueueSelector族**实现了不同的队列选择策略：
- `RoundRobinSelector`: 轮询策略
- `RandomSelector`: 随机策略
- `MessageHashSelector`: 哈希策略

**优势**: 易于扩展新的选择策略，运行时可动态切换。

### 2. 状态模式 (State Pattern)
**BrokerHealthInfo**中的状态管理：
- `HEALTHY`: 健康状态，正常使用
- `DEGRADED`: 降级状态，谨慎使用
- `UNHEALTHY`: 不健康状态，避免使用
- `SUSPENDED`: 暂停状态，完全不使用

**优势**: 清晰的状态转换逻辑，易于理解和维护。

### 3. 缓存模式 (Cache Pattern)
**TopicBrokerMapping**作为路由信息缓存：
- 内存缓存提升性能
- 过期时间管理保证数据新鲜度
- 预构建队列列表优化查询性能

## 性能优化

### 1. 预构建队列列表
`TopicBrokerMapping`在路由更新时预先构建所有可用队列列表，避免每次路由时重新计算。

### 2. 线程安全设计
- 使用`threading.RLock()`保证并发安全
- 细粒度锁减少锁竞争
- 统计信息独立锁管理

### 3. 内存管理
- 路由信息自动过期清理
- 延迟记录只保留最近100次
- 统计信息定期重置支持

## 错误处理

### 1. 异常体系
完整的异常类型定义，便于错误处理和问题排查：

```python
ProducerError (基类)
├── ProducerStartError          # 启动异常
├── ProducerStateError          # 状态异常
├── MessageSendError           # 发送异常
├── RouteNotFoundError         # 路由未找到
├── BrokerNotAvailableError    # Broker不可用
├── QueueNotAvailableError     # 队列不可用
└── TimeoutError               # 超时异常
```

### 2. 故障恢复
- 自动故障检测和状态转换
- 强制恢复API支持手动干预
- 详细的健康状态监控

## 使用示例

### 1. 基本使用
```python
from pyrocketmq.producer import TopicBrokerMapping, MessageRouter

# 创建映射管理器
mapping = TopicBrokerMapping()

# 创建路由器
router = MessageRouter(mapping)

# 路由消息
result = router.route_message("test_topic", message)
if result.success:
    print(f"Selected queue: {result.message_queue.full_name}")
    print(f"Broker address: {result.broker_address}")
```

### 2. 使用不同策略
```python
from pyrocketmq.producer.queue_selectors import MessageHashSelector
from pyrocketmq.producer.router import RoutingStrategy

# 使用消息哈希策略
result = router.route_message(
    "order_topic",
    order_message,
    RoutingStrategy.MESSAGE_HASH
)

# 确保相同订单ID的消息到同一队列
order_message.set_property("SHARDING_KEY", order_id)
```

### 3. 配置管理
```python
from pyrocketmq.producer.config import get_config, create_custom_config

# 使用预定义配置
config = get_config("production")

# 自定义配置
config = create_custom_config(
    producer_group="order_producer",
    namesrv_addr="192.168.1.100:9876",
    retry_times=3
)
```

## 监控和统计

### 1. 路由统计
```python
stats = router.get_routing_stats()
print(f"总路由次数: {stats['total_routing']}")
print(f"成功路由次数: {stats['successful_routing']}")
print(f"策略使用情况: {stats['strategy_usage']}")
```

### 2. Broker健康状态
```python
stats = router.get_routing_stats()
for broker, health in stats['broker_health'].items():
    print(f"Broker {broker}: {health['status']}, "
          f"成功率: {health['success_rate']:.2%}, "
          f"平均延迟: {health['avg_latency']:.2f}ms")
```

## 最佳实践

### 1. 路由策略选择
- **默认场景**: 使用`ROUND_ROBIN`，保证负载均衡
- **顺序消息**: 使用`MESSAGE_HASH`，设置合适的`SHARDING_KEY`
- **高性能场景**: 使用`RANDOM`，减少计算开销

### 2. 消息属性设置
```python
# 顺序性消息
message.set_property("SHARDING_KEY", user_id)

# 消息跟踪
message.set_keys(order_id, payment_id)

# 消息过滤
message.set_tags("priority_high")
```

### 3. 性能调优
- 合理设置`route_timeout`，默认30秒适合大多数场景
- 监控Broker健康状态，及时处理故障节点
- 根据业务特点选择合适的队列选择策略

## 未来扩展

1. **更多路由策略**: 支持基于延迟、负载的动态路由
2. **路由预热**: 启动时预加载常用Topic路由信息
3. **分布式协调**: 支持多Producer实例的路由协调
4. **更多监控指标**: 增加JVM、网络层面的监控
