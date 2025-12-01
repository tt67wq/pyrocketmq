# Producer层消息生产者

**模块概述**: Producer模块是pyrocketmq的高级消息生产者实现，提供完整高效的消息发送、路由管理和故障处理功能。经过架构优化，移除了冗余组件，专注于核心功能实现。

## 核心组件

### Producer核心实现
- **同步Producer**: MVP版本，采用简化的布尔状态管理，专注核心功能
- **异步Producer**: 基于asyncio实现，支持高并发消息发送
- **TransactionProducer**: 完整的事务消息Producer实现
- **AsyncTransactionProducer**: 异步事务消息Producer实现

### MessageRouter智能路由
- **多路由策略**: 支持轮询(RoundRobin)、随机(Random)、消息哈希(MessageHash)三种策略
- **故障感知**: 实时监控Broker健康状态，自动规避故障节点
- **性能监控**: 跟踪延迟、成功率等指标，支持延迟感知优化

### TopicBrokerMapping路由缓存
- **路由信息管理**: 缓存Topic到Broker的映射关系，支持过期管理
- **预构建优化**: 在路由更新时预先构建所有可用队列列表，显著提升性能
- **线程安全**: 使用RLock确保并发访问的安全性

### QueueSelector队列选择器
**同步选择器**: 
- **RoundRobinSelector**: 轮询选择队列，保证负载均衡
- **RandomSelector**: 随机选择队列，适用于无状态消息
- **MessageHashSelector**: 基于消息哈希选择队列，保证同一消息路由到同一队列

**异步选择器**: 
- **AsyncRoundRobinSelector**: 异步轮询选择器
- **AsyncRandomSelector**: 异步随机选择器
- **AsyncMessageHashSelector**: 异步消息哈希选择器

### ProducerConfig配置管理
- **完整配置支持**: 涵盖所有RocketMQ Producer配置参数
- **环境变量支持**: 支持从环境变量加载配置，便于容器化部署
- **预定义配置模板**: 提供开发、生产、高性能等环境的预设配置

## 关键特性

### 发送模式
- **同步/异步**: 支持同步阻塞和异步非阻塞发送
- **普通/批量**: 支持单条和批量消息发送
- **可靠/单向**: 支持需要确认和高性能单向发送
- **事务消息**: 支持完整的事务消息生命周期

### 功能特性
- **路由策略**: 多种智能路由策略，支持负载均衡和顺序保证
- **故障感知**: 自动检测和规避故障Broker节点
- **心跳机制**: 定期向所有Broker发送心跳，确保连接活跃
- **批量发送**: 自动批量合并消息，提高发送效率
- **事务支持**: 完整的分布式事务消息支持

### 高性能架构
- **简化设计**: 移除冗余组件，专注核心功能
- **预构建队列列表**: 避免运行时计算开销，提升性能
- **连接池管理**: 复用TCP连接，减少连接建立开销

### 监控和管理
- **统计信息**: 发送速率、成功率、延迟等关键指标
- **健康状态**: 实时监控Producer和Broker健康状态
- **事务状态**: 完整的事务状态追踪和管理

### 企业级特性
- **配置管理**: 灵活的配置系统，支持多种配置源
- **异常处理**: 完善的异常分类和处理机制
- **错误恢复**: 自动重试和故障恢复机制

## 使用示例

### 基础同步Producer
```python
from pyrocketmq.producer import create_producer
from pyrocketmq.model.message import Message

# 创建同步Producer
producer = create_producer("GID_POETRY", "localhost:9876")
producer.start()

# 发送消息
message = Message(topic="test_topic", body=b"Hello, RocketMQ!")
result = producer.send(message)

print(f"发送结果: {result.message_id}")
```

### 异步Producer
```python
from pyrocketmq.producer import create_async_producer
import asyncio

async def async_send():
    producer = await create_async_producer("GID_POETRY", "localhost:9876")
    await producer.start()

    message = Message(topic="test_topic", body=b"Hello, Async RocketMQ!")
    result = await producer.send(message)
    
    print(f"异步发送结果: {result.message_id}")

asyncio.run(async_send())
```

### 事务消息Producer
```python
from pyrocketmq.producer import create_transaction_producer
from pyrocketmq.producer.transaction import TransactionListener, LocalTransactionState
import json

class OrderTransactionListener(TransactionListener):
    def execute_local_transaction(self, message, transaction_id: str, arg=None) -> LocalTransactionState:
        try:
            # 执行本地事务（如订单创建）
            order_data = json.loads(message.body.decode())
            # create_order(order_data)  # 本地业务逻辑
            print(f"创建订单: {order_data}")
            return LocalTransactionState.COMMIT_MESSAGE
        except Exception as e:
            print(f"订单创建失败: {e}")
            return LocalTransactionState.ROLLBACK_MESSAGE

    def check_local_transaction(self, message, transaction_id: str) -> LocalTransactionState:
        # 检查本地事务状态
        order_id = message.get_property("order_id")
        # if order_exists(order_id):
        #     return LocalTransactionState.COMMIT_MESSAGE
        return LocalTransactionState.COMMIT_MESSAGE

# 创建事务Producer
producer = create_transaction_producer("GID_TRANSACTION", "localhost:9876")
producer.register_transaction_listener(OrderTransactionListener())
producer.start()

# 发送事务消息
order_data = {"order_id": "12345", "amount": 100}
message = Message(topic="order_topic", body=json.dumps(order_data).encode())
message.set_property("order_id", "12345")

result = producer.send_message_in_transaction(message)
print(f"事务结果: {result.transaction_id}, 状态: {result.status}")
```

### 自定义路由策略
```python
from pyrocketmq.producer import TopicBrokerMapping, MessageHashSelector, RandomSelector

# 创建映射管理器（默认轮询）
mapping = TopicBrokerMapping()

# 或者指定自定义选择器
hash_selector = MessageHashSelector()
mapping = TopicBrokerMapping(default_selector=hash_selector)

# 选择队列时可以覆盖选择器
result = mapping.select_queue("topic", message, RandomSelector())
```