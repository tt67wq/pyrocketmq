# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概述

pyrocketmq是一个功能完整的Python实现的RocketMQ客户端库，基于RocketMQ TCP协议实现。项目提供高性能、可靠的RocketMQ消息队列客户端功能，完全兼容Go语言实现的协议规范。

### 项目状态
- **协议模型层**: ✅ 完整实现，包含所有核心数据结构
- **请求工厂**: ✅ RemotingRequestFactory实现，支持所有RocketMQ请求类型
- **网络传输层**: ✅ 基于状态机的TCP连接实现
- **远程通信层**: ✅ 异步/同步通信实现
- **连接池**: ✅ 连接池管理功能
- **NameServer支持**: ✅ 完整客户端实现，支持路由信息查询
- **Broker支持**: ✅ 完整客户端实现，支持消息发送、拉取、偏移量管理等
- **Producer模块**: ✅ 完整版本实现完成，支持同步/异步消息发送、批量消息发送、心跳机制和完整的事务消息功能
- **事务消息模块**: ✅ 完整实现，支持TransactionProducer、事务监听器和完整的生命周期管理

## 开发环境配置

### 环境设置
```bash
# 激活虚拟环境（如果使用uv）
source .venv/bin/activate

# 设置PYTHONPATH（必需）
export PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src

# 安装依赖
pip install -e .
# 或使用uv
uv sync
```

### 测试运行
```bash
# 运行所有测试
export PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src && python -m pytest tests/

# 运行特定模块测试
export PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src && python -m pytest tests/model/ -v
export PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src && python -m pytest tests/transport/ -v
export PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src && python -m pytest tests/remote/ -v
export PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src && python -m pytest tests/broker/ -v
export PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src && python -m pytest tests/nameserver/ -v
export PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src && python -m pytest tests/producer/ -v

# 运行单个测试文件
export PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src && python -m pytest tests/model/test_serializer.py -v

# 运行单个测试方法
export PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src && python -m pytest tests/model/test_serializer.py::TestRemotingCommandSerializer::test_serialize_basic_command -v

# 运行异步测试
export PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src && python -m pytest tests/transport/ -v --asyncio-mode=auto

# 运行Producer模块测试
export PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src && python -m pytest tests/producer/ -v

# 运行示例代码
export PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src && python examples/basic_producer.py
export PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src && python examples/basic_async_producer.py
export PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src && python examples/simple_batch_producer.py
export PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src && python examples/transactional_producer.py
```

## 核心架构

### 分层架构设计
项目采用清晰的分层架构，从底层协议实现到高级客户端功能：

1. **协议模型层** (`model/`): 定义RocketMQ TCP协议的数据结构和序列化
2. **网络传输层** (`transport/`): 基于状态机的TCP连接管理
3. **远程通信层** (`remote/`): 异步/同步RPC通信和连接池
4. **客户端层** (`nameserver/`, `broker/`): NameServer和Broker的客户端封装
5. **生产者层** (`producer/`): 消息生产和路由管理功能

### 关键设计模式

#### 队列选择器策略模式
Producer模块实现了灵活的队列选择策略，支持：
- **RoundRobinSelector**: 轮询负载均衡（默认）
- **RandomSelector**: 随机选择
- **MessageHashSelector**: 基于消息分片键(SHARDING_KEY)的哈希选择
- **自定义选择器**: 实现QueueSelector接口即可

使用示例：
```python
# 使用默认轮询选择器
mapping = TopicBrokerMapping()
result = mapping.select_queue("test_topic", message)

# 使用随机选择器
result = mapping.select_queue("test_topic", message, RandomSelector())

# 使用消息哈希选择器（确保相同分片键的消息到同一队列）
hash_selector = MessageHashSelector()
result = mapping.select_queue("test_topic", message, hash_selector)
```

#### 状态机驱动的连接管理
TCP连接使用状态机模式管理连接生命周期：
- DISCONNECTED → CONNECTING → CONNECTED → CLOSING → CLOSED

#### 预构建队列列表优化性能
TopicBrokerMapping在路由更新时预先构建所有可用队列列表，避免每次选择时重新计算，显著提升性能。

### 数据流架构
```
Client Application
        ↓
    Producer API (QueueSelector)
        ↓
  TopicBrokerMapping (Route Cache)
        ↓
  BrokerManager (Connection Pool)
        ↓
  Remote Communication
        ↓
  Transport Layer (StateMachine)
        ↓
    RocketMQ Broker
```

## 核心模块详解

### Producer层 (`src/pyrocketmq/producer/`)
**关键组件**: TopicBrokerMapping + QueueSelector架构 + 心跳机制 + 事务消息支持

#### TopicBrokerMapping
- **功能**: 管理Topic到Broker的路由信息和队列选择
- **特性**:
  - 路由信息缓存和过期管理
  - 预构建队列列表性能优化
  - 支持自定义队列选择器
  - 线程安全的并发访问

#### QueueSelector策略
- **同步版本**: RoundRobinSelector、RandomSelector、MessageHashSelector
- **异步版本**: AsyncRoundRobinSelector、AsyncRandomSelector、AsyncMessageHashSelector
- **MessageHashSelector**: 基于SHARDING_KEY或KEYS的哈希选择，保证顺序性

#### Producer核心功能
- **同步Producer**: MVP版本，支持生命周期管理和消息发送
- **异步Producer**: 基于asyncio的高性能异步实现
- **批量消息发送**: 支持将多个消息压缩为一个批量消息进行高效发送
- **心跳机制**: 定期向所有Broker发送心跳，确保连接稳定性

#### 事务消息支持 ✅
TransactionProducer模块提供完整的事务消息功能：

**核心组件**:
- **LocalTransactionState**: 本地事务状态枚举 (COMMIT/ROLLBACK/UNKNOWN)
- **TransactionListener**: 事务监听器接口，定义本地事务执行和回查逻辑
- **TransactionSendResult**: 事务发送结果，继承自SendMessageResult
- **TransactionMetadata**: 事务元数据管理，跟踪事务状态和超时
- **SimpleTransactionListener**: 简单实现，用于测试和简单场景
- **TransactionProducer**: 完整的事务消息Producer实现

**关键特性**:
- 完整的事务状态管理
- 支持本地事务执行和回查
- 事务超时检测和重试机制
- 丰富的异常处理和错误分类
- 便利函数简化开发流程
- 完整的事务生命周期管理

### 批量消息发送功能 ✅
新增完整的批量消息发送支持，提升发送效率：

#### 批量编码方法
```python
from pyrocketmq.model.message import encode_batch, Message

# 将多个消息编码为批量消息
msg1 = Message(topic="test", body=b"message1")
msg2 = Message(topic="test", body=b"message2")
batch_msg = encode_batch(msg1, msg2)  # 返回batch=True的Message
```

#### Producer批量发送方法
```python
from pyrocketmq.producer import create_producer

# 同步批量发送
producer = create_producer("group", "nameserver:9876")
producer.start()
result = producer.send_batch(msg1, msg2, msg3)  # 自动编码和发送

# 异步批量发送
from pyrocketmq.producer import create_async_producer
import asyncio

async def async_batch_send():
    producer = await create_async_producer("group", "nameserver:9876")
    await producer.start()
    result = await producer.send_batch(msg1, msg2, msg3)
```

#### 批量消息特性
- **自动编码**: 内置`encode_batch`函数，与Go实现保持一致的序列化格式
- **主题验证**: 确保批量消息中的所有消息主题一致
- **高效传输**: 减少网络调用次数，提升吞吐量
- **错误处理**: 完整的批量发送错误处理和统计
- **消息压缩**: 使用RocketMQ标准的批量消息格式压缩多个消息

### 消息属性键规范
- **SHARDING_KEY**: 分片键，用于MessageHashSelector的顺序性保证
- **KEYS**: 消息键，多个键用空格分隔，SHARDING_KEY的备选
- **TAGS**: 消息标签，用于消息过滤

## 开发模式

### 使用队列选择器模式
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

### Producer使用模式
```python
# 同步Producer
from pyrocketmq.producer import create_producer
from pyrocketmq.model.message import Message

producer = create_producer("GID_POETRY", "nameserver:9876")
producer.start()

message = Message(topic="test_topic", body=b"Hello, RocketMQ!")
result = producer.send(message)

# 异步Producer
from pyrocketmq.producer import create_async_producer
import asyncio

async def async_send():
    producer = create_async_producer("GID_POETRY", "nameserver:9876")
    await producer.start()
    
    message = Message(topic="test_topic", body=b"Hello, Async RocketMQ!")
    result = await producer.send(message)

asyncio.run(async_send())
```

### 批量消息发送模式
```python
from pyrocketmq.producer import create_producer, create_async_producer
from pyrocketmq.model.message import Message

# 同步批量发送
producer = create_producer("GID_BATCH", "nameserver:9876")
producer.start()

# 创建多个消息
messages = [
    Message(topic="batch_topic", body=b"Message 1"),
    Message(topic="batch_topic", body=b"Message 2"),
    Message(topic="batch_topic", body=b"Message 3")
]

# 批量发送（自动编码为批量消息）
result = producer.send_batch(*messages)

# 异步批量发送
async def async_batch_send():
    producer = await create_async_producer("GID_BATCH_ASYNC", "nameserver:9876")
    await producer.start()
    
    result = await producer.send_batch(*messages)
    return result

asyncio.run(async_batch_send())
```

### 消息发送模式
```python
from pyrocketmq.model import Message, RemotingRequestFactory
from pyrocketmq.producer.topic_broker_mapping import MessageHashSelector

# 创建带分片键的消息
message = Message(topic="test_topic", body=b"order_data")
message.set_property("SHARDING_KEY", "user_123")

# 使用消息哈希选择器确保相同用户的消息到同一队列
hash_selector = MessageHashSelector()
mapping = TopicBrokerMapping()
result = mapping.select_queue("test_topic", message, hash_selector)
```

### 事务消息发送模式 ✅
基于TransactionListener的事务消息发送，支持本地事务执行和状态回查：

```python
from pyrocketmq.producer.transaction import (
    TransactionListener, 
    LocalTransactionState,
    SimpleTransactionListener,
    create_transaction_send_result,
    create_simple_transaction_listener,
    create_transaction_message
)
from pyrocketmq.producer import create_transaction_producer
from pyrocketmq.model.message import Message

# 自定义事务监听器
class OrderTransactionListener(TransactionListener):
    def execute_local_transaction(self, message, transaction_id: str, arg=None) -> LocalTransactionState:
        try:
            # 执行本地事务（如订单创建）
            order_data = json.loads(message.body.decode())
            create_order(order_data)
            return LocalTransactionState.COMMIT_MESSAGE
        except Exception as e:
            logger.error(f"Order creation failed: {e}")
            return LocalTransactionState.ROLLBACK_MESSAGE
    
    def check_local_transaction(self, message, transaction_id: str) -> LocalTransactionState:
        # 检查本地事务状态
        order_id = message.get_property("order_id")
        if order_exists(order_id):
            return LocalTransactionState.COMMIT_MESSAGE
        return LocalTransactionState.ROLLBACK_MESSAGE

# 使用简单事务监听器（测试用）
simple_listener = create_simple_transaction_listener(commit=True)

# 创建事务Producer
producer = create_transaction_producer("GID_TRANSACTION", "nameserver:9876")
producer.register_transaction_listener(OrderTransactionListener())
producer.start()

# 创建事务消息
transaction_msg = create_transaction_message(
    topic="order_topic", 
    body=json.dumps({"order_id": "12345", "amount": 100}),
    transaction_id="txn_12345"
)
transaction_msg.set_property("order_id", "12345")

# 发送事务消息
result = producer.send_message_in_transaction(transaction_msg)

# 检查事务结果状态
if result.is_commit:
    print(f"Transaction {result.transaction_id} committed successfully")
elif result.is_rollback:
    print(f"Transaction {result.transaction_id} rolled back")
```

### 扩展自定义选择器
```python
from pyrocketmq.producer.topic_broker_mapping import QueueSelector

class CustomSelector(QueueSelector):
    def select(self, topic, available_queues, message=None):
        # 自定义选择逻辑
        # 例如基于broker负载、地域、消息大小等
        return available_queues[0] if available_queues else None
```

### 示例代码
项目提供完整的示例代码：
- `examples/basic_producer.py`: 同步Producer基础使用示例
- `examples/basic_async_producer.py`: 异步Producer基础使用示例
- `examples/simple_batch_producer.py`: 批量消息发送示例（使用新的send_batch方法）
- `examples/transactional_producer.py`: 事务消息发送示例（完整实现）

## 协议规范

### 数据帧格式
```
| length(4) | header-length(4) | header-data(JSON) | body-data(bytes) |
```

### Flag类型判断逻辑
由于Go语言实现中`RPC_ONEWAY`和`RESPONSE_TYPE`都使用值1：
- `is_request()`: flag == FlagType.RPC_TYPE (0)
- `is_response()`: flag == FlagType.RESPONSE_TYPE (1)
- `is_oneway()`: flag == FlagType.RPC_ONEWAY (1)

### 大小限制
- 最大帧大小: 32MB (33554432字节)
- 最大header大小: 64KB (65536字节)
- 长度字段: 大端序4字节整数

## 依赖管理

### 项目配置
- 使用 `pyproject.toml` 进行现代化项目配置
- 支持 `uv.lock` 文件实现高效依赖管理
- Python 3.11+ 要求，支持完整类型注解

### 开发工具
```bash
# 使用pip安装依赖
pip install -e .

# 使用uv进行快速依赖管理
uv sync
```

## 注意事项

1. **环境变量**: 开发时必须设置`PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src`
2. **分片键策略**: MessageHashSelector优先使用SHARDING_KEY，其次使用KEYS的第一个值
3. **选择器状态**: RoundRobinSelector的计数器在路由更新时会重置
4. **性能优化**: TopicBrokerMapping预构建队列列表，避免运行时计算开销
5. **线程安全**: 所有映射管理器操作都是线程安全的
6. **异步优先**: 网络通信主要基于asyncio，同步模式是其封装
7. **路由过期**: 默认路由过期时间30秒，可配置
8. **类型安全**: 所有代码使用完整类型注解
9. **心跳机制**: Producer会定期向所有Broker发送心跳，确保连接活跃状态
10. **批量消息**: 使用`send_batch()`方法可以高效发送多个消息，自动进行消息编码和主题验证
11. **示例代码**: 参考 `examples/` 目录下的完整使用示例，包括批量消息发送示例
12. **事务消息**: ✅ 事务消息模块已完整实现，支持完整的事务生命周期管理
    - 使用`TransactionListener`接口定义本地事务逻辑
    - 支持三种事务状态：COMMIT_MESSAGE、ROLLBACK_MESSAGE、UNKNOWN
    - 提供`SimpleTransactionListener`用于测试场景
    - 包含完整的事务异常处理和超时管理
    - 便利函数简化事务消息创建和结果处理
    - `create_transaction_producer()` 创建事务Producer实例
    - `create_transaction_message()` 创建事务消息
    - `create_simple_transaction_listener()` 创建简单事务监听器
    - `create_transaction_send_result()` 创建事务发送结果