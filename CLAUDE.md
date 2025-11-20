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
- **Consumer模块**: ✅ 完整实现完成，支持同步和异步两套完整架构
  - ✅ 配置管理：完整的Consumer配置体系
  - ✅ 偏移量存储：本地/远程存储完整实现（同步+异步）
  - ✅ 订阅管理：订阅关系管理和冲突检测
  - ✅ 消息监听器：并发和顺序消费接口（同步+异步）
  - ✅ 队列分配策略：平均分配算法实现
  - ✅ 消费起始位置管理：三种策略支持（同步+异步）
  - ✅ 异常体系：20+种专用异常类型
  - ✅ 监控指标：全面的性能和状态监控
  - ✅ 抽象基类：BaseConsumer和AsyncBaseConsumer完整实现
  - ✅ 工厂模式：OffsetStoreFactory和AsyncOffsetStoreFactory

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

# 运行Consumer模块测试
export PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src && python -m pytest tests/consumer/ -v

# 运行示例代码
export PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src && python examples/basic_producer.py
export PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src && python examples/basic_async_producer.py
export PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src && python examples/simple_batch_producer.py
export PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src && python examples/transactional_producer.py
```

## 核心架构

### 分层架构设计
项目采用清晰的分层架构，从底层协议实现到高级客户端功能，每一层都有明确的职责分工：

1. **协议模型层** (`model/`): 定义RocketMQ TCP协议的数据结构和序列化机制
2. **网络传输层** (`transport/`): 基于状态机的TCP连接管理，提供可靠的字节流传输
3. **远程通信层** (`remote/`): 异步/同步RPC通信和连接池管理，提供高级通信抽象
4. **注册发现层** (`nameserver/`): NameServer客户端，提供路由查询和集群管理
5. **Broker通信层** (`broker/`): Broker客户端封装，提供消息收发等核心功能
6. **高级应用层**:
   - **producer/**: 消息生产者实现，包含路由、事务等高级特性
   - **consumer/**: 消息消费者实现，包含订阅管理、偏移量存储、消息监听等核心功能
7. **日志系统层** (`logging/`): 统一的日志记录和管理系统

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

## 模块依赖关系

### 依赖层次图
```
┌─────────────────────────────────────────────────────────────┐
│                    应用层 (Application)                        │
├─────────────────────────────────────────────────────────────┤
│                Producer层 & Consumer层 (高级功能)              │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐ │
│  │   Producer      │  │   Consumer      │  │TransactionProd │ │
│  │                 │  │                 │  │                 │ │
│  │ AsyncProducer   │  │ ConcurrentCons  │  │ AsyncTransaction│ │
│  │                 │  │ OrderlyConsumer │  │     Producer    │ │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘ │
├─────────────────────────────────────────────────────────────┤
│              客户端层 (NameServer & Broker)                   │
│  ┌─────────────────┐              ┌─────────────────┐         │
│  │  NameServer     │              │     Broker      │         │
│  │     Client      │              │     Client      │         │
│  └─────────────────┘              └─────────────────┘         │
├─────────────────────────────────────────────────────────────┤
│                 远程通信层 (Remote)                           │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐ │
│  │     Remote      │  │   AsyncRemote   │  │ ConnectionPool  │ │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘ │
├─────────────────────────────────────────────────────────────┤
│               传输层 (Transport)                              │
│  ┌─────────────────┐  ┌─────────────────┐                     │
│  │ConnectionState  │  │AsyncConnection  │                     │
│  │    Machine      │  │   StateMachine  │                     │
│  └─────────────────┘  └─────────────────┘                     │
├─────────────────────────────────────────────────────────────┤
│                  协议模型层 (Model)                            │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐ │
│  │ RemotingCommand │  │     Message     │  │ RequestFactory  │ │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘ │
├─────────────────────────────────────────────────────────────┤
│                   日志系统 (Logging)                           │
│              (贯穿所有层，提供统一日志服务)                      │
└─────────────────────────────────────────────────────────────┘
```

### 模块间依赖关系

**Producer模块依赖**:
```
Producer → {MessageRouter, TopicBrokerMapping, Config}
    ↓
MessageRouter → {QueueSelector, TopicBrokerMapping}
    ↓
TopicBrokerMapping → {Model (数据结构)}
    ↓
Producer → {BrokerManager, NameServerClient}
    ↓
BrokerManager/NameServerClient → {Remote, AsyncRemote}
    ↓
Remote/AsyncRemote → {ConnectionPool, Transport}
    ↓
Transport → {ConnectionStateMachine}
    ↓
所有模块 → {Logging (日志记录)}
```

**数据流向**:
1. **应用请求**: Producer.send(message)
2. **路由决策**: MessageRouter.route_message()
3. **连接获取**: BrokerManager.connection()
4. **网络传输**: Remote.send_request()
5. **协议序列化**: Model.Serializer.serialize()
6. **TCP传输**: Transport.send_data()
7. **日志记录**: 贯穿所有步骤

## 快速开始指南

### 1. 环境准备
```bash
# 设置环境变量
export PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src

# 安装依赖
pip install -e .
```

### 2. 基础Producer使用
```python
from pyrocketmq.producer import create_producer
from pyrocketmq.model.message import Message

# 创建Producer
producer = create_producer("test_group", "localhost:9876")
producer.start()

# 发送消息
message = Message(topic="test_topic", body=b"Hello RocketMQ")
result = producer.send(message)
print(f"消息发送成功: {result.msg_id}")

# 清理资源
producer.shutdown()
```

### 3. 异步Producer使用
```python
import asyncio
from pyrocketmq.producer import create_async_producer
from pyrocketmq.model.message import Message

async def async_example():
    # 创建异步Producer
    producer = create_async_producer("async_group", "localhost:9876")
    await producer.start()

    # 发送消息
    message = Message(topic="async_topic", body=b"Async Hello")
    result = await producer.send(message)
    print(f"异步消息发送成功: {result.msg_id}")

    # 清理资源
    await producer.shutdown()

# 运行异步示例
asyncio.run(async_example())
```

### 4. 事务消息使用
```python
from pyrocketmq.producer.transaction import (
    TransactionListener,
    LocalTransactionState,
    create_transaction_producer
)

# 定义事务监听器
class OrderTransactionListener(TransactionListener):
    def execute_local_transaction(self, message, transaction_id, arg=None):
        # 执行本地事务逻辑
        try:
            # 这里执行你的业务逻辑，如创建订单
            print(f"执行本地事务: {transaction_id}")
            return LocalTransactionState.COMMIT_MESSAGE
        except Exception:
            return LocalTransactionState.ROLLBACK_MESSAGE

    def check_local_transaction(self, message, transaction_id):
        # 检查本地事务状态
        print(f"检查事务状态: {transaction_id}")
        return LocalTransactionState.COMMIT_MESSAGE

# 创建事务Producer
producer = create_transaction_producer(
    "transaction_group",
    "localhost:9876",
    OrderTransactionListener()
)
producer.start()

# 发送事务消息
message = Message(topic="order_topic", body=b'{"order_id": "123"}')
result = producer.send_message_in_transaction(message)
print(f"事务消息发送: {result.transaction_id}")

producer.shutdown()
```

### 5. 高级配置使用
```python
from pyrocketmq.producer.config import get_config
from pyrocketmq.producer import create_producer

# 使用生产环境配置
config = get_config("production")
producer = create_producer(
    producer_group="prod_group",
    namesrv_addr="broker1:9876;broker2:9876",
    send_msg_timeout=10000,
    retry_times=5,
    routing_strategy="message_hash"
)

# 或者使用自定义配置
from pyrocketmq.producer.config import create_custom_config

config = create_custom_config(
    producer_group="custom_group",
    async_send_semaphore=20000,  # 更大的并发量
    batch_size=64,               # 更大的批量大小
    compress_msg_body_over_howmuch=1024  # 1KB以上压缩
)

producer = create_producer(**config.__dict__)
```

### 6. 错误处理最佳实践
```python
from pyrocketmq.producer.errors import (
    MessageSendError,
    BrokerNotAvailableError,
    TimeoutError
)
import time

def robust_send_example():
    producer = create_producer("robust_group", "localhost:9876")
    producer.start()

    message = Message(topic="robust_topic", body=b"Robust message")

    max_retries = 3
    for attempt in range(max_retries):
        try:
            result = producer.send(message)
            print(f"消息发送成功: {result.msg_id}")
            break
        except BrokerNotAvailableError as e:
            print(f"Broker不可用，重试 {attempt + 1}/{max_retries}: {e}")
            time.sleep(2 ** attempt)  # 指数退避
        except TimeoutError as e:
            print(f"发送超时，重试 {attempt + 1}/{max_retries}: {e}")
            time.sleep(1)
        except MessageSendError as e:
            print(f"发送失败，不重试: {e}")
            break

    producer.shutdown()
```

## 核心模块详解

### 1. Model层 (`src/pyrocketmq/model/`) - 协议数据模型

**模块概述**: Model模块是pyrocketmq的核心数据层，提供完整的RocketMQ协议数据结构定义、序列化机制和工具函数。严格遵循RocketMQ TCP协议规范，与Go语言实现完全兼容。

**核心组件**:
- **RemotingCommand**: 远程命令数据结构，协议层的核心通信单元
- **Message/MessageExt**: 基础和扩展消息数据结构，支持消息属性和元数据
- **MessageQueue**: 消息队列数据结构，表示Topic下的具体队列
- **RemotingRequestFactory**: 请求工厂，支持所有RocketMQ请求类型构建
- **Serializer**: 高效的二进制序列化器，支持大消息处理

**关键特性**:
- 严格的协议兼容性，与Go语言实现完全兼容
- 使用dataclass和类型注解，提供完整的类型安全保障
- 模块化设计，便于扩展新的协议特性
- 丰富的工具函数，简化常见操作

**使用示例**:
```python
from pyrocketmq.model import Message, RemotingCommand, RemotingRequestFactory

# 创建消息
message = Message(topic="test_topic", body=b"Hello RocketMQ")
message.set_property("KEYS", "order_123")

# 创建请求
factory = RemotingRequestFactory()
request = factory.create_send_message_request(
    producer_group="test_group",
    message_data=message.body,
    message_queue=message_queue,
    properties=message.properties
)
```

### 2. Transport层 (`src/pyrocketmq/transport/`) - 网络传输基础设施

**模块概述**: Transport模块是pyrocketmq的传输层基础设施，提供基于TCP协议的网络通信能力。采用状态机模式管理连接生命周期，同时支持同步和异步两种操作模式。

**核心组件**:
- **ConnectionStateMachine**: 同步TCP连接状态机实现
- **AsyncConnectionStateMachine**: 异步TCP连接状态机实现
- **TransportConfig**: 传输层配置管理，控制连接行为和网络参数

**状态机设计**:
```
DISCONNECTED → CONNECTING → CONNECTED → CLOSING → CLOSED
```

**关键特性**:
- 状态机驱动，精确管理连接生命周期
- 双模式支持：同步和异步连接实现
- 丰富的配置选项，支持不同场景的优化
- 完善的异常处理和资源清理机制
- 协议无关，提供字节流传输服务

**使用示例**:
```python
from pyrocketmq.transport import TransportConfig, ConnectionStateMachine

# 创建传输配置
config = TransportConfig(
    host="localhost",
    port=9876,
    connect_timeout=5000.0,
    read_timeout=30000.0
)

# 创建连接状态机
connection = ConnectionStateMachine(config)
await connection.connect()
```

### 3. Remote层 (`src/pyrocketmq/remote/`) - 远程通信层

**模块概述**: Remote模块是pyrocketmq的核心通信层，提供与RocketMQ服务器进行远程通信的完整功能。内置连接池管理、请求超时控制、并发限制等高级特性。

**核心组件**:
- **Remote**: 同步远程通信实现
- **AsyncRemote**: 异步远程通信实现
- **ConnectionPool**: 连接池实现，管理TCP连接复用
- **RemoteConfig**: 远程通信配置管理

**关键特性**:
- 双模式支持：同步和异步通信模式
- 线程安全，所有操作支持高并发场景
- 内置连接池和等待者管理，有效控制资源使用
- 完善的异常分类和处理机制
- 支持多种预设配置和环境变量配置

**使用示例**:
```python
from pyrocketmq.remote import Remote, RemoteConfig, create_remote

# 便捷创建
remote = create_remote("localhost", 9876)

# 使用连接池
with remote.connection_pool.get_connection("broker1:10911") as conn:
    response = await conn.send_request(request)
```

### 4. NameServer层 (`src/pyrocketmq/nameserver/`) - 注册发现客户端

**模块概述**: NameServer模块提供与RocketMQ NameServer通信的完整功能。NameServer作为RocketMQ的注册发现中心，负责管理Topic路由信息、Broker集群信息等关键数据。

**核心组件**:
- **SyncNameServerClient**: 同步NameServer客户端实现
- **AsyncNameServerClient**: 异步NameServer客户端实现
- **BrokerData**: Broker信息数据结构
- **TopicRouteData**: Topic路由数据结构

**关键特性**:
- 双模式支持：同步和异步客户端
- 严格处理Go语言JSON序列化的整数key兼容性问题
- 专门的NameServer异常体系，精确处理各种错误场景
- 支持路由信息的本地缓存和更新机制
- 完全兼容RocketMQ NameServer协议

**使用示例**:
```python
from pyrocketmq.nameserver import SyncNameServerClient

# 创建客户端
client = SyncNameServerClient(remote, timeout=5000.0)

# 查询Topic路由信息
route_data = client.query_topic_route_info("test_topic")

# 获取Broker集群信息
cluster_info = client.get_broker_cluster_info()
```

### 5. Broker层 (`src/pyrocketmq/broker/`) - Broker通信客户端

**模块概述**: Broker模块是pyrocketmq客户端库的核心组件，提供与RocketMQ Broker通信的完整功能实现。支持消息发送、拉取、偏移量管理、心跳维护等核心功能。

**核心组件**:
- **BrokerClient**: 同步Broker客户端实现
- **AsyncBrokerClient**: 异步Broker客户端实现
- **BrokerManager**: 同步Broker连接管理器
- **AsyncBrokerManager**: 异步Broker连接管理器
- **ConnectionInfo**: Broker连接信息和状态管理

**关键特性**:
- 双模式支持：同步和异步客户端，满足不同性能需求场景
- 定义了20+种专门的异常类型，精确处理各种错误场景
- 严格遵循RocketMQ TCP协议规范，与Go语言实现完全兼容
- 支持oneway通信模式，减少不必要的响应等待
- 智能连接管理，提供with风格的连接获取方法
- 连接复用优化，健康检查创建的连接自动复用

**使用示例**:
```python
from pyrocketmq.broker import BrokerClient, create_broker_manager

# 创建Broker管理器
manager = create_broker_manager("localhost:9876")

# 使用连接发送消息
with manager.connection("broker1:10911") as broker_client:
    result = broker_client.sync_send_message(
        producer_group="test_group",
        message_body=b"Hello RocketMQ",
        message_queue=message_queue,
        properties={}
    )
```

### 6. Producer层 (`src/pyrocketmq/producer/`) - 消息生产者

**模块概述**: Producer模块是pyrocketmq的高级消息生产者实现，提供完整高效的消息发送、路由管理和故障处理功能。经过架构优化，移除了冗余组件，专注于核心功能实现。

**关键组件**: Producer/AsyncProducer + MessageRouter + TopicBrokerMapping + 配置管理 + 事务消息支持

#### Producer核心实现
- **同步Producer**: MVP版本，采用简化的布尔状态管理，专注核心功能
- **异步Producer**: 基于asyncio实现，支持高并发消息发送
- **TransactionProducer**: 完整的事务消息Producer实现
- **AsyncTransactionProducer**: 异步事务消息Producer实现

#### MessageRouter智能路由
- **多路由策略**: 支持轮询(RoundRobin)、随机(Random)、消息哈希(MessageHash)三种策略
- **故障感知**: 实时监控Broker健康状态，自动规避故障节点
- **性能监控**: 跟踪延迟、成功率等指标，支持延迟感知优化

#### TopicBrokerMapping路由缓存
- **路由信息管理**: 缓存Topic到Broker的映射关系，支持过期管理
- **预构建优化**: 在路由更新时预先构建所有可用队列列表，显著提升性能
- **线程安全**: 使用RLock确保并发访问的安全性

#### QueueSelector队列选择器
**同步选择器**: RoundRobinSelector、RandomSelector、MessageHashSelector
**异步选择器**: AsyncRoundRobinSelector、AsyncRandomSelector、AsyncMessageHashSelector

#### ProducerConfig配置管理
- **完整配置支持**: 涵盖所有RocketMQ Producer配置参数
- **环境变量支持**: 支持从环境变量加载配置，便于容器化部署
- **预定义配置模板**: 提供开发、生产、高性能等环境的预设配置

**关键特性**:
- 完整的发送模式：同步/异步 × 普通/批量 × 可靠/单向 × 事务消息
- 丰富的功能特性：路由策略、故障感知、心跳机制、批量发送、事务支持
- 高性能架构：简化设计、预构建队列列表、连接池管理
- 完善的监控：统计信息、健康状态、事务状态追踪
- 企业级特性：配置管理、异常处理、错误恢复、最佳实践指导

### 7. Consumer层 (`src/pyrocketmq/consumer/`) - 消息消费者

**模块概述**: Consumer模块是pyrocketmq的消息消费者实现，提供完整的消息消费、订阅管理、偏移量存储和消息监听功能。采用分层架构设计，支持并发消费和顺序消费两种模式。提供同步和异步两套完整的实现，满足不同应用场景的需求。

**核心组件**:
- **BaseConsumer**: 同步消费者抽象基类，定义生命周期管理
- **AsyncBaseConsumer**: 异步消费者抽象基类，支持高并发异步应用
- **ConsumerConfig**: 消费者配置管理，支持完整的消费行为配置
- **消息监听器体系**: 
  - 同步：MessageListener、MessageListenerConcurrently、MessageListenerOrderly
  - 异步：AsyncMessageListener、SimpleAsyncMessageListener
- **偏移量存储系统**: 
  - 同步：RemoteOffsetStore(集群模式)、LocalOffsetStore(广播模式)
  - 异步：AsyncRemoteOffsetStore(集群模式)、AsyncLocalOffsetStore(广播模式)
- **偏移量存储工厂**:
  - 同步：OffsetStoreFactory
  - 异步：AsyncOffsetStoreFactory
- **订阅管理器**: SubscriptionManager，管理主题订阅和选择器
- **队列分配策略**: AverageAllocateStrategy，实现平均分配算法
- **消费起始位置管理**: 
  - 同步：ConsumeFromWhereManager
  - 异步：AsyncConsumeFromWhereManager

#### AsyncBaseConsumer - 异步消费者抽象基类

**功能描述**: AsyncBaseConsumer是pyrocketmq消费者模块的异步抽象基类，定义了所有异步消费者的通用接口和基础功能。所有网络操作、IO操作都采用asyncio异步编程模型，适用于高并发异步应用场景。

**核心功能**:
- **异步配置管理**: 统一的Consumer配置管理
- **异步订阅管理**: Topic订阅和消息选择器的异步管理
- **异步消息监听**: 支持AsyncMessageListener的注册和处理
- **异步生命周期**: 支持异步启动、停止、资源清理
- **异步错误处理**: 统一的异步异常处理和错误恢复
- **路由管理**: 异步路由刷新和Broker地址收集
- **心跳机制**: 异步心跳发送和统计监控

**核心接口**:
```python
class AsyncBaseConsumer(ABC):
    @abstractmethod
    async def start(self) -> None:
        """异步启动消费者"""
        pass

    @abstractmethod  
    async def shutdown(self) -> None:
        """异步关闭消费者"""
        pass

    @abstractmethod
    async def _consume_message(self, messages: list[MessageExt], context: AsyncConsumeContext) -> ConsumeResult:
        """异步消费消息"""
        pass

    async def subscribe(self, topic: str, selector: MessageSelector) -> None:
        """异步订阅Topic"""
        pass

    async def unsubscribe(self, topic: str) -> None:
        """异步取消订阅Topic"""
        pass

    async def register_message_listener(self, listener: AsyncMessageListener) -> None:
        """异步注册消息监听器"""
        pass
```

**核心内部方法**:

#### 路由管理方法
```python
async def _route_refresh_loop(self) -> None:
    """异步路由刷新循环
    - 定期刷新所有订阅Topic的路由信息
    - 更新TopicBrokerMapping缓存
    - 处理路由更新异常和统计信息
    """

async def _refresh_all_routes(self) -> None:
    """异步刷新所有Topic路由
    - 批量获取所有订阅Topic的路由信息
    - 并发处理多个Topic的路由更新
    - 统计刷新成功/失败数量
    """

async def _collect_broker_addresses(self) -> set[str]:
    """异步收集所有Broker地址
    - 从TopicBrokerMapping获取缓存的Broker地址
    - 合并订阅Topic的Broker地址
    - 过滤空地址，确保连接有效性
    - 返回去重后的Broker地址集合
    """
```

#### 心跳管理方法
```python
async def _heartbeat_loop(self) -> None:
    """异步心跳循环
    - 定期向所有Broker发送心跳
    - 更新心跳统计信息
    - 处理心跳发送异常
    """

def _build_heartbeat_data(self) -> HeartbeatData:
    """构建心跳数据
    - 创建标准HeartbeatData对象
    - 包含消费者组信息、订阅关系等
    - 与同步版本保持完全一致的数据结构
    """

async def _send_heartbeat_to_all_brokers(self) -> None:
    """异步向所有Broker发送心跳
    - 收集所有可用Broker地址
    - 并发发送心跳到各个Broker
    - 统计心跳发送成功/失败数量
    """

async def _send_heartbeat_to_broker(self, broker_addr: str, heartbeat_data: HeartbeatData) -> bool:
    """异步向指定Broker发送心跳
    - 建立与指定Broker的连接
    - 发送标准格式的心跳数据
    - 处理连接异常和发送失败情况
    """
```

#### 生命周期管理方法
```python
async def _async_start(self) -> None:
    """异步启动基础组件
    - 初始化NameServerManager和BrokerManager
    - 创建OffsetStore和消费起始位置管理器
    - 启动路由刷新和心跳任务
    """

async def _async_shutdown(self) -> None:
    """异步关闭基础组件
    - 停止路由刷新和心跳任务
    - 关闭所有管理器和连接
    - 清理异步资源和事件
    """
```

**异步生命周期**:
```python
class MyAsyncConsumer(AsyncBaseConsumer):
    async def start(self):
        await self._async_start()  # 启动基础组件
        # 启动消费循环
        pass
    
    async def shutdown(self):
        await self._async_shutdown()  # 关闭基础组件
        # 停止消费循环
        pass
```

**设计特点**:
- **异步优先**: 所有IO操作都采用async/await模式
- **线程安全**: 使用asyncio.Lock保证并发安全
- **资源管理**: 完善的异步资源清理机制
- **容错处理**: 独立处理每个Topic/Broker的异常
- **统计监控**: 全面的路由和心跳统计信息
- **协议兼容**: 心跳数据与同步版本完全一致
- 可扩展性: 清晰的抽象接口，便于扩展

#### ConsumerConfig配置管理
**配置类别**:
- **基础配置**: consumer_group、namesrv_addr
- **消费行为**: message_model、consume_from_where、allocate_strategy
- **性能配置**: consume_thread_min/max、pull_batch_size、consume_timeout
- **存储配置**: persist_interval、offset_store_path、cache_size
- **高级配置**: auto_commit、message_trace、pull_threshold_for_all

```python
@dataclass
class ConsumerConfig:
    consumer_group: str
    namesrv_addr: str
    message_model: str = MessageModel.CLUSTERING
    consume_thread_min: int = 20
    consume_thread_max: int = 64
    pull_batch_size: int = 32
    persist_interval: int = 5000
    auto_commit: bool = True
```

#### 消息监听器体系
**监听器类型**:
- **MessageListener**: 基础监听器接口
- **MessageListenerConcurrently**: 并发消息监听器，支持多线程并行处理
- **MessageListenerOrderly**: 顺序消息监听器，保证消息顺序性
- **SimpleMessageListener**: 简单监听器实现，便于快速开发

```python
class MessageListenerConcurrently(MessageListener):
    @abstractmethod
    def consume_message_concurrently(self, messages: List[MessageExt], context) -> ConsumeResult:
        """并发消费消息"""
        pass

class MessageListenerOrderly(MessageListener):
    @abstractmethod
    def consume_message_orderly(self, messages: List[MessageExt], context) -> ConsumeResult:
        """顺序消费消息"""
        pass
```

#### 偏移量存储系统
**存储模式**:
- **RemoteOffsetStore**: 集群模式，偏移量存储在Broker端，支持多消费者协调
- **LocalOffsetStore**: 广播模式，偏移量存储在本地文件，每个消费者独立维护
- **AsyncRemoteOffsetStore**: 异步集群模式，偏移量存储在Broker端
- **AsyncLocalOffsetStore**: 异步广播模式，偏移量存储在本地文件
- **OffsetStoreFactory**: 同步偏移量存储工厂模式创建存储实例
- **AsyncOffsetStoreFactory**: 异步偏移量存储工厂模式创建存储实例

#### 异步消息监听器体系

**AsyncMessageListener**: 异步消息监听器基础接口，为高并发异步应用提供消息处理能力。

**核心接口**:
```python
class AsyncMessageListener(ABC):
    @abstractmethod
    async def consume_message(self, messages: list[MessageExt], context: AsyncConsumeContext) -> ConsumeResult:
        """异步消费消息"""
        pass

class SimpleAsyncMessageListener(AsyncMessageListener):
    async def consume_message(self, messages: list[MessageExt], context: AsyncConsumeContext) -> ConsumeResult:
        """简单的异步消息处理实现"""
        for message in messages:
            print(f"异步消费消息: {message.body.decode()}")
        return ConsumeResult.CONSUME_SUCCESS
```

**使用示例**:
```python
# 自定义异步消息监听器
class AsyncOrderProcessor(AsyncMessageListener):
    async def consume_message(self, messages: list[MessageExt], context: AsyncConsumeContext) -> ConsumeResult:
        for message in messages:
            try:
                order_data = json.loads(message.body.decode())
                await process_order_async(order_data)
                print(f"订单异步处理成功: {order_data['order_id']}")
            except Exception as e:
                print(f"订单异步处理失败: {e}")
                return ConsumeResult.RECONSUME_LATER
        return ConsumeResult.CONSUME_SUCCESS
```

**异步消费上下文**:
```python
@dataclass
class AsyncConsumeContext:
    """异步消费上下文，提供消费过程中的状态信息"""
    queue: MessageQueue
    message_count: int
    is_first_time: bool
    processing_start_time: float
    async def get_current_time(self) -> float:
        """获取当前时间戳"""
        return time.time()
    async def get_processing_duration(self) -> float:
        """获取处理时长"""
        return await self.get_current_time() - self.processing_start_time
```

#### 异步偏移量存储工厂

**AsyncOffsetStoreFactory**: 异步偏移量存储工厂类，为高并发异步应用场景提供偏移量存储创建功能。

**核心功能**:
- **异步创建**: 所有创建操作都采用async/await模式
- **异步存储支持**: 集群模式使用AsyncRemoteOffsetStore
- **广播模式兼容**: 支持LocalOffsetStore（本地存储无需异步）
- **异步验证**: 支持配置参数的验证
- **自动启动**: 可选择创建后自动启动服务

**异步创建接口**:
```python
class AsyncOffsetStoreFactory:
    @staticmethod
    async def create_offset_store(
        consumer_group: str,
        message_model: str,
        namesrv_manager: AsyncNameServerManager,
        broker_manager: AsyncBrokerManager,
        store_path: str = "~/.rocketmq/offsets",
        persist_interval: int = 5000,
        auto_start: bool = True,
        **kwargs: Any,
    ) -> AsyncOffsetStore:
        """异步创建偏移量存储实例"""
        pass
```

**便利函数**:
```python
async def create_offset_store(
    consumer_group: str,
    message_model: str,
    namesrv_manager: AsyncNameServerManager,
    broker_manager: AsyncBrokerManager,
    **kwargs: Any,
) -> AsyncOffsetStore:
    """异步便捷函数：创建偏移量存储实例"""
    pass
```

**使用示例**:
```python
# 集群模式异步偏移量存储
remote_store = await AsyncOffsetStoreFactory.create_offset_store(
    consumer_group="test_group",
    message_model=MessageModel.CLUSTERING,
    namesrv_manager=async_namesrv_mgr,
    broker_manager=async_broker_mgr
)

# 广播模式偏移量存储
local_store = await create_offset_store(
    consumer_group="broadcast_group", 
    message_model=MessageModel.BROADCASTING,
    store_path="/tmp/offsets"
)
```

**设计特点**:
- 异步优先: 符合asyncio编程模式
- 向后兼容: 与同步版本保持相同API设计
- 完整功能: 支持所有偏移量存储场景
- 错误安全: 完善的异常处理和参数验证

#### 偏移量存储特性
- **线程安全的偏移量更新和持久化**
- **支持批量提交和定期持久化**  
- **完整的指标收集和监控**
- **原子性文件操作保证数据一致性**

#### 订阅管理器
**核心功能**:
- 主题订阅和消息选择器管理
- 订阅冲突检测和处理
- 订阅数据的导入导出
- 指标收集和监控

```python
class SubscriptionManager:
    def subscribe(self, topic: str, selector: MessageSelector) -> bool:
        """订阅主题"""
        pass
    
    def unsubscribe(self, topic: str) -> bool:
        """取消订阅"""
        pass
    
    def update_selector(self, topic: str, selector: MessageSelector) -> bool:
        """更新消息选择器"""
        pass
```

#### 队列分配策略
**AverageAllocateStrategy**: 
- 基于平均分配算法的队列分配策略
- 考虑消费者顺序和队列顺序的独立性
- 支持边界条件处理（队列数不能被消费者数整除）
- 大规模分配的性能优化

#### 消费起始位置管理
**三种策略**:
- **CONSUME_FROM_LAST_OFFSET**: 从最新偏移量开始消费（默认）
- **CONSUME_FROM_FIRST_OFFSET**: 从最早偏移量开始消费
- **CONSUME_FROM_TIMESTAMP**: 从指定时间戳位置开始消费

**关键特性**:
- 支持Broker交互查询最大/最小偏移量
- 时间戳转换的边界情况处理
- 连接管理和资源清理

**使用示例**:
```python
from pyrocketmq.consumer import ConsumerConfig, create_consumer
from pyrocketmq.consumer.listener import MessageListenerConcurrently, ConsumeResult

# 创建并发消费者
class MyMessageListener(MessageListenerConcurrently):
    def consume_message_concurrently(self, messages, context):
        for message in messages:
            print(f"消费消息: {message.body.decode()}")
        return ConsumeResult.CONSUME_SUCCESS

# 创建消费者
config = ConsumerConfig(
    consumer_group="test_consumer_group",
    namesrv_addr="localhost:9876",
    message_model=MessageModel.CLUSTERING
)

consumer = create_consumer(config, MyMessageListener())
consumer.start()

# 订阅主题
consumer.subscribe("test_topic", "*")

# 等待消息
import time
time.sleep(60)

consumer.shutdown()
```

### 8. Logging层 (`src/pyrocketmq/logging/`) - 日志记录系统

**模块概述**: logging模块为pyrocketmq提供完整的日志记录功能，支持多种格式化器和灵活配置。包含JSON格式化器，支持结构化日志输出，便于日志分析和监控。

**核心组件**:
- **LoggingConfig**: 日志配置数据类，提供完整的日志配置选项
- **LoggerFactory**: Logger工厂类，统一创建和管理Logger实例
- **JSONFormatter**: JSON格式化器实现，支持结构化日志输出
- **ColorFormatter**: 彩色格式化器，提升开发体验

**关键特性**:
- 支持多种日志格式：文本、JSON、彩色输出
- 灵活的配置选项：级别、格式、输出目标
- 结构化日志支持，便于日志分析和监控
- 与Python标准logging完全兼容
- 提供便捷的创建函数和使用接口

**使用示例**:
```python
from pyrocketmq.logging import get_logger, LoggingConfig

# 获取Logger
logger = get_logger(__name__)

# 使用JSON格式化
config = LoggingConfig(
    level="INFO",
    format_type="json",
    output_file="app.log"
)
```

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

### Consumer使用模式
```python
# 并发消费者使用
from pyrocketmq.consumer import ConsumerConfig, create_consumer
from pyrocketmq.consumer.listener import MessageListenerConcurrently, ConsumeResult

class OrderProcessorListener(MessageListenerConcurrently):
    def consume_message_concurrently(self, messages, context):
        for message in messages:
            try:
                # 处理订单消息
                order_data = json.loads(message.body.decode())
                process_order(order_data)
                print(f"订单处理成功: {order_data['order_id']}")
            except Exception as e:
                print(f"订单处理失败: {e}")
                return ConsumeResult.RECONSUME_LATER  # 稍后重试
        
        return ConsumeResult.CONSUME_SUCCESS

# 创建消费者
config = ConsumerConfig(
    consumer_group="order_consumer_group",
    namesrv_addr="localhost:9876",
    message_model=MessageModel.CLUSTERING,
    consume_thread_max=40,  # 增加消费线程数
    pull_batch_size=16      # 批量拉取
)

consumer = create_consumer(config, OrderProcessorListener())
consumer.start()
consumer.subscribe("order_topic", "*")

# 顺序消费者使用（保证同一用户的消息顺序处理）
from pyrocketmq.consumer.listener import MessageListenerOrderly

class UserMessageListener(MessageListenerOrderly):
    def consume_message_orderly(self, messages, context):
        for message in messages:
            # 处理用户相关消息，保证顺序性
            user_id = message.get_property("user_id")
            process_user_message(user_id, message.body)
        
        return ConsumeResult.CONSUME_SUCCESS

# 广播模式消费者（每个消费者都收到所有消息）
broadcast_config = ConsumerConfig(
    consumer_group="notification_group",
    namesrv_addr="localhost:9876",
    message_model=MessageModel.BROADCASTING  # 广播模式
)
```

### 异步Consumer使用模式
```python
import asyncio
from pyrocketmq.consumer import ConsumerConfig
from pyrocketmq.consumer.async_listener import AsyncMessageListener, AsyncConsumeContext, ConsumeResult

# 异步消息监听器实现
class AsyncOrderProcessor(AsyncMessageListener):
    async def consume_message(self, messages: list[MessageExt], context: AsyncConsumeContext) -> ConsumeResult:
        """异步处理订单消息"""
        for message in messages:
            try:
                order_data = json.loads(message.body.decode())
                # 异步处理订单
                await process_order_async(order_data)
                print(f"异步订单处理成功: {order_data['order_id']}")
            except Exception as e:
                print(f"异步订单处理失败: {e}")
                return ConsumeResult.RECONSUME_LATER
        
        return ConsumeResult.CONSUME_SUCCESS

# 异步消费者使用示例
async def async_consumer_example():
    # 创建消费者配置
    config = ConsumerConfig(
        consumer_group="async_order_consumer_group",
        namesrv_addr="localhost:9876",
        message_model=MessageModel.CLUSTERING,
        consume_thread_max=20,  # 异步模式下线程数可适当减少
        pull_batch_size=32      # 批量拉取大小
    )
    
    # 创建异步消费者
    consumer = AsyncConcurrentConsumer(config, AsyncOrderProcessor())
    
    try:
        # 启动消费者
        await consumer.start()
        
        # 订阅主题
        await consumer.subscribe("async_order_topic", "*")
        
        # 保持运行
        while True:
            await asyncio.sleep(1)
            
    except KeyboardInterrupt:
        print("收到停止信号，正在关闭消费者...")
    finally:
        # 关闭消费者
        await consumer.shutdown()

# 运行异步消费者
asyncio.run(async_consumer_example())

# 异步顺序消费者（保证消息顺序性）
class AsyncUserMessageListener(AsyncMessageListener):
    async def consume_message(self, messages: list[MessageExt], context: AsyncConsumeContext) -> ConsumeResult:
        """异步顺序处理用户消息"""
        for message in messages:
            user_id = message.get_property("user_id")
            # 异步处理用户消息，保证同一用户的消息顺序
            await process_user_message_async(user_id, message.body)
        
        return ConsumeResult.CONSUME_SUCCESS

# 异步广播模式消费者
async def async_broadcast_consumer():
    config = ConsumerConfig(
        consumer_group="async_notification_group",
        namesrv_addr="localhost:9876",
        message_model=MessageModel.BROADCASTING  # 广播模式
    )
    
    consumer = AsyncConcurrentConsumer(config, AsyncOrderProcessor())
    await consumer.start()
    await consumer.subscribe("notification_topic", "*")
    
    # 运行一段时间后关闭
    await asyncio.sleep(60)
    await consumer.shutdown()
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
13. **Consumer模块**: 🚧 基础架构完成，核心Consumer实现中
    - **配置管理**: 支持完整的Consumer配置参数，包括线程数、批量大小、消费模式等
    - **消息监听器**: 支持并发消费(`MessageListenerConcurrently`)和顺序消费(`MessageListenerOrderly`)
    - **偏移量存储**: 集群模式使用RemoteOffsetStore存储在Broker，广播模式使用LocalOffsetStore存储在本地
    - **订阅管理**: 支持主题订阅、消息选择器和订阅冲突检测
    - **队列分配**: AverageAllocateStrategy实现平均分配算法，支持大规模分配优化
    - **消费起始位置**: 支持从最新、最早、指定时间戳三种起始位置开始消费
    - **异常处理**: 20+种专用异常类型，精确处理各种消费错误场景
    - **监控指标**: 全面的性能和状态监控，包括消费速率、成功率、延迟等

---

## 📚 文档维护信息

### 版本历史
- **v2.3** (2025-01-20): 异步Consumer核心功能完善
  - ✅ 完善AsyncBaseConsumer核心方法文档，包含路由管理、心跳机制、生命周期管理等
  - ✅ 新增异步Consumer详细使用模式，提供完整的异步消息监听器实现示例
  - ✅ 补充异步路由刷新、心跳发送、Broker地址收集等内部方法的详细说明
  - ✅ 优化`_collect_broker_addresses`方法，与同步版本保持完全一致的实现逻辑
  - ✅ 改进`_build_heartbeat_data`方法，使用标准HeartbeatData数据结构
  - ✅ 更新异步Consumer设计特点，强调容错处理、统计监控、协议兼容等特性
  - ✅ 添加异步并发消费者、顺序消费者、广播消费者的完整使用示例

- **v2.2** (2025-01-20): 异步Consumer架构完整实现
  - ✅ 新增AsyncBaseConsumer异步消费者抽象基类，支持完整异步生命周期管理
  - ✅ 实现AsyncMessageListener异步消息监听器体系，包含AsyncConsumeContext
  - ✅ 完成AsyncOffsetStoreFactory异步偏移量存储工厂，支持集群和广播模式
  - ✅ 新增AsyncConsumeFromWhereManager异步消费起始位置管理器
  - ✅ 完善异步Consumer模块文档，包含完整的API说明和使用示例
  - ✅ 更新项目状态，Consumer模块从"实现中"升级为"完整实现"
  - ✅ 统一同步和异步两套完整架构，满足不同应用场景需求

- **v2.1** (2025-01-07): Consumer模块文档补充
  - ✅ 新增Consumer层详细说明，包含完整的模块功能描述
  - ✅ 添加Consumer配置管理、消息监听器、偏移量存储等核心组件介绍
  - ✅ 更新项目状态，明确Consumer模块的开发进度
  - ✅ 补充Consumer使用模式和示例代码
  - ✅ 更新模块依赖关系图，包含Consumer层
  - ✅ 添加Consumer模块测试运行命令
  - ✅ 完善注意事项，添加Consumer相关说明

- **v2.0** (2025-01-04): 重大文档整合更新
  - ✅ 整合所有子模块CLAUDE.md文档
  - ✅ 新增完整的模块依赖关系图
  - ✅ 添加快速开始指南和使用示例
  - ✅ 补充Model、Transport、Remote、NameServer、Broker、Logging模块详细说明
  - ✅ 统一文档风格和结构
  - ✅ 增强API使用指导和最佳实践

- **v1.0** (2024-12-XX): 初始版本
  - 基础项目概述和架构说明
  - Producer模块核心功能介绍

### 文档结构
```
CLAUDE.md (项目级文档)
├── 项目概述
├── 开发环境配置
├── 核心架构
├── 模块依赖关系
├── 快速开始指南
├── 核心模块详解
│   ├── Model层 - 协议数据模型
│   ├── Transport层 - 网络传输基础设施
│   ├── Remote层 - 远程通信层
│   ├── NameServer层 - 注册发现客户端
│   ├── Broker层 - Broker通信客户端
│   ├── Producer层 - 消息生产者
│   └── Logging层 - 日志记录系统
├── 开发模式
├── 协议规范
├── 依赖管理
└── 注意事项
```

### 子模块文档
每个核心模块都有独立的详细文档，位于 `src/pyrocketmq/{module}/CLAUDE.md`：

- **Model模块**: 详细的协议数据结构、序列化机制、API文档
- **Transport模块**: 连接状态机、配置管理、异步实现细节
- **Remote模块**: 连接池管理、RPC通信、并发控制机制
- **NameServer模块**: 路由查询、集群管理、协议兼容性
- **Broker模块**: 消息收发、心跳管理、异常处理体系
- **Producer模块**: 高级路由、事务消息、配置管理、性能优化
- **Logging模块**: 格式化器、配置选项、结构化日志

### 使用建议
1. **初学者**: 先阅读本项目的快速开始指南，然后根据需要查阅具体模块文档
2. **开发者**: 以本文档为主要参考，深入了解时查阅对应模块的详细文档
3. **贡献者**: 确保代码变更同步更新到相应的模块文档和项目文档

### 文档维护
- **维护者**: pyrocketmq开发团队
- **更新频率**: 随代码版本发布同步更新
- **反馈渠道**: 通过GitHub Issues提交文档改进建议
- **一致性检查**: 定期进行文档与代码的一致性验证

---

**最后更新**: 2025-01-20
**文档版本**: v2.2
**项目状态**: ✅ 生产就绪，Consumer模块完整实现
