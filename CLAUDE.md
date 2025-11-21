# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概述

pyrocketmq是一个功能完整的Python实现的RocketMQ客户端库，基于RocketMQ TCP协议实现。项目提供高性能、可靠的RocketMQ消息队列客户端功能，完全兼容Go语言实现的协议规范。

### 开发环境配置

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
export PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src && python -m pytest tests/consumer/ -v

# 运行单个测试文件
export PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src && python -m pytest tests/model/test_serializer.py -v

# 运行单个测试方法
export PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src && python -m pytest tests/model/test_serializer.py::TestRemotingCommandSerializer::test_serialize_basic_command -v

# 运行异步测试
export PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src && python -m pytest tests/transport/ -v --asyncio-mode=auto
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
7. **工具支持层** (`utils/`): 读写锁、线程安全工具等
8. **日志系统层** (`logging/`): 统一的日志记录和管理系统

### 模块依赖关系

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
│                   工具支持层 (Utils)                           │
│  ┌─────────────────┐                                         │
│  │   SyncRWLock    │                                         │
│  └─────────────────┘                                         │
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
Producer → {BrokerManager, NameServerManager}
    ↓
BrokerManager/NameServerManager → {Remote, AsyncRemote}
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

**模块概述**: Consumer模块是pyrocketmq的消息消费者实现，提供完整的消息消费、订阅管理、偏移量存储和消息监听功能。采用分层架构设计，支持并发消费和顺序消费两种模式。

**核心组件**:
- **BaseConsumer**: 消费者抽象基类，定义生命周期管理
- **ConsumerConfig**: 消费者配置管理，支持完整的消费行为配置
- **消息监听器体系**: 
  - MessageListener、MessageListenerConcurrently、MessageListenerOrderly
- **偏移量存储系统**: 
  - RemoteOffsetStore(集群模式)、LocalOffsetStore(广播模式)
- **订阅管理器**: SubscriptionManager，管理主题订阅和选择器
- **队列分配策略**: AverageAllocateStrategy，实现平均分配算法
- **消费起始位置管理**: ConsumeFromWhereManager，支持三种起始位置策略

#### ConsumerConfig配置管理
- **基础配置**: consumer_group、namesrv_addr、message_model
- **消费行为**: consume_from_where、allocate_strategy、pull_batch_size
- **性能配置**: consume_thread_min/max、consume_timeout、pull_threshold
- **存储配置**: persist_interval、offset_store_path、auto_commit
- **高级配置**: message_trace、max_reconsume_times

#### 消息监听器体系
**监听器类型**:
- **MessageListener**: 基础监听器接口
- **MessageListenerConcurrently**: 并发消息监听器，支持多线程并行处理
- **MessageListenerOrderly**: 顺序消息监听器，保证消息顺序性
- **SimpleMessageListener**: 简单监听器实现，便于快速开发

#### 偏移量存储系统
**存储模式**:
- **RemoteOffsetStore**: 集群模式，偏移量存储在Broker端，支持多消费者协调
- **LocalOffsetStore**: 广播模式，偏移量存储在本地文件，每个消费者独立维护
- **OffsetStoreFactory**: 工厂模式创建存储实例

**偏移量存储特性**:
- 线程安全的偏移量更新和持久化
- 支持批量提交和定期持久化  
- 完整的指标收集和监控
- 原子性文件操作保证数据一致性

#### 订阅管理器
**核心功能**:
- 主题订阅和消息选择器管理
- 订阅冲突检测和处理
- 订阅数据的导入导出
- 指标收集和监控

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

### 8. Utils层 (`src/pyrocketmq/utils/`) - 工具支持

**模块概述**: Utils层为pyrocketmq提供通用的工具支持，包含线程安全的读写锁实现等基础组件。

**核心组件**:
- **SyncRWLock**: 线程安全的读写锁实现，支持高并发读写场景

**关键特性**:
- 线程安全的设计，支持多读者单写者模式
- 适用于Producer/Consumer等需要并发访问共享资源的场景
- 轻量级实现，性能优化

**使用示例**:
```python
from pyrocketmq.utils import SyncRWLock

# 创建读写锁
rw_lock = SyncRWLock()

# 读锁（多个读者可以同时持有）
with rw_lock.reader_lock():
    # 读取共享数据
    data = shared_data.read()

# 写锁（独占访问）
with rw_lock.writer_lock():
    # 修改共享数据
    shared_data.update(new_data)
```

### 9. Logging层 (`src/pyrocketmq/logging/`) - 日志记录系统

**模块概述**: logging模块为pyrocketmq提供完整的日志记录功能，支持多种格式化器和灵活配置。包含JSON格式化器，支持结构化日志输出，便于日志分析和监控。

**核心组件**:
- **LoggingConfig**: 日志配置数据类，提供完整的日志配置选项
- **LoggerFactory**: Logger工厂类，统一创建和管理Logger实例
- **JSONFormatter**: JSON格式化器实现，支持结构化日志输出

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
    producer = await create_async_producer("GID_POETRY", "nameserver:9876")
    await producer.start()

    message = Message(topic="test_topic", body=b"Hello, Async RocketMQ!")
    result = await producer.send(message)

asyncio.run(async_send())
```

### 事务消息发送模式
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

### 扩展自定义选择器
```python
from pyrocketmq.producer.topic_broker_mapping import QueueSelector

class CustomSelector(QueueSelector):
    def select(self, topic, available_queues, message=None):
        # 自定义选择逻辑
        # 例如基于broker负载、地域、消息大小等
        return available_queues[0] if available_queues else None
```

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
10. **事务消息**: 事务消息模块已完整实现，支持完整的事务生命周期管理
    - 使用`TransactionListener`接口定义本地事务逻辑
    - 支持三种事务状态：COMMIT_MESSAGE、ROLLBACK_MESSAGE、UNKNOWN
    - 提供`SimpleTransactionListener`用于测试场景
    - 包含完整的事务异常处理和超时管理
    - 便利函数简化事务消息创建和结果处理
    - `create_transaction_producer()` 创建事务Producer实例
    - `create_transaction_message()` 创建事务消息
    - `create_simple_transaction_listener()` 创建简单事务监听器
    - `create_transaction_send_result()` 创建事务发送结果
11. **Consumer模块**: 完整实现，提供完整的消费者功能
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
- **v1.0** (2025-01-04): 初始版本，基础项目概述和架构说明
- **v1.1** (2025-01-07): 补充Consumer模块详细说明和核心组件介绍
- **v1.2** (2025-01-07): 更新模块依赖关系图和测试运行命令
- **v1.3** (2025-01-07): 添加Utils层和Logging层详细说明
- **v1.4** (2025-01-07): 完善开发模式和使用示例
- **v1.5** (2025-01-07): 更新注意事项和最佳实践

### 文档结构
```
CLAUDE.md (项目级文档)
├── 项目概述
├── 开发环境配置
├── 核心架构
├── 模块依赖关系
├── 核心模块详解
│   ├── Model层 - 协议数据模型
│   ├── Transport层 - 网络传输基础设施
│   ├── Remote层 - 远程通信层
│   ├── NameServer层 - 注册发现客户端
│   ├── Broker层 - Broker通信客户端
│   ├── Producer层 - 消息生产者
│   ├── Consumer层 - 消息消费者
│   ├── Utils层 - 工具支持
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

**最后更新**: 2025-01-07
**文档版本**: v1.5
**项目状态**: ✅ 生产就绪，所有核心模块完整实现