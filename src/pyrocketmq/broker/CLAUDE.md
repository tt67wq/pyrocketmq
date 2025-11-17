# pyrocketmq Broker 模块技术文档

## 概述

`pyrocketmq.broker` 模块是 pyrocketmq 项目中与 RocketMQ Broker 通信的核心组件，提供完整的 Broker 客户端功能和连接管理。该模块采用分层架构设计，支持同步和异步两种编程模式，提供高效、可靠的 Broker 通信能力，完全兼容 RocketMQ TCP 协议规范。

## 核心功能

### 🔗 连接管理
- **多 Broker 支持**：同时管理多个 Broker 实例的连接
- **连接池管理**：基于 `ConnectionPool`/`AsyncConnectionPool` 实现连接的高效复用
- **健康检查**：实时监控 Broker 连接的健康状态
- **故障恢复**：自动检测连接异常和故障恢复机制

### 📡 消息通信
- **消息发送**：支持同步/异步、单向/响应、普通/批量消息发送
- **消息拉取**：完整的消息拉取功能，支持批量拉取和偏移量管理
- **心跳机制**：定期向 Broker 发送心跳，维持连接活跃状态
- **事务消息**：支持事务消息的完整生命周期管理

### 🏷️ 偏移量管理
- **消费者偏移量查询**：查询指定消费者组的消费偏移量
- **偏移量更新**：支持同步和异步偏移量更新
- **时间戳查询**：根据时间戳搜索对应的偏移量
- **最大偏移量查询**：获取队列的最大偏移量信息

### ⚡ 双模式支持
- **同步模式**：适用于简单同步应用场景
- **异步模式**：基于 asyncio 实现，适用于高并发异步应用场景

## 模块架构

### 分层架构设计

```
┌─────────────────────────────────────────────────────────────┐
│                    应用层 (Application)                        │
├─────────────────────────────────────────────────────────────┤
│              客户端层 (Broker Client)                          │
│  ┌─────────────────┐              ┌─────────────────┐         │
│  │  BrokerClient   │              │ AsyncBrokerClient│         │
│  │   (同步客户端)     │              │   (异步客户端)     │         │
│  └─────────────────┘              └─────────────────┘         │
├─────────────────────────────────────────────────────────────┤
│              管理层 (Broker Manager)                          │
│  ┌─────────────────┐              ┌─────────────────┐         │
│  │  BrokerManager  │              │AsyncBrokerManager│         │
│  │   (同步管理器)     │              │   (异步管理器)     │         │
│  └─────────────────┘              └─────────────────┘         │
├─────────────────────────────────────────────────────────────┤
│                  异常层 (Exceptions)                           │
│              20+种专用异常类型，精确处理各种错误场景               │
└─────────────────────────────────────────────────────────────┘
```

### 数据流向图

```
应用请求
    ↓
BrokerManager/AsyncBrokerManager (连接池管理)
    ↓
ConnectionPool/AsyncConnectionPool (连接复用)
    ↓
BrokerClient/AsyncBrokerClient (通信实现)
    ↓
Remote/AsyncRemote (远程通信层)
    ↓
Transport Layer (传输层)
    ↓
RocketMQ Broker
```

## 核心组件

### 1. BrokerClient - 同步客户端

**模块概述**: `BrokerClient` 是同步模式的 Broker 客户端实现，基于 `Remote` 类进行同步通信，提供与 Broker 交互的完整功能。

**核心功能**:
- **连接管理**: 建立和断开与 Broker 的连接
- **消息发送**: 支持普通发送、批量发送、单向发送
- **消息拉取**: 完整的消息拉取和偏移量管理功能
- **心跳维护**: 定期发送心跳保持连接活跃

**关键方法**:

```python
# 连接管理
def connect(self) -> None
def disconnect(self) -> None
@property
def is_connected(self) -> bool

# 消息发送
def sync_send_message(self, producer_group: str, body: bytes, mq: MessageQueue, properties: dict[str, str] | None = None, **kwargs: Any) -> SendMessageResult
def oneway_send_message(self, producer_group: str, body: bytes, mq: MessageQueue, properties: dict[str, str] | None = None, **kwargs: Any) -> None
def sync_batch_send_message(self, producer_group: str, body: bytes, mq: MessageQueue, properties: dict[str, str] | None = None, **kwargs: Any) -> SendMessageResult
def oneway_batch_send_message(self, producer_group: str, body: bytes, mq: MessageQueue, properties: dict[str, str] | None = None, **kwargs: Any) -> None

# 消息拉取和偏移量管理
def pull_message(self, consumer_group: str, topic: str, queue_id: int, queue_offset: int, max_msg_nums: int = 32, sys_flag: int = 0, commit_offset: int = 0, **kwargs: Any) -> PullMessageResult
def query_consumer_offset(self, consumer_group: str, topic: str, queue_id: int) -> int
def update_consumer_offset(self, consumer_group: str, topic: str, queue_id: int, commit_offset: int) -> None
def search_offset_by_timestamp(self, topic: str, queue_id: int, timestamp: int) -> int
def get_max_offset(self, topic: str, queue_id: int) -> int

# 心跳管理
def send_heartbeat(self, heartbeat_data: HeartbeatData) -> None
```

**使用示例**:
```python
from pyrocketmq.broker import create_broker_client
from pyrocketmq.model import MessageQueue

# 创建同步客户端
client = create_broker_client("localhost:9876")
client.connect()

# 发送消息
message_queue = MessageQueue(topic="test_topic", broker_name="broker1", queue_id=0)
result = client.sync_send_message("producer_group", b"Hello RocketMQ", message_queue)

# 拉取消息
pull_result = client.pull_message("consumer_group", "test_topic", 0, 0)

# 断开连接
client.disconnect()
```

### 2. AsyncBrokerClient - 异步客户端

**模块概述**: `AsyncBrokerClient` 是异步模式的 Broker 客户端实现，基于 `AsyncRemote` 类进行异步通信，专为高并发应用场景设计。

**关键特性**:
- **异步通信**: 所有 I/O 操作都是非阻塞的，提升并发性能
- **async/await 支持**: 完全支持 Python 的 async/await 语法
- **并发连接管理**: 支持同时处理多个异步请求

**使用示例**:
```python
import asyncio
from pyrocketmq.broker import create_async_broker_client

async def async_example():
    # 创建异步客户端
    client = create_async_broker_client("localhost:9876")
    await client.connect()

    # 异步发送消息
    result = await client.sync_send_message("producer_group", b"Async Message", message_queue)

    # 异步拉取消息
    pull_result = await client.pull_message("consumer_group", "test_topic", 0, 0)

    await client.disconnect()

# 运行异步示例
asyncio.run(async_example())
```

### 3. BrokerManager - 同步管理器

**模块概述**: `BrokerManager` 是同步模式的 Broker 连接管理器，负责管理多个 Broker 的连接池，提供统一的服务接口。

**核心功能**:
- **连接池管理**: 为每个 Broker 创建和维护独立的连接池
- **动态配置**: 支持运行时添加和移除 Broker
- **线程安全**: 使用锁机制确保多线程环境下的安全性
- **资源管理**: 统一的生命周期管理和资源清理

**关键方法**:

```python
def __init__(self, remote_config: RemoteConfig, transport_config: TransportConfig | None = None, max_consecutive_failures: int = 3, connection_pool_size: int = 5)
def start(self) -> None
def shutdown(self) -> None
def add_broker(self, broker_addr: str, broker_name: str | None = None) -> None
def remove_broker(self, broker_addr: str) -> None
def connection_pool(self, broker_addr: str) -> ConnectionPool | None
```

**使用示例**:
```python
from pyrocketmq.broker import BrokerManager
from pyrocketmq.remote.config import RemoteConfig

# 创建远程通信配置
remote_config = RemoteConfig(
    connect_timeout=5000.0,
    request_timeout=30000.0
)

# 创建管理器
manager = BrokerManager(
    remote_config=remote_config,
    connection_pool_size=10
)
manager.start()

# 添加 Broker
manager.add_broker("localhost:9876", "broker1")

# 获取连接池
pool = manager.connection_pool("localhost:9876")

# 关闭管理器
manager.shutdown()
```

### 4. AsyncBrokerManager - 异步管理器

**模块概述**: `AsyncBrokerManager` 是异步模式的 Broker 连接管理器，专为高并发异步应用场景设计。

**关键特性**:
- **异步连接池管理**: 基于 `AsyncConnectionPool` 实现异步连接池
- **并发安全**: 使用 `asyncio.Lock` 确保异步环境下的线程安全
- **高性能**: 支持大规模并发连接管理

**使用示例**:
```python
import asyncio
from pyrocketmq.broker import AsyncBrokerManager

async def async_manager_example():
    # 创建异步管理器
    manager = AsyncBrokerManager(remote_config, connection_pool_size=20)
    await manager.start()

    # 添加 Broker
    await manager.add_broker("localhost:9876", "broker1")

    # 获取异步连接池
    pool = await manager.connection_pool("localhost:9876")

    await manager.shutdown()

asyncio.run(async_manager_example())
```

### 5. 异常体系

**模块概述**: broker 模块定义了完整的异常体系，包含 9 种专用异常类型，用于精确处理与 Broker 交互时的各种错误场景。

**异常层次结构**:

```
BrokerError (基础异常)
├── BrokerConnectionError (连接错误)
├── BrokerTimeoutError (超时错误)
├── BrokerResponseError (响应错误)
├── BrokerProtocolError (协议错误)
├── AuthorizationError (授权异常)
├── BrokerBusyError (Broker繁忙异常)
├── MessagePullError (消息拉取异常)
├── OffsetError (偏移量异常)
└── BrokerSystemError (Broker系统异常)
```

**异常特性**:
- **结构化错误信息**: 每种异常都包含相关的上下文信息
- **错误代码支持**: 部分异常包含 RocketMQ 的错误代码
- **详细日志记录**: 完整的错误日志记录和监控

**使用示例**:
```python
from pyrocketmq.broker.errors import (
    BrokerConnectionError,
    MessagePullError,
    OffsetError
)

try:
    result = client.pull_message("consumer_group", "test_topic", 0, 0)
except BrokerConnectionError as e:
    print(f"连接失败: {e}, Broker地址: {e.broker_address}")
except MessagePullError as e:
    print(f"消息拉取失败: {e}, Topic: {e.topic}, QueueId: {e.queue_id}")
except OffsetError as e:
    print(f"偏移量错误: {e}, Topic: {e.topic}, QueueId: {e.queue_id}")
```

## 便捷函数

### 客户端创建函数

```python
def create_broker_client(broker_addr: str, timeout: float = 30.0) -> BrokerClient:
    """创建同步Broker客户端的便捷函数
    
    Args:
        broker_addr: Broker地址，格式为"host:port"
        timeout: 请求超时时间，默认30秒
        
    Returns:
        BrokerClient: 配置好的同步客户端实例
    """
    # 解析地址
    host, port = broker_addr.split(":")
    
    # 创建配置
    transport_config = TransportConfig(host=host, port=port)
    remote_config = RemoteConfig(request_timeout=timeout)
    
    # 创建Remote实例
    remote = Remote(
        config=remote_config,
        transport_config=transport_config
    )
    
    return BrokerClient(remote, timeout)

def create_async_broker_client(broker_addr: str, timeout: float = 30.0) -> AsyncBrokerClient:
    """创建异步Broker客户端的便捷函数
    
    Args:
        broker_addr: Broker地址，格式为"host:port"
        timeout: 请求超时时间，默认30秒
        
    Returns:
        AsyncBrokerClient: 配置好的异步客户端实例
    """
    # 解析地址
    host, port = broker_addr.split(":")
    
    # 创建配置
    transport_config = TransportConfig(host=host, port=port)
    remote_config = RemoteConfig(request_timeout=timeout)
    
    # 创建AsyncRemote实例
    remote = AsyncRemote(
        config=remote_config,
        transport_config=transport_config
    )
    
    return AsyncBrokerClient(remote, timeout)
```

### 管理器创建函数

```python
def create_broker_manager(namesrv_addr: str, **kwargs) -> BrokerManager:
    """创建同步Broker管理器的便捷函数
    
    Args:
        namesrv_addr: NameServer地址列表，用分号分隔
        **kwargs: 其他配置参数
        
    Returns:
        BrokerManager: 配置好的同步管理器实例
    """
    # 使用默认配置创建管理器
    remote_config = RemoteConfig(**kwargs)
    
    return BrokerManager(
        remote_config=remote_config,
        **kwargs
    )
```

## 使用模式

### 1. 基础通信模式

```python
from pyrocketmq.broker import create_broker_client

# 创建客户端
client = create_broker_client("localhost:9876")

# 建立连接
client.connect()

try:
    # 发送消息
    result = client.sync_send_message(
        producer_group="test_group",
        body=b"Hello RocketMQ",
        mq=message_queue,
        properties={"KEYS": "order_123"}
    )
    print(f"消息发送成功: {result.msg_id}")
    
finally:
    # 断开连接
    client.disconnect()
```

### 2. 高并发异步模式

```python
import asyncio
from pyrocketmq.broker import create_async_broker_client

async def async_concurrent_example():
    # 创建异步客户端
    client = create_async_broker_client("localhost:9876")
    await client.connect()
    
    # 并发发送多个消息
    tasks = []
    for i in range(10):
        task = client.sync_send_message(
            producer_group="async_group",
            body=f"Async Message {i}".encode(),
            mq=message_queue
        )
        tasks.append(task)
    
    # 等待所有消息发送完成
    results = await asyncio.gather(*tasks)
    print(f"批量发送完成，成功 {len(results)} 条消息")
    
    await client.disconnect()

asyncio.run(async_concurrent_example())
```

### 3. 连接池管理模式

```python
from pyrocketmq.broker import BrokerManager
from pyrocketmq.broker.client import create_broker_client

# 创建管理器
manager = BrokerManager(remote_config, connection_pool_size=10)
manager.start()

# 添加多个Broker
manager.add_broker("broker1:9876", "broker1")
manager.add_broker("broker2:9876", "broker2")

# 使用连接池
pool = manager.connection_pool("broker1:9876")
if pool:
    with pool.get_connection() as connection:
        # 使用连接进行通信
        client = create_broker_client("broker1:9876")
        result = client.sync_send_message("group", b"Hello", mq)

manager.shutdown()
```

### 4. 批量操作模式

```python
# 批量发送消息
batch_messages = [
    {"body": b"Message 1", "properties": {"KEYS": "msg1"}},
    {"body": b"Message 2", "properties": {"KEYS": "msg2"}},
    {"body": b"Message 3", "properties": {"KEYS": "msg3"}}
]

# 编码批量消息
batch_body = encode_batch_messages(batch_messages)

# 发送批量消息
result = client.sync_batch_send_message(
    producer_group="batch_group",
    body=batch_body,
    mq=message_queue
)

print(f"批量发送成功: {result.msg_id}")
```

### 5. 消费者偏移量管理模式

```python
# 查询当前偏移量
try:
    current_offset = client.query_consumer_offset("consumer_group", "test_topic", 0)
    print(f"当前偏移量: {current_offset}")
except OffsetError as e:
    if e.topic and e.queue_id is not None:
        print(f"偏移量不存在，从0开始: Topic={e.topic}, QueueId={e.queue_id}")
        current_offset = 0

# 拉取消息
pull_result = client.pull_message(
    consumer_group="consumer_group",
    topic="test_topic",
    queue_id=0,
    queue_offset=current_offset,
    max_msg_nums=32
)

# 处理消息后更新偏移量
new_offset = current_offset + len(pull_result.messages)
client.update_consumer_offset("consumer_group", "test_topic", 0, new_offset)
```

### 6. 错误处理模式

```python
from pyrocketmq.broker.errors import (
    BrokerConnectionError,
    BrokerTimeoutError,
    MessagePullError
)

def robust_client_example():
    client = create_broker_client("localhost:9876")
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            client.connect()
            break
        except BrokerConnectionError as e:
            if attempt == max_retries - 1:
                raise
            print(f"连接失败，重试 {attempt + 1}/{max_retries}: {e}")
            time.sleep(2 ** attempt)  # 指数退避
    
    try:
        result = client.sync_send_message("group", b"Hello", mq)
        return result
    except BrokerTimeoutError as e:
        print(f"发送超时: {e}")
        raise
    except BrokerConnectionError as e:
        print(f"连接断开: {e}")
        # 尝试重连
        client.connect()
        return client.sync_send_message("group", b"Hello", mq)
    finally:
        client.disconnect()
```

### 7. 监控和日志模式

```python
import logging
from pyrocketmq.logging import get_logger

# 设置日志级别
logging.basicConfig(level=logging.INFO)
logger = get_logger(__name__)

# 创建客户端时会自动记录详细日志
client = create_broker_client("localhost:9876")

# 所有操作都会记录结构化日志
client.connect()  # 记录连接日志
result = client.sync_send_message("group", b"Hello", mq)  # 记录发送日志
client.disconnect()  # 记录断开日志

# 日志包含丰富的上下文信息
# - client_id: 客户端唯一标识
# - operation_type: 操作类型
# - timestamp: 时间戳
# - status: 操作状态
# - execution_time: 执行时间
# - error_message: 错误信息（如果失败）
```

## 依赖项

### 必需依赖

| 模块 | 版本要求 | 说明 |
|------|----------|------|
| `logging` | Python 标准库 | 日志记录 |
| `threading` | Python 标准库 | 线程同步（同步模式） |
| `asyncio` | Python 标准库 | 异步I/O（异步模式） |
| `time` | Python 标准库 | 时间处理 |
| `uuid` | Python 标准库 | 唯一标识生成 |
| `json` | Python 标准库 | JSON序列化 |
| `typing` | Python 标准库 | 类型注解 |

### 项目内依赖

| 模块 | 说明 |
|------|------|
| `pyrocketmq.logging` | 项目日志系统 |
| `pyrocketmq.remote.config` | 远程通信配置 |
| `pyrocketmq.remote.sync_remote` | 同步远程通信 |
| `pyrocketmq.remote.async_remote` | 异步远程通信 |
| `pyrocketmq.remote.pool` | 连接池实现 |
| `pyrocketmq.transport.config` | 传输层配置 |
| `pyrocketmq.model` | 数据模型和枚举 |
| `pyrocketmq.model.factory` | 请求工厂 |

## 性能优化

### 连接池配置

```python
# 高性能场景配置
manager = BrokerManager(
    remote_config=remote_config,
    connection_pool_size=20,  # 更大的连接池
    max_consecutive_failures=2  # 更快的故障检测
)

# 低延迟场景配置
remote_config = RemoteConfig(
    connect_timeout=1000.0,    # 1秒连接超时
    request_timeout=5000.0     # 5秒请求超时
)
```

### 异步模式优化

```python
# 大规模并发配置
async_manager = AsyncBrokerManager(
    remote_config=remote_config,
    connection_pool_size=50  # 更大的异步连接池
)

# 批量操作优化
async def batch_send_async(client, messages):
    """批量异步发送消息"""
    tasks = [
        client.sync_send_message(msg.body, msg.queue, msg.properties)
        for msg in messages
    ]
    return await asyncio.gather(*tasks)
```

### 内存使用优化

```python
# 合理的批量大小
MAX_BATCH_SIZE = 64  # 推荐32-64

# 连接池大小控制
CONNECTION_POOL_SIZE = min(20, cpu_count() * 2)

# 超时时间设置
TIMEOUT_CONFIG = {
    "connect_timeout": 5000.0,   # 5秒连接超时
    "request_timeout": 30000.0,  # 30秒请求超时
    "idle_timeout": 60000.0      # 60秒空闲超时
}
```

## 最佳实践

### 1. 连接管理
- **及时释放连接**: 使用 `with` 语句或 `try-finally` 确保连接正确释放
- **连接池复用**: 优先使用 BrokerManager 进行连接池管理
- **健康检查**: 定期检查连接状态，及时清理失效连接

### 2. 错误处理
- **分类处理**: 针对不同类型的异常采用不同的处理策略
- **重试机制**: 对网络错误实现指数退避重试
- **降级策略**: 在 Broker 不可用时实现服务降级

### 3. 性能优化
- **批量操作**: 尽可能使用批量发送和拉取
- **异步优先**: 在高并发场景下优先使用异步模式
- **连接池调优**: 根据实际负载调整连接池大小

### 4. 监控和日志
- **结构化日志**: 利用内置的结构化日志进行监控
- **性能指标**: 关注连接创建、销毁和使用频率等指标
- **异常监控**: 监控各类异常的发生频率和模式

## 版本变更记录

### v3.0.0 (当前版本)
**发布日期**: 2025-01-17

#### 🔥 重大变更
- **模块重构**: 整合了原有的分散模块，统一为 `pyrocketmq.broker` 模块
- **双模式支持**: 同时提供同步和异步两种完整的客户端实现
- **异常体系完善**: 新增 9 种专用异常类型，提供精确的错误处理

#### ✨ 新增功能
- **AsyncBrokerClient**: 完整的异步客户端实现
- **AsyncBrokerManager**: 异步连接管理器
- **便捷函数**: 提供 `create_broker_client()` 和 `create_async_broker_client()` 便捷创建函数
- **完整日志**: 所有操作都包含详细的结构化日志记录

#### 🛠️ 改进
- **性能提升**: 异步模式支持大规模并发操作
- **资源管理**: 优化的连接池管理和资源清理机制
- **错误处理**: 更完善的异常分类和错误恢复机制
- **类型安全**: 完整的类型注解和参数验证

#### 📝 文档更新
- 完整的模块架构设计文档
- 详细的使用示例和最佳实践
- 性能优化建议和配置指南
- 异常处理指南和错误码说明

### v2.0.0 (重构版本)
- **架构重构**: 基于 `ConnectionPool` 重新设计 `BrokerManager`
- **代码简化**: 移除冗余的连接管理逻辑
- **API 统一**: 直接使用标准的 `ConnectionPool` 接口

### v1.x.x (历史版本)
- 初始版本实现
- 基础的 Broker 客户端功能
- 同步模式支持

---

**最后更新**: 2025-01-17  
**文档版本**: v3.0.0  
**维护状态**: ✅ 活跃维护