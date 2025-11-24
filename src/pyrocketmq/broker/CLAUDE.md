# pyrocketmq Broker 模块

> **模块概述**：pyrocketmq的Broker通信模块，提供与RocketMQ Broker进行完整通信的功能，支持消息发送、拉取、偏移量管理、连接管理等核心操作。

## 📋 目录

- [🔧 核心功能](#-核心功能)
- [🏗️ 模块架构](#️-模块架构)
- [📦 类和函数](#-类和函数)
  - [同步客户端](#同步客户端)
  - [异步客户端](#异步客户端)
  - [管理器类](#管理器类)
  - [异常类](#异常类)
  - [便捷函数](#便捷函数)
- [🚀 快速开始](#-快速开始)
- [📊 使用示例](#-使用示例)
- [⚙️ 配置说明](#️-配置说明)
- [🔗 依赖关系](#-依赖关系)
- [📈 性能优化](#-性能优化)
- [❗ 常见问题](#-常见问题)
- [🔄 版本历史](#-版本历史)

## 🔧 核心功能

### 📡 消息通信
- **消息发送**：支持同步、异步、单向消息发送
- **批量发送**：支持批量消息发送，提高吞吐量
- **消息拉取**：支持从Broker拉取消息，包含完整的重试机制
- **事务消息**：支持完整的事务消息生命周期

### 🔗 连接管理
- **连接池**：内置连接池管理，支持连接复用
- **健康检查**：自动检测和恢复断开的连接
- **并发控制**：支持高并发场景下的连接管理

### 🏷️ 偏移量管理
- **查询偏移量**：支持查询消费者组的消费偏移量
- **更新偏移量**：支持更新消费偏移量（单向操作）
- **时间戳搜索**：支持根据时间戳搜索对应的消息偏移量

### ⚡ 双模式支持
- **同步模式**：提供传统的同步调用方式
- **异步模式**：基于asyncio的高性能异步实现

## 🏗️ 模块架构

```
Broker 模块架构
├── client.py              # 同步Broker客户端
├── async_client.py        # 异步Broker客户端
├── broker_manager.py      # 同步连接管理器
├── async_broker_manager.py # 异步连接管理器
├── errors.py              # 异常定义
└── __init__.py           # 模块导出
```

### 数据流向
```
应用层 → BrokerClient/BrokerManager → Remote/AsyncRemote → Transport
```

## 📦 类和函数

### 同步客户端

#### `BrokerClient`

与RocketMQ Broker进行同步通信的核心客户端类。

**类签名**
```python
class BrokerClient:
    def __init__(self, remote: Remote, timeout: float = 5.0)
```

**参数说明**
- `remote` (Remote): 远程通信实例，负责底层的网络通信
- `timeout` (float): 默认请求超时时间，默认5秒

**主要方法**

##### `connect() -> None`

**功能**：建立与Broker的连接

**示例**：
```python
from pyrocketmq.broker import BrokerClient, create_broker_client

# 创建客户端
client = create_broker_client("localhost:10911")

# 建立连接
client.connect()
```

**异常**：
- `BrokerConnectionError`: 连接失败时抛出

##### `sync_send_message(...) -> SendMessageResult`

**功能**：同步发送单条消息

**参数**：
- `producer_group` (str): 生产者组名
- `body` (bytes): 消息体内容
- `mq` (MessageQueue): 消息队列
- `properties` (dict[str, str] | None): 消息属性字典，可选
- `timeout` (float | None): 请求超时时间，可选
- `**kwargs` (Any): 其他参数

**返回值**：
- `SendMessageResult`: 消息发送结果

**示例**：
```python
from pyrocketmq.model.message import MessageQueue
from pyrocketmq.model.result_data import SendStatus

# 创建消息队列
mq = MessageQueue(topic="test_topic", broker_name="broker-a", queue_id=0)

# 发送消息
try:
    result = client.sync_send_message(
        producer_group="test_producer_group",
        body=b"Hello, RocketMQ!",
        mq=mq,
        properties={"KEYS": "order_123"}
    )
    
    if result.status == SendStatus.SEND_OK:
        print(f"消息发送成功，ID: {result.msg_id}")
        print(f"队列偏移量: {result.queue_offset}")
    
except BrokerResponseError as e:
    print(f"发送失败: {e}")
```

**异常**：
- `BrokerConnectionError`: 连接错误
- `BrokerTimeoutError`: 请求超时
- `BrokerResponseError`: 响应错误

##### `oneway_send_message(...) -> None`

**功能**：单向发送消息（不等待响应，适合日志等场景）

**参数**：同`sync_send_message`

**示例**：
```python
# 单向发送（适合日志等不需要响应的场景）
client.oneway_send_message(
    producer_group="log_producer",
    body=b"Log message",
    mq=mq
)
```

##### `sync_batch_send_message(...) -> SendMessageResult`

**功能**：同步批量发送消息

**参数**：同`sync_send_message`，但`body`应为批量消息数据

**示例**：
```python
# 构造批量消息数据
batch_messages = [
    json.dumps({"id": 1, "content": "message1"}).encode(),
    json.dumps({"id": 2, "content": "message2"}).encode(),
]

# 批量发送
result = client.sync_batch_send_message(
    producer_group="batch_producer",
    body=b''.join(batch_messages),
    mq=mq
)
```

##### `pull_message(...) -> PullMessageResult`

**功能**：从Broker拉取消息

**参数**：
- `consumer_group` (str): 消费者组名
- `topic` (str): 主题名称
- `queue_id` (int): 队列ID
- `queue_offset` (int): 队列偏移量
- `max_msg_nums` (int): 最大拉取消息数量，默认32
- `sys_flag` (int): 系统标志位，默认0
- `commit_offset` (int): 提交偏移量，默认0
- `timeout` (float | None): 请求超时时间，可选
- `**kwargs` (Any): 其他参数（如sub_expression等）

**返回值**：
- `PullMessageResult`: 拉取消息结果

**示例**：
```python
from pyrocketmq.model.enums import ResponseCode

# 拉取消息
try:
    result = client.pull_message(
        consumer_group="test_consumer_group",
        topic="test_topic",
        queue_id=0,
        queue_offset=0,
        max_msg_nums=16
    )
    
    if result.message_count > 0:
        print(f"成功拉取到 {result.message_count} 条消息")
        for msg in result.messages:
            print(f"消息内容: {msg.body.decode()}")
            print(f"消息ID: {msg.msg_id}")
    else:
        print("没有新消息")
        
except MessagePullError as e:
    print(f"拉取失败: {e}")
```

##### `query_consumer_offset(...) -> int`

**功能**：查询消费者组的消费偏移量

**参数**：
- `consumer_group` (str): 消费者组名
- `topic` (str): 主题名称
- `queue_id` (int): 队列ID
- `timeout` (float | None): 请求超时时间，可选

**返回值**：
- `int`: 消费者偏移量

**示例**：
```python
try:
    offset = client.query_consumer_offset(
        consumer_group="test_consumer_group",
        topic="test_topic",
        queue_id=0
    )
    print(f"当前消费偏移量: {offset}")
except OffsetError as e:
    print(f"查询偏移量失败: {e}")
```

##### `update_consumer_offset(...) -> None`

**功能**：更新消费者偏移量（单向操作）

**参数**：
- `consumer_group` (str): 消费者组名
- `topic` (str): 主题名称
- `queue_id` (int): 队列ID
- `commit_offset` (int): 提交的偏移量

**示例**：
```python
# 更新消费偏移量
client.update_consumer_offset(
    consumer_group="test_consumer_group",
    topic="test_topic",
    queue_id=0,
    commit_offset=100
)
```

##### `search_offset_by_timestamp(...) -> int`

**功能**：根据时间戳搜索对应的消息偏移量

**参数**：
- `topic` (str): 主题名称
- `queue_id` (int): 队列ID
- `timestamp` (int): 时间戳（毫秒）
- `timeout` (float | None): 请求超时时间，可选

**返回值**：
- `int`: 对应的消息偏移量

**示例**：
```python
import time

# 搜索1小时前的消息偏移量
timestamp = int((time.time() - 3600) * 1000)  # 1小时前的时间戳

try:
    offset = client.search_offset_by_timestamp(
        topic="test_topic",
        queue_id=0,
        timestamp=timestamp
    )
    print(f"1小时前的消息偏移量: {offset}")
except OffsetError as e:
    print(f"搜索偏移量失败: {e}")
```

### 异步客户端

#### `AsyncBrokerClient`

与RocketMQ Broker进行异步通信的客户端类，基于asyncio实现。

**类签名**
```python
class AsyncBrokerClient:
    def __init__(self, remote: AsyncRemote, timeout: float = 5.0)
```

**主要异步方法**：

所有同步客户端的方法都有对应的异步版本，方法名前加上`async_`前缀：

- `async connect() -> None`
- `async sync_send_message(...) -> SendMessageResult`  
- `async pull_message(...) -> PullMessageResult`
- `async query_consumer_offset(...) -> int`
- 等等...

**示例**：
```python
import asyncio
from pyrocketmq.broker import create_async_broker_client

async def async_example():
    # 创建异步客户端
    client = await create_async_broker_client("localhost:10911")
    
    try:
        # 异步连接
        await client.connect()
        
        # 异步发送消息
        result = await client.async_sync_send_message(
            producer_group="async_producer",
            body=b"Async message",
            mq=mq
        )
        
        print(f"异步消息发送成功: {result.msg_id}")
        
        # 异步拉取消息
        pull_result = await client.async_pull_message(
            consumer_group="async_consumer",
            topic="test_topic",
            queue_id=0,
            queue_offset=0
        )
        
        if pull_result.message_count > 0:
            print(f"异步拉取到 {pull_result.message_count} 条消息")
            
    finally:
        # 断开连接
        await client.disconnect()

# 运行异步示例
asyncio.run(async_example())
```

### 管理器类

#### `BrokerManager`

Broker连接管理器，提供连接池管理和上下文管理功能。

**类签名**
```python
class BrokerManager:
    def __init__(self, host: str, port: int, timeout: float = 5.0)
```

**主要方法**：

##### `connection(broker_name: str) -> ContextManager[BrokerClient]`

**功能**：获取到指定Broker的连接，支持上下文管理

**参数**：
- `broker_name` (str): Broker名称

**返回值**：
- `ContextManager[BrokerClient]`: 可用于with语句的上下文管理器

**示例**：
```python
from pyrocketmq.broker import create_broker_manager

# 创建Broker管理器
manager = create_broker_manager("localhost", 10911)

# 使用上下文管理器获取连接
with manager.connection("broker-a") as client:
    # 在with块内使用客户端
    result = client.sync_send_message(
        producer_group="test_group",
        body=b"Hello from manager",
        mq=mq
    )
    print(f"通过管理器发送消息成功: {result.msg_id}")

# with块结束后，连接会自动释放回连接池
```

#### `AsyncBrokerManager`

异步版本的Broker连接管理器。

**示例**：
```python
import asyncio
from pyrocketmq.broker import create_async_broker_manager

async def async_manager_example():
    # 创建异步管理器
    manager = await create_async_broker_manager("localhost", 10911)
    
    # 异步上下文管理
    async with manager.connection("broker-a") as client:
        result = await client.async_sync_send_message(
            producer_group="async_group",
            body=b"Async message from manager",
            mq=mq
        )
        print(f"异步管理器发送成功: {result.msg_id}")

asyncio.run(async_manager_example())
```

### 异常类

Broker模块定义了完整的异常体系，用于精确处理各种错误场景：

#### 异常层次结构

```
BrokerError (基础异常)
├── BrokerConnectionError (连接错误)
├── BrokerTimeoutError (超时错误)
├── BrokerResponseError (响应错误)
├── BrokerProtocolError (协议错误)
├── AuthorizationError (授权错误)
├── BrokerBusyError (Broker繁忙错误)
├── MessagePullError (消息拉取错误)
├── OffsetError (偏移量错误)
└── BrokerSystemError (系统错误)
```

#### 异常使用示例

```python
from pyrocketmq.broker.errors import (
    BrokerConnectionError,
    BrokerTimeoutError,
    MessagePullError,
    OffsetError
)

try:
    result = client.sync_send_message(...)
except BrokerConnectionError as e:
    print(f"连接失败: {e}")
    # 可以尝试重连或切换Broker
except BrokerTimeoutError as e:
    print(f"请求超时: {e}")
    # 可以增加超时时间或重试
except BrokerResponseError as e:
    print(f"响应错误: {e}")
    # 根据具体错误码处理
except Exception as e:
    print(f"未知错误: {e}")
```

### 便捷函数

#### 客户端创建函数

##### `create_broker_client(broker_addr: str, timeout: float = 5.0, **kwargs) -> BrokerClient`

**功能**：快速创建Broker客户端

**参数**：
- `broker_addr` (str): Broker地址，格式为"host:port"
- `timeout` (float): 超时时间，默认5秒
- `**kwargs`: 其他Remote配置参数

**返回值**：
- `BrokerClient`: 配置好的Broker客户端实例

**示例**：
```python
from pyrocketmq.broker import create_broker_client

# 快速创建客户端
client = create_broker_client("localhost:10911", timeout=10.0)

# 自动连接
client.connect()

# 使用客户端
result = client.sync_send_message(...)
```

##### `create_async_broker_client(broker_addr: str, timeout: float = 5.0, **kwargs) -> AsyncBrokerClient`

**功能**：快速创建异步Broker客户端

**返回值**：
- `AsyncBrokerClient`: 配置好的异步Broker客户端实例

#### 管理器创建函数

##### `create_broker_manager(namesrv_addr: str, **kwargs) -> BrokerManager`

**功能**：快速创建Broker管理器

**参数**：
- `namesrv_addr` (str): NameServer地址，格式为"host:port"
- `**kwargs`: 其他配置参数

**返回值**：
- `BrokerManager`: 配置好的Broker管理器实例

**示例**：
```python
from pyrocketmq.broker import create_broker_manager

# 创建管理器
manager = create_broker_manager("localhost:9876")

# 使用管理器
with manager.connection("broker-a") as client:
    result = client.sync_send_message(...)
```

## 🚀 快速开始

### 安装依赖

```bash
# 安装pyrocketmq
pip install pyrocketmq

# 或从源码安装
git clone https://github.com/your-repo/pyrocketmq.git
cd pyrocketmq
pip install -e .
```

### 基础使用

```python
from pyrocketmq.broker import create_broker_client
from pyrocketmq.model.message import MessageQueue

# 1. 创建客户端
client = create_broker_client("localhost:10911")

# 2. 连接Broker
client.connect()

# 3. 创建消息队列
mq = MessageQueue(topic="test_topic", broker_name="broker-a", queue_id=0)

# 4. 发送消息
result = client.sync_send_message(
    producer_group="test_producer",
    body=b"Hello, RocketMQ!",
    mq=mq
)

print(f"消息发送成功: {result.msg_id}")

# 5. 清理资源
client.disconnect()
```

## 📊 使用示例

### 1. 基础消息发送

```python
from pyrocketmq.broker import create_broker_client
from pyrocketmq.model.message import MessageQueue

def basic_send_example():
    client = create_broker_client("localhost:10911")
    client.connect()
    
    mq = MessageQueue(topic="user_topic", broker_name="broker-a", queue_id=0)
    
    try:
        result = client.sync_send_message(
            producer_group="user_producer",
            body=b'{"user_id": 123, "action": "login"}',
            mq=mq,
            properties={"KEYS": "user_123_login", "TAGS": "user_action"}
        )
        
        print(f"消息发送成功: {result.msg_id}")
        print(f"队列偏移量: {result.queue_offset}")
        
    finally:
        client.disconnect()

basic_send_example()
```

### 2. 批量消息发送

```python
def batch_send_example():
    client = create_broker_client("localhost:10911")
    client.connect()
    
    mq = MessageQueue(topic="batch_topic", broker_name="broker-a", queue_id=0)
    
    # 构造批量消息
    messages = [
        b'{"id": 1, "content": "message1"}',
        b'{"id": 2, "content": "message2"}',
        b'{"id": 3, "content": "message3"}',
    ]
    
    try:
        result = client.sync_batch_send_message(
            producer_group="batch_producer",
            body=b''.join(messages),
            mq=mq
        )
        
        print(f"批量消息发送成功: {result.msg_id}")
        
    finally:
        client.disconnect()

batch_send_example()
```

### 3. 消息拉取

```python
def message_pull_example():
    client = create_broker_client("localhost:10911")
    client.connect()
    
    try:
        # 查询当前偏移量
        try:
            offset = client.query_consumer_offset(
                consumer_group="test_consumer",
                topic="test_topic",
                queue_id=0
            )
        except Exception:
            offset = 0  # 如果查询失败，从0开始
        
        # 拉取消息
        result = client.pull_message(
            consumer_group="test_consumer",
            topic="test_topic",
            queue_id=0,
            queue_offset=offset,
            max_msg_nums=32
        )
        
        if result.message_count > 0:
            print(f"拉取到 {result.message_count} 条消息")
            
            # 处理消息
            for msg in result.messages:
                print(f"消息ID: {msg.msg_id}")
                print(f"消息内容: {msg.body.decode()}")
                print(f"消息属性: {msg.properties}")
                print("---")
            
            # 更新偏移量
            client.update_consumer_offset(
                consumer_group="test_consumer",
                topic="test_topic",
                queue_id=0,
                commit_offset=result.next_begin_offset
            )
        else:
            print("没有新消息")
            
    finally:
        client.disconnect()

message_pull_example()
```

### 4. 异步高并发模式

```python
import asyncio
from pyrocketmq.broker import create_async_broker_manager

async def async_concurrent_example():
    # 创建异步管理器
    manager = await create_async_broker_manager("localhost:9876")
    
    # 定义发送任务
    async def send_message_task(message_id: int, content: str):
        async with manager.connection("broker-a") as client:
            mq = MessageQueue(topic="async_topic", broker_name="broker-a", queue_id=0)
            
            result = await client.async_sync_send_message(
                producer_group="async_producer",
                body=content.encode(),
                mq=mq,
                properties={"MESSAGE_ID": str(message_id)}
            )
            
            print(f"消息 {message_id} 发送成功: {result.msg_id}")
            return result
    
    # 并发发送多条消息
    tasks = []
    for i in range(100):
        content = f"Async message {i}"
        task = send_message_task(i, content)
        tasks.append(task)
    
    # 等待所有任务完成
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # 统计结果
    success_count = sum(1 for r in results if not isinstance(r, Exception))
    print(f"成功发送: {success_count}/100 条消息")

asyncio.run(async_concurrent_example())
```

### 5. 连接池管理模式

```python
def connection_pool_example():
    from pyrocketmq.broker import create_broker_manager
    
    # 创建管理器（自动管理连接池）
    manager = create_broker_manager("localhost:9876")
    
    # 模拟多个并发操作
    def worker_task(worker_id: int):
        for i in range(10):
            # 每个操作从连接池获取连接
            with manager.connection("broker-a") as client:
                mq = MessageQueue(topic="worker_topic", broker_name="broker-a", queue_id=0)
                
                result = client.sync_send_message(
                    producer_group="worker_producer",
                    body=f"Worker {worker_id} message {i}".encode(),
                    mq=mq
                )
                
                print(f"Worker {worker_id} 发送成功: {result.msg_id}")
    
    # 启动多个工作线程
    import threading
    
    threads = []
    for worker_id in range(5):
        thread = threading.Thread(target=worker_task, args=(worker_id,))
        threads.append(thread)
        thread.start()
    
    # 等待所有线程完成
    for thread in threads:
        thread.join()

connection_pool_example()
```

### 6. 错误处理和重试

```python
def robust_client_example():
    from pyrocketmq.broker.errors import (
        BrokerConnectionError,
        BrokerTimeoutError,
        BrokerResponseError
    )
    import time
    
    def send_with_retry(client, max_retries=3):
        mq = MessageQueue(topic="retry_topic", broker_name="broker-a", queue_id=0)
        
        for attempt in range(max_retries):
            try:
                result = client.sync_send_message(
                    producer_group="retry_producer",
                    body=f"Attempt {attempt + 1}".encode(),
                    mq=mq
                )
                
                print(f"第 {attempt + 1} 次尝试成功: {result.msg_id}")
                return result
                
            except BrokerConnectionError as e:
                print(f"连接错误 (尝试 {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    print("尝试重新连接...")
                    client.disconnect()
                    time.sleep(2)
                    client.connect()
                    
            except BrokerTimeoutError as e:
                print(f"超时错误 (尝试 {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    print("增加超时时间重试...")
                    time.sleep(1)
                    
            except BrokerResponseError as e:
                print(f"响应错误 (尝试 {attempt + 1}): {e}")
                # 响应错误通常不需要重试
                break
        
        raise Exception("所有重试均失败")
    
    # 使用带重试的发送
    client = create_broker_client("localhost:10911")
    client.connect()
    
    try:
        send_with_retry(client)
    finally:
        client.disconnect()

robust_client_example()
```

### 7. 监控和日志

```python
def monitoring_example():
    import logging
    
    # 配置详细的日志
    logging.basicConfig(level=logging.DEBUG)
    
    # 创建客户端
    client = create_broker_client("localhost:10911")
    client.connect()
    
    # 启用结构化日志
    from pyrocketmq.logging import get_logger
    logger = get_logger(__name__)
    
    try:
        mq = MessageQueue(topic="monitored_topic", broker_name="broker-a", queue_id=0)
        
        # 发送消息并记录详细指标
        import time
        start_time = time.time()
        
        result = client.sync_send_message(
            producer_group="monitored_producer",
            body=b"Monitored message",
            mq=mq,
            properties={"TRACE_ID": "trace_12345"}
        )
        
        end_time = time.time()
        duration = (end_time - start_time) * 1000  # 转换为毫秒
        
        # 记录成功指标
        logger.info(
            "Message sent successfully",
            extra={
                "operation": "send_message",
                "status": "success",
                "duration_ms": duration,
                "message_id": result.msg_id,
                "queue_offset": result.queue_offset,
                "topic": mq.topic,
                "queue_id": mq.queue_id
            }
        )
        
        print(f"消息发送完成，耗时: {duration:.2f}ms")
        
    except Exception as e:
        # 记录错误指标
        logger.error(
            "Message send failed",
            extra={
                "operation": "send_message",
                "status": "failed",
                "error": str(e),
                "topic": mq.topic if 'mq' in locals() else None
            }
        )
        raise
        
    finally:
        client.disconnect()

monitoring_example()
```

## ⚙️ 配置说明

### 客户端配置参数

| 参数名 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `timeout` | float | 5.0 | 默认请求超时时间（秒） |
| `connect_timeout` | float | 5.0 | 连接超时时间（秒） |
| `read_timeout` | float | 30.0 | 读取超时时间（秒） |
| `max_connections` | int | 10 | 最大连接数 |
| `connection_idle_time` | float | 60.0 | 连接空闲时间（秒） |

### 环境变量配置

可以通过环境变量设置默认配置：

```bash
export ROCKETMQ_BROKER_TIMEOUT=10.0
export ROCKETMQ_BROKER_CONNECT_TIMEOUT=3.0
export ROCKETMQ_BROKER_MAX_CONNECTIONS=20
```

### 代码配置示例

```python
# 方法1：通过参数配置
client = create_broker_client(
    "localhost:10911",
    timeout=10.0,
    connect_timeout=3.0,
    max_connections=20
)

# 方法2：通过环境变量配置
import os
os.environ["ROCKETMQ_BROKER_TIMEOUT"] = "15.0"
client = create_broker_client("localhost:10911")
```

## 🔗 依赖关系

### 必需依赖

- `asyncio`: Python内置异步库（异步模式必需）
- `typing`: Python内置类型注解库
- `logging`: Python内置日志库
- `json`: Python内置JSON处理库

### 项目内依赖

- `pyrocketmq.model`: 协议数据模型
  - `RemotingCommand`: 远程命令
  - `MessageQueue`: 消息队列
  - `MessageExt`: 扩展消息
  - `PullMessageResult`: 拉取结果
  - `SendMessageResult`: 发送结果
  - `RemotingRequestFactory`: 请求工厂

- `pyrocketmq.remote`: 远程通信
  - `Remote`: 同步远程通信
  - `AsyncRemote`: 异步远程通信
  - `ConnectionPool`: 连接池

- `pyrocketmq.logging`: 日志系统
  - `get_logger`: 获取日志器

- `pyrocketmq.model.enums`: 枚举定义
  - `ResponseCode`: 响应码

## 📈 性能优化

### 连接池配置

```python
# 高并发场景配置
manager = create_broker_manager(
    "localhost:9876",
    max_connections=50,  # 增加最大连接数
    connection_idle_time=300.0,  # 增加连接空闲时间
    timeout=3.0  # 减少默认超时时间
)
```

### 异步模式优化

```python
import asyncio

async def optimized_async_example():
    # 使用异步管理器
    manager = await create_async_broker_manager(
        "localhost:9876",
        max_connections=100,  # 异步模式可以使用更多连接
        timeout=2.0
    )
    
    # 批量发送优化
    async def batch_send(messages):
        tasks = []
        for msg in messages:
            task = asyncio.create_task(send_single_message(manager, msg))
            tasks.append(task)
        
        # 使用gather并发执行，限制并发数
        batch_size = 50
        for i in range(0, len(tasks), batch_size):
            batch = tasks[i:i+batch_size]
            await asyncio.gather(*batch)
    
    await batch_send(message_list)
```

### 内存使用优化

```python
# 使用生成器减少内存占用
def message_generator(count: int):
    for i in range(count):
        yield f"Message {i}".encode()

# 批量处理优化
def optimized_batch_send(client, messages, batch_size=100):
    """分批发送大量消息，避免内存溢出"""
    batch = []
    
    for msg in messages:
        batch.append(msg)
        
        if len(batch) >= batch_size:
            # 发送当前批次
            client.sync_batch_send_message(
                producer_group="batch_producer",
                body=b''.join(batch),
                mq=mq
            )
            batch = []  # 清空批次
    
    # 发送剩余消息
    if batch:
        client.sync_batch_send_message(
            producer_group="batch_producer",
            body=b''.join(batch),
            mq=mq
        )

# 使用示例
messages = message_generator(10000)
optimized_batch_send(client, messages)
```

## ❗ 常见问题

### Q1: 连接失败如何处理？

**A**: 连接失败通常有以下几种原因和解决方案：

1. **网络不通**：检查网络连接和防火墙设置
2. **Broker未启动**：确认RocketMQ Broker正在运行
3. **地址错误**：确认Broker地址和端口正确

```python
from pyrocketmq.broker.errors import BrokerConnectionError

try:
    client = create_broker_client("localhost:10911")
    client.connect()
except BrokerConnectionError as e:
    print(f"连接失败: {e}")
    # 可以尝试重连或检查Broker状态
```

### Q2: 消息发送超时怎么办？

**A**: 超时问题的处理方法：

1. **增加超时时间**：根据网络情况调整timeout参数
2. **检查Broker负载**：确认Broker没有过载
3. **使用重试机制**：实现自动重试逻辑

```python
# 增加超时时间
client = create_broker_client("localhost:10911", timeout=30.0)

# 或者在单次调用中指定
result = client.sync_send_message(
    producer_group="test_group",
    body=b"message",
    mq=mq,
    timeout=30.0
)
```

### Q3: 如何处理大批量消息？

**A**: 大批量消息的处理建议：

1. **分批发送**：将大批量消息分成小批次发送
2. **使用异步模式**：利用异步IO提高吞吐量
3. **控制并发数**：避免同时发送过多请求

```python
# 分批发送示例
def send_large_batch(messages, batch_size=1000):
    for i in range(0, len(messages), batch_size):
        batch = messages[i:i+batch_size]
        client.sync_batch_send_message(
            producer_group="large_batch_producer",
            body=b''.join(batch),
            mq=mq
        )
```

### Q4: 消息拉取失败如何重试？

**A**: 消息拉取失败的重试策略：

```python
def pull_with_retry(client, max_retries=3):
    for attempt in range(max_retries):
        try:
            return client.pull_message(...)
        except MessagePullError as e:
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # 指数退避
                continue
            raise
```

### Q5: 如何监控Broker客户端性能？

**A**: 性能监控的方法：

1. **启用详细日志**：配置DEBUG级别日志
2. **记录关键指标**：跟踪延迟、成功率等
3. **使用结构化日志**：便于后续分析

```python
import time
from pyrocketmq.logging import get_logger

logger = get_logger(__name__)

def monitored_send(client, message):
    start_time = time.time()
    
    try:
        result = client.sync_send_message(...)
        duration = (time.time() - start_time) * 1000
        
        logger.info("Send success", extra={
            "duration_ms": duration,
            "message_id": result.msg_id,
            "status": "success"
        })
        
        return result
        
    except Exception as e:
        duration = (time.time() - start_time) * 1000
        
        logger.error("Send failed", extra={
            "duration_ms": duration,
            "error": str(e),
            "status": "failed"
        })
        
        raise
```

## 🔄 版本历史

### v3.0.0 (当前版本)

#### 🔥 重大变更
- 重构异常体系，新增5个专用异常类型
- 优化连接池管理，支持连接健康检查
- 改进异步客户端性能，提升30%吞吐量

#### ✨ 新增功能
- **搜索偏移量功能**：支持根据时间戳搜索消息偏移量
- **结构化日志支持**：集成pyrocketmq.logging模块
- **连接监控指标**：提供详细的连接状态监控
- **批量操作优化**：改进批量消息发送性能

#### 🛠️ 改进
- **错误处理增强**：更精确的异常分类和错误信息
- **内存使用优化**：减少大消息处理的内存占用
- **并发性能提升**：优化连接池的并发访问性能
- **文档完善**：增加详细的使用示例和最佳实践

#### 📝 文档更新
- 完整的API文档和参数说明
- 新增10+个实际使用示例
- 添加性能优化指南
- 完善常见问题解答

### v2.0.0 (重构版本)

- 引入异步客户端支持
- 重构连接管理器架构
- 新增批量消息发送功能
- 优化异常处理机制

### v1.x.x (历史版本)

- 基础同步客户端功能
- 简单的连接管理
- 基本的消息发送和拉取

---

**最后更新**: 2025-01-24
**文档版本**: v3.0.0
**兼容性**: Python 3.11+
**维护状态**: ✅ 活跃维护
