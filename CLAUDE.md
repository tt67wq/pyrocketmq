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
- **测试覆盖**: ✅ 20+个测试用例，覆盖所有核心功能

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

# 运行单个测试文件
export PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src && python -m pytest tests/model/test_serializer.py -v

# 运行单个测试方法
export PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src && python -m pytest tests/model/test_serializer.py::TestRemotingCommandSerializer::test_serialize_basic_command -v

# 运行异步测试
export PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src && python -m pytest tests/transport/ -v --asyncio-mode=auto
```

### 开发工具
```bash
# 启用调试日志
export PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src && python -c "
import sys
sys.path.insert(0, 'src')
from pyrocketmq.logging import LoggerFactory, LoggingConfig
LoggerFactory.setup_default_config(LoggingConfig(level='DEBUG'))
"
```

## 核心架构

### 项目结构
```
src/pyrocketmq/
├── model/              # RocketMQ协议模型层 ✅
│   ├── command.py      # 核心数据结构 RemotingCommand
│   ├── serializer.py   # 二进制序列化/反序列化器
│   ├── enums.py        # 协议枚举定义（与Go语言实现一致）
│   ├── factory.py      # 工厂方法和构建器
│   ├── headers.py      # 请求Header数据结构定义
│   ├── utils.py        # 工具函数
│   ├── errors.py       # 模型层异常定义
│   ├── message.py      # 消息数据结构
│   ├── message_ext.py  # 扩展消息数据结构
│   ├── message_queue.py # 消息队列数据结构
│   ├── message_results.py # 消息处理结果
│   ├── producer_consumer.py # 生产者消费者模型
│   ├── heart_beat.py   # 心跳数据结构
│   ├── client_data.py  # 客户端数据
│   └── result_data.py  # 结果数据结构
├── transport/          # 网络传输层 ✅
│   ├── abc.py          # 传输层抽象接口
│   ├── tcp.py          # TCP连接实现（基于状态机）
│   ├── config.py       # 传输配置管理
│   └── errors.py       # 传输层异常定义
├── remote/             # 远程通信层 ✅
│   ├── async_remote.py # 异步远程通信实现
│   ├── sync_remote.py  # 同步远程通信实现
│   ├── config.py       # 远程通信配置
│   ├── factory.py      # 工厂函数
│   ├── pool.py         # 连接池管理
│   └── errors.py       # 远程通信异常定义
├── logging/           # 日志模块 ✅
│   ├── logger.py       # 日志记录器
│   └── config.py       # 日志配置
├── nameserver/        # NameServer客户端 ✅
│   ├── client.py       # NameServer客户端实现
│   ├── models.py       # NameServer数据模型
│   └── errors.py       # NameServer异常定义
└── broker/            # Broker客户端 ✅
    ├── client.py       # Broker客户端实现
    └── errors.py       # Broker异常定义
```

### 核心设计原则

#### 1. 协议兼容性
- 完全兼容RocketMQ Go语言实现的TCP协议格式
- 枚举值与Go实现保持一致（如FlagType.RPC_TYPE=0, RPC_ONEWAY=1, RESPONSE_TYPE=1）
- 支持所有标准请求代码和响应代码

#### 2. 数据流格式
```
| length(4) | header-length(4) | header-data(JSON) | body-data(bytes) |
```

#### 3. 类型安全
- 全面的类型注解（Python 3.11+）
- 严格的数据验证
- 丰富的异常处理层次

#### 4. 异步优先
- 基于asyncio的异步网络通信
- 同步接口作为异步接口的封装
- 状态机驱动的连接管理

## 核心模块详解

### Model层 (`src/pyrocketmq/model/`)

#### RemotingCommand核心数据结构
- **位置**: `model/command.py`
- **功能**: RocketMQ协议的核心数据结构，支持所有协议字段
- **特性**:
  - 支持扩展字段管理
  - 内置flag类型判断（is_request, is_response, is_oneway）
  - 自动header序列化/反序列化

#### 消息数据结构
- **Message**: `model/message.py` - 基础消息结构
- **MessageExt**: `model/message_ext.py` - 扩展消息结构，包含事务状态、偏移量等信息
- **MessageQueue**: `model/message_queue.py` - 消息队列信息
- **MessageResults**: `model/message_results.py` - 消息处理结果

#### RemotingCommandSerializer序列化器
- **位置**: `model/serializer.py`
- **功能**: 二进制序列化和反序列化
- **协议**: RocketMQ TCP协议格式
- **特性**:
  - 大小限制检查（最大32MB帧，64KB header）
  - JSON格式header序列化
  - 完整的错误处理

#### 枚举定义
- **位置**: `model/enums.py`
- **内容**: LanguageCode, RequestCode, FlagType, ResponseCode
- **特点**: 与Go语言实现完全兼容

#### RemotingRequestFactory请求工厂
- **位置**: `model/factory.py`
- **功能**: 基于Go语言实现的快速请求创建工厂
- **支持的请求类型**:
  - **消息操作**: 发送消息、拉取消息、批量发送消息
  - **消费者管理**: 获取消费者列表、查询/更新消费者偏移量
  - **路由信息**: 获取主题路由信息、获取所有主题列表
  - **事务操作**: 结束事务、检查事务状态
  - **主题管理**: 创建主题、删除主题
  - **系统管理**: 心跳请求、消费者运行信息
  - **偏移量操作**: 搜索偏移量、获取最大/最小偏移量
  - **消息查询**: 根据键查询消息、根据偏移量查看消息
  - **消息编号**: 保存/获取消息编号

#### RemotingCommandBuilder构建器
- **位置**: `model/factory.py`
- **功能**: 提供链式调用来构建RemotingCommand对象
- **特性**: 灵活的参数设置和链式调用

### Remote层 (`src/pyrocketmq/remote/`)

#### 异步远程通信
- **位置**: `remote/async_remote.py`
- **功能**: 异步RPC通信实现，支持请求-响应模式
- **特性**:
  - 基于asyncio的异步通信
  - 内置超时处理和重试机制
  - 请求等待者管理
  - 自动连接状态维护

#### 同步远程通信
- **位置**: `remote/sync_remote.py`
- **功能**: 同步RPC通信实现
- **特性**:
  - 基于asyncio.run的同步封装
  - 与异步接口相同的API设计
  - 适合阻塞式调用场景

#### 连接池管理
- **位置**: `remote/pool.py`
- **功能**: 连接池实现，支持连接复用
- **特性**:
  - 异步连接池 (AsyncConnectionPool)
  - 同步连接池 (ConnectionPool)
  - 连接生命周期管理
  - 负载均衡支持

### Transport层 (`src/pyrocketmq/transport/`)

#### 抽象接口
- **位置**: `transport/abc.py`
- **设计**: 基于ABC的抽象接口定义
- **支持**: 同步和异步双模式

#### TCP实现
- **位置**: `transport/tcp.py`
- **功能**: 基于python-statemachine的TCP连接状态机
- **状态**: DISCONNECTED, CONNECTING, CONNECTED, CLOSING, CLOSED
- **特性**:
  - 状态机驱动的连接管理
  - 自动重连机制
  - 心跳检测支持

### NameServer客户端 (`src/pyrocketmq/nameserver/`)

#### 客户端实现
- **位置**: `nameserver/client.py`
- **功能**: NameServer通信客户端
- **特性**: 
  - 支持路由信息查询和主题管理
  - 完整的NameServer数据模型
  - 异步/同步通信支持
  - 自动路由发现和更新

#### 数据模型
- **位置**: `nameserver/models.py`
- **功能**: NameServer相关的数据结构定义
- **包含**: 路由信息、队列数据、Broker信息等

### Broker客户端 (`src/pyrocketmq/broker/`)

#### 客户端实现
- **位置**: `broker/client.py`
- **功能**: Broker通信客户端
- **特性**: 
  - 支持消息发送、拉取、偏移量管理
  - 批量消息操作支持
  - 队列锁定/解锁功能
  - 消费者运行信息查询
  - 完整的错误处理和重试机制

## 开发模式

### 命令创建模式

#### 使用RemotingRequestFactory（推荐）
```python
from pyrocketmq.model import RemotingRequestFactory

# 创建发送消息请求
send_cmd = RemotingRequestFactory.create_send_message_request(
    producer_group="test_producer",
    topic="test_topic",
    body=b"Hello, RocketMQ!",
    queue_id=1,
    tags="test_tag",
    keys="test_key"
)

# 创建拉取消息请求
pull_cmd = RemotingRequestFactory.create_pull_message_request(
    consumer_group="test_consumer",
    topic="test_topic",
    queue_id=0,
    queue_offset=100,
    max_msg_nums=32
)

# 创建获取路由信息请求
route_cmd = RemotingRequestFactory.create_get_route_info_request("test_topic")

# 创建心跳请求
heartbeat_cmd = RemotingRequestFactory.create_heartbeat_request()

# 创建事务请求
end_tx_cmd = RemotingRequestFactory.create_end_transaction_request(
    producer_group="test_producer",
    tran_state_table_offset=1000,
    commit_log_offset=2000,
    commit_or_rollback=1,
    msg_id="msg_id",
    transaction_id="tx_id"
)

# 创建批量消息请求
batch_cmd = RemotingRequestFactory.create_send_batch_message_request(
    producer_group="test_producer",
    topic="test_topic",
    body=b"Message1\nMessage2\nMessage3"
)

# 创建主题管理请求
create_topic_cmd = RemotingRequestFactory.create_create_topic_request(
    topic="new_topic",
    read_queue_nums=16,
    write_queue_nums=16
)

# 创建队列锁定请求
lock_mq_cmd = RemotingRequestFactory.create_lock_batch_mq_request(
    consumer_group="test_consumer",
    client_id="test_client",
    mq_set=[...],  # MessageQueue对象列表
    retry_broker_id=0
)

# 创建队列解锁请求
unlock_mq_cmd = RemotingRequestFactory.create_unlock_batch_mq_request(
    consumer_group="test_consumer",
    client_id="test_client",
    mq_set=[...]  # MessageQueue对象列表
)
```

#### 使用RemotingCommandBuilder
```python
from pyrocketmq.model import RemotingCommandBuilder, RequestCode

command = (RemotingCommandBuilder(code=RequestCode.SEND_MESSAGE)
          .with_topic("test_topic")
          .with_body(b"message content")
          .with_producer_group("test_group")
          .as_request()
          .build())
```

#### 直接构造
```python
from pyrocketmq.model import RemotingCommand, RequestCode, LanguageCode

command = RemotingCommand(
    code=RequestCode.SEND_MESSAGE,
    language=LanguageCode.PYTHON,
    ext_fields={
        "topic": "test_topic",
        "producerGroup": "test_group"
    },
    body=b"message content"
)
```

### 序列化/反序列化模式

```python
from pyrocketmq.model import RemotingCommandSerializer

# 序列化
data = RemotingCommandSerializer.serialize(command)

# 反序列化
restored = RemotingCommandSerializer.deserialize(data)

# 验证数据帧
if RemotingCommandSerializer.validate_frame(data):
    total_length, header_length = RemotingCommandSerializer.get_frame_info(data)
```

### 工具函数使用

```python
from pyrocketmq.model.utils import (
    validate_command, generate_opaque, is_success_response,
    get_topic_from_command, get_command_summary
)

# 验证命令
validate_command(command)

# 生成唯一opaque
opaque = generate_opaque()

# 检查响应状态
if is_success_response(response):
    print("请求成功")

# 提取信息
topic = get_topic_from_command(command)
summary = get_command_summary(command)
```

### 远程通信使用

#### 异步远程通信
```python
from pyrocketmq.remote import create_async_remote
from pyrocketmq.remote.config import RemoteConfig
from pyrocketmq.transport.config import TransportConfig

# 创建配置
transport_config = TransportConfig(host="localhost", port=9876)
remote_config = RemoteConfig()

# 创建异步远程客户端
async_remote = await create_async_remote(transport_config, remote_config)

# 发送请求
request = RemotingRequestFactory.create_send_message_request(
    topic="test_topic",
    body=b"Hello, RocketMQ!",
    producer_group="test_group"
)

# 异步发送并等待响应
response = await async_remote.invoke(request)
```

#### 同步远程通信
```python
from pyrocketmq.remote import create_sync_remote

# 创建同步远程客户端
sync_remote = create_sync_remote(transport_config, remote_config)

# 同步发送请求
response = sync_remote.invoke(request)
```

#### 连接池使用
```python
from pyrocketmq.remote import AsyncConnectionPool

# 创建连接池
pool = AsyncConnectionPool(transport_config, remote_config, max_size=5)

# 从池中获取连接
async with pool.get_connection() as conn:
    response = await conn.invoke(request)
```

## 协议规范

### Flag类型说明
由于Go语言实现中`RPC_ONEWAY`和`RESPONSE_TYPE`都使用值1，判断逻辑如下：
- `is_request()`: flag == FlagType.RPC_TYPE (0)
- `is_response()`: flag == FlagType.RESPONSE_TYPE (1)
- `is_oneway()`: flag == FlagType.RPC_ONEWAY (1)

### 大小限制
- 最大帧大小: 32MB (33554432字节)
- 最大header大小: 64KB (65536字节)
- 长度字段格式: 大端序4字节整数

### 错误处理层次
- **Model层**: `RemotingCommandError`, `SerializationError`, `DeserializationError`, `ProtocolError`
- **Transport层**: `TransportError`, `ConnectionError`, `ConnectionClosedError`
- **Remote层**: `RemoteError`, `RpcTimeoutError`, `ConfigurationError`, `MaxWaitersExceededError`
- **NameServer层**: `NameServerError`
- **Broker层**: `BrokerError`

## 测试策略

### 测试覆盖
- **模型层测试**: 协议序列化/反序列化、数据结构验证、请求工厂测试
- **传输层测试**: 连接状态机、TCP通信、异步连接测试
- **远程通信测试**: 异步/同步RPC调用、连接池测试
- **Broker测试**: 偏移量查询、消息拉取、搜索功能测试
- **NameServer测试**: 路由信息、客户端通信测试
- **边界条件测试**: 大小限制、空数据、无效数据
- **性能测试**: 大消息体处理、并发连接

### 测试运行
必须设置`PYTHONPATH`环境变量以确保能正确导入模块：
```bash
export PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src
```

### 异步测试配置
项目使用pytest-asyncio进行异步测试，配置在`pyproject.toml`中：
```toml
[tool.pytest.ini_options]
asyncio_mode = "auto"
```

## 常见任务

### 使用RemotingRequestFactory创建请求
RemotingRequestFactory提供了所有标准RocketMQ请求的创建方法：

```python
from pyrocketmq.model import RemotingRequestFactory

# 消息相关请求
send_request = RemotingRequestFactory.create_send_message_request(
    producer_group="my_producer", topic="my_topic", body=b"msg"
)
pull_request = RemotingRequestFactory.create_pull_message_request(
    consumer_group="my_consumer", topic="my_topic", 
    queue_id=0, queue_offset=100, max_msg_nums=32
)
batch_request = RemotingRequestFactory.create_send_batch_message_request(
    producer_group="my_producer", topic="my_topic", body=b"batch_msg"
)

# 消费者相关请求
consumer_list = RemotingRequestFactory.create_get_consumer_list_request("my_group")
query_offset = RemotingRequestFactory.create_query_consumer_offset_request(
    consumer_group="my_group", topic="my_topic", queue_id=0
)
update_offset = RemotingRequestFactory.create_update_consumer_offset_request(
    consumer_group="my_group", topic="my_topic", queue_id=0, commit_offset=200
)

# 路由和集群信息
route_info = RemotingRequestFactory.create_get_route_info_request("my_topic")
cluster_info = RemotingCommand(
    code=RequestCode.GET_BROKER_CLUSTER_INFO,
    language=LanguageCode.PYTHON,
    flag=FlagType.RPC_TYPE,
)
all_topics = RemotingRequestFactory.create_get_all_topic_list_request()

# 事务相关请求
end_tx = RemotingRequestFactory.create_end_transaction_request(
    producer_group="my_producer", tran_state_table_offset=1000,
    commit_log_offset=2000, commit_or_rollback=1
)
check_tx = RemotingRequestFactory.create_check_transaction_state_request(
    tran_state_table_offset=1000, commit_log_offset=2000
)

# 系统管理请求
heartbeat = RemotingRequestFactory.create_heartbeat_request()
consumer_info = RemotingRequestFactory.create_get_consumer_running_info_request(
    consumer_group="my_group", client_id="my_client"
)

# 主题管理请求
create_topic = RemotingRequestFactory.create_create_topic_request(
    topic="new_topic", read_queue_nums=16, write_queue_nums=16
)
delete_topic = RemotingRequestFactory.create_delete_topic_request("old_topic")
```

### 使用NameServer客户端
```python
from pyrocketmq.nameserver.client import NameServerClient
from pyrocketmq.nameserver.models import RouteData

# 创建NameServer客户端
ns_client = NameServerClient("localhost", 9876)

# 获取主题路由信息
route_info = await ns_client.get_route_info("test_topic")

# 获取所有主题列表
topics = await ns_client.get_all_topic_list()

# 获取Broker集群信息
cluster_info = await ns_client.get_broker_cluster_info()
```

### 使用Broker客户端
```python
from pyrocketmq.broker.client import BrokerClient
from pyrocketmq.model.message_queue import MessageQueue

# 创建Broker客户端
broker_client = BrokerClient("localhost", 10911)

# 获取消费者列表
consumers = await broker_client.get_consumers_by_group("test_consumer")

# 查询消费者偏移量
offset = await broker_client.query_consumer_offset(
    consumer_group="test_consumer",
    topic="test_topic", 
    queue_id=0
)

# 更新消费者偏移量
await broker_client.update_consumer_offset(
    consumer_group="test_consumer",
    topic="test_topic",
    queue_id=0,
    commit_offset=1000
)

# 搜索偏移量
search_result = await broker_client.search_offset(
    topic="test_topic",
    queue_id=0,
    timestamp=int(time.time() * 1000)
)

# 获取最大/最小偏移量
max_offset = await broker_client.get_max_offset("test_topic", 0)
min_offset = await broker_client.get_min_offset("test_topic", 0)

# 查看消息
view_message = await broker_client.view_message(
    topic="test_topic",
    queue_id=0,
    offset=100
)

# 根据消息ID查询消息
message_by_id = await broker_client.get_message_by_id("msg_id")

# 批量锁定消息队列
mq_set = [
    MessageQueue(broker_name="broker-a", topic="test_topic", queue_id=0),
    MessageQueue(broker_name="broker-a", topic="test_topic", queue_id=1)
]
lock_result = await broker_client.lock_batch_mq(
    consumer_group="test_consumer",
    client_id="client_001",
    mq_set=mq_set
)

# 解锁消息队列
await broker_client.unlock_batch_mq(
    consumer_group="test_consumer", 
    client_id="client_001",
    mq_set=mq_set
)
```

### 添加新的请求代码
1. 在`model/enums.py`的RequestCode中添加新枚举
2. 在`model/headers.py`中定义对应的Header数据结构
3. 在RemotingRequestFactory中添加对应的创建方法
4. 在相应的客户端中添加调用方法
5. 添加相应的测试用例

### 扩展协议字段
1. 在`RemotingCommand`类中添加新属性
2. 更新序列化逻辑
3. 更新工厂和构建器方法
4. 更新相关数据模型
5. 添加工具函数支持

### 配置远程通信
```python
# 生产环境配置
from pyrocketmq.remote.config import PRODUCTION_CONFIG
config = PRODUCTION_CONFIG.copy(
    timeout=10.0,
    max_retries=3,
    pool_size=10
)
```

### 调试连接问题
```python
# 启用详细日志
from pyrocketmq.logging import LoggerFactory, LoggingConfig
LoggerFactory.setup_default_config(LoggingConfig(level="DEBUG"))

# 检查连接状态
print(f"连接状态: {async_remote.transport.current_state_name}")
print(f"是否已连接: {async_remote.transport.is_connected}")
```

### 运行特定测试
```bash
# 运行异步传输测试
export PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src && python -m pytest tests/transport/test_async_connection.py -v

# 运行状态机测试
export PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src && python -m pytest tests/transport/test_state_transitions.py -v

# 运行远程通信测试
export PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src && python -m pytest tests/remote/test_async_remote.py -v
```

## 依赖管理

项目使用uv作为依赖管理工具：
- `pyproject.toml`: 项目配置和依赖声明
- `uv.lock`: 锁定的依赖版本
- `.python-version`: 指定Python 3.11

### 安装依赖
```bash
# 使用pip
pip install -e .

# 使用uv（推荐）
uv sync
```

### 性能优化建议
1. **连接池**: 在高并发场景下使用连接池，避免频繁创建连接
2. **批量操作**: 尽可能使用批量消息操作，减少网络开销
3. **异步优先**: 优先使用异步API，可以获得更好的性能
4. **合理配置**: 根据业务需求调整超时时间和重试次数
5. **监控指标**: 关注连接状态、消息处理延迟等关键指标

### 故障排除
1. **连接问题**: 检查网络连接和防火墙设置
2. **序列化错误**: 确认消息体大小不超过32MB限制
3. **超时问题**: 调整timeout配置或检查网络延迟
4. **协议错误**: 确认RocketMQ版本兼容性

## 注意事项

1. **环境变量**: 开发时必须设置`PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src`
2. **字节处理**: bytes类型不可变，直接赋值即可
3. **Flag判断**: 由于Go语言实现特点，RPC_ONEWAY和RESPONSE_TYPE值相同
4. **大小限制**: 严格检查帧大小和header大小限制
5. **类型安全**: 所有代码都使用类型注解，确保编译时类型检查
6. **异步模式**: 远程通信主要基于asyncio，同步模式是其封装
7. **连接管理**: 使用连接池可以提高性能，避免频繁创建连接
8. **测试模式**: 异步测试需要设置`--asyncio-mode=auto`参数
9. **依赖管理**: 推荐使用uv进行依赖管理，确保版本一致性
10. **生产环境**: 建议在生产环境中启用日志监控和错误追踪
11. **资源清理**: 使用完客户端后记得正确关闭连接
12. **异常处理**: 建议在业务代码中妥善处理各种异常情况