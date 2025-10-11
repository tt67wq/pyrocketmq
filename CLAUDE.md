# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概述

pyrocketmq是一个Python实现的RocketMQ客户端库，基于RocketMQ TCP协议实现。项目旨在提供高性能、可靠的RocketMQ消息队列客户端功能，完全兼容Go语言实现的协议规范。

### 项目状态
- **协议模型层**: ✅ 完整实现，包含所有核心数据结构
- **请求工厂**: ✅ RemotingRequestFactory实现，支持所有RocketMQ请求类型
- **网络传输层**: 🚧 基本完成，支持TCP连接状态机
- **远程通信层**: ✅ 异步/同步通信实现
- **连接池**: ✅ 连接池管理功能
- **NameServer支持**: 🚧 开发中

## 开发环境配置

### 环境设置
```bash
# 激活虚拟环境
source .venv/bin/activate

# 设置PYTHONPATH（必需）
export PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src

# 安装依赖
pip install -e .
```

### 测试运行
```bash
# 运行所有测试
export PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src && python -m pytest tests/

# 运行模型层测试
export PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src && python -m pytest tests/model/ -v

# 运行传输层测试
export PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src && python -m pytest tests/transport/ -v

# 运行单个测试文件
export PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src && python -m pytest tests/model/test_serializer.py -v

# 运行单个测试方法
export PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src && python -m pytest tests/model/test_serializer.py::TestRemotingCommandSerializer::test_serialize_basic_command -v
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
│   └── errors.py       # 模型层异常定义
├── transport/          # 网络传输层 🚧
│   ├── abc.py          # 传输层抽象接口
│   ├── tcp.py          # TCP连接实现（基于状态机）
│   ├── config.py       # 传输配置管理
│   ├── states.py       # 连接状态机定义
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
└── nameserver/        # NameServer支持 🚧
    └── ns.md           # NameServer协议文档
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

## 核心模块详解

### Model层 (`src/pyrocketmq/model/`)

#### RemotingCommand核心数据结构
- **位置**: `model/command.py`
- **功能**: RocketMQ协议的核心数据结构，支持所有协议字段
- **特性**:
  - 支持扩展字段管理
  - 内置flag类型判断（is_request, is_response, is_oneway）
  - 自动header序列化/反序列化

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

## 测试策略

### 测试覆盖
- **模型层测试**: 协议序列化/反序列化、数据结构验证
- **传输层测试**: 连接状态机、TCP通信
- **远程通信测试**: 异步/同步RPC调用
- **边界条件测试**: 大小限制、空数据、无效数据
- **性能测试**: 大消息体处理、并发连接

### 测试运行
必须设置`PYTHONPATH`环境变量以确保能正确导入模块：
```bash
export PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src
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

### 添加新的请求代码
1. 在`model/enums.py`的RequestCode中添加新枚举
2. 在`model/headers.py`中定义对应的Header数据结构
3. 在RemotingRequestFactory中添加对应的创建方法
4. 添加相应的测试用例

### 扩展协议字段
1. 在`RemotingCommand`类中添加新属性
2. 更新序列化逻辑
3. 更新工厂和构建器方法
4. 添加工具函数支持

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

## 注意事项

1. **环境变量**: 开发时必须设置`PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src`
2. **字节处理**: bytes类型不可变，直接赋值即可
3. **Flag判断**: 由于Go语言实现特点，RPC_ONEWAY和RESPONSE_TYPE值相同
4. **大小限制**: 严格检查帧大小和header大小限制
5. **类型安全**: 所有代码都使用类型注解，确保编译时类型检查
6. **异步模式**: 远程通信主要基于asyncio，同步模式是其封装
7. **连接管理**: 使用连接池可以提高性能，避免频繁创建连接
