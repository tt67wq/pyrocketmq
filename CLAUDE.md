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
- **Producer模块**: 🚧 实现中，已完成Topic-Broker映射管理和队列选择器

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
**关键组件**: TopicBrokerMapping + QueueSelector架构

#### TopicBrokerMapping
- **功能**: 管理Topic到Broker的路由信息和队列选择
- **特性**:
  - 路由信息缓存和过期管理
  - 预构建队列列表性能优化
  - 支持自定义队列选择器
  - 线程安全的并发访问

#### QueueSelector策略
- **RoundRobinSelector**: 默认轮询负载均衡，维护计数器状态
- **RandomSelector**: 随机选择，适合无状态负载均衡
- **MessageHashSelector**: 基于SHARDING_KEY或KEYS的哈希选择，保证顺序性

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

## 注意事项

1. **环境变量**: 开发时必须设置`PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src`
2. **分片键策略**: MessageHashSelector优先使用SHARDING_KEY，其次使用KEYS的第一个值
3. **选择器状态**: RoundRobinSelector的计数器在路由更新时会重置
4. **性能优化**: TopicBrokerMapping预构建队列列表，避免运行时计算开销
5. **线程安全**: 所有映射管理器操作都是线程安全的
6. **异步优先**: 网络通信主要基于asyncio，同步模式是其封装
7. **路由过期**: 默认路由过期时间30秒，可配置
8. **类型安全**: 所有代码使用完整类型注解