# Broker模块描述

## 模块概述

Broker模块是pyrocketmq客户端库的核心组件之一，提供与RocketMQ Broker通信的完整功能实现。该模块基于RocketMQ TCP协议，支持同步和异步两种通信模式，为生产者和消费者提供消息发送、拉取、偏移量管理、心跳维护等核心功能。

## 模块架构

### 文件结构
```
broker/
├── __init__.py          # 模块入口，导出主要类
├── client.py            # 同步Broker客户端实现
├── async_client.py      # 异步Broker客户端实现
└── errors.py            # Broker模块异常定义
```

### 核心设计原则

1. **双模式支持**: 同时提供同步和异步客户端，满足不同性能需求场景
2. **完整错误处理**: 定义了20+种专门的异常类型，精确处理各种错误场景
3. **协议兼容性**: 严格遵循RocketMQ TCP协议规范，与Go语言实现完全兼容
4. **性能优化**: 支持oneway通信模式，减少不必要的响应等待
5. **线程安全**: 所有操作都是线程安全的，支持高并发场景

## 核心数据结构

### BrokerClient
同步Broker客户端，提供完整的同步API接口。

**核心属性**:
- `remote: Remote` - 同步远程通信实例
- `timeout: float` - 请求超时时间
- `_client_id: str` - 客户端唯一标识

**主要方法**:
- `connect()/disconnect()` - 连接管理
- `sync_send_message()` - 同步发送单条消息
- `sync_batch_send_message()` - 同步批量发送消息
- `pull_message()` - 拉取消息
- `query_consumer_offset()` - 查询消费者偏移量
- `update_consumer_offset()` - 更新消费者偏移量
- `send_heartbeat()` - 发送心跳

### AsyncBrokerClient
异步Broker客户端，基于asyncio提供高性能异步API。

**核心属性**:
- `remote: AsyncRemote` - 异步远程通信实例
- `timeout: float` - 请求超时时间
- `_client_id: str` - 客户端唯一标识

**主要方法**:
- `connect()/disconnect()` - 异步连接管理
- `async_send_message()` - 异步发送单条消息
- `async_batch_send_message()` - 异步批量发送消息
- `async_pull_message()` - 异步拉取消息

## 主要功能模块

### 1. 消息发送模块

#### 单条消息发送
- **功能**: 发送单条消息到指定队列
- **支持特性**:
  - 消息属性设置（SHARDING_KEY, KEYS, TAGS等）
  - 发送状态跟踪（SEND_OK, FLUSH_DISK_TIMEOUT等）
  - 消息ID自动生成和返回
  - 事务消息支持

#### 批量消息发送
- **功能**: 一次发送多条消息，提升吞吐量
- **优势**: 减少网络往返次数，提高发送效率

### 2. 消息拉取模块

#### 拉取请求处理
- **功能**: 从指定队列拉取消息
- **支持参数**:
  - `consumer_group` - 消费者组名
  - `topic` - 主题名称
  - `queue_id` - 队列ID
  - `queue_offset` - 起始偏移量
  - `max_msg_nums` - 最大拉取数量（默认32）

#### 拉取结果处理
- **成功场景**: 返回PullMessageResult，包含消息列表和下次拉取偏移量
- **无消息场景**: 返回空结果，提供下次拉取偏移量
- **错误场景**:
  - `PULL_NOT_FOUND` - 无消息可拉取
  - `PULL_OFFSET_MOVED` - 偏移量已移动
  - `PULL_RETRY_IMMEDIATELY` - 需要立即重试

### 3. 偏移量管理模块

#### 查询功能
- `query_consumer_offset()` - 查询消费者消费偏移量
- `get_max_offset()` - 获取队列最大偏移量
- `search_offset_by_timestamp()` - 根据时间戳搜索对应偏移量

#### 更新功能
- `update_consumer_offset()` - 更新消费者偏移量
  - 使用oneway模式，无需等待响应
  - 提高消费性能，减少网络延迟

### 4. 心跳和连接管理

#### 心跳机制
- **功能**: 定期向Broker发送心跳，保持连接活跃
- **内容**: 包含客户端ID、生产者组、消费者组信息
- **作用**:
  - 维持客户端- Broker连接
  - 同步客户端状态信息

#### 连接管理
- **连接状态检查**: 提供is_connected属性检查连接状态
- **自动重连**: 支持连接断开后的自动重连
- **优雅关闭**: 支持连接的优雅关闭

### 5. 队列锁定机制

#### 批量锁定
- `lock_batch_mq()` - 批量锁定消息队列
- **作用**: 顺序消费场景下的队列分配

#### 批量解锁
- `unlock_batch_mq()` - 批量解锁消息队列
- **作用**: 消费者关闭时的队列释放

### 6. 事务消息支持

#### 事务结束
- `end_transaction()` - 提交或回滚事务消息
- **特性**:
  - 使用oneway模式，提高性能
  - 支持本地事务状态到事务状态的转换
  - 提供完整的事务ID跟踪

### 7. 消息重试机制

#### 消费重试
- `consumer_send_msg_back()` - 消息重试处理
- **功能**:
  - 支持延迟级别设置
  - 提供最大重试次数控制
  - 原消息信息保留

## 异常处理体系

### 异常层次结构
```
BrokerError (基础异常)
├── BrokerConnectionError (连接错误)
├── BrokerTimeoutError (超时错误)
├── BrokerResponseError (响应错误)
├── MessageSendError (消息发送错误)
├── MessagePullError (消息拉取错误)
├── OffsetError (偏移量错误)
├── TopicNotFoundError (主题不存在)
├── QueueNotFoundError (队列不存在)
├── AuthenticationError (认证错误)
├── AuthorizationError (授权错误)
├── BrokerBusyError (Broker繁忙)
├── TransactionError (事务错误)
├── ProducerError (生产者错误)
└── ConsumerError (消费者错误)
```

### 错误处理特点
1. **精确定位**: 每个异常都包含具体的错误上下文信息
2. **分类处理**: 按照错误类型进行分类，便于上层应用精确处理
3. **上下文保留**: 保留相关的topic、queue_id、client_id等上下文信息
4. **错误码映射**: 与RocketMQ响应码一一对应

## 性能优化特性

### 1. 通信优化
- **oneway模式**: 对于心跳、偏移量更新等无需响应的操作使用oneway模式
- **批量操作**: 支持批量消息发送，减少网络开销
- **异步支持**: 提供异步客户端，支持高并发场景

### 2. 连接管理优化
- **连接池**: 与远程通信层的连接池配合，避免频繁建连
- **心跳保活**: 定期心跳维持连接，避免连接超时断开
- **状态检查**: 提供连接状态检查接口，便于上层应用判断

### 3. 内存优化
- **响应复用**: 合理复用响应对象，减少内存分配
- **异常复用**: 异常对象包含足够上下文，减少重复查询

## 使用示例

### 同步客户端使用
```python
from pyrocketmq.broker import create_broker_client
from pyrocketmq.model import Message

# 创建客户端
client = create_broker_client("localhost", 10911)

# 连接并发送消息
client.connect()
result = client.sync_send_message(
    producer_group="test_group",
    body=b"Hello RocketMQ",
    mq=MessageQueue(topic="test", broker_name="broker1", queue_id=0)
)
```

### 异步客户端使用
```python
from pyrocketmq.broker import create_async_broker_client

# 创建异步客户端
client = await create_async_broker_client("localhost", 10911)

# 异步发送消息
await client.connect()
result = await client.async_send_message(
    producer_group="test_group",
    body=b"Hello Async RocketMQ",
    mq=MessageQueue(topic="test", broker_name="broker1", queue_id=0)
)
```

## 协议兼容性

### RocketMQ协议支持
- **版本兼容**: 完全兼容RocketMQ 4.x/5.x版本
- **请求类型**: 支持所有核心请求类型（发送、拉取、心跳、偏移量管理等）
- **响应处理**: 正确处理所有响应码和错误场景
- **数据格式**: 严格遵循RocketMQ数据帧格式和序列化规范

### 与Go语言实现的兼容性
- **接口对齐**: API设计与Go语言实现保持一致
- **行为一致**: 错误处理、状态转换等行为与Go实现一致
- **性能匹配**: 在Python环境下提供与Go实现相近的性能表现

该模块是pyrocketmq实现RocketMQ客户端功能的核心基础，为上层生产者和消费者模块提供稳定、高效的Broker通信能力。
