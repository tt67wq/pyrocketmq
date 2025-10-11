# Broker 模块文档

## 概述

消费者和生产者都是先和nameserver建联，获取到topic的路由信息后再与对应broker建联，broker本质也是一组remote服务。

## 模块结构

```
broker/
├── __init__.py          # 模块入口，导出公共API
├── models.py            # 数据结构定义 ✅
├── client.py            # Broker客户端 (待实现)
├── producer.py          # 消息生产者 (待实现)
├── consumer.py          # 消息消费者 (待实现)
├── errors.py            # 异常定义 (待实现)
├── config.py            # 配置管理 (待实现)
├── factory.py           # 工厂函数 (待实现)
├── utils.py             # 工具函数 (待实现)
└── broker.md            # 模块文档
```

## 已完成功能

### 数据结构模型 (models.py) ✅

实现了与RocketMQ Broker通信所需的核心数据结构：

#### 核心数据结构

1. **MessageQueue** - 消息队列信息
   - `topic`: 主题名称
   - `broker_name`: broker名称
   - `queue_id`: 队列ID
   - 支持相等性比较和哈希操作

2. **MessageExt** - 扩展消息信息
   - 包含完整的消息信息（主题、消息体、标签、keys等）
   - 支持系统属性（消息ID、队列ID、偏移量、时间戳等）
   - 支持用户属性管理
   - 事务消息支持

3. **ProducerData** - 生产者信息
   - `group_name`: 生产者组名
   - 用于向Broker注册生产者信息

4. **ConsumerData** - 消费者信息
   - `group_name`: 消费者组名
   - `consume_type`: 消费类型（PUSH/PULL）
   - `message_model`: 消费模式（BROADCASTING/CLUSTERING）
   - `consume_from_where`: 消费起始位置
   - `subscription_data`: 订阅关系

5. **SendMessageResult** - 发送消息结果
   - `msg_id`: 消息ID
   - `queue_id`: 队列ID
   - `queue_offset`: 队列偏移量
   - 支持事务消息信息

6. **PullMessageResult** - 拉取消息结果
   - `messages`: 消息列表
   - `next_begin_offset`: 下次拉取起始偏移量
   - `min_offset`/`max_offset`: 偏移量范围
   - `suggest_which_broker_id`: 建议的broker ID

7. **OffsetResult** - 偏移量查询结果
   - `offset`: 偏移量值
   - `retry_times`: 重试次数

#### 枚举类型

- `ConsumeType`: 消费类型（PUSH/PULL）
- `MessageModel`: 消息模式（BROADCASTING/CLUSTERING）
- `ConsumeFromWhere`: 消费起始位置

#### 设计特性

1. **类型安全**: 使用dataclass和类型注解确保编译时类型检查
2. **Go语言兼容**: 使用ast.literal_eval处理Go语言返回的整数key格式JSON
3. **数据验证**: 包含完整的字段验证和错误处理
4. **序列化支持**: 所有数据结构都支持to_dict/from_dict转换
5. **字节数据处理**: 支持从bytes直接创建实例

#### 使用示例

```python
from pyrocketmq.broker import (
    MessageQueue, MessageExt, ProducerData, ConsumerData,
    SendMessageResult, PullMessageResult, OffsetResult,
    ConsumeType, MessageModel, ConsumeFromWhere
)

# 创建消息队列
mq = MessageQueue(topic="test_topic", broker_name="broker-a", queue_id=0)

# 创建消息
message = MessageExt(
    topic="test_topic",
    body=b"Hello, RocketMQ!",
    tags="test_tag",
    keys=["key1", "key2"]
)
message.set_property("user_key", "user_value")

# 创建生产者信息
producer = ProducerData(group_name="test_producer_group")

# 创建消费者信息
consumer = ConsumerData(
    group_name="test_consumer_group",
    consume_type=ConsumeType.PUSH,
    message_model=MessageModel.CLUSTERING,
    consume_from_where=ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET
)

# 从字节数据解析结果
result_bytes = b"{'msgId': 'msg123', 'queueId': 0, 'queueOffset': 100, 'regionId': 'DefaultRegion'}"
send_result = SendMessageResult.from_bytes(result_bytes)
```

## 待实现功能

### 客户端实现 (client.py)
- BrokerClient基础客户端类
- AsyncBrokerClient异步客户端
- 消息发送API
- 消息拉取API
- 偏移量管理API
- 心跳API
- 客户端注册/注销

### 消息生产者 (producer.py)
- DefaultProducer默认生产者实现
- 消息路由选择策略
- 重试机制
- 批量发送支持
- 事务消息支持

### 消息消费者 (consumer.py)
- PushConsumer推式消费者
- PullConsumer拉式消费者
- 消息监听器机制
- 消息确认机制
- 负载均衡策略

### 异常处理 (errors.py)
- BrokerError基础异常
- 各种具体的异常类型
- 分层的异常体系

### 配置管理 (config.py)
- BrokerConfig配置类
- 预定义的配置模板
- 连接池配置

### 工厂函数 (factory.py)
- BrokerClientFactory客户端工厂
- 便捷的创建接口

### 工具函数 (utils.py)
- 消息队列选择算法
- 消息ID生成器
- 压缩/解压缩工具
- 序列化工具

## 测试覆盖

- ✅ 数据结构模型测试：29个测试用例全部通过
- ⏳ 客户端功能测试：待实现
- ⏳ 集成测试：待实现

## 下一步计划

1. 实现异常处理体系 (errors.py)
2. 实现Broker客户端核心功能 (client.py)
3. 实现消息生产者功能 (producer.py)
4. 实现消息消费者功能 (consumer.py)
5. 完善配置和工具类
6. 编写完整的测试套件
