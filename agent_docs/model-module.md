# Model层协议数据模型

**模块概述**: Model模块是pyrocketmq的核心数据层，提供完整的RocketMQ协议数据结构定义、序列化机制和工具函数。严格遵循RocketMQ TCP协议规范，与Go语言实现完全兼容。

## 核心组件

### RemotingCommand
远程命令数据结构，协议层的核心通信单元，包含请求/响应的所有元数据。

### Message/MessageExt
基础和扩展消息数据结构，支持消息属性和元数据：
- **Message**: 基础消息结构，包含topic、body、properties等核心字段
- **MessageExt**: 扩展消息结构，添加了队列ID、偏移量、重试次数等运行时信息

### MessageQueue
消息队列数据结构，表示Topic下的具体队列，包含：
- topic: 主题名称
- broker_name: Broker名称
- queue_id: 队列ID

### RemotingRequestFactory
请求工厂，支持所有RocketMQ请求类型构建：
- 发送消息请求
- 拉取消息请求
- 心跳请求
- 获取消费者偏移量请求
- 更新消费者偏移量请求
- 查询Topic路由信息请求
- 注册Producer/Consumer请求

### Serializer
高效的二进制序列化器，支持大消息处理：
- 基于RocketMQ TCP协议的帧格式
- 支持JSON header和二进制body
- 大小端序处理
- 大消息分片处理

## 关键特性

- **严格的协议兼容性**: 与Go语言实现完全兼容，确保互操作性
- **类型安全保障**: 使用dataclass和类型注解，提供完整的类型检查
- **模块化设计**: 便于扩展新的协议特性和消息类型
- **丰富的工具函数**: 简化常见操作，如消息创建、属性设置等
- **性能优化**: 高效的序列化/反序列化实现

## 使用示例

```python
from pyrocketmq.model import Message, RemotingCommand, RemotingRequestFactory

# 创建消息
message = Message(topic="test_topic", body=b"Hello RocketMQ")
message.set_property("KEYS", "order_123")
message.set_property("TAGS", "order")

# 创建请求
factory = RemotingRequestFactory()
request = factory.create_send_message_request(
    producer_group="test_group",
    message_data=message.body,
    message_queue=message_queue,
    properties=message.properties
)

# 序列化命令
serializer = Serializer()
serialized_data = serializer.serialize(request)
```