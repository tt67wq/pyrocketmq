# Consumer层消息消费者

**模块概述**: Consumer模块是pyrocketmq的消息消费者实现，提供完整的消息消费、订阅管理、偏移量存储和消息监听功能。采用分层架构设计，支持并发消费和顺序消费两种模式。

## 核心组件

### BaseConsumer
消费者抽象基类，定义生命周期管理：
- **生命周期管理**: start、shutdown方法管理消费者启动和停止
- **状态管理**: 跟踪消费者的运行状态
- **资源清理**: 自动清理连接、线程等资源

### ConsumerConfig配置管理
完整的消费者配置支持：
- **基础配置**: consumer_group、namesrv_addr、message_model
- **消费行为**: consume_from_where、allocate_strategy、pull_batch_size
- **性能配置**: consume_thread_min/max、consume_timeout、pull_threshold
- **存储配置**: persist_interval、offset_store_path、auto_commit
- **高级配置**: message_trace、max_reconsume_times

### 消息监听器体系
**监听器类型**:
- **MessageListener**: 基础监听器接口，定义通用方法
- **MessageListenerConcurrently**: 并发消息监听器，支持多线程并行处理
- **MessageListenerOrderly**: 顺序消息监听器，保证消息顺序性
- **SimpleMessageListener**: 简单监听器实现，便于快速开发

### 偏移量存储系统
**存储模式**:
- **RemoteOffsetStore**: 集群模式，偏移量存储在Broker端，支持多消费者协调
- **LocalOffsetStore**: 广播模式，偏移量存储在本地文件，每个消费者独立维护
- **OffsetStoreFactory**: 工厂模式创建存储实例

**偏移量存储特性**:
- **线程安全**: 线程安全的偏移量更新和持久化
- **批量提交**: 支持批量提交和定期持久化
- **监控指标**: 完整的指标收集和监控
- **原子操作**: 原子性文件操作保证数据一致性

### 订阅管理器
**核心功能**:
- **主题订阅**: 主题订阅和消息选择器管理
- **冲突检测**: 订阅冲突检测和处理
- **导入导出**: 订阅数据的导入导出
- **监控统计**: 指标收集和监控

### 队列分配策略
**AverageAllocateStrategy**: 
- **平均分配**: 基于平均分配算法的队列分配策略
- **顺序独立**: 考虑消费者顺序和队列顺序的独立性
- **边界处理**: 支持边界条件处理（队列数不能被消费者数整除）
- **性能优化**: 大规模分配的性能优化

### 消费起始位置管理
**三种策略**:
- **CONSUME_FROM_LAST_OFFSET**: 从最新偏移量开始消费（默认）
- **CONSUME_FROM_FIRST_OFFSET**: 从最早偏移量开始消费
- **CONSUME_FROM_TIMESTAMP**: 从指定时间戳位置开始消费

## 关键特性

### 消费模式
- **并发消费**: 多线程并行处理消息，提高吞吐量
- **顺序消费**: 保证同一队列内的消息顺序性
- **广播消费**: 每个消费者都收到所有消息
- **集群消费**: 多个消费者协调消费不同队列

### 可靠性保证
- **偏移量管理**: 精确的消费偏移量跟踪和管理
- **消息重试**: 支持消息消费失败后的重试机制
- **故障恢复**: 自动故障检测和恢复

### 性能优化
- **批量拉取**: 批量从服务器拉取消息，减少网络开销
- **线程池**: 消费线程池管理，控制并发度
- **预取**: 预取消息减少等待时间

### 监控和管理
- **消费指标**: 消费速率、成功率、延迟等关键指标
- **健康检查**: 实时监控消费者健康状态
- **统计报告**: 详细的消费统计和报告

## 使用示例

### 并发消费者
```python
from pyrocketmq.consumer import ConsumerConfig, create_consumer
from pyrocketmq.consumer.listener import MessageListenerConcurrently, ConsumeResult

class MyMessageListener(MessageListenerConcurrently):
    def consume_message_concurrently(self, messages, context):
        for message in messages:
            print(f"消费消息: {message.body.decode()}")
            # 处理业务逻辑
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

### 订单处理消费者
```python
from pyrocketmq.consumer import ConsumerConfig, create_consumer
from pyrocketmq.consumer.listener import MessageListenerConcurrently, ConsumeResult
import json

class OrderProcessorListener(MessageListenerConcurrently):
    def consume_message_concurrently(self, messages, context):
        for message in messages:
            try:
                # 处理订单消息
                order_data = json.loads(message.body.decode())
                process_order(order_data)  # 业务处理函数
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
```

### 顺序消费者
```python
from pyrocketmq.consumer.listener import MessageListenerOrderly

class UserMessageListener(MessageListenerOrderly):
    def consume_message_orderly(self, messages, context):
        for message in messages:
            # 处理用户相关消息，保证顺序性
            user_id = message.get_property("user_id")
            process_user_message(user_id, message.body)
        
        return ConsumeResult.CONSUME_SUCCESS

# 创建顺序消费者
orderly_config = ConsumerConfig(
    consumer_group="user_orderly_consumer",
    namesrv_addr="localhost:9876",
    message_model=MessageModel.CLUSTERING,
    consume_thread_max=1  # 顺序消费通常使用单线程
)

consumer = create_consumer(orderly_config, UserMessageListener())
consumer.start()
consumer.subscribe("user_events", "*")
```

### 广播模式消费者
```python
# 广播模式消费者（每个消费者都收到所有消息）
broadcast_config = ConsumerConfig(
    consumer_group="notification_group",
    namesrv_addr="localhost:9876",
    message_model=MessageModel.BROADCASTING  # 广播模式
)

class NotificationListener(MessageListenerConcurrently):
    def consume_message_concurrently(self, messages, context):
        for message in messages:
            # 每个消费者都会收到所有通知消息
            send_notification(message.body.decode())
        return ConsumeResult.CONSUME_SUCCESS

consumer = create_consumer(broadcast_config, NotificationListener())
consumer.start()
consumer.subscribe("notifications", "*")
```