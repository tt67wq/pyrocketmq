# 开发模式和使用示例

## 使用队列选择器模式

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

## Producer使用模式

### 同步Producer
```python
# 同步Producer
from pyrocketmq.producer import create_producer
from pyrocketmq.model.message import Message

producer = create_producer("GID_POETRY", "nameserver:9876")
producer.start()

message = Message(topic="test_topic", body=b"Hello, RocketMQ!")
result = producer.send(message)

print(f"发送结果: {result.message_id}, 状态: {result.status}")
```

### 异步Producer
```python
# 异步Producer
from pyrocketmq.producer import create_async_producer
import asyncio

async def async_send():
    producer = await create_async_producer("GID_POETRY", "nameserver:9876")
    await producer.start()

    message = Message(topic="test_topic", body=b"Hello, Async RocketMQ!")
    result = await producer.send(message)
    
    print(f"异步发送结果: {result.message_id}")

asyncio.run(async_send())
```

## 事务消息发送模式

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
import json

# 自定义事务监听器
class OrderTransactionListener(TransactionListener):
    def execute_local_transaction(self, message, transaction_id: str, arg=None) -> LocalTransactionState:
        try:
            # 执行本地事务（如订单创建）
            order_data = json.loads(message.body.decode())
            # create_order(order_data)  # 执行实际的本地事务
            print(f"创建订单: {order_data['order_id']}")
            return LocalTransactionState.COMMIT_MESSAGE
        except Exception as e:
            logger.error(f"Order creation failed: {e}")
            return LocalTransactionState.ROLLBACK_MESSAGE

    def check_local_transaction(self, message, transaction_id: str) -> LocalTransactionState:
        # 检查本地事务状态
        order_id = message.get_property("order_id")
        # if order_exists(order_id):
        #     return LocalTransactionState.COMMIT_MESSAGE
        return LocalTransactionState.COMMIT_MESSAGE

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

## Consumer使用模式

### 并发消费者使用
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
                # process_order(order_data)  # 业务处理函数
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

### 顺序消费者使用
```python
# 顺序消费者使用（保证同一用户的消息顺序处理）
from pyrocketmq.consumer.listener import MessageListenerOrderly

class UserMessageListener(MessageListenerOrderly):
    def consume_message_orderly(self, messages, context):
        for message in messages:
            # 处理用户相关消息，保证顺序性
            user_id = message.get_property("user_id")
            # process_user_message(user_id, message.body)
            print(f"处理用户 {user_id} 的消息")
        
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
            # send_notification(message.body.decode())
            print(f"发送通知: {message.body.decode()}")
        return ConsumeResult.CONSUME_SUCCESS

consumer = create_consumer(broadcast_config, NotificationListener())
consumer.start()
consumer.subscribe("notifications", "*")
```

## 扩展自定义选择器

```python
from pyrocketmq.producer.topic_broker_mapping import QueueSelector

class CustomSelector(QueueSelector):
    def select(self, topic, available_queues, message=None):
        # 自定义选择逻辑
        # 例如基于broker负载、地域、消息大小等
        
        # 基于消息大小的负载均衡
        if message and len(message.body) > 1024:  # 大消息
            # 选择负载较低的broker
            return available_queues[0] if available_queues else None
        else:
            # 小消息使用轮询
            return available_queues[len(message.body) % len(available_queues)]

# 使用自定义选择器
custom_selector = CustomSelector()
mapping = TopicBrokerMapping(default_selector=custom_selector)
```

## 完整的生产环境示例

### 生产者完整配置
```python
from pyrocketmq.producer import ProducerConfig, create_producer
from pyrocketmq.logging import configure_logging, LoggingConfig

# 配置日志
log_config = LoggingConfig(
    level="INFO",
    format_type="json",
    output_file="producer.log"
)
configure_logging(log_config)

# 生产者配置
config = ProducerConfig(
    producer_group="order_producer_group",
    namesrv_addr="nameserver1:9876;nameserver2:9876",
    send_msg_timeout=5000,
    retry_times_when_send_failed=3,
    compress_msg_body_over_howmuch=1024*1024,  # 1MB以上压缩
    max_message_size=4*1024*1024,              # 最大4MB
    retry_another_broker_when_not_store_ok=True
)

producer = create_producer(config)
producer.start()

# 发送消息
try:
    message = Message(
        topic="order_events",
        body=json.dumps(order_data).encode(),
        tags="order_created"
    )
    message.set_property("order_id", order_id)
    message.set_property("user_id", user_id)
    
    result = producer.send(message)
    logger.info(f"Message sent successfully: {result.message_id}")
except Exception as e:
    logger.error(f"Failed to send message: {e}")
```

### 消费者完整配置
```python
from pyrocketmq.consumer import ConsumerConfig, create_consumer
from pyrocketmq.consumer.listener import MessageListenerConcurrently, ConsumeResult

# 消费者配置
config = ConsumerConfig(
    consumer_group="order_processor_group",
    namesrv_addr="nameserver1:9876;nameserver2:9876",
    message_model=MessageModel.CLUSTERING,
    consume_from_where=ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET,
    consume_thread_min=20,
    consume_thread_max=50,
    pull_batch_size=32,
    pull_threshold_for_queue=1000,
    consume_timeout=15*60*1000,  # 15分钟
    max_reconsume_times=16
)

class OrderProcessingListener(MessageListenerConcurrently):
    def consume_message_concurrently(self, messages, context):
        for message in messages:
            try:
                order_data = json.loads(message.body.decode())
                order_id = order_data.get('order_id')
                
                # 业务处理
                if process_order(order_data):
                    logger.info(f"Order processed successfully: {order_id}")
                else:
                    logger.warning(f"Order processing failed: {order_id}")
                    return ConsumeResult.RECONSUME_LATER
                    
            except Exception as e:
                logger.error(f"Error processing message: {e}", exc_info=True)
                return ConsumeResult.RECONSUME_LATER
        
        return ConsumeResult.CONSUME_SUCCESS

consumer = create_consumer(config, OrderProcessingListener())
consumer.start()
consumer.subscribe("order_events", "order_created || order_updated")

# 优雅关闭
import signal
import sys

def signal_handler(signum, frame):
    logger.info("Received shutdown signal, shutting down consumer...")
    consumer.shutdown()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# 保持运行
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    consumer.shutdown()
```

## 监控和运维

### 健康检查
```python
from pyrocketmq.producer import create_producer

class HealthChecker:
    def __init__(self, producer):
        self.producer = producer
    
    def check_broker_health(self):
        """检查Broker健康状态"""
        try:
            # 发送测试消息
            test_message = Message(topic="__health_check__", body=b"ping")
            result = self.producer.send(test_message)
            return True
        except Exception as e:
            logger.error(f"Broker health check failed: {e}")
            return False

# 使用健康检查
producer = create_producer("health_checker", "localhost:9876")
health_checker = HealthChecker(producer)

if health_checker.check_broker_health():
    print("Broker is healthy")
else:
    print("Broker is not healthy")
```