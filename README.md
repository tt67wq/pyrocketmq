# pyrocketmq

[![Python](https://img.shields.io/badge/Python-3.11+-blue.svg)](https://www.python.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Development Status](https://img.shields.io/badge/Development-Production%20Ready-brightgreen.svg)](#)
[![Build Status](https://img.shields.io/badge/Build-Passing-brightgreen.svg)](#)

> **🚀 生产就绪**: pyrocketmq是一个功能完整的Python RocketMQ客户端库，提供高性能、可靠的消息队列生产者和消费者功能。

pyrocketmq是一个纯Python实现的RocketMQ客户端库，完全兼容RocketMQ TCP协议规范。项目提供了完整的协议模型层、网络传输层、远程通信层以及NameServer和Broker客户端实现，同时包含完整的生产者和消费者功能。

## ✨ 核心特性

### 🎯 完整的客户端实现
- **消息生产**: 完整的Producer实现，支持同步/异步/事务消息发送
- **消息消费**: 完整的Consumer实现，支持并发/顺序消费、集群/广播模式
- **协议兼容性**: 完全兼容RocketMQ Go语言实现的TCP协议格式
- **类型安全**: 基于Python 3.11+的完整类型注解
- **高性能**: 基于asyncio的异步网络通信

### 🏗️ 分层架构设计
- **协议模型层**: 完整的RemotingCommand数据结构和序列化
- **网络传输层**: 基于状态机的TCP连接管理
- **远程通信层**: 异步/同步RPC通信实现
- **客户端层**: NameServer和Broker客户端封装
- **应用层**: Producer和Consumer高级功能实现

### 🔧 开发友好
- **便利工厂**: 提供简便的创建方法，快速上手
- **完整配置**: 丰富的配置选项和合理的默认值
- **完整测试**: 20+个测试用例，覆盖所有核心功能
- **详细文档**: 完整的API文档和使用示例
- **多种模式**: 支持同步、异步、事务、并发、顺序等多种消息模式

## 🚀 快速开始

### 安装

```bash
# 使用pip安装
pip install pyrocketmq

# 或从源码安装
git clone https://github.com/tt67wq/pyrocketmq.git
cd pyrocketmq
pip install -e .
```

### 消息生产者使用

```python
import time
from pyrocketmq.producer import create_producer
from pyrocketmq.model.message import Message

# 创建生产者
producer = create_producer("test_producer_group", "localhost:9876")
producer.start()

try:
    # 发送单条消息
    message = Message(
        topic="test_topic",
        body=b"Hello, pyrocketmq!"
    )
    message.set_tags("test_tag")
    message.set_keys("order_123")
    
    result = producer.send(message)
    print(f"消息发送结果: {result}")

    # 发送批量消息
    messages = [
        Message(topic="test_topic", body=f"消息 {i}".encode())
        for i in range(5)
    ]
    batch_result = producer.send_batch(*messages)
    print(f"批量消息发送结果: {batch_result}")

finally:
    producer.shutdown()
```

### 消息消费者使用

```python
from pyrocketmq.consumer import create_concurrent_consumer
from pyrocketmq.consumer.listener import ConsumeResult
from pyrocketmq.model import create_tag_selector

def message_handler(messages):
    """消息处理函数"""
    for message in messages:
        print(f"收到消息: {message.body.decode()}")
        print(f"主题: {message.topic}, 标签: {message.get_tags()}")
    return ConsumeResult.CONSUME_SUCCESS

# 创建消费者
consumer = create_concurrent_consumer("test_consumer_group", "localhost:9876")

# 订阅主题
consumer.subscribe(
    "test_topic", 
    create_tag_selector("*"),
    message_handler
)

try:
    # 启动消费者
    consumer.start()
    print("消费者启动成功，等待消息...")
    
    # 保持运行
    while True:
        time.sleep(1)
        
except KeyboardInterrupt:
    print("关闭消费者...")
    consumer.shutdown()
```

### 异步消费者使用

```python
import asyncio
from pyrocketmq.consumer import create_async_concurrent_consumer, create_async_message_listener
from pyrocketmq.model import ConsumeResult, create_tag_selector

async def async_message_handler(messages):
    """异步消息处理函数"""
    for message in messages:
        # 异步处理消息
        await process_message_async(message)
        print(f"异步处理完成: {message.body.decode()}")
    return ConsumeResult.CONSUME_SUCCESS

async def main():
    # 创建异步消费者
    consumer = await create_async_concurrent_consumer("async_consumer_group", "localhost:9876")
    
    # 订阅主题
    await consumer.subscribe(
        "test_topic",
        create_tag_selector("*"),
        create_async_message_listener(async_message_handler)
    )
    
    try:
        await consumer.start()
        print("异步消费者启动成功")
        
        # 运行30秒
        await asyncio.sleep(30)
        
    finally:
        await consumer.shutdown()

asyncio.run(main())
```

### 事务消息使用

```python
from pyrocketmq.producer import create_transaction_producer
from pyrocketmq.producer.transaction import TransactionListener, LocalTransactionState

class OrderTransactionListener(TransactionListener):
    def execute_local_transaction(self, message, transaction_id, arg=None):
        """执行本地事务"""
        try:
            # 处理订单
            order_data = eval(message.body.decode())
            create_order(order_data)
            return LocalTransactionState.COMMIT_MESSAGE
        except Exception:
            return LocalTransactionState.ROLLBACK_MESSAGE
    
    def check_local_transaction(self, message, transaction_id):
        """检查本地事务状态"""
        order_id = message.get_property("order_id")
        if order_exists(order_id):
            return LocalTransactionState.COMMIT_MESSAGE
        return LocalTransactionState.ROLLBACK_MESSAGE

# 创建事务生产者
producer = create_transaction_producer("order_producer_group", "localhost:9876")
producer.register_transaction_listener(OrderTransactionListener())
producer.start()

try:
    # 发送事务消息
    order_data = {"order_id": "12345", "amount": 100}
    transaction_message = Message(
        topic="order_topic",
        body=str(order_data).encode()
    )
    transaction_message.set_property("order_id", "12345")
    
    result = producer.send_message_in_transaction(transaction_message)
    print(f"事务消息发送结果: {result}")
    
finally:
    producer.shutdown()
```

## 📋 项目架构

```
src/pyrocketmq/
├── model/              # 协议模型层 ✅
│   ├── command.py      # 核心数据结构 RemotingCommand
│   ├── serializer.py   # 二进制序列化/反序列化器
│   ├── message.py      # 消息数据结构
│   ├── message_ext.py  # 扩展消息数据结构
│   ├── message_queue.py # 消息队列数据结构
│   ├── factory.py      # 请求工厂和构建器
│   └── ...             # 其他模型组件
├── transport/          # 网络传输层 ✅
│   ├── tcp.py          # TCP连接实现（状态机驱动）
│   ├── config.py       # 传输配置管理
│   └── ...             # 传输层组件
├── remote/             # 远程通信层 ✅
│   ├── async_remote.py # 异步远程通信实现
│   ├── sync_remote.py  # 同步远程通信实现
│   ├── pool.py         # 连接池管理
│   └── ...             # 远程通信组件
├── nameserver/         # NameServer客户端 ✅
│   ├── manager.py      # NameServer管理器
│   ├── async_manager.py # 异步NameServer管理器
│   └── ...             # NameServer客户端组件
├── broker/             # Broker客户端 ✅
│   ├── client.py       # Broker客户端
│   ├── manager.py      # Broker管理器
│   └── ...             # Broker客户端组件
├── producer/           # 消息生产者 ✅
│   ├── producer.py     # 同步生产者实现
│   ├── async_producer.py # 异步生产者实现
│   ├── transactional_producer.py # 事务生产者
│   ├── config.py       # 生产者配置管理
│   ├── router.py       # 消息路由管理
│   └── ...             # 生产者相关组件
├── consumer/           # 消息消费者 ✅
│   ├── concurrent_consumer.py # 并发消费者
│   ├── async_concurrent_consumer.py # 异步并发消费者
│   ├── oredrly_consumer.py # 顺序消费者
│   ├── config.py       # 消费者配置管理
│   ├── listener.py     # 消息监听器
│   ├── async_listener.py # 异步消息监听器
│   ├── subscription_manager.py # 订阅管理
│   ├── offset_store.py # 偏移量存储
│   └── ...             # 消费者相关组件
├── logging/            # 日志模块 ✅
│   ├── logger.py       # 日志记录器
│   ├── config.py       # 日志配置
│   └── json_formatter.py # JSON格式化器
└── utils/              # 工具模块 ✅
    ├── rwlock.py       # 读写锁
    └── async_rwlock.py # 异步读写锁
```

## 💡 核心功能详解

### 1. 消息生产功能

#### 支持的消息类型
- **同步消息**: 阻塞等待发送结果，确保消息可靠投递
- **异步消息**: 非阻塞发送，通过回调获取结果
- **单向消息**: 不关心发送结果，最高性能
- **事务消息**: 支持分布式事务，保证消息一致性
- **批量消息**: 一次发送多条消息，提高吞吐量

#### 消息路由策略
```python
from pyrocketmq.producer.queue_selectors import (
    RoundRobinSelector,      # 轮询选择队列
    RandomSelector,          # 随机选择队列
    MessageHashSelector      # 基于消息哈希选择队列
)

# 使用自定义队列选择器
producer = create_producer("test_group", "localhost:9876")
producer.set_queue_selector(MessageHashSelector())
```

### 2. 消息消费功能

#### 消费模式
- **并发消费**: 多线程并行处理消息，高吞吐量
- **顺序消费**: 保证同一队列内消息的顺序性
- **集群消费**: 一个消息只被组内一个消费者消费
- **广播消费**: 所有消费者都能收到消息

#### 监听器类型
```python
from pyrocketmq.consumer.listener import (
    MessageListener,              # 基础监听器
    MessageListenerConcurrently,  # 并发消息监听器
    MessageListenerOrderly        # 顺序消息监听器
)

from pyrocketmq.consumer.async_listener import (
    AsyncMessageListener,              # 异步基础监听器
    AsyncMessageListenerConcurrently,  # 异步并发监听器
    AsyncMessageListenerOrderly        # 异步顺序监听器
)
```

#### 偏移量管理
- **远程偏移量存储**: 集群模式，存储在Broker端
- **本地偏移量存储**: 广播模式，存储在本地文件
- **自动提交**: 定期持久化消费进度
- **手动提交**: 应用程序控制提交时机

### 3. 高级配置

#### 生产者配置
```python
from pyrocketmq.producer.config import (
    ProducerConfig,
    PRODUCTION_CONFIG,
    HIGH_PERFORMANCE_CONFIG,
    DEVELOPMENT_CONFIG
)

# 使用预设配置
config = PRODUCTION_CONFIG
config.producer_group = "my_group"
config.namesrv_addr = "localhost:9876"

# 自定义配置
custom_config = ProducerConfig(
    producer_group="high_perf_producer",
    namesrv_addr="broker1:9876;broker2:9876",
    send_msg_timeout=3000,
    retry_times_when_send_failed=3,
    compress_msg_body_over_howmuch=1024
)
```

#### 消费者配置
```python
from pyrocketmq.consumer.config import ConsumerConfig

config = ConsumerConfig(
    consumer_group="test_consumer_group",
    namesrv_addr="localhost:9876",
    message_model="CLUSTERING",  # 或 "BROADCASTING"
    consume_thread_min=20,
    consume_thread_max=64,
    pull_batch_size=32,
    consume_timeout=15
)
```

## 🧪 运行示例

项目提供了丰富的示例代码，位于 `examples/` 目录：

### 生产者示例
```bash
# 基础生产者
export PYTHONPATH=/path/to/pyrocketmq/src
python examples/producers/basic_producer.py

# 异步生产者
python examples/producers/basic_async_producer.py

# 事务生产者
python examples/producers/transactional_producer.py

# 异步事务生产者
python examples/producers/async_transactional_producer.py
```

### 消费者示例
```bash
# 集群并发消费者
python examples/consumers/cluster_concurrent_consumer.py

# 广播并发消费者
python examples/consumers/broadcast_concurrent_consumer.py

# 集群顺序消费者
python examples/consumers/cluster_orderly_consumer.py

# 异步并发消费者
python examples/consumers/async_concurrent_consumer.py
```

## 🧪 运行测试

```bash
# 设置环境变量（必需）
export PYTHONPATH=/path/to/pyrocketmq/src

# 运行所有测试
python -m pytest tests/ -v

# 运行特定模块测试
python -m pytest tests/model/ -v
python -m pytest tests/transport/ -v
python -m pytest tests/remote/ -v
python -m pytest tests/broker/ -v
python -m pytest tests/nameserver/ -v
python -m pytest tests/producer/ -v
python -m pytest tests/consumer/ -v

# 运行异步测试
python -m pytest tests/transport/ -v --asyncio-mode=auto
python -m pytest tests/consumer/ -v --asyncio-mode=auto
```

## 📊 性能特性

- **高吞吐量**: 支持数万级TPS的消息处理
- **低延迟**: 毫秒级的消息处理延迟
- **高并发**: 支持数百个并发线程/异步任务
- **连接复用**: 智能连接池管理，减少连接开销
- **自动重连**: 内置重连机制和故障恢复
- **负载均衡**: 支持多Broker负载均衡和路由

## 🔍 错误处理

项目提供了完整的异常处理层次：

```python
# 生产者异常
from pyrocketmq.producer.errors import (
    ProducerError,
    ProducerStartError,
    MessageSendError,
    TransactionError
)

# 消费者异常
from pyrocketmq.consumer.errors import (
    ConsumerError,
    ConsumerStartError,
    MessageConsumeError,
    SubscribeError
)

# 基础异常
from pyrocketmq.transport.errors import TransportError
from pyrocketmq.remote.errors import RemoteError
from pyrocketmq.broker.errors import BrokerError
from pyrocketmq.nameserver.errors import NameServerError
```

## 📈 监控和指标

### 生产者指标
- 消息发送成功/失败数量
- 消息发送延迟
- 事务消息状态统计
- 队列选择策略效果

### 消费者指标
- 消息消费成功/失败数量
- 消费延迟和TPS
- 重平衡次数
- 偏移量提交状态

### 获取指标信息
```python
# 生产者指标
producer_stats = producer.get_stats()
print(f"发送成功率: {producer_stats.success_rate}")

# 消费者指标
consumer_stats = consumer.get_stats()
print(f"消费TPS: {consumer_stats.consume_tps}")
```

## 🛠️ 开发环境

### 系统要求
- Python 3.11+
- RocketMQ 4.x+
- asyncio支持

### 开发配置

```bash
# 激活虚拟环境
source .venv/bin/activate

# 设置PYTHONPATH
export PYTHONPATH=/path/to/pyrocketmq/src

# 安装开发依赖
pip install -e .
# 或使用uv
uv sync
```

### 调试配置

```python
from pyrocketmq.logging import setup_logging, LoggingConfig

# 启用调试日志
setup_logging(LoggingConfig(
    level='DEBUG',
    json_output=True,
    file_path='debug.log'
))
```

## 🔧 配置管理

### 环境变量支持
```bash
# 生产者配置
export ROCKETMQ_PRODUCER_GROUP="my_producer_group"
export ROCKETMQ_NAMESRV_ADDR="localhost:9876"
export ROCKETMQ_SEND_MSG_TIMEOUT="3000"

# 消费者配置
export ROCKETMQ_CONSUMER_GROUP="my_consumer_group"
export ROCKETMQ_CONSUME_THREAD_MAX="64"
export ROCKETMQ_PULL_BATCH_SIZE="32"
```

### 配置文件支持
```python
# 从JSON配置文件加载
from pyrocketmq.producer.config import load_config_from_file

config = load_config_from_file("producer_config.json")
producer = create_producer_from_config(config)
```

## 🚀 生产最佳实践

### 生产者最佳实践
1. **合理设置超时时间**: 根据网络环境设置合适的发送超时
2. **使用批量消息**: 提高消息发送吞吐量
3. **选择合适的队列策略**: 根据业务需求选择路由策略
4. **监控发送指标**: 及时发现和解决问题
5. **处理异常情况**: 完善的错误处理和重试机制

### 消费者最佳实践
1. **合理设置线程数**: 根据CPU核心数和处理复杂度配置线程池
2. **批量处理消息**: 提高消费效率
3. **避免阻塞操作**: 不要在监听器中执行耗时操作
4. **正确处理异常**: 返回合适的消费结果
5. **监控消费延迟**: 确保消息及时处理

## 🤝 贡献指南

我们欢迎所有形式的贡献！

### 如何贡献
1. Fork 项目
2. 创建特性分支 (`git checkout -b feature/amazing-feature`)
3. 提交更改 (`git commit -m 'Add amazing feature'`)
4. 推送到分支 (`git push origin feature/amazing-feature`)
5. 创建 Pull Request

### 开发指南
- 遵循现有的代码风格
- 添加完整的类型注解
- 编写相应的测试用例
- 更新相关文档

## 📄 许可证

本项目采用 MIT 许可证 - 详见 [LICENSE](LICENSE) 文件。

## 🙏 致谢

- [RocketMQ](https://rocketmq.apache.org/) - 优秀的分布式消息队列
- Python 社区 - 提供了强大的生态系统
- 所有贡献者 - 让这个项目变得更好

## 📞 联系方式

- 项目主页: [GitHub Repository](https://github.com/tt67wq/pyrocketmq)
- 问题反馈: [GitHub Issues](https://github.com/tt67wq/pyrocketmq/issues)
- 开发讨论: [GitHub Discussions](https://github.com/tt67wq/pyrocketmq/discussions)

---

**🚀 pyrocketmq**: 为Python开发者提供功能完整、性能优异的RocketMQ客户端解决方案！

**当前状态**: ✅ 生产就绪，完整的Producer和Consumer功能实现
