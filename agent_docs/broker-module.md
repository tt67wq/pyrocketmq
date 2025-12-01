# Broker层通信客户端

**模块概述**: Broker模块是pyrocketmq客户端库的核心组件，提供与RocketMQ Broker通信的完整功能实现。支持消息发送、拉取、偏移量管理、心跳维护等核心功能。

## 核心组件

### BrokerClient
同步Broker客户端实现：
- **同步消息操作**: 发送消息、拉取消息、查询消息
- **偏移量管理**: 查询、更新、重置消费者偏移量
- **心跳机制**: 定期向Broker发送心跳保持连接
- **事务支持**: 支持事务消息的提交和回滚

### AsyncBrokerClient
异步Broker客户端实现：
- **异步消息操作**: 基于asyncio的非阻塞消息处理
- **高并发支持**: 支持大量并发消息操作
- **批量处理**: 批量发送和拉取消息提高性能
- **异步事务**: 支持异步事务消息处理

### BrokerManager
同步Broker连接管理器：
- **连接管理**: 统一管理到多个Broker的连接
- **连接池**: 复用连接减少建立连接的开销
- **故障转移**: 自动检测和切换到可用Broker
- **负载均衡**: 在多个Broker间分配请求负载

### AsyncBrokerManager
异步Broker连接管理器：
- **异步连接管理**: 基于asyncio的连接池管理
- **并发优化**: 优化高并发场景下的连接使用
- **智能调度**: 根据Broker状态智能调度请求

### ConnectionInfo
Broker连接信息和状态管理：
- **连接状态**: 跟踪连接的健康状态
- **性能指标**: 记录延迟、吞吐量等性能数据
- **配置信息**: 保存Broker的连接配置

## 核心功能

### 消息发送
- **单条发送**: 发送单条消息到指定队列
- **批量发送**: 批量发送多条消息提高效率
- **事务发送**: 支持事务消息的两阶段提交
- **单向发送**: 不需要响应确认的高性能发送

### 消息拉取
- **同步拉取**: 阻塞式拉取消息
- **异步拉取**: 非阻塞式拉取消息
- **批量拉取**: 一次拉取多条消息
- **长轮询**: 支持服务器长轮询减少空请求

### 偏移量管理
- **查询偏移量**: 获取消费者当前的消费偏移量
- **更新偏移量**: 提交消费偏移量到Broker
- **重置偏移量**: 重置到指定位置的偏移量
- **本地存储**: 支持偏移量的本地缓存

### 心跳维护
- **定期心跳**: 定期向Broker发送心跳
- **状态同步**: 同步客户端状态信息
- **故障检测**: 检测连接故障并自动恢复

## 关键特性

- **双模式支持**: 同步和异步客户端，满足不同性能需求场景
- **20+种异常类型**: 精确处理各种错误场景，便于问题定位
- **协议兼容**: 严格遵循RocketMQ TCP协议规范，与Go语言实现完全兼容
- **oneway通信**: 支持单向通信模式，减少不必要的响应等待
- **智能连接管理**: 提供with风格的连接获取方法，自动资源管理
- **连接复用优化**: 健康检查创建的连接自动复用，提高效率
- **故障恢复**: 自动检测连接故障并进行重连
- **性能监控**: 内置性能指标收集，支持监控和调优

## 使用示例

```python
from pyrocketmq.broker import BrokerClient, create_broker_manager
from pyrocketmq.model.message import Message, MessageQueue

# 创建Broker管理器
manager = create_broker_manager("localhost:9876")

# 使用连接发送消息
with manager.connection("broker1:10911") as broker_client:
    # 发送单条消息
    message = Message(topic="test_topic", body=b"Hello RocketMQ")
    message_queue = MessageQueue("test_topic", "broker1", 0)
    
    result = broker_client.sync_send_message(
        producer_group="test_group",
        message_body=message.body,
        message_queue=message_queue,
        properties=message.properties
    )
    print(f"发送结果: {result}")

    # 批量发送消息
    messages = [
        Message(topic="test_topic", body=f"Message {i}".encode())
        for i in range(5)
    ]
    
    batch_results = broker_client.sync_send_batch_message(
        producer_group="test_group",
        messages=messages,
        message_queue=message_queue
    )
    print(f"批量发送结果: {batch_results}")

    # 拉取消息
    pull_result = broker_client.sync_pull_message(
        consumer_group="test_consumer",
        message_queue=message_queue,
        offset=0,
        max_nums=32
    )
    print(f"拉取结果: {pull_result}")

    # 查询偏移量
    offset = broker_client.query_consumer_offset(
        consumer_group="test_consumer",
        message_queue=message_queue
    )
    print(f"当前偏移量: {offset}")

    # 更新偏移量
    broker_client.update_consumer_offset(
        consumer_group="test_consumer",
        message_queue=message_queue,
        offset=100
    )

# 异步Broker客户端使用
async def async_broker_example():
    from pyrocketmq.broker import AsyncBrokerClient, create_async_broker_manager
    
    manager = await create_async_broker_manager("localhost:9876")
    
    async with manager.connection("broker1:10911") as broker_client:
        # 异步发送消息
        message = Message(topic="test_topic", body=b"Hello Async RocketMQ")
        result = await broker_client.async_send_message(
            producer_group="test_group",
            message_body=message.body,
            message_queue=message_queue,
            properties=message.properties
        )
        print(f"异步发送结果: {result}")

# 运行异步示例
import asyncio
asyncio.run(async_broker_example())
```