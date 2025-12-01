# NameServer层注册发现客户端

**模块概述**: NameServer模块提供与RocketMQ NameServer通信的完整功能。NameServer作为RocketMQ的注册发现中心，负责管理Topic路由信息、Broker集群信息等关键数据。

## 核心组件

### SyncNameServerClient
同步NameServer客户端实现：
- **同步调用**: 阻塞式请求响应模式
- **路由查询**: 获取Topic路由信息
- **集群管理**: 查询Broker集群信息
- **简单易用**: 适合传统同步应用

### AsyncNameServerClient
异步NameServer客户端实现：
- **异步IO**: 基于asyncio的非阻塞操作
- **高并发**: 支持大量并发路由查询
- **性能优化**: 减少线程切换开销
- **现代应用**: 适合高并发和异步应用

### BrokerData
Broker信息数据结构：
- **集群信息**: broker名称、集群名称
- **地址列表**: broker地址列表（主备）
- **状态信息**: broker状态和版本信息

### TopicRouteData
Topic路由数据结构：
- **队列数据**: 消息队列分布信息
- **Broker信息**: topic所属的broker信息
- **版本控制**: 路由信息的版本号

## 核心功能

### 路由查询
- **查询Topic路由**: 获取指定Topic的路由信息
- **获取Broker列表**: 查询集群中的所有Broker
- **路由缓存**: 本地缓存路由信息，减少网络请求

### 集群管理
- **集群信息查询**: 获取Broker集群的详细信息
- **状态监控**: 监控Broker的状态变化
- **动态更新**: 支持路由信息的动态更新

### 客户端注册
- **Producer注册**: 向NameServer注册Producer信息
- **Consumer注册**: 向NameServer注册Consumer信息
- **心跳维护**: 定期发送心跳保持连接

## 关键特性

- **双模式支持**: 同步和异步客户端，适应不同应用场景
- **JSON兼容性**: 严格处理Go语言JSON序列化的整数key兼容性问题
- **专门异常体系**: 精确处理NameServer通信的各种错误场景
- **路由缓存**: 支持路由信息的本地缓存和过期管理
- **协议兼容**: 完全兼容RocketMQ NameServer协议
- **自动重试**: 内置重试机制，提高通信可靠性

## 使用示例

```python
from pyrocketmq.nameserver import SyncNameServerClient
from pyrocketmq.remote import create_remote

# 创建同步NameServer客户端
remote = create_remote("localhost", 9876)
client = SyncNameServerClient(remote, timeout=5000.0)

# 查询Topic路由信息
route_data = client.query_topic_route_info("test_topic")
print(f"Topic路由: {route_data}")

# 获取Broker集群信息
cluster_info = client.get_broker_cluster_info()
print(f"集群信息: {cluster_info}")

# 注册Producer
client.register_producer("producer_group", "producer_client_id")

# 注册Consumer
client.register_consumer("consumer_group", "consumer_client_id")

# 异步NameServer客户端
async def async_example():
    from pyrocketmq.nameserver import AsyncNameServerClient
    
    async_remote = await create_async_remote("localhost", 9876)
    async_client = AsyncNameServerClient(async_remote, timeout=5000.0)
    
    # 异步查询Topic路由
    route_data = await async_client.query_topic_route_info("test_topic")
    print(f"异步Topic路由: {route_data}")
    
    # 异步获取集群信息
    cluster_info = await async_client.get_broker_cluster_info()
    print(f"异步集群信息: {cluster_info}")

# 运行异步示例
import asyncio
asyncio.run(async_example())
```