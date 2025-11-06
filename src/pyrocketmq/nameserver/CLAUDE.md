# NameServer模块文档

## 模块概述

NameServer模块是pyrocketmq的核心注册发现组件，提供与RocketMQ NameServer通信的完整功能。NameServer作为RocketMQ的注册发现中心，负责管理Topic路由信息、Broker集群信息等关键数据。

该模块支持同步和异步两种通信模式，提供统一的路由查询和缓存管理功能，封装了broker地址查询、路由信息缓存等通用逻辑，避免在多个模块中重复实现。

### 核心特性
- **双模式支持**: 同时提供同步和异步客户端，满足不同应用场景需求
- **智能缓存**: 自动管理broker地址和路由信息缓存，减少NameServer查询频率
- **线程安全**: 采用锁机制保证并发访问安全
- **容错机制**: 自动重试和故障处理，提升系统稳定性
- **统一管理**: 提供NameServer管理器，统一管理连接和缓存

## 架构设计

### 分层架构
```
┌─────────────────────────────────────────────────────────────┐
│                    应用层 (Application)                      │
├─────────────────────────────────────────────────────────────┤
│              管理层 (Manager Layer)                          │
│  ┌─────────────────┐              ┌─────────────────┐         │
│  │ NameServer      │              │ AsyncNameServer  │         │
│  │ Manager         │              │ Manager          │         │
│  └─────────────────┘              └─────────────────┘         │
├─────────────────────────────────────────────────────────────┤
│               客户端层 (Client Layer)                        │
│  ┌─────────────────┐              ┌─────────────────┐         │
│  │ SyncNameServer  │              │ AsyncNameServer  │         │
│  │ Client          │              │ Client           │         │
│  └─────────────────┘              └─────────────────┘         │
├─────────────────────────────────────────────────────────────┤
│                模型层 (Model Layer)                           │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐ │
│  │ TopicRouteData  │  │ BrokerData      │  │ BrokerCluster   │ │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘ │
├─────────────────────────────────────────────────────────────┤
│                异常层 (Error Layer)                           │
│              NameServerError异常体系                          │
└─────────────────────────────────────────────────────────────┘
```

### 数据流设计
```
Application Request
        ↓
NameServerManager (缓存检查)
        ↓
NameServer Client (网络通信)
        ↓
RocketMQ NameServer
        ↓
Response & Cache Update
```

## 核心组件

### 1. NameServerManager (同步管理器)

统一管理NameServer连接和缓存的同步管理器。

#### 输入参数
- `config: NameServerConfig` - NameServer配置对象

#### 输出返回
- 提供broker地址查询、Topic路由查询、缓存管理等功能
- 返回值类型: `str | None`, `TopicRouteData | None`, `dict[str, Any]`等

#### 使用示例
```python
from pyrocketmq.nameserver import create_nameserver_manager

# 创建管理器
manager = create_nameserver_manager(
    nameserver_addrs="localhost:9876;localhost:9877",
    timeout=30.0,
    broker_cache_ttl=600  # 10分钟缓存
)

# 启动管理器
manager.start()

# 查询broker地址
address = manager.get_broker_address("broker-a", "test-topic")
print(f"Broker地址: {address}")

# 查询Topic路由
route_data = manager.get_topic_route("test-topic")
print(f"路由信息: {route_data}")

# 获取缓存信息
cache_info = manager.get_cache_info()
print(f"缓存状态: {cache_info}")

# 停止管理器
manager.stop()
```

### 2. AsyncNameServerManager (异步管理器)

提供异步接口的NameServer管理器，适用于高并发异步应用。

#### 输入参数
- `config: NameServerConfig` - NameServer配置对象

#### 输出返回
- 所有方法返回协程对象，需要使用await关键字
- 返回值类型: `str | None`, `TopicRouteData | None`, `dict[str, Any]`等

#### 使用示例
```python
import asyncio
from pyrocketmq.nameserver import create_async_nameserver_manager

async def async_example():
    # 创建异步管理器
    manager = create_async_nameserver_manager(
        nameserver_addrs="localhost:9876;localhost:9877",
        timeout=30.0
    )
    
    # 启动管理器
    await manager.start()
    
    # 异步查询broker地址
    address = await manager.get_broker_address("broker-a", "test-topic")
    print(f"Broker地址: {address}")
    
    # 异步查询Topic路由
    route_data = await manager.get_topic_route("test-topic")
    print(f"路由信息: {route_data}")
    
    # 获取所有broker地址
    addresses = await manager.get_all_broker_addresses("test-topic")
    print(f"所有Broker: {addresses}")
    
    # 停止管理器
    await manager.stop()

# 运行异步示例
asyncio.run(async_example())
```

### 3. SyncNameServerClient (同步客户端)

与RocketMQ NameServer进行同步通信的客户端实现。

#### 主要方法
- `query_topic_route_info(topic: str) -> TopicRouteData`: 查询Topic路由信息
- `get_broker_cluster_info() -> BrokerClusterInfo`: 获取Broker集群信息
- `connect() -> None`: 建立连接
- `disconnect() -> None`: 断开连接

#### 使用示例
```python
from pyrocketmq.nameserver import create_sync_client
from pyrocketmq.remote.factory import create_sync_remote

# 创建远程连接
remote = create_sync_remote("localhost", 9876)
remote.connect()

# 创建客户端
client = create_sync_client(remote, timeout=30.0)

# 查询Topic路由
route_data = client.query_topic_route_info("test-topic")
print(f"Topic路由: {route_data}")

# 获取集群信息
cluster_info = client.get_broker_cluster_info()
print(f"集群信息: {cluster_info}")
```

### 4. AsyncNameServerClient (异步客户端)

与RocketMQ NameServer进行异步通信的客户端实现。

#### 主要方法
- `query_topic_route_info(topic: str) -> TopicRouteData`: 异步查询Topic路由信息
- `get_broker_cluster_info() -> BrokerClusterInfo`: 异步获取Broker集群信息
- `connect() -> None`: 异步建立连接
- `disconnect() -> None`: 异步断开连接

#### 使用示例
```python
import asyncio
from pyrocketmq.nameserver import create_async_client
from pyrocketmq.remote.factory import create_async_remote

async def async_client_example():
    # 创建异步远程连接
    remote = create_async_remote("localhost", 9876)
    await remote.connect()

    # 创建异步客户端
    client = create_async_client(remote, timeout=30.0)

    # 异步查询Topic路由
    route_data = await client.query_topic_route_info("test-topic")
    print(f"Topic路由: {route_data}")

    # 异步获取集群信息
    cluster_info = await client.get_broker_cluster_info()
    print(f"集群信息: {cluster_info}")

asyncio.run(async_client_example())
```

## 配置管理

### NameServerConfig配置类

```python
@dataclass
class NameServerConfig:
    """NameServer管理器配置类"""
    
    nameserver_addrs: str                    # NameServer地址，多个用分号分隔
    timeout: float = 30.0                    # 请求超时时间，单位秒
    connect_timeout: float = 10.0            # 连接超时时间，单位秒
    broker_cache_ttl: int = 300              # broker地址缓存TTL，单位秒(5分钟)
    route_cache_ttl: int = 300               # 路由信息缓存TTL，单位秒(5分钟)
    max_retry_times: int = 3                 # 最大重试次数
    retry_interval: float = 1.0              # 重试间隔，单位秒
```

#### 配置示例
```python
# 基础配置
config = NameServerConfig(nameserver_addrs="localhost:9876")

# 生产环境配置
config = NameServerConfig(
    nameserver_addrs="broker1:9876;broker2:9876;broker3:9876",
    timeout=60.0,
    connect_timeout=15.0,
    broker_cache_ttl=1800,    # 30分钟
    route_cache_ttl=1800,     # 30分钟
    max_retry_times=5,
    retry_interval=2.0
)
```

## 数据模型

### TopicRouteData
Topic路由数据结构，包含Topic下的所有Broker和队列信息。

```python
@dataclass
class TopicRouteData:
    topic: str                          # Topic名称
    queue_data_list: list[QueueData]     # 队列数据列表
    broker_data_list: list[BrokerData]   # Broker数据列表
    order_topic_conf: str                 # 顺序Topic配置
```

### BrokerData
Broker数据结构，包含Broker的基本信息和地址列表。

```python
@dataclass
class BrokerData:
    broker_name: str                    # Broker名称
    broker_addrs: dict[str, str]         # broker地址映射(broker_id -> address)
    cluster: str                        # 集群名称
```

### BrokerClusterInfo
Broker集群信息，包含整个集群的元数据。

```python
@dataclass
class BrokerClusterInfo:
    broker_addr_table: dict[str, BrokerData]  # broker地址表
    cluster_addr_table: dict[str, str]        # 集群地址表
    # ... 其他字段
```

## 异常处理

### 异常层次结构
```
NameServerError (基础异常)
├── NameServerConnectionError    # 连接异常
├── NameServerTimeoutError       # 超时异常
├── NameServerProtocolError      # 协议异常
├── NameServerResponseError      # 响应异常
└── NameServerDataParseError     # 数据解析异常
```

### 异常使用示例
```python
from pyrocketmq.nameserver import NameServerError, NameServerTimeoutError

try:
    route_data = client.query_topic_route_info("test-topic")
except NameServerTimeoutError as e:
    print(f"查询超时: {e}")
except NameServerConnectionError as e:
    print(f"连接失败: {e}")
except NameServerError as e:
    print(f"NameServer错误: {e}")
```

## 缓存机制

### 缓存策略
- **broker地址缓存**: 缓存broker名称到地址的映射，TTL默认5分钟
- **路由信息缓存**: 缓存Topic到路由数据的映射，TTL默认5分钟
- **自动过期**: 使用时间戳检查缓存是否过期
- **线程安全**: 使用锁机制保证并发访问安全

### 缓存管理示例
```python
# 查看缓存状态
cache_info = manager.get_cache_info()
print(f"缓存的broker地址数量: {cache_info['cached_broker_addresses']}")
print(f"缓存的路由数量: {cache_info['cached_routes']}")
print(f"连接的NameServer数量: {cache_info['connected_nameservers']}")

# 清理缓存
manager.clear_cache()
print("缓存已清理")
```

## 依赖项列表

### 内部依赖
- `pyrocketmq.remote` - 远程通信模块
- `pyrocketmq.logging` - 日志模块
- `pyrocketmq.model` - 数据模型模块

### 外部依赖
- `asyncio` - 异步编程支持
- `dataclasses` - 数据类支持
- `threading` - 线程同步支持
- `typing` - 类型注解支持
- `time` - 时间相关功能

## 性能优化

### 连接池管理
- 自动维护与NameServer的连接池
- 支持多NameServer地址，提供故障转移能力
- 连接复用，减少连接建立开销

### 缓存优化
- 智能缓存策略，减少网络查询
- 缓存TTL管理，平衡数据新鲜度和性能
- 线程安全的缓存操作

### 重试机制
- 可配置的重试次数和间隔
- 自动故障恢复
- 错误日志记录

## 最佳实践

### 1. 连接管理
```python
# 推荐：使用管理器统一管理连接
manager = create_nameserver_manager("ns1:9876;ns2:9876")
manager.start()

# 使用完毕后正确清理
manager.stop()
```

### 2. 异步使用
```python
# 推荐：在高并发场景使用异步管理器
async def handle_request():
    manager = create_async_nameserver_manager("ns1:9876")
    await manager.start()
    
    try:
        # 业务逻辑
        route_data = await manager.get_topic_route("test-topic")
        return route_data
    finally:
        await manager.stop()
```

### 3. 错误处理
```python
# 推荐：完善的错误处理
try:
    route_data = manager.get_topic_route("test-topic")
    if not route_data:
        logger.warning("Topic路由不存在")
        return None
except NameServerTimeoutError:
    logger.error("NameServer查询超时")
    raise
except NameServerError as e:
    logger.error(f"NameServer查询失败: {e}")
    raise
```

### 4. 缓存使用
```python
# 推荐：定期检查缓存状态
def monitor_cache():
    cache_info = manager.get_cache_info()
    
    if cache_info['cached_routes'] > 1000:
        logger.warning("路由缓存数量过多，考虑清理")
        
    if cache_info['connected_nameservers'] == 0:
        logger.error("没有可用的NameServer连接")
```

## 故障排查

### 常见问题

#### 1. 连接失败
**现象**: 无法建立NameServer连接
**原因**: 网络问题或NameServer未启动
**解决**: 检查网络连接和NameServer状态

```python
# 诊断代码
try:
    manager.start()
except NameServerError as e:
    print(f"连接失败: {e}")
    # 检查NameServer地址配置
    print(f"配置的地址: {manager.config.nameserver_addrs}")
```

#### 2. 查询超时
**现象**: 查询请求超时
**原因**: 网络延迟或NameServer负载过高
**解决**: 调整超时配置或增加重试次数

```python
# 调整配置
config = NameServerConfig(
    nameserver_addrs="localhost:9876",
    timeout=60.0,        # 增加超时时间
    max_retry_times=5    # 增加重试次数
)
```

#### 3. 缓存问题
**现象**: 返回的路由信息不是最新的
**原因**: 缓存未过期
**解决**: 手动清理缓存

```python
# 清理缓存
manager.clear_cache()
```

### 调试技巧
1. **启用详细日志**: 设置日志级别为DEBUG
2. **监控缓存状态**: 定期检查`get_cache_info()`返回的信息
3. **网络诊断**: 使用telnet或nc工具测试NameServer连通性
4. **性能监控**: 监控查询延迟和成功率

## 版本变更记录

### v1.0.0 (2025-11-06)
- ✅ 初始版本发布
- ✅ 实现同步和异步NameServer客户端
- ✅ 实现NameServer管理器，提供统一的路由查询和缓存管理
- ✅ 完整的数据模型定义
- ✅ 异常处理体系
- ✅ 详细的文档和示例

### 主要特性
- **统一接口**: 同步和异步版本使用相同的配置和缓存策略
- **智能缓存**: 自动管理broker地址和路由信息缓存
- **故障容错**: 自动重试和故障转移机制
- **类型安全**: 完整的类型注解和文档字符串
- **线程安全**: 支持高并发访问场景

---

**文档维护**: pyrocketmq开发团队  
**最后更新**: 2025-11-06  
**版本**: v1.0.0  
**状态**: ✅ 生产就绪