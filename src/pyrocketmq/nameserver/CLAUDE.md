# NameServer模块设计文档

## 模块概述

NameServer模块是pyrocketmq的核心组件之一，提供与RocketMQ NameServer通信的完整功能。NameServer作为RocketMQ的注册发现中心，负责管理Topic路由信息、Broker集群信息等关键数据。该模块实现了完整的NameServer客户端功能，支持同步和异步两种通信模式，为Producer和Consumer提供路由信息服务。

## 模块架构

### 文件结构
```
nameserver/
├── __init__.py          # 模块入口，统一导出接口
├── client.py            # NameServer客户端实现（同步/异步）
├── models.py            # 数据结构模型导入
└── errors.py            # NameServer异常定义
```

### 架构设计原则

1. **双模式支持**: 同时提供同步(SyncNameServerClient)和异步(AsyncNameServerClient)客户端
2. **数据兼容性**: 严格处理Go语言JSON序列化的整数key兼容性问题
3. **异常分层**: 定义专门的NameServer异常体系，精确处理各种错误场景
4. **路由缓存**: 支持路由信息的本地缓存和更新机制
5. **协议兼容**: 完全兼容RocketMQ NameServer协议

## 核心数据结构

### 1. BrokerData - Broker信息
表示单个Broker的详细信息，包含集群归属、名称和地址信息。

```python
@dataclass
class BrokerData:
    cluster: str                    # 集群名称
    broker_name: str               # Broker名称
    broker_addresses: Dict[int, str]  # Broker地址映射 {brokerId: address}
```

**关键特性**:
- **地址映射**: 支持多个Broker地址，以brokerId为key
- **兼容性处理**: 专门处理Go语言JSON整数key的兼容性问题
- **序列化支持**: 提供完整的序列化/反序列化方法

### 2. QueueData - 队列信息
描述Broker上的队列配置信息。

```python
@dataclass
class QueueData:
    broker_name: str               # Broker名称
    read_queue_nums: int           # 读队列数量
    write_queue_nums: int          # 写队列数量
    perm: int                      # 权限标识
    topic_syn_flag: int            # Topic同步标志
    compression_type: str = "gzip" # 压缩类型
```

**权限标识**:
- `6`: 可读可写
- `4`: 只读
- `2`: 只写

### 3. TopicRouteData - Topic路由信息
核心数据结构，包含Topic的完整路由信息。

```python
@dataclass
class TopicRouteData:
    order_topic_conf: str                           # 顺序Topic配置
    queue_data_list: List[QueueData]               # 队列数据列表
    broker_data_list: List[BrokerData]             # Broker数据列表
```

**数据组成**:
- **队列信息**: 包含该Topic的所有队列配置
- **Broker信息**: 包含承载该Topic的所有Broker信息
- **顺序配置**: 顺序消息的特殊配置

### 4. BrokerClusterInfo - Broker集群信息
整个集群的概览信息，包含所有Broker和集群的映射关系。

```python
@dataclass
class BrokerClusterInfo:
    broker_addr_table: Dict[str, BrokerData]       # Broker地址表
    cluster_addr_table: Dict[str, List[str]]       # 集群地址表
```

## 客户端实现

### SyncNameServerClient - 同步客户端
基于Remote实现的同步NameServer客户端。

**核心方法**:
- `query_topic_route_info(topic)` - 查询Topic路由信息
- `get_broker_cluster_info()` - 获取Broker集群信息
- `connect()/disconnect()` - 连接管理
- `is_connected()` - 连接状态检查

**使用示例**:
```python
from pyrocketmq.nameserver import create_sync_client

# 创建同步客户端
client = create_sync_client("localhost", 9876)
client.connect()

# 查询Topic路由
route_data = client.query_topic_route_info("test_topic")
print(f"Found {len(route_data.broker_data_list)} brokers")
```

### AsyncNameServerClient - 异步客户端
基于AsyncRemote实现的异步NameServer客户端。

**核心方法**:
- `async_query_topic_route_info(topic)` - 异步查询Topic路由信息
- `async_get_broker_cluster_info()` - 异步获取Broker集群信息
- `async_connect()/async_disconnect()` - 异步连接管理
- `is_connected()` - 连接状态检查

**使用示例**:
```python
from pyrocketmq.nameserver import create_async_client

# 创建异步客户端
client = await create_async_client("localhost", 9876)
await client.connect()

# 异步查询Topic路由
route_data = await client.async_query_topic_route_info("test_topic")
print(f"Found {len(route_data.broker_data_list)} brokers")
```

## 协议处理

### 请求类型
NameServer主要支持以下请求类型：

1. **GET_ROUTE_INFO** - 获取Topic路由信息
   - 请求代码: `RequestCode.GET_ROUTEINFO`
   - 请求参数: topic名称
   - 响应数据: TopicRouteData

2. **GET_BROKER_CLUSTER_INFO** - 获取Broker集群信息
   - 请求代码: `RequestCode.GET_BROKER_CLUSTER_INFO`
   - 请求参数: 无
   - 响应数据: BrokerClusterInfo

### 数据兼容性处理
专门处理Go语言JSON序列化的兼容性问题：

```python
# Go语言返回的JSON可能包含整数key
{
    "brokerAddrs": {
        "0": "127.0.0.1:10911",  // 字符串形式的整数
        "1": "127.0.0.1:10912"
    }
}

# 使用ast.literal_eval处理，保持整数类型
import ast
data_str = data.decode("utf-8")
parsed_data = ast.literal_eval(data_str)

# 转换为标准的整数key
normalized_addrs = {}
for broker_id_str, address in broker_addrs.items():
    broker_id = int(broker_id_str)  # 转换为整数
    normalized_addrs[broker_id] = str(address)
```

## 异常处理体系

### 异常层次结构
```
NameServerError (基础异常)
├── NameServerConnectionError (连接错误)
├── NameServerTimeoutError (超时错误)
├── NameServerProtocolError (协议错误)
├── NameServerResponseError (响应错误)
└── NameServerDataParseError (数据解析错误)
```

### 异常处理特点
1. **精确分类**: 按照错误类型进行精确分类
2. **上下文保留**: 保留相关的错误上下文信息
3. **兼容性考虑**: 特别处理Go语言服务端的特殊错误格式

## 工厂函数

### create_sync_client()
创建同步NameServer客户端的工厂函数。

```python
def create_sync_client(host: str, port: int, timeout: float = 30.0) -> SyncNameServerClient
```

**参数**:
- `host`: NameServer主机地址
- `port`: NameServer端口
- `timeout`: 请求超时时间

**返回**: 配置好的SyncNameServerClient实例

### create_async_client()
创建异步NameServer客户端的工厂函数。

```python
async def create_async_client(host: str, port: int, timeout: float = 30.0) -> AsyncNameServerClient
```

**参数**: 与同步版本相同
**返回**: 配置好的AsyncNameServerClient实例

## 路由信息管理

### 路由查询流程
1. **客户端请求**: 向NameServer发送Topic路由查询请求
2. **NameServer响应**: 返回TopicRouteData结构
3. **数据处理**: 
   - 解析队列信息(QueueData)
   - 解析Broker信息(BrokerData)
   - 处理Go语言兼容性问题
4. **结果返回**: 返回完整的路由信息

### 路由信息组成
Topic路由信息包含两个核心部分：

1. **队列数据(QueueData)**: 描述队列的配置信息
   - 读/写队列数量
   - 权限设置
   - 同步标志

2. **Broker数据(BrokerData)**: 描述Broker的地址信息
   - Broker集群归属
   - 多地址支持
   - BrokerId映射

## 使用场景

### 1. Producer路由发现
Producer启动时需要查询Topic的路由信息，确定消息发送的目标Broker。

```python
# Producer获取路由信息
route_data = name_server_client.query_topic_route_info(topic)

# 选择Broker发送消息
for broker_data in route_data.broker_data_list:
    for broker_id, address in broker_data.broker_addresses.items():
        # 连接Broker并发送消息
        send_message_to_broker(address, message)
```

### 2. Consumer路由发现
Consumer启动时需要查询Topic的路由信息，确定消息消费的来源Broker。

```python
# Consumer获取路由信息
route_data = name_server_client.query_topic_route_info(topic)

# 建立消费连接
for queue_data in route_data.queue_data_list:
    # 连接指定的队列进行消费
    start_consume_from_queue(queue_data.broker_name, queue_data)
```

### 3. 集群管理
通过查询Broker集群信息，了解整个集群的拓扑结构。

```python
# 获取集群概览
cluster_info = name_server_client.get_broker_cluster_info()

# 分析集群状态
for cluster_name, broker_list in cluster_info.cluster_addr_table.items():
    print(f"Cluster {cluster_name}: {len(broker_list)} brokers")
```

## 性能优化

### 1. 连接复用
- 客户端复用底层TCP连接，减少连接建立开销
- 支持长连接保持，避免频繁断连重连

### 2. 异步支持
- 提供异步客户端，支持高并发场景
- 使用asyncio实现非阻塞IO操作

### 3. 缓存机制
- 路由信息可以在客户端缓存，减少重复查询
- 支持定时刷新，保证路由信息的时效性

### 4. 批量查询
- 支持一次查询多个Topic的路由信息
- 减少网络往返次数，提高查询效率

## 协议兼容性

### RocketMQ协议支持
- **版本兼容**: 支持RocketMQ 4.x/5.x版本
- **请求类型**: 支持所有核心NameServer请求
- **数据格式**: 严格遵循RocketMQ数据格式规范

### 跨语言兼容
- **Go语言兼容**: 专门处理Go语言JSON序列化的兼容性问题
- **整数key处理**: 使用ast.literal_eval处理Go返回的整数key格式
- **字段映射**: 字段名称与Go语言实现保持一致

## 最佳实践

### 1. 连接管理
```python
# 推荐的连接管理模式
client = create_sync_client(nameserver_host, nameserver_port)
try:
    client.connect()
    # 使用客户端...
finally:
    client.disconnect()
```

### 2. 异步操作
```python
# 异步操作推荐模式
async def with_nameserver():
    client = await create_async_client(nameserver_host, nameserver_port)
    try:
        await client.connect()
        route_data = await client.async_query_topic_route_info(topic)
        return route_data
    finally:
        await client.disconnect()
```

### 3. 异常处理
```python
# 完善的异常处理
try:
    route_data = client.query_topic_route_info(topic)
except NameServerConnectionError:
    # 处理连接错误
    reconnect_client()
except NameServerTimeoutError:
    # 处理超时错误
    retry_with_backoff()
except NameServerError as e:
    # 处理其他NameServer错误
    log_error_and_alert(e)
```

### 4. 路由缓存
```python
# 简单的路由缓存实现
class RouteCache:
    def __init__(self, ttl: int = 30):
        self.cache = {}
        self.ttl = ttl
    
    def get_route(self, topic: str) -> Optional[TopicRouteData]:
        # 实现缓存逻辑
        pass
    
    def refresh_route(self, topic: str, client: SyncNameServerClient):
        # 刷新缓存逻辑
        pass
```

NameServer模块是pyrocketmq实现服务发现的核心组件，为整个消息系统提供可靠的路由信息服务，确保Producer和Consumer能够准确定位和访问RocketMQ Broker。