# NameServer 模块文档

## 概述

NameServer是RocketMQ架构中的核心组件，负责维护Broker的路由信息。客户端通过与NameServer通信可以获取：

- Topic的路由信息，包括队列分布和Broker地址
- Broker集群的完整信息，包括集群拓扑和Broker地址映射

## 模块结构

```
nameserver/
├── __init__.py          # 模块入口，导出公共API
├── models.py            # 数据结构定义
├── client.py            # 客户端实现
└── errors.py            # 异常定义
```

## 核心功能

NameServer模块对客户端暴露了两个核心API：

1. **query_topic_route_info**: 获取指定Topic的路由信息
2. **get_broker_cluster_info**: 获取NameServer上所有注册的Broker信息

## 数据结构

### BrokerData
```python
@dataclass
class BrokerData:
    """Broker 信息"""
    cluster: str
    broker_name: str
    broker_addresses: Dict[int, str]  # brokerId -> address
```

### QueueData
```python
@dataclass
class QueueData:
    """队列信息"""
    broker_name: str
    read_queue_nums: int
    write_queue_nums: int
    perm: int
    topic_syn_flag: int
    compression_type: str = "gzip"
```

### TopicRouteData
```python
@dataclass
class TopicRouteData:
    """Topic 路由信息"""
    order_topic_conf: str = ""
    queue_data_list: List[QueueData] = field(default_factory=list)
    broker_data_list: List[BrokerData] = field(default_factory=list)
```

### BrokerClusterInfo
```python
@dataclass
class BrokerClusterInfo:
    """Broker 集群信息"""
    broker_addr_table: Dict[str, BrokerData] = field(default_factory=dict)
    cluster_addr_table: Dict[str, List[str]] = field(default_factory=dict)
```

## 客户端实现

### 同步客户端
```python
from pyrocketmq.nameserver import create_sync_client

# 创建同步客户端
client = create_sync_client("localhost", 9876, timeout=30.0)

try:
    # 查询Topic路由信息
    route_info = client.query_topic_route_info("test_topic")
    print(f"Topic路由信息: {route_info}")

    # 获取Broker集群信息
    cluster_info = client.get_broker_cluster_info()
    print(f"Broker集群信息: {cluster_info}")
finally:
    client.disconnect()
```

### 异步客户端
```python
from pyrocketmq.nameserver import create_async_client

# 创建异步客户端
async_client = await create_async_client("localhost", 9876, timeout=30.0)

try:
    # 异步查询Topic路由信息
    route_info = await async_client.query_topic_route_info("test_topic")
    print(f"Topic路由信息: {route_info}")

    # 异步获取Broker集群信息
    cluster_info = await async_client.get_broker_cluster_info()
    print(f"Broker集群信息: {cluster_info}")
finally:
    await async_client.disconnect()
```

### 上下文管理器使用
```python
# 同步方式
with create_sync_client("localhost", 9876) as client:
    route_info = client.query_topic_route_info("test_topic")

# 异步方式
async with await create_async_client("localhost", 9876) as client:
    route_info = await client.query_topic_route_info("test_topic")
```

## 数据处理特性

### Go语言兼容性
由于RocketMQ的Go语言实现返回的JSON中使用整数作为key，Python的json.loads无法直接解析。NameServer模块使用ast.literal_eval来解决这个问题：

```python
# 支持Go语言返回的整数key格式
# {"brokerAddrs": {"0": "192.168.1.100:10911", "1": "192.168.1.101:10911"}}
# 而不是标准JSON格式
# {"brokerAddrs": {"0": "192.168.1.100:10911", "1": "192.168.1.101:10911"}}
```

### 数据验证
所有数据结构都包含完整的字段验证和错误处理：

```python
try:
    broker_data = BrokerData.from_bytes(response_body)
except ValueError as e:
    # 处理数据格式错误
    print(f"数据解析失败: {e}")
```

## 异常处理

NameServer模块定义了完整的异常层次结构：

- `NameServerError`: 基础异常
- `NameServerConnectionError`: 连接错误
- `NameServerTimeoutError`: 超时错误
- `NameServerProtocolError`: 协议错误
- `NameServerResponseError`: 响应错误
- `NameServerDataParseError`: 数据解析错误

## 客户端配置

### 超时配置
```python
# 设置30秒超时
client = create_sync_client("localhost", 9876, timeout=30.0)

# 异步客户端同样支持超时配置
async_client = await create_async_client("localhost", 9876, timeout=30.0)
```

### 连接管理
```python
# 检查连接状态
if client.is_connected():
    print("客户端已连接")
else:
    print("客户端未连接")

# 手动连接和断开
client.connect()
# ... 执行操作 ...
client.disconnect()
```

## 日志记录

NameServer模块使用项目统一的日志系统：

```python
from pyrocketmq.logging import LoggerFactory, LoggingConfig

# 启用DEBUG级别日志
LoggerFactory.setup_default_config(LoggingConfig(level="DEBUG"))
```

## 协议映射

NameServer操作的RequestCode映射：

- `query_topic_route_info`: `RequestCode.QUERY_TOPIC_ROUTE_INFO` (200)
- `get_broker_cluster_info`: `RequestCode.GET_BROKER_CLUSTER_INFO` (106)

## 使用示例

### 完整的同步示例
```python
from pyrocketmq.nameserver import create_sync_client

def main():
    client = create_sync_client("localhost", 9876)

    try:
        # 连接NameServer
        client.connect()

        # 查询Topic路由信息
        topic = "test_topic"
        route_info = client.query_topic_route_info(topic)

        print(f"Topic: {topic}")
        print(f"队列数据数量: {len(route_info.queue_data_list)}")
        print(f"Broker数据数量: {len(route_info.broker_data_list)}")

        # 获取Broker集群信息
        cluster_info = client.get_broker_cluster_info()
        print(f"Broker集群数量: {len(cluster_info.broker_addr_table)}")

    except NameServerError as e:
        print(f"NameServer操作失败: {e}")
    finally:
        client.disconnect()

if __name__ == "__main__":
    main()
```

### 完整的异步示例
```python
import asyncio
from pyrocketmq.nameserver import create_async_client

async def main():
    client = await create_async_client("localhost", 9876)

    try:
        # 连接NameServer
        await client.connect()

        # 查询Topic路由信息
        topic = "test_topic"
        route_info = await client.query_topic_route_info(topic)

        print(f"Topic: {topic}")
        print(f"队列数据数量: {len(route_info.queue_data_list)}")
        print(f"Broker数据数量: {len(route_info.broker_data_list)}")

        # 获取Broker集群信息
        cluster_info = await client.get_broker_cluster_info()
        print(f"Broker集群数量: {len(cluster_info.broker_addr_table)}")

    except NameServerError as e:
        print(f"NameServer操作失败: {e}")
    finally:
        await client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
```
