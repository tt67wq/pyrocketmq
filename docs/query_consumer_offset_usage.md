# query_consumer_offset 方法使用指南

## 概述

`query_consumer_offset` 方法用于查询指定消费者组在特定主题和队列上的消费偏移量。该方法是 `BrokerClient` 类的一个核心功能，支持实时获取消费进度信息。

## 方法签名

```python
def query_consumer_offset(
    self,
    consumer_group: str,
    topic: str,
    queue_id: int,
) -> int:
```

## 参数说明

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `consumer_group` | `str` | 是 | 消费者组名称 |
| `topic` | `str` | 是 | 主题名称 |
| `queue_id` | `int` | 是 | 队列ID，从0开始 |

## 返回值

- **类型**: `int`
- **说明**: 返回消费者在指定队列上的当前偏移量

## 异常处理

该方法可能抛出以下异常：

| 异常类型 | 说明 | 处理建议 |
|----------|------|----------|
| `BrokerConnectionError` | Broker连接失败 | 检查网络连接和Broker状态 |
| `BrokerTimeoutError` | 请求超时 | 增加超时时间或检查网络延迟 |
| `BrokerResponseError` | Broker响应错误 | 检查主题和消费者组是否存在 |
| `OffsetError` | 偏移量查询错误 | 消费者组可能尚未开始消费该队列 |

## 基本使用示例

### 简单查询

```python
from pyrocketmq.broker import create_broker_client

# 创建客户端
client = create_broker_client("localhost", 9876)

try:
    # 连接到Broker
    client.connect()
    
    # 查询消费者偏移量
    offset = client.query_consumer_offset(
        consumer_group="my_consumer_group",
        topic="my_topic",
        queue_id=0
    )
    
    print(f"当前偏移量: {offset}")
    
finally:
    client.disconnect()
```

### 完整错误处理

```python
from pyrocketmq.broker import create_broker_client
from pyrocketmq.broker.errors import (
    BrokerConnectionError,
    BrokerResponseError,
    BrokerTimeoutError,
    OffsetError,
)

client = create_broker_client("localhost", 9876)

try:
    client.connect()
    
    offset = client.query_consumer_offset(
        consumer_group="test_group",
        topic="test_topic",
        queue_id=0
    )
    
    print(f"查询成功，偏移量: {offset}")
    
except BrokerConnectionError as e:
    print(f"连接错误: {e}")
except BrokerTimeoutError as e:
    print(f"请求超时: {e}")
except BrokerResponseError as e:
    print(f"Broker响应错误: {e}")
    print(f"响应代码: {e.response_code}")
except OffsetError as e:
    print(f"偏移量查询错误: {e}")
    print("可能是消费者组尚未开始消费该队列")
except Exception as e:
    print(f"未知错误: {e}")
finally:
    client.disconnect()
```

### 批量查询多个队列

```python
def query_all_queues(client, consumer_group, topic, queue_count):
    """查询多个队列的偏移量"""
    results = {}
    
    for queue_id in range(queue_count):
        try:
            offset = client.query_consumer_offset(
                consumer_group=consumer_group,
                topic=topic,
                queue_id=queue_id
            )
            results[queue_id] = offset
            print(f"队列 {queue_id}: {offset}")
        except Exception as e:
            print(f"队列 {queue_id} 查询失败: {e}")
            results[queue_id] = None
    
    return results

# 使用示例
client = create_broker_client("localhost", 9876)
client.connect()

try:
    offsets = query_all_queues(
        client,
        consumer_group="my_group",
        topic="my_topic",
        queue_count=4
    )
    
    # 计算总偏移量
    total_offset = sum(offset for offset in offsets.values() if offset is not None)
    print(f"总偏移量: {total_offset}")
    
finally:
    client.disconnect()
```

## 高级用法

### 监控消费进度

```python
import time
from pyrocketmq.broker import create_broker_client

def monitor_consumer_progress(client, consumer_group, topic, interval=5):
    """监控消费者进度"""
    queue_ids = [0, 1, 2, 3]  # 假设有4个队列
    
    while True:
        print(f"\n=== {time.strftime('%Y-%m-%d %H:%M:%S')} ===")
        
        total_offset = 0
        successful_queries = 0
        
        for queue_id in queue_ids:
            try:
                offset = client.query_consumer_offset(
                    consumer_group=consumer_group,
                    topic=topic,
                    queue_id=queue_id
                )
                total_offset += offset
                successful_queries += 1
                print(f"队列 {queue_id}: {offset}")
            except Exception as e:
                print(f"队列 {queue_id}: 查询失败 - {e}")
        
        print(f"总偏移量: {total_offset}")
        print(f"成功率: {successful_queries}/{len(queue_ids)}")
        
        time.sleep(interval)

# 使用示例
client = create_broker_client("localhost", 9876)
client.connect()

try:
    monitor_consumer_progress(
        client,
        consumer_group="monitor_group",
        topic="monitor_topic",
        interval=10
    )
except KeyboardInterrupt:
    print("监控停止")
finally:
    client.disconnect()
```

### 性能优化建议

```python
# 1. 连接复用
client = create_broker_client("localhost", 9876, timeout=5.0)
client.connect()

# 批量查询时复用同一个连接
for i in range(100):
    try:
        offset = client.query_consumer_offset("group", "topic", i % 4)
        # 处理结果...
    except Exception as e:
        # 错误处理...
        pass

client.disconnect()

# 2. 异常处理优化
def safe_query_offset(client, consumer_group, topic, queue_id, default_value=0):
    """安全的偏移量查询，失败时返回默认值"""
    try:
        return client.query_consumer_offset(consumer_group, topic, queue_id)
    except OffsetError:
        # 消费者组未开始消费，返回0
        return default_value
    except (BrokerConnectionError, BrokerTimeoutError) as e:
        # 网络问题，重新抛出异常
        raise
    except Exception:
        # 其他错误，返回默认值
        return default_value

# 3. 超时设置
# 对于批量查询，建议使用较短的超时时间
client = create_broker_client("localhost", 9876, timeout=3.0)
```

## 响应代码说明

该方法可能返回的响应代码：

| 响应代码 | 枚举值 | 说明 | 处理方式 |
|----------|--------|------|----------|
| `SUCCESS` | 0 | 查询成功 | 正常返回偏移量 |
| `ERROR` | 1 | 通用错误 | 检查参数和Broker状态 |
| `TOPIC_NOT_EXIST` | 17 | 主题不存在 | 确认主题名称正确 |
| `QUERY_NOT_FOUND` | 22 | 查询不到偏移量 | 消费者组可能未开始消费 |
| `SERVICE_NOT_AVAILABLE` | 14 | 服务不可用 | 等待Broker恢复 |

## 最佳实践

### 1. 连接管理

```python
# 好的做法：使用上下文管理器
class BrokerClientContext:
    def __init__(self, host, port, timeout=30.0):
        self.client = create_broker_client(host, port, timeout)
    
    def __enter__(self):
        self.client.connect()
        return self.client
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.client.disconnect()

# 使用示例
with BrokerClientContext("localhost", 9876) as client:
    offset = client.query_consumer_offset("group", "topic", 0)
    print(f"偏移量: {offset}")
```

### 2. 错误重试

```python
import time

def query_with_retry(client, consumer_group, topic, queue_id, max_retries=3):
    """带重试的偏移量查询"""
    for attempt in range(max_retries):
        try:
            return client.query_consumer_offset(consumer_group, topic, queue_id)
        except (BrokerTimeoutError, BrokerConnectionError) as e:
            if attempt == max_retries - 1:
                raise
            print(f"查询失败，第 {attempt + 1} 次重试...")
            time.sleep(1)
    
    raise RuntimeError("重试次数用尽")
```

### 3. 日志配置

```python
from pyrocketmq.logging import LoggerFactory, LoggingConfig

# 配置详细日志
LoggerFactory.setup_default_config(LoggingConfig(level="DEBUG"))

# 或者只显示错误
LoggerFactory.setup_default_config(LoggingConfig(level="ERROR"))
```

## 故障排除

### 常见问题

1. **连接失败**
   - 检查Broker是否运行
   - 确认主机地址和端口正确
   - 检查防火墙设置

2. **偏移量查询失败**
   - 确认消费者组名称正确
   - 确认主题存在
   - 检查消费者组是否已开始消费

3. **超时错误**
   - 增加超时时间
   - 检查网络延迟
   - 确认Broker负载正常

### 调试技巧

```python
# 启用详细日志
from pyrocketmq.logging import LoggerFactory, LoggingConfig
LoggerFactory.setup_default_config(LoggingConfig(level="DEBUG"))

# 检查连接状态
if not client.is_connected:
    print("客户端未连接")

# 输出详细错误信息
try:
    offset = client.query_consumer_offset("group", "topic", 0)
except Exception as e:
    print(f"错误类型: {type(e)}")
    print(f"错误信息: {e}")
    if hasattr(e, 'response_code'):
        print(f"响应代码: {e.response_code}")
```

## 更多示例

查看 `examples/query_consumer_offset_demo.py` 了解更多使用示例。

## 相关文档

- [BrokerClient API 文档](broker_client_api.md)
- [错误处理指南](error_handling.md)
- [性能优化建议](performance_optimization.md)