# pyrocketmq Broker Manager 技术文档

## 概述

`BrokerManager` 是 pyrocketmq 项目中用于管理多个 RocketMQ Broker 连接的核心组件，采用同步编程模式实现。该模块基于重构后的连接池架构，提供了高效、可靠的 Broker 连接管理功能，支持连接池复用、健康检查和故障恢复机制。

## 核心功能

### 🔗 连接池管理
- **多 Broker 支持**：同时管理多个 Broker 实例的连接池
- **连接复用**：基于 `ConnectionPool` 实现连接的高效复用
- **动态配置**：支持运行时添加和移除 Broker
- **线程安全**：使用锁机制确保多线程环境下的安全性

### 🏥 健康检查
- **连接状态监控**：实时监控 Broker 连接的健康状态
- **故障检测**：自动检测连接异常和故障
- **恢复机制**：支持故障后的自动恢复

### ⚙️ 配置管理
- **灵活配置**：支持传输层和远程通信的独立配置
- **参数优化**：提供连接池大小、超时时间等可调参数

## 类设计

### BrokerManager

```python
class BrokerManager:
    """同步版本的Broker连接管理器"""
```

#### 构造函数参数

| 参数名 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `remote_config` | `RemoteConfig` | 必需 | 远程通信配置 |
| `transport_config` | `TransportConfig \| None` | `None` | 传输层配置 |
| `health_check_interval` | `float` | `30.0` | 健康检查间隔（秒） |
| `health_check_timeout` | `float` | `5.0` | 健康检查超时时间（秒） |
| `max_consecutive_failures` | `int` | `3` | 最大连续失败次数 |
| `connection_pool_size` | `int` | `5` | 每个Broker的连接池大小 |

#### 核心属性

| 属性名 | 类型 | 说明 |
|--------|------|------|
| `remote_config` | `RemoteConfig` | 远程通信配置对象 |
| `transport_config` | `TransportConfig \| None` | 传输层配置对象 |
| `health_check_interval` | `float` | 健康检查间隔时间 |
| `connection_pool_size` | `int` | 连接池大小 |
| `_broker_pools` | `dict[str, ConnectionPool]` | Broker地址到连接池的映射 |

## API 参考

### 主要方法

#### `__init__()`

初始化 Broker 管理器实例。

```python
def __init__(
    self,
    remote_config: RemoteConfig,
    transport_config: TransportConfig | None = None,
    health_check_interval: float = 30.0,
    health_check_timeout: float = 5.0,
    max_consecutive_failures: int = 3,
    connection_pool_size: int = 5,
) -> None
```

**参数说明**：
- `remote_config`: 远程通信配置，包含连接超时、请求超时等设置
- `transport_config`: 传输层配置，包含主机、端口、连接参数等
- `health_check_interval`: 健康检查的时间间隔，单位为秒
- `health_check_timeout`: 单次健康检查的超时时间，单位为秒
- `max_consecutive_failures`: 允许的最大连续失败次数
- `connection_pool_size`: 每个 Broker 的连接池大小

#### `start()`

启动 Broker 管理器。

```python
def start(self) -> None
```

**功能**：启动后台服务线程，开始健康检查等维护任务。

#### `shutdown()`

关闭 Broker 管理器。

```python
def shutdown(self) -> None
```

**功能**：
- 停止所有后台线程
- 关闭所有连接池
- 清理资源

#### `add_broker()`

添加新的 Broker 到管理器。

```python
def add_broker(self, broker_addr: str, broker_name: str | None = None) -> None
```

**参数说明**：
- `broker_addr`: Broker 地址，格式为 "host:port"
- `broker_name`: Broker 名称，可选参数，未提供时从地址中提取

**异常**：
- `ValueError`: 当 Broker 地址格式无效时抛出

**示例**：
```python
manager.add_broker("localhost:9876", "broker1")
manager.add_broker("192.168.1.100:10911")  # 名称自动提取为 "192.168.1.100"
```

#### `remove_broker()`

从管理器中移除指定 Broker。

```python
def remove_broker(self, broker_addr: str) -> None
```

**参数说明**：
- `broker_addr`: 要移除的 Broker 地址

**功能**：
- 关闭对应的连接池
- 清理相关资源
- 从管理列表中移除

#### `connection_pool()`

获取指定 Broker 的连接池。

```python
def connection_pool(self, broker_addr: str) -> ConnectionPool | None
```

**参数说明**：
- `broker_addr`: Broker 地址

**返回值**：
- `ConnectionPool`: 连接池实例
- `None`: 如果 Broker 不存在则返回 None

## 使用示例

### 基本使用

```python
from pyrocketmq.broker.broker_manager import BrokerManager
from pyrocketmq.remote.config import RemoteConfig
from pyrocketmq.transport.config import TransportConfig

# 创建配置
remote_config = RemoteConfig(
    connect_timeout=5000.0,
    request_timeout=30000.0
)

transport_config = TransportConfig(
    timeout=10000.0
)

# 创建 Broker 管理器
manager = BrokerManager(
    remote_config=remote_config,
    transport_config=transport_config,
    connection_pool_size=10
)

# 启动管理器
manager.start()

# 添加 Broker
manager.add_broker("localhost:9876", "broker1")
manager.add_broker("192.168.1.100:10911", "broker2")

# 获取连接池
pool = manager.connection_pool("localhost:9876")
if pool:
    with pool.get_connection() as connection:
        # 使用连接进行通信
        result = connection.send_request(request)

# 关闭管理器
manager.shutdown()
```

### 高级配置

```python
# 创建高性能配置
remote_config = RemoteConfig(
    connect_timeout=3000.0,
    request_timeout=15000.0,
    connection_pool_timeout=10.0
)

# 生产环境配置
manager = BrokerManager(
    remote_config=remote_config,
    health_check_interval=15.0,  # 更频繁的健康检查
    health_check_timeout=3.0,    # 更短的超时时间
    max_consecutive_failures=2,  # 更严格的失败阈值
    connection_pool_size=20      # 更大的连接池
)
```

### 错误处理

```python
try:
    manager.add_broker("invalid_address", "broker1")
except ValueError as e:
    print(f"添加 Broker 失败: {e}")

# 检查连接池是否存在
pool = manager.connection_pool("nonexistent_broker")
if pool is None:
    print("Broker 不存在")
```

## 依赖项

### 必需依赖

| 模块 | 版本要求 | 说明 |
|------|----------|------|
| `logging` | Python 标准库 | 日志记录 |
| `threading` | Python 标准库 | 线程同步 |
| `time` | Python 标准库 | 时间处理 |

### 项目内依赖

| 模块 | 说明 |
|------|------|
| `pyrocketmq.logging` | 项目日志系统 |
| `pyrocketmq.remote.config` | 远程通信配置 |
| `pyrocketmq.remote.pool` | 连接池实现 |
| `pyrocketmq.transport.config` | 传输层配置 |

## 版本变更记录

### v2.0.0 (重构版本)
**发布日期**: 2025-01-17

#### 🔥 重大变更
- **架构重构**: 基于 `ConnectionPool` 重新设计，移除重复的连接管理逻辑
- **代码简化**: 删除冗余的 `BrokerConnectionPool` 和 `_BrokerConnectionWrapper` 类
- **API 统一**: 直接使用标准的 `ConnectionPool` 接口

#### ✨ 新增功能
- **连接池复用**: 完全基于 `remote.pool.ConnectionPool` 实现
- **线程安全增强**: 改进多线程环境下的安全性
- **配置灵活性**: 支持更细粒度的配置选项

#### 🛠️ 改进
- **性能提升**: 减少连接创建和销毁的开销
- **资源管理**: 更高效的资源利用和清理机制
- **错误处理**: 完善异常处理和错误恢复

#### 🗑️ 移除
- **重复代码**: 移除自定义的连接池实现
- **包装器类**: 删除 `_BrokerConnectionWrapper` 类
- **复杂状态管理**: 简化连接状态的管理逻辑

#### ⚠️ 破坏性变更
- **API 变更**: `get_connection()` 方法现在返回标准的 `Remote` 对象
- **配置参数**: 部分构造函数参数的默认值发生调整
- **依赖变更**: 强依赖 `pyrocketmq.remote.pool` 模块

#### 📝 文档更新
- 添加完整的使用示例和最佳实践
- 更新 API 文档和参数说明
- 补充性能调优建议

### v1.x.x (历史版本)
- 初始版本实现
- 基础的 Broker 管理功能
- 自定义连接池实现

## 最佳实践

### 性能优化

1. **连接池大小**: 根据并发需求调整 `connection_pool_size`
   - 开发环境: 3-5 个连接
   - 生产环境: 10-20 个连接

2. **健康检查间隔**: 平衡性能和实时性
   - 高频场景: 10-15 秒
   - 普通场景: 30-60 秒

3. **超时设置**: 根据网络环境调整超时参数
   - 局域网: 3-5 秒
   - 广域网: 10-30 秒

### 错误处理

1. **地址验证**: 确保 Broker 地址格式正确
2. **异常捕获**: 妥善处理连接和通信异常
3. **资源清理**: 确保在异常情况下正确清理资源

### 监控建议

1. **连接状态**: 定期检查连接池的健康状态
2. **性能指标**: 监控连接创建、销毁和使用频率
3. **错误日志**: 关注连接失败和异常日志

---

**最后更新**: 2025-01-17  
**文档版本**: v2.0.0  
**维护状态**: ✅ 活跃维护