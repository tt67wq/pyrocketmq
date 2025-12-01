# Transport层网络传输基础设施

**模块概述**: Transport模块是pyrocketmq的传输层基础设施，提供基于TCP协议的网络通信能力。采用状态机模式管理连接生命周期，同时支持同步和异步两种操作模式。

## 核心组件

### ConnectionStateMachine
同步TCP连接状态机实现，提供完整的连接生命周期管理：
- **状态管理**: DISCONNECTED → CONNECTING → CONNECTED → CLOSING → CLOSED
- **错误处理**: 连接失败、超时、网络中断等异常情况处理
- **资源管理**: 自动资源清理和连接池管理

### AsyncConnectionStateMachine
异步TCP连接状态机实现，基于asyncio框架：
- **异步IO**: 支持高并发连接处理
- **协程友好**: 与asyncio生态系统完美集成
- **非阻塞操作**: 提高系统吞吐量

### TransportConfig
传输层配置管理，控制连接行为和网络参数：
- **连接参数**: host、port、connect_timeout、read_timeout
- **缓冲区设置**: send_buffer_size、receive_buffer_size
- **心跳配置**: heartbeat_interval、heartbeat_timeout
- **重连策略**: max_reconnect_attempts、reconnect_delay

## 状态机设计

```
DISCONNECTED → CONNECTING → CONNECTED → CLOSING → CLOSED
     ↑              ↓              ↓              ↓
     └───── reconnect ←─────────┘              ↓
                        error/timeout        cleanup
```

### 状态转换规则
- **DISCONNECTED**: 初始状态，可以开始连接
- **CONNECTING**: 正在建立连接，等待连接完成
- **CONNECTED**: 连接已建立，可以进行数据传输
- **CLOSING**: 正在关闭连接，清理资源
- **CLOSED**: 连接已关闭，可以重新连接

## 关键特性

- **状态机驱动**: 精确管理连接生命周期，确保连接状态的一致性
- **双模式支持**: 同步和异步连接实现，满足不同性能需求
- **丰富的配置选项**: 支持不同场景的优化配置
- **完善的异常处理**: 自动重连、超时处理、连接恢复
- **协议无关**: 提供字节流传输服务，可适配多种协议
- **性能优化**: 连接复用、缓冲区管理、IO事件驱动

## 使用示例

```python
from pyrocketmq.transport import TransportConfig, ConnectionStateMachine

# 创建传输配置
config = TransportConfig(
    host="localhost",
    port=9876,
    connect_timeout=5000.0,
    read_timeout=30000.0,
    heartbeat_interval=30000.0,
    max_reconnect_attempts=3
)

# 同步连接状态机
connection = ConnectionStateMachine(config)
connection.connect()

# 发送数据
connection.send_data(b"Hello RocketMQ")

# 接收数据
data = connection.receive_data()

# 异步连接状态机
async_connection = AsyncConnectionStateMachine(config)
await async_connection.connect()

# 异步发送数据
await async_connection.send_data(b"Hello Async RocketMQ")

# 异步接收数据
data = await async_connection.receive_data()
```