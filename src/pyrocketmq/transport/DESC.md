# Transport层模块说明

## 概述

`pyrocketmq.transport` 模块是pyrocketmq的网络传输层，提供了统一的网络传输接口，支持同步和异步两种操作模式。该模块基于状态机设计，实现了完整的TCP连接管理、消息收发和配置管理功能。

## 核心组件

### 1. 抽象接口定义 (`abc.py`)

定义了传输层的核心抽象接口，为不同实现提供统一规范：

**Transport (同步传输接口)**：
- `output(msg)` - 发送二进制消息
- `recv(size)` - 接收二进制消息
- `recv_pkg()` - 接收完整数据包（header+body）
- `connect()` - 建立连接
- `close()` - 关闭连接
- `is_connected/is_connecting/is_disconnected` - 状态查询

**AsyncTransport (异步传输接口)**：
- 提供与Transport相同的异步版本方法
- 所有方法均为async/await风格

**ConnectionObserver (连接观察者)**：
- `on_connected()` - 连接建立回调
- `on_disconnected()` - 连接断开回调
- `on_error()` - 连接错误回调

**TransportMetrics (传输指标)**：
- `bytes_sent/bytes_received` - 字节统计
- `connection_count/error_count` - 连接和错误统计
- `reset_metrics()` - 重置指标

### 2. 配置管理 (`config.py`)

**TransportConfig** 提供了全面的传输层配置选项：

**基础连接配置**：
- `host/port` - 服务器地址和端口
- `timeout/connect_timeout` - 操作和连接超时

**重连机制**：
- `retry_interval` - 重连间隔
- `max_retries` - 最大重连次数（-1表示无限）

**性能优化**：
- `buffer_size` - 缓冲区大小配置
- `enable_nodelay` - TCP_NODELAY选项
- `enable_reuse_address` - 地址重用

**保活机制**：
- `keep_alive` - 是否启用TCP KeepAlive
- `keep_alive_interval` - 保活探测间隔
- `keep_alive_timeout` - 保活超时时间

**消息大小限制**：
- `max_message_size` - 最大消息大小限制

**预定义配置**：
- `DEFAULT_CONFIG` - 默认配置
- `HIGH_PERFORMANCE_CONFIG` - 高性能配置
- `DEBUG_CONFIG` - 调试配置

### 3. 连接状态机 (`tcp.py`)

模块包含两个基于python-statemachine的状态机实现：

**ConnectionStateMachine (同步状态机)**：
- **状态**: DISCONNECTED → CONNECTING → CONNECTED → CLOSED
- **自动重连**: 根据配置自动处理重连逻辑
- **TCP KeepAlive**: 完整的保活机制实现
- **消息收发**: 支持基本消息和数据包收发
- **完整的数据包接收**: `recv_pkg()` 方法支持分包接收和错误处理

**AsyncConnectionStateMachine (异步状态机)**：
- **异步状态转换**: 所有操作均为async/await
- **异步IO**: 基于asyncio.StreamReader/Writer
- **非阻塞操作**: 适合高并发场景
- **与同步版本相同的状态管理**

**数据包格式**：
```
| length(4字节) | body-data |
```

**recv_pkg() 方法特性**：
- 支持完整的长度前缀协议解析
- 自动处理分包接收
- 完善的错误处理机制
- 支持空消息、大消息、Unicode和二进制内容
- 连接状态验证和超时处理

**状态管理特性**：
- 完整的状态转换验证
- 自动资源清理
- 优雅的连接关闭
- 详细的日志记录

### 4. 异常体系 (`errors.py`)

提供完整的异常处理层次：

- **TransportError** - 基础传输异常
- **ConnectionError** - 连接相关异常
- **TimeoutError** - 超时异常
- **ConfigurationError** - 配置异常
- **InvalidStateTransitionError** - 状态转换异常
- **ConnectionClosedError** - 连接关闭异常

## 使用模式

### 同步连接使用

```python
from pyrocketmq.transport import TransportConfig, ConnectionStateMachine

# 创建配置
config = TransportConfig(host="localhost", port=9876)

# 创建状态机
state_machine = ConnectionStateMachine(config)

# 启动连接
state_machine.start()

# 发送消息
state_machine.output(b"Hello World")

# 接收消息
data = state_machine.recv(1024)

# 接收数据包
package = state_machine.recv_pkg()

# 关闭连接
state_machine.stop()
```

### 异步连接使用

```python
from pyrocketmq.transport import TransportConfig, AsyncConnectionStateMachine

# 创建配置
config = TransportConfig(host="localhost", port=9876)

# 创建异步状态机
state_machine = AsyncConnectionStateMachine(config)

# 启动连接
await state_machine.start()

# 发送消息
await state_machine.output(b"Hello World")

# 接收消息
data = await state_machine.recv(1024)

# 接收数据包
package = await state_machine.recv_pkg()

# 关闭连接
await state_machine.stop()
```

### 配置定制

```python
# 使用高性能配置
config = TransportConfig(
    host="broker.example.com",
    port=9876,
    timeout=10.0,
    connect_timeout=5.0,
    retry_interval=1.0,
    buffer_size=16384,
    keep_alive=True,
    keep_alive_interval=15.0,
    max_message_size=1024 * 1024  # 1MB消息大小限制
)

# 从预定义配置创建
config = HIGH_PERFORMANCE_CONFIG.copy_with(host="custom.host")
```

## 测试覆盖

### 单元测试
模块包含完整的单元测试覆盖：

- **状态转换测试** (`test_state_transitions.py`)：
  - 测试各种状态转换场景
  - 验证自动重连机制
  - 测试连接成功和失败情况

- **异步连接测试** (`test_async_connection.py`, `test_simple_async.py`)：
  - 测试异步状态机基本功能
  - 验证异步状态转换
  - 测试异步连接管理

- **数据包接收测试** (`test_recv_pkg.py`)：
  - 正常数据包接收
  - 空消息和大消息处理
  - 分包接收和超时处理
  - 错误情况处理（连接关闭、格式错误等）
  - Unicode和二进制内容支持

### 运行测试
```bash
# 设置环境变量
export PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src

# 运行所有transport测试
python -m pytest tests/transport/ -v

# 运行特定测试文件
python -m pytest tests/transport/test_recv_pkg.py -v
```

## 设计特点

### 1. 双模式支持
- **同步模式**: 适合简单场景和传统应用
- **异步模式**: 适合高并发和现代Python应用

### 2. 状态机设计
- **明确的状态定义**: DISCONNECTED, CONNECTING, CONNECTED, CLOSED
- **受控的状态转换**: 防止无效状态转换
- **自动资源管理**: 状态变化时自动清理资源

### 3. 完善的配置系统
- **灵活的配置选项**: 支持各种网络场景
- **配置验证**: 自动验证配置参数有效性
- **预定义配置**: 提供常用场景的配置模板

### 4. 可靠的连接管理
- **自动重连**: 根据配置自动处理网络中断
- **TCP KeepAlive**: 保持长连接活跃
- **优雅关闭**: 确保资源正确释放

### 5. 详细的日志记录
- **状态变化日志**: 记录所有状态转换
- **操作日志**: 记录消息收发操作
- **错误日志**: 详细的错误信息记录

### 6. 性能优化
- **TCP_NODELAY**: 禁用Nagle算法，减少延迟
- **可调缓冲区**: 根据场景调整缓冲区大小
- **非阻塞IO**: 异步模式支持高并发

### 7. 完整的错误处理
- **连接状态验证**: 防止在无效状态下操作
- **超时处理**: 支持配置超时时间
- **协议格式验证**: 严格的数据包格式检查
- **资源清理**: 异常时自动清理资源

## 技术细节

### 协议格式
传输层使用简单的长度前缀协议：
- **Header**: 4字节大端序整数，表示body长度
- **Body**: 实际消息数据

### recv_pkg() 方法实现
- **长度验证**: 检查body长度是否超过配置限制
- **分包处理**: 支持多次recv调用接收完整数据包
- **错误处理**: 处理连接关闭、超时、格式错误等情况
- **类型支持**: 支持文本、Unicode和二进制数据

### 错误处理
- **连接失败**: 自动重连机制
- **超时处理**: 可配置的超时时间
- **状态验证**: 防止在无效状态下操作
- **协议错误**: 严格的格式验证和错误报告

### 线程安全
- **同步状态机**: 需要外部同步保护
- **异步状态机**: 基于asyncio的异步安全

### 资源管理
- **Socket资源**: 状态变化时自动清理
- **内存管理**: 合理的缓冲区管理
- **连接池**: 支持连接复用（通过配置）

## 集成建议

1. **与Model层集成**: 使用RemotingCommandSerializer处理协议序列化
2. **日志配置**: 配置适当的日志级别和输出
3. **监控集成**: 利用TransportMetrics接口进行监控
4. **错误处理**: 实现ConnectionObserver进行错误处理
5. **性能调优**: 根据场景选择合适的配置参数
6. **测试策略**: 利用现有的单元测试框架进行功能验证

## 开发注意事项

1. **环境变量**: 开发时必须设置`PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src`
2. **测试运行**: 使用pytest运行测试，确保所有功能正常
3. **异步测试**: 异步功能测试需要使用asyncio.run()或事件循环
4. **Mock使用**: 测试中使用unittest.mock模拟网络操作
5. **状态调试**: 使用日志记录状态转换和操作过程
