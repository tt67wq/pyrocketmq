# Transport模块设计文档

## 模块概述

Transport模块是pyrocketmq的传输层基础设施，提供基于TCP协议的网络通信能力。该模块采用状态机模式管理连接生命周期，同时支持同步和异步两种操作模式，为上层Remote模块提供稳定可靠的字节流传输服务。模块设计注重连接状态的精确控制、网络异常的优雅处理以及高性能的数据传输。

## 模块架构

### 文件结构
```
transport/
├── __init__.py          # 模块入口，导出核心接口
├── config.py            # 传输层配置管理
├── tcp.py               # TCP连接实现和状态机
└── errors.py            # 传输层异常定义
```

### 架构设计原则

1. **状态机驱动**: 使用状态机模式精确管理连接生命周期
2. **双模式支持**: 同时提供同步(ConnectionStateMachine)和异步(AsyncConnectionStateMachine)实现
3. **配置灵活**: 丰富的配置选项，支持不同场景的优化
4. **异常安全**: 完善的异常处理和资源清理机制
5. **协议无关**: 提供字节流传输，支持上层协议的灵活实现

## 核心数据结构

### 1. TransportConfig - 传输层配置
传输层的核心配置类，控制连接行为和网络参数。

```python
@dataclass
class TransportConfig:
    # 基础连接配置
    host: str = "localhost"                # 服务器主机
    port: int = 8080                       # 服务器端口
    
    # 超时配置
    timeout: float = 30.0                  # 操作超时时间
    connect_timeout: float = 10.0          # 连接超时时间
    
    # 重连配置
    retry_interval: float = 5.0            # 重连间隔
    max_retries: int = -1                  # 最大重连次数(-1表示无限)
    
    # 缓冲区配置
    buffer_size: int = 8192                # 基础缓冲区大小
    send_buffer_size: int = 8192           # 发送缓冲区大小
    receive_buffer_size: int = 8192        # 接收缓冲区大小
    
    # 保活配置
    keep_alive: bool = True                # 启用TCP KeepAlive
    keep_alive_interval: float = 30.0      # KeepAlive间隔
    keep_alive_timeout: float = 10.0       # KeepAlive超时
    
    # 其他TCP配置
    enable_nodelay: bool = True            # 启用TCP_NODELAY
    enable_reuse_address: bool = True      # 启用地址重用
    
    # 高级配置
    max_message_size: int = 1024 * 1024    # 最大消息大小(1MB)
    idle_timeout: Optional[float] = None   # 空闲超时
```

**预设配置**:
- `DEFAULT_CONFIG`: 默认配置，适用于一般场景
- `HIGH_PERFORMANCE_CONFIG`: 高性能配置，优化缓冲区和超时参数
- `DEBUG_CONFIG`: 调试配置，延长超时时间便于调试

### 2. ConnectionStateMachine - 同步连接状态机
基于python-statemachine实现的同步TCP连接状态机。

```python
class ConnectionStateMachine(StateMachine):
    # 状态定义
    disconnected = State(initial=True)     # 断开连接状态(初始状态)
    connecting = State()                   # 连接中状态
    connected = State()                    # 已连接状态
    closed = State(final=True)             # 关闭状态(最终状态)
    
    # 事件定义
    connect = disconnected.to(connecting)              # 开始连接
    connect_success = connecting.to(connected)         # 连接成功
    disconnect = connected.to(disconnected) | connecting.to(disconnected)  # 断开连接
    close = disconnected.to(closed) | connected.to(closed) | connecting.to(closed)  # 关闭连接
```

**核心方法**:
- `start()` - 启动连接过程
- `stop()` - 停止连接
- `output(msg: bytes)` - 发送二进制数据
- `recv(size: int)` - 接收指定长度数据
- `recv_pkg()` - 接收完整数据包

### 3. AsyncConnectionStateMachine - 异步连接状态机
异步版本的连接状态机，基于asyncio实现。

```python
class AsyncConnectionStateMachine(StateMachine):
    # 状态定义(与同步版本相同)
    disconnected = State(initial=True)
    connecting = State()
    connected = State()
    closed = State(final=True)
    
    # 事件定义(与同步版本相同)
    connect = disconnected.to(connecting)
    connect_success = connecting.to(connected)
    disconnect = connected.to(disconnected) | connecting.to(disconnected)
    close = disconnected.to(closed) | connected.to(closed) | connecting.to(closed)
```

**核心方法**:
- `async start()` - 异步启动连接过程
- `async stop()` - 异步停止连接
- `async output(msg: bytes)` - 异步发送二进制数据
- `async recv(size: int)` - 异步接收指定长度数据
- `async recv_pkg()` - 异步接收完整数据包

## 状态机设计

### 状态转换图
```
    [disconnected] --connect--> [connecting] --connect_success--> [connected]
         ^                      |                              |
         |                      v                              v
         +<---disconnect<-------+<---------------disconnect------+
         |
         v
      [closed] (final state)
```

### 状态详细说明

#### 1. disconnected - 断开连接状态
- **描述**: 初始状态，连接未建立
- **可执行操作**: `connect` - 开始连接过程
- **状态特点**: 无网络资源占用，可随时发起连接

#### 2. connecting - 连接中状态  
- **描述**: 正在建立TCP连接的过程中
- **可执行操作**: `connect_success` - 连接成功，`disconnect` - 取消连接
- **状态特点**: 正在进行TCP握手，占用网络资源

#### 3. connected - 已连接状态
- **描述**: TCP连接已建立，可以进行数据传输
- **可执行操作**: `disconnect` - 断开连接，`close` - 关闭连接
- **状态特点**: 可进行数据收发，维持心跳保活

#### 4. closed - 关闭状态
- **描述**: 连接已关闭，资源已清理
- **可执行操作**: 无(最终状态)
- **状态特点**: 无法再次使用，需要创建新实例

### 状态转换回调

#### on_connect() - 连接开始
```python
def on_connect(self):
    """进入连接中状态"""
    self._logger.info(f"开始连接到 {self.config.address}")
    try:
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.settimeout(self.config.connect_timeout)
        self._socket.connect(self.config.address)
    except Exception as e:
        self._logger.error(f"连接失败: {e}")
        self.disconnect()
    else:
        self._logger.info("连接成功")
        self.connect_success()
```

#### on_connect_success() - 连接成功
```python
def on_connect_success(self):
    """进入已连接状态"""
    # 设置socket选项
    if self._socket:
        self._socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self._socket.settimeout(self.config.timeout)
        
        # 配置TCP KeepAlive
        if self.config.keep_alive:
            self._set_keepalive()
```

#### on_disconnect() - 连接断开
```python
def on_disconnect(self):
    """进入断开连接状态"""
    self._close_socket()  # 清理socket资源
    
    # 自动重连逻辑
    if self.config.max_retries != 0 and self.config.retry_interval > 0:
        self._logger.info("准备重连...")
        time.sleep(self.config.retry_interval)
        self.connect()
```

#### on_close() - 连接关闭
```python
def on_close(self):
    """进入关闭状态"""
    self._close_socket()  # 清理socket资源
```

## 数据传输机制

### 数据包格式
Transport层采用简单的长度前缀协议：

```
| Length(4 bytes, big-endian) | Body(variable length) |
```

- **Length字段**: 4字节大端序整数，表示Body的长度
- **Body字段**: 实际传输的数据内容

### 发送机制
```python
def output(self, msg: bytes) -> None:
    """发送二进制消息"""
    if not self.is_connected:
        raise RuntimeError("连接未建立，无法发送消息")
    
    # 检查消息大小限制
    if len(msg) > self.config.max_message_size:
        raise ValueError(f"消息大小超过限制: {len(msg)} > {self.config.max_message_size}")
    
    # 发送消息(支持部分发送)
    total_sent = 0
    while total_sent < len(msg):
        sent = self._socket.send(msg[total_sent:])
        if sent == 0:
            raise ConnectionError("连接已断开")
        total_sent += sent
```

### 接收机制
```python
def recv_pkg(self) -> bytes:
    """接收完整的数据包"""
    # 1. 接收4字节长度头
    header_data = self._recv_exactly(4)
    if not header_data:
        return b""  # 连接关闭
    
    # 2. 解析body长度(大端序)
    body_length = int.from_bytes(header_data, byteorder="big", signed=False)
    
    # 3. 验证长度合理性
    if body_length > self.config.max_message_size:
        raise ValueError(f"Body长度超过限制: {body_length}")
    
    # 4. 接收body数据
    if body_length == 0:
        return b""  # 空消息
    body_data = self._recv_exactly(body_length)
    
    return body_data
```

### 精确接收机制
```python
def _recv_exactly(self, size: int) -> bytes:
    """精确接收指定长度的数据"""
    data = b""
    remaining = size
    
    while remaining > 0:
        chunk = self._socket.recv(remaining)
        if not chunk:
            return b""  # 连接关闭
        
        data += chunk
        remaining -= len(chunk)
    
    return data
```

## TCP配置优化

### TCP_NODELAY配置
```python
# 禁用Nagle算法，减少延迟
self._socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
```

### TCP KeepAlive配置
```python
def _set_keepalive(self):
    """配置TCP KeepAlive"""
    if not self._socket:
        return
    
    try:
        # 启用KeepAlive
        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        
        # 设置KeepAlive参数(系统相关)
        if hasattr(socket, "TCP_KEEPIDLE"):
            # 空闲时间后开始探测
            self._socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 
                                   int(self.config.keep_alive_interval))
        
        if hasattr(socket, "TCP_KEEPINTVL"):
            # 探测间隔
            self._socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL,
                                   int(self.config.keep_alive_interval // 2))
        
        if hasattr(socket, "TCP_KEEPCNT"):
            # 最大探测次数
            self._socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 3)
            
    except Exception as e:
        self._logger.warning(f"设置TCP KeepAlive失败: {e}")
```

### 缓冲区配置
```python
# 发送缓冲区
self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 
                       self.config.send_buffer_size)

# 接收缓冲区  
self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF,
                       self.config.receive_buffer_size)
```

## 异步实现特点

### 异步连接建立
```python
async def on_connect(self) -> None:
    """异步连接建立"""
    try:
        # 使用asyncio.open_connection
        self._reader, self._writer = await asyncio.wait_for(
            asyncio.open_connection(*self.config.address),
            timeout=self.config.timeout,
        )
        
        # 获取底层socket进行配置
        if self._writer:
            self._socket = self._writer.get_extra_info("socket")
            # 配置socket选项...
            
    except Exception as e:
        self._logger.error(f"异步连接失败: {e}")
        await self.disconnect()
    else:
        await self.connect_success()
```

### 异步数据传输
```python
async def async_output(self, msg: bytes) -> None:
    """异步发送数据"""
    if not self._writer:
        raise RuntimeError("Writer未初始化")
    
    self._writer.write(msg)
    await self._writer.drain()

async def async_recv_pkg(self) -> bytes:
    """异步接收数据包"""
    if not self._reader:
        raise RuntimeError("Reader未初始化")
    
    # 接收长度头
    header_data = await self._reader.readexactly(4)
    body_length = int.from_bytes(header_data, byteorder="big")
    
    # 接收body
    if body_length == 0:
        return b""
    
    body_data = await self._reader.readexactly(body_length)
    return body_data
```

## 异常处理体系

### 异常层次结构
```
TransportError (基础异常)
├── ConnectionError (连接异常)
├── TimeoutError (超时异常)  
├── ConfigurationError (配置异常)
├── InvalidStateTransitionError (无效状态转换异常)
└── ConnectionClosedError (连接已关闭异常)
```

### 异常处理策略

#### 连接异常处理
```python
try:
    # 网络操作
    data = self._socket.recv(size)
except socket.timeout:
    raise TimeoutError("接收超时")
except ConnectionResetError:
    self.disconnect()  # 状态转换
    raise ConnectionError("连接被重置")
except Exception as e:
    self.disconnect()  # 状态转换
    raise TransportError(f"网络错误: {e}")
```

#### 状态转换保护
```python
def output(self, msg: bytes) -> None:
    """发送消息(带状态检查)"""
    if not self.is_connected:
        raise RuntimeError(f"无效状态: {self.current_state_name}, 无法发送消息")
    
    # 执行发送逻辑...
```

## 配置管理

### 配置验证
```python
def __post_init__(self):
    """配置验证"""
    if self.port < 0 or self.port > 65535:
        raise ValueError("Port must be between 0 and 65535")
    
    if self.timeout <= 0:
        raise ValueError("Timeout must be positive")
    
    if self.max_message_size <= 0:
        raise ValueError("Max message size must be positive")
```

### 配置复制
```python
def copy_with(self, **kwargs) -> "TransportConfig":
    """创建配置副本并更新指定字段"""
    return TransportConfig(
        host=kwargs.get("host", self.host),
        port=kwargs.get("port", self.port),
        timeout=kwargs.get("timeout", self.timeout),
        # ... 其他字段
    )
```

### 配置序列化
```python
def to_dict(self) -> dict:
    """转换为字典"""
    return {
        "host": self.host,
        "port": self.port,
        "timeout": self.timeout,
        # ... 其他字段
    }

@classmethod
def from_dict(cls, config_dict: dict) -> "TransportConfig":
    """从字典创建配置"""
    return cls(**config_dict)
```

## 性能特性

### 1. 缓冲区优化
- **可配置缓冲区**: 支持自定义发送/接收缓冲区大小
- **内存复用**: 减少内存分配和拷贝开销
- **批量传输**: 支持大数据的分块传输

### 2. 网络优化
- **TCP_NODELAY**: 禁用Nagle算法，减少延迟
- **KeepAlive**: 长连接保活，减少连接建立开销
- **地址重用**: 支持地址重用，提高连接成功率

### 3. 异步性能
- **非阻塞IO**: 基于asyncio的高性能异步IO
- **零拷贝**: 尽可能减少数据拷贝
- **并发支持**: 支持大量并发连接

### 4. 资源管理
- **自动清理**: 状态转换时自动清理网络资源
- **超时控制**: 防止资源长时间占用
- **内存限制**: 限制消息大小，防止内存耗尽

## 使用示例

### 同步连接使用
```python
from pyrocketmq.transport import TransportConfig, ConnectionStateMachine

# 创建配置
config = TransportConfig(
    host="localhost",
    port=9876,
    timeout=30.0,
    keep_alive=True
)

# 创建状态机
connection = ConnectionStateMachine(config)

try:
    # 启动连接
    connection.start()
    
    # 等待连接建立
    while not connection.is_connected:
        time.sleep(0.1)
    
    # 发送数据
    message = b"Hello RocketMQ"
    connection.output(message)
    
    # 接收数据
    response = connection.recv_pkg()
    print(f"收到响应: {response}")
    
finally:
    # 关闭连接
    connection.stop()
```

### 异步连接使用
```python
from pyrocketmq.transport import TransportConfig, AsyncConnectionStateMachine

async def async_example():
    # 创建配置
    config = TransportConfig(host="localhost", port=9876)
    
    # 创建异步状态机
    connection = AsyncConnectionStateMachine(config)
    
    try:
        # 启动异步连接
        await connection.start()
        
        # 等待连接建立
        while not connection.is_connected:
            await asyncio.sleep(0.1)
        
        # 异步发送数据
        message = b"Hello Async RocketMQ"
        await connection.async_output(message)
        
        # 异步接收数据
        response = await connection.async_recv_pkg()
        print(f"收到响应: {response}")
        
    finally:
        # 关闭异步连接
        await connection.stop()
```

### 配置定制使用
```python
# 高性能配置
high_perf_config = TransportConfig(
    host="localhost",
    port=9876,
    timeout=10.0,
    buffer_size=16384,
    send_buffer_size=32768,
    receive_buffer_size=32768,
    keep_alive_interval=15.0,
    max_message_size=2 * 1024 * 1024  # 2MB
)

# 调试配置
debug_config = TransportConfig(
    host="localhost",
    port=9876,
    timeout=60.0,
    connect_timeout=30.0,
    retry_interval=10.0,
    idle_timeout=300.0  # 5分钟空闲超时
)
```

## 最佳实践

### 1. 连接管理
- **及时关闭**: 使用完毕后及时关闭连接
- **状态检查**: 在数据传输前检查连接状态
- **异常处理**: 妥善处理网络异常和状态转换

### 2. 配置优化
- **环境适配**: 根据网络环境调整超时和缓冲区参数
- **性能调优**: 在高并发场景下优化缓冲区大小
- **保活设置**: 在长连接场景下合理配置KeepAlive参数

### 3. 错误处理
- **分类处理**: 根据异常类型采用不同的恢复策略
- **日志记录**: 记录关键状态转换和错误信息
- **优雅降级**: 在网络异常时优雅降级处理

### 4. 资源管理
- **资源清理**: 确保socket等资源的及时清理
- **内存控制**: 监控内存使用，防止内存泄漏
- **并发控制**: 在高并发场景下合理控制资源使用

Transport模块为pyrocketmq提供了稳定、高效、可配置的网络传输基础设施，通过状态机模式确保连接状态的精确控制，为整个消息系统的可靠通信奠定了坚实基础。