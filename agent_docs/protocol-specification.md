# 协议规范

## 数据帧格式

RocketMQ TCP协议采用二进制帧格式，结构如下：

```
| length(4) | header-length(4) | header-data(JSON) | body-data(bytes) |
```

### 字段说明

- **length (4 bytes)**: 整个帧的总长度，包括长度字段本身（大端序）
- **header-length (4 bytes)**: JSON header的长度（大端序）
- **header-data (JSON)**: 协议头部，包含所有元数据和控制信息
- **body-data (bytes)**: 消息体数据，可以是二进制数据

### 帧结构示例
```
[4字节总长度] [4字节header长度] [JSON header数据] [二进制body数据]
     ^            ^                 ^                 ^
     |            |                 |                 |
   0x00000050  0x00000100    {"code":1,"opaque":1}     "Hello RocketMQ"
```

## JSON Header结构

### 请求Header结构
```json
{
  "code": 1,                    // 请求代码
  "language": "PYTHON",         // 客户端语言
  "version": 1,                 // 协议版本
  "opaque": 12345,              // 请求ID，用于匹配响应
  "flag": 0,                    // 标志位
  "remark": "remark text",      // 备注信息
  "extFields": {                // 扩展字段
    "producerGroup": "test_group",
    "topic": "test_topic",
    "queueId": 0,
    " bornTimestamp": 1234567890
  }
}
```

### 响应Header结构
```json
{
  "code": 0,                    // 响应代码（0表示成功）
  "language": "JAVA",           // 服务端语言
  "version": 393,               // 服务端版本
  "opaque": 12345,              // 对应的请求ID
  "flag": 1,                    // 标志位（响应）
  "remark": "",                 // 备注信息
  "extFields": {                // 扩展字段
    "msgId": "7F0000017D2C2F2F2F2F2F2F2F2F2F2F",
    "queueId": 0,
    "queueOffset": 100
  }
}
```

## Flag类型判断逻辑

由于Go语言实现中`RPC_ONEWAY`和`RESPONSE_TYPE`都使用值1，判断逻辑如下：

- **is_request()**: `flag == FlagType.RPC_TYPE (0)`
- **is_response()**: `flag == FlagType.RESPONSE_TYPE (1)`
- **is_oneway()**: `flag == FlagType.RPC_ONEWAY (1)`

### Flag位定义
```python
class FlagType:
    RPC_TYPE = 0         # 请求
    RPC_ONEWAY = 1       # 单向请求（不需要响应）
    RESPONSE_TYPE = 1    # 响应
```

## 请求代码定义

### 基础请求代码
```python
class RequestCode:
    # Broker相关请求
    SEND_MESSAGE = 10
    PULL_MESSAGE = 11
    QUERY_MESSAGE = 12
    VIEW_MESSAGE_BY_ID = 83
    
    # 偏移量管理
    GET_MIN_OFFSET = 19
    GET_MAX_OFFSET = 20
    SEARCH_OFFSET_BY_TIMESTAMP = 21
    UPDATE_CONSUMER_OFFSET = 38
    QUERY_CONSUMER_OFFSET = 39
    
    # 集群管理
    GET_BROKER_CLUSTER_INFO = 106
    UPDATE_NAMESRV_CONFIG = 388
    
    # 事务相关
    END_TRANSACTION = 37
    
    # 心跳和注册
    HEART_BEAT = 34
    REGISTER_PRODUCER = 104
    REGISTER_CONSUMER = 105
    UNREGISTER_PRODUCER = 110
    UNREGISTER_CONSUMER = 111
    
    # NameServer相关
    GET_ROUTEINTO_BY_TOPIC = 105
    GET_BROKER_CLUSTER_INFO = 106
    
    # 配置管理
    GET_ALL_TOPIC_CONFIG_FROM_NAMESERVER = 356
    UPDATE_NAMESRV_CONFIG = 388
```

### 响应代码定义
```python
class ResponseCode:
    SUCCESS = 0
    SYSTEM_ERROR = 1
    SYSTEM_BUSY = 2
    REQUEST_CODE_NOT_SUPPORTED = 3
    PARAM_ILLEGAL = 4
    
    # 网络相关
    NO_PERMISSION = 17
    NETTY_DECODER_ERROR = 21
    
    # 消息相关
    FLUSH_DISK_TIMEOUT = 10
    SLAVE_NOT_AVAILABLE = 11
    FLUSH_SLAVE_TIMEOUT = 12
    
    # 偏移量相关
    QUERY_NOT_FOUND = 13
    SUBSCRIPTION_GROUP_NOT_EXIST = 17
    
    # 事务相关
    TRANSACTION_SHOULD_COMMIT = 200
    TRANSACTION_SHOULD_ROLLBACK = 201
    TRANSACTION_STATE_UNKNOW = 202
    
    # 消费者相关
    NO_BUYER_ID = 19
    OFFSET_ILLEGAL = 22
    BROKER_NOT_EXIST = 23
```

## 大小限制

### 帧大小限制
- **最大帧大小**: 32MB (33,554,432 字节)
- **最大header大小**: 64KB (65,536 字节)
- **长度字段**: 4字节大端序整数

### 消息大小限制
- **默认最大消息大小**: 4MB
- **压缩阈值**: 1MB（超过此大小的消息会被压缩）
- **批量消息**: 单次最多发送32条消息

## 序列化和反序列化

### 序列化过程
```python
def serialize_frame(header: dict, body: bytes = None) -> bytes:
    """
    序列化RocketMQ协议帧
    
    Args:
        header: JSON header字典
        body: 消息体字节数据
        
    Returns:
        完整的协议帧字节数据
    """
    import json
    import struct
    
    # 序列化header
    header_json = json.dumps(header).encode('utf-8')
    header_length = len(header_json)
    
    # 计算总长度
    body_length = len(body) if body else 0
    total_length = 4 + 4 + header_length + body_length
    
    # 构建帧
    frame = bytearray()
    frame.extend(struct.pack('>I', total_length))      # 总长度
    frame.extend(struct.pack('>I', header_length))     # header长度
    frame.extend(header_json)                          # header数据
    if body:
        frame.extend(body)                             # body数据
        
    return bytes(frame)
```

### 反序列化过程
```python
def deserialize_frame(data: bytes) -> tuple:
    """
    反序列化RocketMQ协议帧
    
    Args:
        data: 帧字节数据
        
    Returns:
        (header_dict, body_bytes)
    """
    import json
    import struct
    
    # 读取长度字段
    total_length = struct.unpack('>I', data[0:4])[0]
    header_length = struct.unpack('>I', data[4:8])[0]
    
    # 读取header数据
    header_data = data[8:8+header_length]
    header = json.loads(header_data.decode('utf-8'))
    
    # 读取body数据
    body_start = 8 + header_length
    body_length = total_length - body_start
    body = data[body_start:body_start+body_length] if body_length > 0 else b''
    
    return header, body
```

## 协议特性

### 连接管理
- **长连接**: 客户端与服务端保持长连接
- **心跳机制**: 定期发送心跳维持连接
- **连接复用**: 单个连接支持多个请求/响应

### 可靠性保证
- **请求确认**: 每个请求都有对应的响应
- **重试机制**: 支持请求重试
- **超时控制**: 请求超时自动断开

### 性能优化
- **零拷贝**: 高效的数据传输
- **批量操作**: 支持批量消息处理
- **压缩**: 大消息自动压缩

### 兼容性
- **向后兼容**: 支持多版本协议
- **语言无关**: 支持多种编程语言实现
- **平台支持**: 跨平台兼容

## 错误处理

### 网络错误
- **连接超时**: 建立连接超时
- **读写超时**: 数据传输超时
- **连接断开**: 网络连接异常断开

### 协议错误
- **帧格式错误**: 不符合协议格式
- **长度错误**: 长度字段不正确
- **JSON解析错误**: header格式错误

### 业务错误
- **请求代码不支持**: 服务端不支持该请求
- **参数错误**: 请求参数不合法
- **权限错误**: 没有执行权限

## 示例数据

### 发送消息请求示例
```
Total Length: 234
Header Length: 156
Header JSON: {
  "code": 10,
  "language": "PYTHON",
  "version": 1,
  "opaque": 12345,
  "flag": 0,
  "extFields": {
    "producerGroup": "test_group",
    "topic": "test_topic",
    "queueId": 0,
    "bornTimestamp": 1640995200000,
    "flag": 0,
    "properties": "{\"KEYS\":\"order_123\",\"TAGS\":\"order\"}"
  }
}
Body: "Hello RocketMQ Message"
```

### 发送消息响应示例
```
Total Length: 189
Header Length: 142
Header JSON: {
  "code": 0,
  "language": "JAVA",
  "version": 393,
  "opaque": 12345,
  "flag": 1,
  "extFields": {
    "msgId": "7F0000017D2C2F2F2F2F2F2F2F2F2F2F2F",
    "queueId": 0,
    "queueOffset": 1000,
    "regionId": "DefaultRegion"
  }
}
Body: ""
```