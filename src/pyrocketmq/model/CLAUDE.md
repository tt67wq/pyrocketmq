# Model模块设计文档

## 模块概述

Model模块是pyrocketmq的核心数据层，提供完整的RocketMQ协议数据结构定义、序列化机制和工具函数。该模块严格遵循RocketMQ TCP协议规范，与Go语言实现完全兼容，为整个pyrocketmq项目提供统一的数据模型基础。

## 模块架构

### 文件结构
```
model/
├── __init__.py              # 模块入口，统一导出接口
├── command.py               # 远程命令数据结构
├── serializer.py            # 协议序列化器
├── message.py               # 基础消息数据结构
├── message_ext.py           # 扩展消息数据结构
├── message_queue.py         # 消息队列数据结构
├── result_data.py           # 操作结果数据结构
├── enums.py                 # 协议枚举定义
├── factory.py               # 请求工厂和构建器
├── headers.py               # 请求头数据结构
├── heart_beat.py            # 心跳数据结构
├── client_data.py           # 客户端数据结构
├── nameserver_models.py     # NameServer相关模型
├── utils.py                 # 工具函数
└── errors.py                # 模块异常定义
```

### 设计原则

1. **协议兼容性**: 严格遵循RocketMQ TCP协议，与Go语言实现完全兼容
2. **类型安全**: 使用dataclass和类型注解，提供完整的类型安全保障
3. **序列化优化**: 高效的二进制序列化，支持大消息处理
4. **扩展性**: 模块化设计，便于扩展新的协议特性
5. **工具丰富**: 提供丰富的工具函数，简化常见操作

## 核心数据结构

### 1. RemotingCommand - 远程命令
协议层的核心数据结构，表示所有RocketMQ通信的命令单元。

```python
@dataclass
class RemotingCommand:
    code: int                    # 请求/响应代码
    language: LanguageCode       # 客户端语言
    version: int                 # 协议版本
    opaque: int                  # 请求标识符
    flag: int                    # 命令标志（请求/响应/oneway）
    remark: Optional[str]        # 备注信息
    ext_fields: Dict[str, str]   # 扩展字段
    body: Optional[bytes]        # 消息体
```

**关键特性**:
- **标志位处理**: 通过flag属性区分请求、响应和oneway消息
- **扩展字段**: 支持任意key-value扩展信息
- **语言标识**: 支持多语言客户端互操作

### 2. Message - 基础消息
RocketMQ消息的基础数据结构，包含消息的核心信息。

```python
@dataclass
class Message:
    topic: str                   # 主题名称
    body: bytes                  # 消息体内容
    flag: int = 0               # 消息标志
    transaction_id: Optional[str] = None  # 事务ID
    batch: bool = False         # 是否批量消息
    compress: bool = False      # 是否压缩
    queue: Optional[MessageQueue] = None  # 指定队列
    properties: Dict[str, str] = field(default_factory=dict)  # 消息属性
```

**消息属性支持**:
- **SHARDING_KEY**: 分片键，用于顺序消息
- **KEYS**: 消息键，用于消息查询
- **TAGS**: 消息标签，用于消息过滤
- **DELAY_TIME_LEVEL**: 延迟级别
- **RETRY_TOPIC**: 重试主题

### 3. MessageExt - 扩展消息
继承自Message，包含完整的消息系统属性。

```python
@dataclass
class MessageExt(Message):
    msg_id: Optional[str] = None           # 消息ID
    offset_msg_id: Optional[str] = None    # 偏移消息ID
    store_size: Optional[int] = None       # 存储大小
    queue_offset: Optional[int] = None     # 队列偏移量
    sys_flag: int = 0                     # 系统标志
    born_timestamp: Optional[int] = None   # 生产时间戳
    born_host: Optional[str] = None        # 生产主机
    store_timestamp: Optional[int] = None  # 存储时间戳
    store_host: Optional[str] = None       # 存储主机
    commit_log_offset: Optional[int] = None  # 提交日志偏移量
    body_crc: Optional[int] = None         # 消息体CRC32
    reconsume_times: int = 0              # 重新消费次数
    prepared_transaction_offset: Optional[int] = None  # 预提交事务偏移量
```

### 4. MessageQueue - 消息队列
表示主题下的具体队列，包含路由信息。

```python
@dataclass
class MessageQueue:
    topic: str          # 主题名称
    broker_name: str    # Broker名称
    queue_id: int       # 队列ID
```

**序列化支持**:
- `to_dict()` / `from_dict()`: 字典转换
- `full_name`: 完整名称格式 `topic@brokerName:queueId`
- `is_valid()`: 有效性验证

### 5. HeartbeatData - 心跳数据
客户端心跳信息，包含生产者和消费者注册信息。

```python
@dataclass
class HeartbeatData:
    client_id: str                      # 客户端ID
    producer_data_set: List[ProducerData]  # 生产者数据集合
    consumer_data_set: List[ConsumerData]  # 消费者数据集合
```

## 序列化机制

### RemotingCommandSerializer
实现RocketMQ TCP协议的二进制序列化。

**协议格式**:
```
| length(4) | header-length(4) | header-data(JSON) | body-data(bytes) |
```

**序列化流程**:
1. **Header序列化**: 将RemotingCommand的header部分序列化为JSON
2. **长度计算**: 计算header和body的总长度
3. **二进制组装**: 按照协议格式组装二进制数据

**反序列化流程**:
1. **长度读取**: 读取4字节的总长度
2. **Header长度读取**: 读取4字节的header长度
3. **数据分离**: 分离header和body数据
4. **对象重建**: 反序列化为RemotingCommand对象

**配置限制**:
- `MAX_FRAME_SIZE`: 32MB - 最大帧大小
- `MAX_HEADER_SIZE`: 64KB - 最大header大小

## 结果数据模型

### SendMessageResult - 发送结果
```python
@dataclass
class SendMessageResult:
    status: SendStatus              # 发送状态
    msg_id: str                     # 消息ID
    message_queue: MessageQueue     # 消息队列
    queue_offset: int              # 队列偏移量
    transaction_id: Optional[str]  # 事务ID
    offset_msg_id: Optional[str]   # 偏移消息ID
    region_id: str                 # 区域ID
    trace_on: bool                 # 是否开启Trace
```

**发送状态类型**:
- `SEND_OK`: 发送成功
- `SEND_FLUSH_DISK_TIMEOUT`: 刷盘超时
- `SEND_FLUSH_SLAVE_TIMEOUT`: 从节点刷盘超时
- `SEND_SLAVE_NOT_AVAILABLE`: 从节点不可用
- `SEND_UNKNOWN_ERROR`: 未知错误

### PullMessageResult - 拉取结果
```python
@dataclass
class PullMessageResult:
    messages: List[MessageExt]     # 消息列表
    next_begin_offset: int         # 下次起始偏移量
    min_offset: int               # 最小偏移量
    max_offset: int               # 最大偏移量
    pull_rt: float = 0.0          # 拉取耗时
```

## 枚举定义

### 核心枚举类型

#### RequestCode - 请求代码
```python
class RequestCode(IntEnum):
    SEND_MESSAGE = 10              # 发送消息
    PULL_MESSAGE = 11              # 拉取消息
    QUERY_MESSAGE = 12             # 查询消息
    VIEW_MESSAGE_BY_ID = 13        # 根据ID查看消息
    QUERY_CONSUMER_OFFSET = 14     # 查询消费者偏移量
    UPDATE_CONSUMER_OFFSET = 15    # 更新消费者偏移量
    CREATE_TOPIC = 17              # 创建主题
    SEARCH_OFFSET_BY_TIMESTAMP = 29  # 根据时间戳搜索偏移量
    GET_MAX_OFFSET = 30            # 获取最大偏移量
    GET_MIN_OFFSET = 31            # 获取最小偏移量
    HEART_BEAT = 34                # 心跳
    UNREGISTER_CLIENT = 35         # 注销客户端
```

#### ResponseCode - 响应代码
```python
class ResponseCode(IntEnum):
    SUCCESS = 0                    # 成功
    SYSTEM_ERROR = 1              # 系统错误
    SYSTEM_BUSY = 2               # 系统繁忙
    REQUEST_CODE_NOT_SUPPORTED = 3  # 不支持的请求代码
    CREATE_TOPIC_FAILED = 5       # 创建主题失败
    PULL_NOT_FOUND = 12           # 拉取不到消息
    PULL_OFFSET_MOVED = 14        # 拉取偏移量已移动
    PULL_RETRY_IMMEDIATELY = 15   # 立即重试拉取
    QUERY_NOT_FOUND = 17          # 查询不到
    TOPIC_NOT_EXIST = 17          # 主题不存在
    SUBSCRIPTION_GROUP_NOT_EXIST = 19  # 订阅组不存在
    FLUSH_DISK_TIMEOUT = 10       # 刷盘超时
    SLAVE_NOT_AVAILABLE = 11      # 从节点不可用
    SERVICE_NOT_AVAILABLE = 1     # 服务不可用
```

#### FlagType - 标志类型
```python
class FlagType:
    RPC_TYPE = 0                  # 请求类型
    RESPONSE_TYPE = 1             # 响应类型
    RPC_ONEWAY = 1                # 单向消息
```

#### LanguageCode - 语言代码
```python
class LanguageCode(IntEnum):
    JAVA = 0                      # Java
    CPP = 1                       # C++
    DOTNET = 2                    # .NET
    PYTHON = 3                    # Python
    GO = 9                        # Go
```

#### LocalTransactionState - 本地事务状态
```python
class LocalTransactionState(IntEnum):
    COMMIT_MESSAGE_STATE = 1      # 提交消息
    ROLLBACK_MESSAGE_STATE = 2    # 回滚消息
    UNKNOW_STATE = 3              # 未知状态
```

## 工厂模式

### RemotingRequestFactory
统一的请求创建工厂，支持所有RocketMQ请求类型的创建。

**主要方法**:
- `create_send_message_request()` - 创建发送消息请求
- `create_pull_message_request()` - 创建拉取消息请求
- `create_heartbeat_request()` - 创建心跳请求
- `create_query_consumer_offset_request()` - 创建查询偏移量请求
- `create_update_consumer_offset_request()` - 创建更新偏移量请求

### RemotingCommandBuilder
命令构建器，提供链式调用方式构建复杂命令。

## 工具函数

### 命令处理工具
- `validate_command()` - 验证命令有效性
- `generate_opaque()` - 生成请求标识符
- `create_response_for_request()` - 为请求创建响应
- `copy_command_with_new_opaque()` - 复制命令并更改opaque

### 信息提取工具
- `get_topic_from_command()` - 从命令提取topic
- `get_group_from_command()` - 从命令提取组名
- `get_queue_id_from_command()` - 从命令提取队列ID
- `get_offset_from_command()` - 从命令提取偏移量

### 统计分析工具
- `get_command_summary()` - 获取命令摘要
- `get_command_stats()` - 获取命令统计
- `filter_commands_by_topic()` - 按topic过滤命令
- `filter_commands_by_group()` - 按组过滤命令

### 序列化工具
- `command_to_dict()` - 命令转字典
- `commands_to_json()` - 命令列表转JSON
- `parse_command_from_json()` - 从JSON解析命令

## 异常处理

### 异常层次结构
```python
RemotingCommandError (基础异常)
├── SerializationError (序列化错误)
├── DeserializationError (反序列化错误)
├── ProtocolError (协议错误)
├── ValidationError (验证错误)
├── MessageTooLargeError (消息过大)
├── HeaderTooLargeError (header过大)
├── InvalidHeaderError (无效header)
├── InvalidMessageError (无效消息)
├── ConnectionClosedError (连接已关闭)
├── TimeoutError (超时错误)
├── UnsupportedVersionError (不支持的版本)
├── UnsupportedLanguageError (不支持的语言)
└── UnsupportedRequestCodeError (不支持的请求代码)
```

## 消息创建函数

### create_message()
创建标准消息
```python
message = create_message(
    topic="test_topic",
    body=b"Hello RocketMQ",
    properties={"KEYS": "order_123"}
)
```

### create_transaction_message()
创建事务消息
```python
message = create_transaction_message(
    topic="test_topic",
    body=b"Transaction data",
    transaction_id="tx_123"
)
```

### create_delay_message()
创建延迟消息
```python
message = create_delay_message(
    topic="test_topic",
    body=b"Delay data",
    delay_level=3  # 延迟级别3
)
```

## 协议兼容性

### RocketMQ协议支持
- **TCP协议**: 完整支持RocketMQ TCP协议
- **数据格式**: 严格遵循二进制数据帧格式
- **请求类型**: 支持所有核心请求类型
- **响应处理**: 正确处理所有响应码

### 与Go语言实现的兼容性
- **数据结构**: 数据结构字段与Go实现一一对应
- **序列化格式**: 二进制序列化格式完全兼容
- **枚举值**: 所有枚举值与Go实现保持一致
- **行为一致性**: 错误处理、边界条件等行为保持一致

## 使用示例

### 创建和序列化命令
```python
from pyrocketmq.model import RemotingCommand, RequestCode, RemotingCommandSerializer

# 创建命令
command = RemotingCommand(
    code=RequestCode.SEND_MESSAGE,
    opaque=12345,
    body=b"Hello RocketMQ"
)

# 序列化
data = RemotingCommandSerializer.serialize(command)

# 反序列化
restored_command = RemotingCommandSerializer.deserialize(data)
```

### 创建消息
```python
from pyrocketmq.model import create_message, MessageProperty

# 创建带属性的消息
message = create_message(
    topic="test_topic",
    body=b"Hello World",
    properties={
        MessageProperty.KEYS: "order_123",
        MessageProperty.TAGS: "order",
        MessageProperty.SHARDING_KEY: "user_456"
    }
)
```

### 使用工厂创建请求
```python
from pyrocketmq.model import RemotingRequestFactory

# 创建发送消息请求
request = RemotingRequestFactory.create_send_message_request(
    producer_group="test_group",
    topic="test_topic",
    body=b"Hello",
    queue_id=0
)
```

该Model模块为pyrocketmq提供了完整、类型安全、高性能的数据模型基础，确保与RocketMQ服务器的完美兼容。