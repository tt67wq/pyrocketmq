# Model模块设计文档

## 模块概述

Model模块是pyrocketmq的核心数据层，提供完整的RocketMQ协议数据结构定义、序列化机制和工具函数。该模块严格遵循RocketMQ TCP协议规范，与Go语言实现完全兼容，为上层应用提供可靠的数据基础。

### 核心职责
- **协议数据结构**：定义完整的RocketMQ协议数据模型
- **序列化机制**：提供高效的二进制序列化和反序列化
- **消息模型**：实现消息、队列、路由等核心数据结构
- **异常处理**：定义完整的异常体系和错误处理机制
- **工具函数**：提供便捷的数据处理和验证工具

### 设计原则
- **协议兼容性**：与Go语言RocketMQ实现完全兼容
- **类型安全**：使用完整的类型注解和dataclass
- **性能优化**：高效的序列化和内存管理
- **易用性**：提供丰富的便利函数和工具

## 模块架构

### 文件结构

```
model/
├── __init__.py                # 模块导出和公共接口
├── command.py                 # RemotingCommand核心数据结构
├── message.py                 # Message基础消息数据结构
├── message_ext.py            # MessageExt扩展消息数据结构
├── message_queue.py          # MessageQueue消息队列数据结构
├── message_id.py             # MessageID消息ID处理
├── client_data.py            # 客户端数据结构（Producer/Consumer）
├── consumer.py               # 消费者相关数据结构
├── enums.py                  # 枚举类型定义
├── errors.py                 # 异常定义
├── headers.py                # 请求/响应头数据结构
├── heart_beat.py             # 心跳数据结构
├── nameserver_models.py      # NameServer相关数据结构
├── result_data.py            # 结果数据结构
├── serializer.py             # 序列化器实现
├── subscription.py           # 订阅相关数据结构
├── utils.py                  # 工具函数
├── factory.py                # 工厂模式和构建器
└── CLAUDE.md                 # 本文档
```

### 模块依赖关系

```
Model模块依赖层次:
┌─────────────────────────────────────────┐
│              应用层接口                   │
│         (__init__.py导出)               │
├─────────────────────────────────────────┤
│              核心数据层                   │
│  command, message, message_ext, queue   │
├─────────────────────────────────────────┤
│              业务数据层                   │
│  client_data, consumer, subscription     │
├─────────────────────────────────────────┤
│              协议支持层                   │
│  headers, nameserver_models, result_data │
├─────────────────────────────────────────┤
│              基础设施层                   │
│    enums, errors, serializer, utils     │
├─────────────────────────────────────────┤
│              工厂构建层                   │
│         factory, tools                  │
└─────────────────────────────────────────┘
```

## 核心数据结构

### 1. RemotingCommand - 远程命令

**文件位置**: `command.py`

**功能描述**: RocketMQ远程通信的核心数据结构，表示客户端与服务器之间的通信协议单元。

**核心属性**:
```python
@dataclass
class RemotingCommand:
    code: int                    # 请求/响应代码
    language: LanguageCode       # 语言代码（默认PYTHON）
    version: int = 1            # 协议版本
    opaque: int = 0             # 请求标识符
    flag: int = 0               # 标志位（请求/响应/单向）
    remark: str | None = None   # 备注信息
    ext_fields: dict[str, str]  # 扩展字段
    body: bytes | None = None   # 消息体
```

**关键方法**:
- `is_request`: 判断是否为请求类型
- `is_response`: 判断是否为响应类型
- `is_oneway`: 判断是否为单向消息
- `set_request()`: 设置为请求类型
- `set_response()`: 设置为响应类型
- `set_oneway()`: 设置为单向消息
- `add_ext_field()`: 添加扩展字段
- `get_ext_field()`: 获取扩展字段
- `encode_header()`: 编码头部为JSON
- `make_custom_header()`: 创建自定义头部

**协议格式**:
```
| length(4) | header-length(4) | header-data(JSON) | body-data(bytes) |
```

### 2. Message - 基础消息

**文件位置**: `message.py`

**功能描述**: RocketMQ消息的基础数据结构，包含消息的基本信息和属性。

**核心属性**:
```python
@dataclass
class Message:
    topic: str                           # 主题名称
    body: bytes                          # 消息体内容
    flag: int = 0                       # 消息标志
    transaction_id: str | None = None   # 事务ID
    batch: bool = False                 # 是否为批量消息
    compress: bool = False              # 是否压缩
    queue: MessageQueue | None = None   # 指定的消息队列
    properties: dict[str, str] = field(default_factory=dict)  # 消息属性
```

**关键方法**:
- `get_property()`: 获取消息属性
- `set_property()`: 设置消息属性
- `remove_property()`: 移除消息属性
- `get_properties_string()`: 获取属性字符串
- `set_properties_string()`: 从字符串设置属性
- `clear_property()`: 清除所有属性
- `encode()`: 编码消息为字节格式
- `decode()`: 从字节格式解码消息

**消息属性常量**:
```python
class MessageProperty:
    TAGS = "TAGS"                                    # 消息标签
    KEYS = "KEYS"                                    # 消息键
    WAIT_STORE_MSG_OK = "WAIT_STORE_MSG_OK"          # 是否等待存储
    DELIVERY_TIME = "DELIVERY_TIME"                  # 定时消息投递时间
    DEFERRED_TIME = "DEFERRED_TIME"                  # 延迟时间
    TRANSACTION_PREPARED = "TRANSACTION_PREPARED"    # 事务准备状态
    TRANSACTION_ID = "TRANSACTION_ID"                # 事务ID
    PRODUCER_GROUP = "PRODUCER_GROUP"                # 生产者组
    CONSUMER_GROUP = "CONSUMER_GROUP"                # 消费者组
    UNIQUE_CLIENT_MESSAGE_ID_KEY_INDEX = "UNIQ_MSG"  # 唯一消息ID
    # ... 更多属性常量
```

### 3. MessageExt - 扩展消息

**文件位置**: `message_ext.py`

**功能描述**: 继承自Message，扩展了消费者相关的属性，用于消息消费场景。

**扩展属性**:
```python
@dataclass
class MessageExt(Message):
    queue_id: int = 0                    # 队列ID
    store_size: int = 0                  # 存储大小
    queue_offset: int = 0                # 队列偏移量
    sys_flag: int = 0                    # 系统标志
    born_timestamp: int = 0              # 消息产生时间戳
    born_host: str | None = None         # 消息产生主机
    store_timestamp: int = 0             # 存储时间戳
    store_host: str | None = None        # 存储主机
    msg_id: str | None = None            # 消息ID
    commit_log_offset: int = 0           # 提交日志偏移量
    body_crc: int = 0                    # 消��体CRC校验
    reconsume_times: int = 0             # 重新消费次数
    prepared_transaction_offset: int = 0 # 预提交事务偏移量
```

**关键方法**:
- `is_transaction_prepared()`: 判断是否为事务准备状态
- `is_transaction_commit()`: 判断是否为事务提交状态
- `is_transaction_rollback()`: 判断是否为事务回滚状态
- `get_transaction_state()`: 获取事务状态
- `set_transaction_state()`: 设置事务状态

### 4. MessageQueue - 消息队列

**文件位置**: `message_queue.py`

**功能描述**: 表示RocketMQ中的消息队列，是消息分发的基本单元。

**核心属性**:
```python
@dataclass
class MessageQueue:
    topic: str        # 主题名称
    broker_name: str  # Broker名称
    queue_id: int     # 队列ID
```

**关键方法**:
- `equals()`: 判断队列是否相等
- `hash_code()`: 计算队列哈希值
- `to_string()`: 转换为字符串表示
- `from_string()`: 从字符串创建队列对象

### 5. MessageID - 消息ID

**文件位置**: `message_id.py`

**功能描述**: RocketMQ消息的唯一标识符，支持IP地址和时间戳的组合生成。

**核心功能**:
- **唯一ID生成**: 基于IP地址、端口、时间戳、序列号生成唯一ID
- **ID解析**: 解析消息ID获取地址和时间信息
- **格式化**: 提供多种格式的ID表示

**关键函数**:
```python
def create_message_id() -> str:                    # 创建新消息ID
def create_message_id_from_bytes(data: bytes) -> str: # 从字节数据创建ID
def unmarshal_msg_id(msg_id: str) -> bytes:        # 反序列化消息ID
def is_valid_message_id(msg_id: str) -> bool:      # 验证消息ID有效性
def parse_message_id_from_string(msg_id: str) -> dict: # 解析消息ID
def format_message_id_info(msg_id: str) -> str:    # 格式化ID信息
def get_address_by_bytes(addr_bytes: bytes) -> str: # 从字节获取地址
```

### 6. HeartbeatData - 心跳数据

**文件位置**: `heart_beat.py`

**功能描述**: 客户端与Broker之间心跳通信的数据结构，用于保持连接活跃和状态同步。

**核心属性**:
```python
@dataclass
class HeartbeatData:
    client_id: str                               # 客户端ID
    producer_data_set: list[ProducerData]        # 生产者数据集合
    consumer_data_set: list[ConsumerData]        # 消费者数据集合
    heartbeat_data_version: int = 1              # 心跳数据版本
```

**关键方法**:
- `add_producer_data()`: 添加生产者数据
- `add_consumer_data()`: 添加消费者数据
- `encode()`: 编码心跳数据
- `decode()`: 解码心跳数据

### 7. ProducerData/ConsumerData - 客户端数据

**文件位置**: `client_data.py`

**ProducerData**:
```python
@dataclass
class ProducerData:
    group_name: str               # 生产者组名
```

**ConsumerData**:
```python
@dataclass
class ConsumerData:
    group_name: str               # 消费者组名
    consume_type: ConsumeType     # 消费类型
    message_model: MessageModel   # 消息模式
    consume_from_where: ConsumeFromWhere  # 消费起始位置
    subscription_data_set: list[SubscriptionData]  # 订阅数据集合
    unit_mode: bool = False       # 单元模式
```

### 8. MessageSelector - 消息选择器

**文件位置**: `client_data.py`

**功能描述**: 用于消息订阅时的过滤条件，支持基于标签和SQL92表达式的消息过滤。

**核心属性**:
```python
@dataclass
class MessageSelector:
    type: ExpressionType    # 表达式类型（TAG或SQL92）
    expression: str         # 过滤表达式
```

**关键方法**:
```python
@classmethod
def by_tag(cls, tag_expression: str) -> "MessageSelector":
    """创建基于标签的消息选择器"""

@classmethod  
def by_sql(cls, sql_expression: str) -> "MessageSelector":
    """创建基于SQL92的消息选择器（已废弃）"""

def is_tag_type() -> bool:      # 判断是否为TAG类型
def is_sql_type() -> bool:      # 判断是否为SQL92类型
def is_subscribe_all() -> bool: # 判断是否订阅所有消息
def validate() -> bool:         # 验证选择器有效性
```

**便利函数**:
```python
def create_tag_selector(tag_expression: str = "*") -> MessageSelector:
    """创建TAG选择器的便利函数"""
    
def create_sql_selector(sql_expression: str) -> MessageSelector:
    """创建SQL92选择器的便利函数（已废弃）"""
    
def is_tag_type(expression: str) -> bool:
    """判断表达式是否为TAG类型"""
```

### 9. SubscriptionData - 订阅数据

**文件位置**: `client_data.py`

**功能描述**: 表示消费者对主题的订阅信息。

**核心属性**:
```python
@dataclass
class SubscriptionData:
    topic: str                    # 主题名称
    sub_string: str              # 订阅表达式
    sub_version: int = 0         # 订阅版本
    expression_type: str = "TAG" # 表达式类型
    tags: list[str] = field(default_factory=list)     # 标签列表
    codes: list[int] = field(default_factory=list)    # 代码列表
    class_filter_mode: bool = False  # 类过滤模式
```

### 10. SubscriptionEntry - 订阅条目

**文件位置**: `subscription.py`

**功能描述**: 存储单个Topic的完整订阅信息，包括选择器、时间戳等元数据。

**核心属性**:
```python
@dataclass
class SubscriptionEntry:
    topic: str                    # 订阅的Topic
    selector: MessageSelector     # 消息选择器
    subscription_data: SubscriptionData  # 订阅数据
    created_at: datetime          # 创建时间
    updated_at: datetime          # 更新时间
    is_active: bool = True        # 是否活跃
```

**关键方法**:
- `update_timestamp()`: 更新时间戳为当前时间
- `to_dict()`: 转换为字典格式
- `from_dict()`: 从字典创建订阅条目

### 11. SubscriptionConflict - 订阅冲突

**文件位置**: `subscription.py`

**功能描述**: 记录订阅冲突的详细信息，用于问题追踪和分析。

**核心属性**:
```python
@dataclass
class SubscriptionConflict:
    topic: str                    # 冲突的Topic
    existing_selector: MessageSelector  # 现有选择器
    new_selector: MessageSelector       # 新选择器
    conflict_type: str           # 冲突类型
    timestamp: datetime          # 冲突发生时间
    description: str | None = None      # 冲突描述
```

## 序列化机制

### RemotingCommandSerializer

**文件位置**: `serializer.py`

**功能描述**: RocketMQ远程命令的序列化器，处理二进制数据的编码和解码。

**协议格式**:
```
| length(4) | header-length(4) | header-data(JSON) | body-data(bytes) |
```

**核心方法**:
```python
class RemotingCommandSerializer:
    @classmethod
    def serialize(cls, command: RemotingCommand) -> bytes:
        """序列化命令为二进制数据"""
        
    @classmethod
    def deserialize(cls, data: bytes) -> RemotingCommand:
        """从二进制数据反序列化命令"""
        
    @classmethod
    def _serialize_header(cls, command: RemotingCommand) -> bytes:
        """序列化命令头部"""
        
    @classmethod
    def _deserialize_header(cls, header_data: bytes) -> dict:
        """反序列化命令头部"""
```

**配置限制**:
- `MAX_FRAME_SIZE`: 32MB (最大帧大小)
- `MAX_HEADER_SIZE`: 64KB (最大头部大小)

**错误处理**:
- `SerializationError`: 序列化失败
- `DeserializationError`: 反序列化失败
- `MessageTooLargeError`: 消息过大
- `HeaderTooLargeError`: 头部过大
- `ProtocolError`: 协议错误

## 结果数据模型

### SendMessageResult - 发送结果

**文件位置**: `result_data.py`

**功能描述**: 消息发送结果的数据结构，包含发送状态和相关元数据。

**核心属性**:
```python
@dataclass
class SendMessageResult:
    msg_id: str                    # 消息ID
    queue_id: int                  # 队列ID
    queue_offset: int              # 队列偏移量
    transaction_id: str | None = None  # 事务ID
    region_id: str | None = None   # 区域ID
    trace_on: bool = False         # 是否启用跟踪
```

**SendStatus枚举**:
- `SEND_OK`: 发送成功
- `FLUSH_DISK_TIMEOUT`: 刷盘超时
- `FLUSH_SLAVE_TIMEOUT`: 从节点刷盘超时
- `SLAVE_NOT_AVAILABLE`: 从节点不可用
- `CREATE_TOPIC_FAILED`: 创建主题失败

### PullMessageResult - 拉取结果

**文件位置**: `result_data.py`

**功能描述**: 消息拉取结果的数据结构，包含拉取的消息列表和相关状态。

**核心属性**:
```python
@dataclass
class PullMessageResult:
    messages: list[MessageExt]     # 消息列表
    next_begin_offset: int         # 下次开始偏移量
    min_offset: int                # 最小偏移量
    max_offset: int                # 最大偏移量
    pull_status: PullStatus        # 拉取状态
    suggest_which_broker_id: int | None = None  # 建议的Broker ID
```

**PullStatus枚举**:
- `FOUND`: 找到消息
- `NO_NEW_MSG`: 没有新消息
- `NO_MATCHED_MSG`: 没有匹配的消息
- `OFFSET_ILLEGAL`: 偏移量非法
- `BROKER_TIMEOUT`: Broker超时

### OffsetResult - 偏移量结果

**文件位置**: `result_data.py`

**功能描述**: 偏移量查询结果的数据结构。

**核心属性**:
```python
@dataclass
class OffsetResult:
    offset: int                    # 偏移量值
    result_code: int               # 结果代码
    result_msg: str | None = None  # 结果消息
```

## 枚举定义

### 核心枚举类型

**文件位置**: `enums.py`

#### RequestCode - 请求代码
定义了RocketMQ支持的所有请求类型，包括：
- 消息发送相关：`SEND_MESSAGE`, `SEND_MESSAGE_V2`, `SEND_BATCH_MESSAGE`
- 消息拉取相关：`PULL_MESSAGE`, `GET_MESSAGE_BY_ID`
- 消费者管理：`GET_CONSUMER_LIST_BY_GROUP`, `UPDATE_CONSUMER_OFFSET`
- 事务消息：`END_TRANSACTION`, `CHECK_TRANSACTION_STATE`
- 系统管理：`GET_BROKER_CLUSTER_INFO`, `UPDATE_AND_CREATE_TOPIC`

#### ResponseCode - 响应代码
定义了所有可能的响应状态，包括：
- 成功状态：`SUCCESS`, `SUCCESS_NOT_FOUND`
- 系统错误：`SYSTEM_ERROR`, `SYSTEM_BUSY`
- 参数错误：`REQUEST_CODE_NOT_SUPPORTED`, `ILLEGAL_PARAMETER`
- 认证错误：`AUTHENTICATION_FAILED`, `NO_PERMISSION`
- 网络错误：`CONNECT_EXCEPTION`, `TRANSPORT_EXCEPTION`

#### FlagType - 标志类型
定义了命令的标志位：
- `RPC_TYPE`: RPC请求类型标志
- `RPC_ONEWAY`: 单向通信标志
- `RESPONSE_TYPE`: 响应类型标志

#### LanguageCode - 语言代码
定义了客户端支持的语言类型：
- `JAVA`: Java语言
- `PYTHON`: Python语言（新增）
- `GO`: Go语言
- `CPP`: C++语言
- `DOTNET`: .NET语言

#### LocalTransactionState - 本地事务状态
定义了本地事务的状态：
- `COMMIT_MESSAGE`: 提交消息
- `ROLLBACK_MESSAGE`: 回滚消息
- `UNKNOWN`: 未知状态

#### ConsumeType - 消费类型
- `CONSUME_ACTIVELY`: 主动消费
- `CONSUME_PASSIVELY`: 被动消费

#### MessageModel - 消息模式
- `CLUSTERING`: 集群消费
- `BROADCASTING`: 广播消费

#### ConsumeFromWhere - 消费起始位置
- `CONSUME_FROM_LAST_OFFSET`: 从最新偏移量开始
- `CONSUME_FROM_FIRST_OFFSET`: 从最早偏移量开始
- `CONSUME_FROM_TIMESTAMP`: 从指定时间戳开始

#### AllocateQueueStrategy - 队列分配策略
- `AVERAGE`: 平均分配
- `AVERAGE_BY_CIRCLE`: 环形平均分配

## 工厂模式

### RemotingRequestFactory

**文件位置**: `factory.py`

**功能描述**: RocketMQ请求命令的工厂类，提供创建各种类型请求的便捷方法。

**核心方法**:
```python
class RemotingRequestFactory:
    @classmethod
    def create_send_message_request(cls, ...) -> RemotingCommand:
        """创建发送消息请求"""
        
    @classmethod
    def create_pull_message_request(cls, ...) -> RemotingCommand:
        """创建拉取消息请求"""
        
    @classmethod
    def create_heartbeat_request(cls, ...) -> RemotingCommand:
        """创建心跳请求"""
        
    @classmethod
    def create_get_consumer_list_request(cls, ...) -> RemotingCommand:
        """获取消费者列表请求"""
        
    @classmethod
    def create_update_consumer_offset_request(cls, ...) -> RemotingCommand:
        """更新消费者偏移量请求"""
        
    # ... 更多请求创建方法
```

### RemotingCommandBuilder

**文件位置**: `factory.py`

**功能描述**: RemotingCommand的构建器模式实现，提供链式调用方式构建命令。

**使用示例**:
```python
command = (RemotingCommandBuilder()
    .code(RequestCode.SEND_MESSAGE)
    .opaque(generate_opaque())
    .remark("send message")
    .add_ext_field("topic", "test_topic")
    .body(message_body)
    .build())
```

## 工具函数

### 命令处理工具

**文件位置**: `utils.py`

**核心函数**:
```python
def validate_command(command: RemotingCommand) -> None:
    """验证命令的有效性"""

def generate_opaque() -> int:
    """生成唯一的opaque值"""

def create_response_for_request(request: RemotingCommand, 
                               response_code: ResponseCode) -> RemotingCommand:
    """为请求创建响应"""

def copy_command_with_new_opaque(command: RemotingCommand, 
                                opaque: int) -> RemotingCommand:
    """复制命令并设置新的opaque值"""
```

### 信息提取工具

**核心函数**:
```python
def get_topic_from_command(command: RemotingCommand) -> str | None:
    """从命令中提取topic信息"""

def get_group_from_command(command: RemotingCommand) -> str | None:
    """从命令中提取group信息"""

def get_queue_id_from_command(command: RemotingCommand) -> int | None:
    """从命令中提取queue_id信息"""

def get_offset_from_command(command: RemotingCommand) -> int | None:
    """从命令中提取offset信息"""
```

### 统计分析工具

**核心函数**:
```python
def get_command_stats(commands: list[RemotingCommand]) -> dict[str, int]:
    """统计命令信息"""

def get_command_summary(commands: list[RemotingCommand]) -> str:
    """获取命令摘要"""

def get_command_type_name(code: int) -> str:
    """获取命令类型名称"""
```

### 过滤和查询工具

**核心函数**:
```python
def filter_commands_by_topic(commands: list[RemotingCommand], 
                           topic: str) -> list[RemotingCommand]:
    """按topic过滤命令"""

def filter_commands_by_group(commands: list[RemotingCommand], 
                           group: str) -> list[RemotingCommand]:
    """按group过滤命令"""

def is_send_message_command(command: RemotingCommand) -> bool:
    """判断是否为发送消息命令"""

def is_pull_message_command(command: RemotingCommand) -> bool:
    """判断是否为拉取消息命令"""

def is_heartbeat_command(command: RemotingCommand) -> bool:
    """判断是否为心跳命令"""

def is_success_response(command: RemotingCommand) -> bool:
    """判断是否为成功响应"""

def is_error_response(command: RemotingCommand) -> bool:
    """判断是否为错误响应"""
```

### 序列化工具

**核心函数**:
```python
def command_to_dict(command: RemotingCommand) -> dict[str, Any]:
    """将命令转换为字典"""

def commands_to_json(commands: list[RemotingCommand]) -> str:
    """将命令列表转换为JSON"""

def parse_command_from_json(json_str: str) -> list[RemotingCommand]:
    """从JSON解析命令列表"""

def format_ext_fields_for_display(ext_fields: dict[str, str]) -> str:
    """格式化扩展字段用于显示"""
```

### 事务状态工具

**核心函数**:
```python
def transaction_state(state: LocalTransactionState) -> int:
    """获取事务状态对应的整数值"""
```

### 唯一ID生成

**核心变量和函数**:
```python
# 全局计数器
COUNTER: int = 0
COUNTER_LOCK: threading.Lock = threading.Lock()

# 起始时间戳
START_TIMESTAMP: int = int(time.time())

def create_uniq_id() -> str:
    """创建唯一ID"""
    
def _get_process_id() -> int:
    """获取进程ID"""
    
def _get_ip_address() -> str:
    """获取本机IP地址"""
```

## 消息创建函数

**文件位置**: `message.py`

### create_message()
```python
def create_message(
    topic: str,
    body: bytes,
    tags: str | None = None,
    keys: str | None = None,
    flag: int = 0,
    **properties
) -> Message:
    """创建基础消息的便利函数"""
```

### create_transaction_message()
```python
def create_transaction_message(
    topic: str,
    body: bytes,
    transaction_id: str | None = None,
    tags: str | None = None,
    keys: str | None = None,
    **properties
) -> Message:
    """创建事务消息的便利函数"""
```

### create_delay_message()
```python
def create_delay_message(
    topic: str,
    body: bytes,
    delay_time_level: int,
    tags: str | None = None,
    keys: str | None = None,
    **properties
) -> Message:
    """创建延迟消息的便利函数"""
```

## 异常处理

### 异常层次结构

**文件位置**: `errors.py`

```
RemotingCommandError (基础异常)
├── SerializationError (序列化异常)
├── DeserializationError (反序列化异常)
├── ProtocolError (协议错误)
│   ├── MessageTooLargeError (消息过大错误)
│   ├── HeaderTooLargeError (Header过大错误)
│   ├── InvalidHeaderError (无效Header错误)
│   ├── InvalidMessageError (无效消息错误)
│   ├── UnsupportedVersionError (不支持的版本错误)
│   ├── UnsupportedLanguageError (不支持的语言错误)
│   └── UnsupportedRequestCodeError (不支持的请求代码错误)
├── ValidationError (数据验证错误)
├── ConnectionClosedError (连接已关闭错误)
└── TimeoutError (超时错误)
```

### 异常详细说明

#### RemotingCommandError
- **描述**: 远程命令相关异常的基类
- **用途**: 所有Model层异常的父类

#### SerializationError
- **描述**: 序列化过程中发生的错误
- **场景**: 将RemotingCommand序列化为字节数据时失败

#### DeserializationError
- **描述**: 反序列化过程中发生的错误
- **场景**: 从字节数据反序列化为RemotingCommand时失败

#### ProtocolError
- **描述**: RocketMQ协议相关的错误
- **用途**: 处理协议格式、版本、大小限制等问题

#### MessageTooLargeError
- **描述**: 消息大小超过限制
- **属性**: `size` (实际大小), `max_size` (最大限制)
- **场景**: 发送或接收的消息超过32MB限制

#### HeaderTooLargeError
- **描述**: 消息头部大小超过限制
- **属性**: `size` (实际大小), `max_size` (最大限制)
- **场景**: 消息头部超过64KB限制

#### TimeoutError
- **描述**: 操作超时
- **属性**: `timeout` (超时时间)
- **场景**: 网络操作或数据处理超时

## 协议兼容性

### RocketMQ协议支持

Model模块严格遵循RocketMQ TCP协议规范，确保与官方实现的完全兼容性：

#### 协议版本
- 支持RocketMQ 4.x及5.x版本
- 向后兼容早期版本
- 协议版本号：1 (默认)

#### 消息格式
- **消息体**: 原始字节数据，支持任意格式
- **消息属性**: JSON格式的键值对
- **消息ID**: 基于IP地址和时间戳的唯一标识
- **序列化**: 使用JSON序列化头部，二进制传输消息体

#### 通信协议
- **协议类型**: TCP长连接
- **帧格式**: 长度前缀 + 头部长度 + JSON头部 + 消息体
- **通信模式**: 同步请求/响应、异步、单向
- **心跳机制**: 定期发送心跳保持连接

### 与Go语言实现的兼容性

Model模块与Go语言RocketMQ实现保持完全兼容：

#### 数据结构兼容
- **RemotingCommand**: 与Go结构体字段完全对应
- **Message**: 支持Go版本的所有属性和方法
- **MessageQueue**: 主题、Broker名称、队列ID完全匹配
- **MessageSelector**: TAG和SQL92过滤语法兼容

#### 序列化兼容
- **协议格式**: 使用相同的二进制协议格式
- **字段顺序**: JSON头部字段顺序与Go版本一致
- **类型映射**: Python类型与Go类型正确映射
- **时间戳**: Unix时间戳格式保持一致

#### 请求代码兼容
- 所有RequestCode枚举值与Go版本完全匹配
- 请求头结构完全对应
- 响应格式和状态码一致

#### 错误处理兼容
- 错误代码与Go版本完全对应
- 异常消息格式兼容
- 超时和重试机制一致

## 使用示例

### 创建和序列化命令

```python
from pyrocketmq.model import (
    RemotingCommand, 
    RemotingCommandSerializer,
    RequestCode,
    ResponseCode,
    LanguageCode
)

# 创建请求命令
request = RemotingCommand(
    code=RequestCode.SEND_MESSAGE,
    language=LanguageCode.PYTHON,
    opaque=12345,
    remark="send test message"
)
request.set_request()
request.add_ext_field("topic", "test_topic")
request.body = b"Hello, RocketMQ!"

# 序列化命令
serializer = RemotingCommandSerializer()
data = serializer.serialize(request)

# 反序列化命令
response = serializer.deserialize(data)
print(f"Response code: {response.code}")
print(f"Body: {response.body}")
```

### 创建消息

```python
from pyrocketmq.model import (
    Message, 
    create_message, 
    create_transaction_message,
    MessageProperty
)

# 方式1：使用构造函数
message = Message(
    topic="test_topic",
    body=b"Hello, RocketMQ!",
    flag=0
)
message.set_property(MessageProperty.TAGS, "order")
message.set_property(MessageProperty.KEYS, "order_123")

# 方式2：使用便利函数
message = create_message(
    topic="test_topic",
    body=b"Hello, RocketMQ!",
    tags="order",
    keys="order_123"
)

# 创建事务消息
tx_message = create_transaction_message(
    topic="order_topic",
    body=b'{"order_id": "123", "amount": 100}',
    transaction_id="tx_456",
    tags="order"
)
```

### 使用工厂创建请求

```python
from pyrocketmq.model import RemotingRequestFactory, MessageQueue

# 创建发送消息请求
queue = MessageQueue(topic="test_topic", broker_name="broker1", queue_id=0)
request = RemotingRequestFactory.create_send_message_request(
    producer_group="test_producer",
    message_data=b"Hello, RocketMQ!",
    message_queue=queue,
    properties={"TAGS": "order"}
)

# 创建拉取消息请求
pull_request = RemotingRequestFactory.create_pull_message_request(
    consumer_group="test_consumer",
    topic="test_topic",
    queue_id=0,
    offset=0,
    max_nums=32,
    subscription_data=None
)

# 创建心跳请求
heartbeat_request = RemotingRequestFactory.create_heartbeat_request(
    client_id="client_123",
    producer_data_set=[],
    consumer_data_set=[]
)
```

### 使用工具函数

```python
from pyrocketmq.model.utils import (
    validate_command,
    get_topic_from_command,
    get_command_summary,
    filter_commands_by_topic
)

# 验证命令
validate_command(request)

# 提取信息
topic = get_topic_from_command(request)
print(f"Topic: {topic}")

# 获取摘要
summary = get_command_summary([request])
print(f"Command summary: {summary}")

# 过滤命令
filtered = filter_commands_by_topic([request], "test_topic")
print(f"Filtered commands: {len(filtered)}")
```

### 消息选择器使用

```python
from pyrocketmq.model import MessageSelector, ExpressionType

# 创建TAG选择器
tag_selector = MessageSelector.by_tag("order || payment || refund")

# 创建SQL选择器（已废弃）
sql_selector = MessageSelector.by_sql("color = 'red' AND price > 100")

# 使用便利函数
from pyrocketmq.model import create_tag_selector, create_sql_selector

tag_selector = create_tag_selector("order")
sql_selector = create_sql_selector("price > 100")  # 不推荐

# 验证选择器
if tag_selector.validate():
    print(f"TAG selector valid: {tag_selector.expression}")

# 判断选择器类型
if tag_selector.is_tag_type():
    print("This is a TAG selector")

if tag_selector.is_subscribe_all():
    print("Subscribe to all messages")
```

### 订阅管理相关操作

```python
from pyrocketmq.model import (
    SubscriptionEntry, 
    SubscriptionData,
    MessageSelector,
    create_tag_selector
)
from datetime import datetime

# 创建订阅数据
subscription_data = SubscriptionData(
    topic="order_topic",
    sub_string="order || payment",
    expression_type="TAG"
)

# 创建消息选择器
selector = create_tag_selector("order || payment")

# 创建订阅条目
subscription = SubscriptionEntry(
    topic="order_topic",
    selector=selector,
    subscription_data=subscription_data,
    created_at=datetime.now(),
    updated_at=datetime.now()
)

# 更新时间戳
subscription.update_timestamp()

# 转换为字典
sub_dict = subscription.to_dict()
print(f"Subscription dict: {sub_dict}")
```

### 异常处理示例

```python
from pyrocketmq.model import (
    RemotingCommand,
    RemotingCommandSerializer,
    MessageTooLargeError,
    SerializationError
)

try:
    # 创建超大消息
    large_message = RemotingCommand(
        code=1,
        body=b"x" * (33 * 1024 * 1024)  # 33MB，超过32MB限制
    )
    
    serializer = RemotingCommandSerializer()
    data = serializer.serialize(large_message)
    
except MessageTooLargeError as e:
    print(f"消息过大: {e}")
    print(f"实际大小: {e.size}, 最大限制: {e.max_size}")
    
except SerializationError as e:
    print(f"序列化失败: {e}")
    
except Exception as e:
    print(f"未知错误: {e}")
```

## 依赖项列表

### 内部依赖

Model模块内部文件依赖关系：

```
__init__.py (导出所有公共接口)
├── command.py (核心数据结构)
├── enums.py (枚举定义)
├── errors.py (异常定义)
├── message.py (消息数据结构)
├── message_ext.py (扩展消息)
├── message_queue.py (消息队列)
├── message_id.py (消息ID)
├── client_data.py (客户端数据)
├── consumer.py (消费者数据)
├── subscription.py (订阅数据)
├── heart_beat.py (心跳数据)
├── nameserver_models.py (NameServer数据)
├── result_data.py (结果数据)
├── headers.py (协议头)
├── serializer.py (序列化器)
├── factory.py (工厂和构建器)
├── utils.py (工具函数)
└── producer_consumer.py (生产消费者相关)
```

### 外部依赖

#### 标准库依赖
- `json`: JSON序列化/反序列化
- `struct`: 二进制数据处理
- `time`: 时间戳生成
- `threading`: 线程安全和ID生成
- `socket`: 网络地址获取
- `os`: 操作系统相关信息
- `dataclasses`: 数据类装饰器
- `typing`: 类型注解支持
- `datetime`: 日期时间处理
- `warnings`: 警告信息处理
- `hashlib`: 哈希计算

#### 项目内部依赖
- `pyrocketmq.model.utils`: 工具函数（内部循环引用）

### Python版本要求
- **最低版本**: Python 3.11+
- **推荐版本**: Python 3.11+
- **特性要求**: 
  - 类型注解支持
  - dataclass装饰器
  - union类型语法
  - match语句（可选）

### 性能要求
- **内存**: 建议512MB以上（处理大量消息时）
- **CPU**: 支持多核并发处理
- **网络**: 稳定的TCP连接

## 版本变更记录

### v1.3.0 (2025-01-12) - 消费者工厂增强
**新增功能**:
- ✅ 在`consumer_factory.py`中新增`create_message_selector`方法
- ✅ 新增`create_tag_selector`便利函数，简化TAG选择器创建
- ✅ 完善消息选择器的参数验证和错误处理
- ✅ 增加完整的使用示例和文档说明

**功能增强**:
- 📈 支持统一的消息选择器创建接口
- 📈 提供详细的错误信息和类型检查
- 📈 增加SQL92废弃警告，引导用户使用TAG过滤
- 📈 完善日志记录，包含选择器创建的详细信息

**文档更新**:
- 📚 更新模块文档，添加消息选择器相关说明
- 📚 补充消费者工厂的使用示例
- 📚 增加参数验证和异常处理的详细说明

### v1.2.0 (2025-01-05) - 订阅管理增强
**新增功能**:
- ✅ 新增`SubscriptionEntry`数据结构，支持完整的订阅信息管理
- ✅ 新增`SubscriptionConflict`数据结构，记录订阅冲突信息
- ✅ 在`client_data.py`中完善`MessageSelector`的实现
- ✅ 新增`create_tag_selector`和`create_sql_selector`便利函数

**功能增强**:
- 📈 支持订阅数据的时间戳管理
- 📈 提供订阅数据的导入导出功能
- 📈 增加订阅冲突检测和记录
- 📈 完善TAG和SQL92表达式的支持

**API变更**:
- 🔄 新增`SubscriptionEntry`类和相关方法
- 🔄 新增`SubscriptionConflict`类和相关方法
- 🔄 扩展`MessageSelector`的功能
- 🔄 新增订阅相关的便利函数

### v1.1.0 (2024-12-XX) - 核心协议支持
**新增功能**:
- ✅ 实现完整的`RemotingCommand`数据结构
- ✅ 实现高效的`RemotingCommandSerializer`序列化器
- ✅ 新增完整的消息数据结构（Message、MessageExt、MessageQueue）
- ✅ 新增`MessageID`生成和解析功能
- ✅ 实现`RemotingRequestFactory`请求工厂
- ✅ 新增`RemotingCommandBuilder`构建器模式

**协议支持**:
- 📡 完整支持RocketMQ TCP协议
- 📡 兼容Go语言实现的二进制格式
- 📡 支持所有核心请求类型
- 📡 实现完整的序列化/反序列化机制

**数据结构**:
- 🏗️ 核心命令结构：`RemotingCommand`
- 🏗️ 消息结构：`Message`、`MessageExt`
- 🏗️ 队列结构：`MessageQueue`
- 🏗️ 客户端数据：`ProducerData`、`ConsumerData`
- 🏗️ 心跳数据：`HeartbeatData`

**工具函数**:
- 🔧 命令验证和处理工具
- 🔧 信息提取和统计工具
- 🔧 序列化和格式化工具
- 🔧 唯一ID生成工具

### v1.0.0 (2024-11-XX) - 初始版本
**基础功能**:
- ✅ 实现基础的枚举定义
- ✅ 新增核心异常体系
- ✅ 实现基础的消息数据结构
- ✅ 新增协议头定义
- ✅ 实现基础的序列化框架

**模块结构**:
- 📁 建立完整的模块文件结构
- 📁 实现基础的依赖关系
- 📁 建立开发规范和编码标准

**文档**:
- 📚 初始技术文档
- 📚 API使用指南
- 📚 开发者指南

## 总结

Model模块作为pyrocketmq的核心数据层，提供了完整、可靠、高性能的RocketMQ协议实现。模块严格遵循RocketMQ TCP协议规范，与Go语言实现保持完全兼容，为上层应用提供了坚实的数据基础。

### 核心优势

1. **协议兼容性**: 与RocketMQ官方实现完全兼容，支持所有核心功能
2. **类型安全**: 使用完整的类型注解和dataclass，提供编译时类型检查
3. **高性能**: 优化的序列化机制和内存管理，支持高并发场景
4. **易用性**: 丰富的便利函数和工具，简化开发工作
5. **可扩展性**: 清晰的模块化设计，便于功能扩展和维护

### 应用场景

- **消息生产**: 支持同步、异步、单向、批量、事务消息
- **消息消费**: 支持并发消费、顺序消费、广播消费
- **消息路由**: 完整的Topic和队列管理
- **系统管理**: 心跳、状态监控、配置管理
- **协议扩展**: 支持自定义协议头和扩展字段

Model模块为pyrocketmq项目提供了强大的数据基础，是整个系统可靠性和性能的重要保障。

---

**最后更新**: 2025-01-12
**文档版本**: v1.3.0
**模块状态**: ✅ 生产就绪，功能完整
