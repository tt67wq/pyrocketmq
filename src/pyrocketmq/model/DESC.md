# Model层模块说明

## 概述

`pyrocketmq.model` 模块是pyrocketmq的核心数据层，提供了完整的RocketMQ TCP协议的Python实现。该模块实现了与Go语言版本完全兼容的协议规范，包括数据结构、序列化、工厂方法和工具函数。

## 核心组件

### 1. 数据结构 (`command.py`)

**RemotingCommand** 是整个协议的核心数据结构，包含以下关键字段：

- **基础字段**: `code`, `language`, `version`, `opaque`, `flag`, `remark`
- **扩展字段**: `ext_fields` (Dict[str, str]) - 存储协议扩展信息
- **消息体**: `body` (Optional[bytes]) - 消息负载

**特性**：
- 支持3种通信类型：请求、响应、单向消息
- 自动flag类型判断（`is_request`, `is_response`, `is_oneway`）
- 内置header序列化/反序列化
- 完整的扩展字段管理API

### 2. 协议枚举 (`enums.py`)

与Go语言实现完全兼容的枚举定义：

- **LanguageCode**: 支持多种编程语言 (JAVA, CPP, PYTHON, GO等)
- **RequestCode**: 完整的RocketMQ请求代码 (SEND_MESSAGE, PULL_MESSAGE等)
- **FlagType**: 通信类型标志位
- **ResponseCode**: 响应状态码 (SUCCESS, SYSTEM_ERROR等)

### 3. 序列化器 (`serializer.py`)

**RemotingCommandSerializer** 实现RocketMQ TCP协议的二进制序列化：

**协议格式**：
```
| length(4) | header-length(4) | header-data(JSON) | body-data(bytes) |
```

**特性**：
- 大小限制检查 (32MB帧，64KB header)
- JSON格式header序列化
- 完整的错误处理和验证
- 支持数据帧验证和解析

### 4. 工厂和构建器 (`factory.py`)

提供两种便捷的命令创建方式：

**RemotingCommandBuilder**：
- 链式调用构建器
- 支持流畅的API设计
- 内置常用字段的快捷方法

**RemotingCommandFactory**：
- 静态工厂方法
- 预定义的命令创建方法
- 支持请求、响应、单向消息的快速创建

### 5. 工具函数 (`utils.py`)

丰富的工具函数集合：

**验证功能**：
- `validate_command()` - 完整的数据验证
- `generate_opaque()` - 生成唯一ID

**命令分析**：
- `is_success_response()` / `is_error_response()` - 响应状态判断
- `get_command_summary()` - 命令摘要信息
- `get_topic_from_command()` / `get_group_from_command()` - 字段提取

**数据处理**：
- `copy_command_with_new_opaque()` - 命令复制
- `filter_commands_by_topic()` / `filter_commands_by_group()` - 列表过滤
- `get_command_stats()` - 统计信息

**格式转换**：
- `command_to_dict()` / `commands_to_json()` - 格式转换
- `parse_command_from_json()` - JSON解析

### 6. 异常体系 (`errors.py`)

完整的异常处理层次：

- **RemotingCommandError** - 基础异常
- **SerializationError** / **DeserializationError** - 序列化异常
- **ProtocolError** - 协议格式错误
- **ValidationError** - 数据验证错误
- **MessageTooLargeError** / **HeaderTooLargeError** - 大小限制错误
- **ConnectionClosedError** / **TimeoutError** - 网络相关错误
- **UnsupportedXxxError** - 不支持的功能错误

## 使用模式

### 创建命令

```python
# 使用工厂方法
command = RemotingCommandFactory.create_send_message_request(
    topic="test_topic",
    body=b"message content",
    producer_group="test_group"
)

# 使用构建器
command = (RemotingCommandBuilder(code=RequestCode.SEND_MESSAGE)
          .with_topic("test_topic")
          .with_body(b"message content")
          .as_request()
          .build())
```

### 序列化/反序列化

```python
# 序列化
data = RemotingCommandSerializer.serialize(command)

# 反序列化
restored = RemotingCommandSerializer.deserialize(data)

# 验证数据帧
if RemotingCommandSerializer.validate_frame(data):
    total_length, header_length = RemotingCommandSerializer.get_frame_info(data)
```

### 工具函数使用

```python
# 验证命令
validate_command(command)

# 生成opaque
opaque = generate_opaque()

# 检查响应状态
if is_success_response(response):
    print("请求成功")

# 提取信息
topic = get_topic_from_command(command)
summary = get_command_summary(command)
```

## 设计特点

1. **协议兼容性**: 完全兼容RocketMQ Go语言实现的TCP协议格式
2. **类型安全**: 全面的类型注解，确保编译时类型检查
3. **性能优化**: 紧凑JSON格式，直接bytes操作
4. **错误处理**: 完整的异常体系，详细的错误信息
5. **工具丰富**: 提供大量实用工具函数，简化开发
6. **API设计**: 支持工厂模式和构建器模式，使用灵活

## 协议规范要点

- **Flag类型**: RPC_TYPE=0, RPC_ONEWAY=1, RESPONSE_TYPE=1
- **大小限制**: 最大32MB帧，64KB header
- **字节序**: 大端序4字节整数
- **编码**: UTF-8 JSON格式header
- **扩展性**: 通过ext_fields支持协议扩展
