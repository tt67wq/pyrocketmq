# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概述

pyrocketmq是一个Python实现的RocketMQ客户端库，基于RocketMQ TCP协议实现。项目旨在提供高性能、可靠的RocketMQ消息队列客户端功能，完全兼容Go语言实现的协议规范。

## 开发环境配置

### 环境设置
```bash
# 激活虚拟环境
source .venv/bin/activate

# 设置PYTHONPATH
export PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src

# 安装依赖
pip install -e .
```

### 测试运行
```bash
# 运行所有测试
export PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src && python -m pytest tests/

# 运行单个测试文件
export PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src && python -m pytest tests/model/test_serializer.py -v

# 运行单个测试方法
export PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src && python -m pytest tests/model/test_serializer.py::TestRemotingCommandSerializer::test_serialize_basic_command -v
```

## 核心架构

### 项目结构
```
src/pyrocketmq/
├── model/              # RocketMQ协议模型层
│   ├── command.py      # 核心数据结构 RemotingCommand
│   ├── serializer.py   # 二进制序列化/反序列化器
│   ├── enums.py        # 协议枚举定义（与Go语言实现一致）
│   ├── factory.py      # 工厂方法和构建器
│   ├── utils.py        # 工具函数
│   └── errors.py       # 模型层异常定义
├── transport/          # 网络传输层
│   ├── abc.py          # 传输层抽象接口
│   ├── tcp.py          # TCP连接实现
│   ├── config.py       # 传输配置管理
│   ├── states.py       # 连接状态机
│   └── errors.py       # 传输层异常定义
└── logging/           # 日志模块
    ├── logger.py       # 日志记录器
    └── config.py       # 日志配置
```

### 核心设计原则

#### 1. 协议兼容性
- 完全兼容RocketMQ Go语言实现的TCP协议格式
- 枚举值与Go实现保持一致（如FlagType.RPC_TYPE=0, RPC_ONEWAY=1, RESPONSE_TYPE=1）
- 支持所有标准请求代码和响应代码

#### 2. 数据流格式
```
| length(4) | header-length(4) | header-data(JSON) | body-data(bytes) |
```

#### 3. 类型安全
- 全面的类型注解（Python 3.11+）
- 严格的数据验证
- 丰富的异常处理层次

## 核心模块详解

### Model层 (`src/pyrocketmq/model/`)

#### RemotingCommand核心数据结构
- **位置**: `model/command.py`
- **功能**: RocketMQ协议的核心数据结构，支持所有协议字段
- **特性**: 
  - 支持扩展字段管理
  - 内置flag类型判断（is_request, is_response, is_oneway）
  - 自动header序列化/反序列化

#### RemotingCommandSerializer序列化器
- **位置**: `model/serializer.py`
- **功能**: 二进制序列化和反序列化
- **协议**: RocketMQ TCP协议格式
- **特性**:
  - 大小限制检查（最大32MB帧，64KB header）
  - JSON格式header序列化
  - 完整的错误处理

#### 枚举定义
- **位置**: `model/enums.py`
- **内容**: LanguageCode, RequestCode, FlagType, ResponseCode
- **特点**: 与Go语言实现完全兼容

#### 工厂和构建器
- **位置**: `model/factory.py`
- **功能**: 提供便捷的命令创建方式
- **特性**:
  - RemotingCommandBuilder: 链式调用构建器
  - RemotingCommandFactory: 静态工厂方法
  - 预定义的命令创建方法（如create_send_message_request）

### Transport层 (`src/pyrocketmq/transport/`)

#### 抽象接口
- **位置**: `transport/abc.py`
- **设计**: 基于ABC的抽象接口定义
- **支持**: 同步和异步双模式

#### TCP实现
- **位置**: `transport/tcp.py`
- **功能**: 基于python-statemachine的TCP连接状态机
- **状态**: DISCONNECTED, CONNECTING, CONNECTED, CLOSING, CLOSED

## 开发模式

### 命令创建模式

#### 使用工厂方法
```python
from pyrocketmq.model import RemotingCommandFactory, RequestCode
from pyrocketmq.model.enums import LanguageCode

# 创建发送消息请求
command = RemotingCommandFactory.create_send_message_request(
    topic="test_topic",
    body=b"message content",
    producer_group="test_group"
)
```

#### 使用构建器
```python
from pyrocketmq.model import RemotingCommandBuilder, RequestCode

command = (RemotingCommandBuilder(code=RequestCode.SEND_MESSAGE)
          .with_topic("test_topic")
          .with_body(b"message content")
          .with_producer_group("test_group")
          .as_request()
          .build())
```

#### 直接构造
```python
from pyrocketmq.model import RemotingCommand, RequestCode, LanguageCode

command = RemotingCommand(
    code=RequestCode.SEND_MESSAGE,
    language=LanguageCode.PYTHON,
    ext_fields={
        "topic": "test_topic",
        "producerGroup": "test_group"
    },
    body=b"message content"
)
```

### 序列化/反序列化模式

```python
from pyrocketmq.model import RemotingCommandSerializer

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
from pyrocketmq.model.utils import (
    validate_command, generate_opaque, is_success_response,
    get_topic_from_command, get_command_summary
)

# 验证命令
validate_command(command)

# 生成唯一opaque
opaque = generate_opaque()

# 检查响应状态
if is_success_response(response):
    print("请求成功")

# 提取信息
topic = get_topic_from_command(command)
summary = get_command_summary(command)
```

## 协议规范

### Flag类型说明
由于Go语言实现中`RPC_ONEWAY`和`RESPONSE_TYPE`都使用值1，判断逻辑如下：
- `is_request()`: flag == FlagType.RPC_TYPE (0)
- `is_response()`: flag == FlagType.RESPONSE_TYPE (1) 
- `is_oneway()`: flag == FlagType.RPC_ONEWAY (1)

### 大小限制
- 最大帧大小: 32MB (33554432字节)
- 最大header大小: 64KB (65536字节)
- 长度字段格式: 大端序4字节整数

### 错误处理层次
- `RemotingCommandError`: 基础异常
- `SerializationError`/`DeserializationError`: 序列化异常
- `ProtocolError`: 协议格式错误
- `MessageTooLargeError`/`HeaderTooLargeError`: 大小限制错误

## 测试策略

### 测试覆盖
- 单元测试覆盖所有核心功能
- 边界条件测试（大小限制、空数据、无效数据）
- Unicode字符支持测试
- 大消息体性能测试

### 测试运行
必须设置`PYTHONPATH`环境变量以确保能正确导入模块：
```bash
export PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src
```

## 常见任务

### 添加新的请求代码
1. 在`model/enums.py`的RequestCode中添加新枚举
2. 在工厂类中添加对应的创建方法
3. 添加相应的测试用例

### 扩展协议字段
1. 在`RemotingCommand`类中添加新属性
2. 更新序列化逻辑
3. 更新工厂和构建器方法
4. 添加工具函数支持

### 性能优化
- 序列化器使用紧凑JSON格式
- bytes类型直接赋值（无需copy）
- 大消息体大小限制检查

## 注意事项

1. **环境变量**: 开发时必须设置`PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src`
2. **字节处理**: bytes类型不可变，直接赋值即可
3. **Flag判断**: 由于Go语言实现特点，RPC_ONEWAY和RESPONSE_TYPE值相同
4. **大小限制**: 严格检查帧大小和header大小限制
5. **类型安全**: 所有代码都使用类型注解，确保编译时类型检查