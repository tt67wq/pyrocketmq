# pyrocketmq

[![Python](https://img.shields.io/badge/Python-3.11+-blue.svg)](https://www.python.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Build Status](https://img.shields.io/badge/Build-Passing-brightgreen.svg)](#)

pyrocketmq 是一个高性能的 Python RocketMQ 客户端库，完全兼容 RocketMQ TCP 协议规范。

## ✨ 特性

- 🔧 **完全兼容** - 与 RocketMQ Go 语言实现完全兼容
- 🚀 **高性能** - 基于 Python 3.11+ 的高效实现
- 🛡️ **类型安全** - 全面的类型注解和严格的数据验证
- 📦 **易于使用** - 提供工厂方法、构建器和丰富的工具函数
- 🧪 **充分测试** - 完整的单元测试覆盖
- 📊 **协议完整** - 支持所有标准请求代码和响应代码

## 🚀 快速开始

### 安装

```bash
git clone https://github.com/your-username/pyrocketmq.git
cd pyrocketmq

# 激活虚拟环境
source .venv/bin/activate

# 安装依赖
pip install -e .
```

### 基本使用

#### 创建消息命令

```python
from pyrocketmq.model import RemotingCommandFactory, RequestCode

# 创建发送消息请求
command = RemotingCommandFactory.create_send_message_request(
    topic="test_topic",
    body=b"Hello, RocketMQ!",
    producer_group="test_group"
)

# 序列化
from pyrocketmq.model import RemotingCommandSerializer
data = RemotingCommandSerializer.serialize(command)

# 反序列化
restored = RemotingCommandSerializer.deserialize(data)
```

#### 使用构建器模式

```python
from pyrocketmq.model import RemotingCommandBuilder, RequestCode

command = (RemotingCommandBuilder(code=RequestCode.SEND_MESSAGE)
          .with_topic("test_topic")
          .with_body(b"Hello, RocketMQ!")
          .with_producer_group("test_group")
          .with_tags("important")
          .as_request()
          .build())
```

#### 创建响应

```python
from pyrocketmq.model import RemotingCommandFactory

# 创建成功响应
response = RemotingCommandFactory.create_success_response(
    opaque=command.opaque,
    body=b"Message received"
)

# 创建错误响应
error_response = RemotingCommandFactory.create_error_response(
    opaque=command.opaque,
    remark="Topic not found"
)
```

## 📋 系统要求

- Python 3.11+
- RocketMQ 4.x+

## 📚 API 文档

### 核心组件

#### RemotingCommand
RocketMQ 协议的核心数据结构：

```python
from pyrocketmq.model import RemotingCommand, RequestCode, LanguageCode

command = RemotingCommand(
    code=RequestCode.SEND_MESSAGE,
    language=LanguageCode.PYTHON,
    version=1,
    opaque=123,
    flag=0,
    remark="test remark",
    ext_fields={
        "topic": "test_topic",
        "producerGroup": "test_group"
    },
    body=b"message content"
)

# 检查命令类型
if command.is_request:
    print("这是一个请求命令")
elif command.is_response:
    print("这是一个响应命令")
elif command.is_oneway:
    print("这是一个单向消息")
```

#### 序列化器
高效的二进制序列化和反序列化：

```python
from pyrocketmq.model import RemotingCommandSerializer

# 序列化
data = RemotingCommandSerializer.serialize(command)

# 反序列化
restored = RemotingCommandSerializer.deserialize(data)

# 验证数据帧
if RemotingCommandSerializer.validate_frame(data):
    total_length, header_length = RemotingCommandSerializer.get_frame_info(data)
    print(f"总长度: {total_length}, Header长度: {header_length}")
```

#### 工具函数
丰富的实用工具函数：

```python
from pyrocketmq.model.utils import (
    validate_command, generate_opaque, is_success_response,
    get_topic_from_command, get_command_summary
)

# 验证命令
validate_command(command)

# 生成唯一ID
opaque = generate_opaque()

# 检查响应状态
if is_success_response(response):
    print("请求成功")

# 提取信息
topic = get_topic_from_command(command)
summary = get_command_summary(command)
print(f"主题: {topic}")
print(f"摘要: {summary}")
```

### 支持的请求类型

- **消息相关**: `SEND_MESSAGE`, `PULL_MESSAGE`, `QUERY_MESSAGE`
- **消费者相关**: `CONSUMER_SEND_MSG_BACK`, `GET_CONSUMER_LIST_BY_GROUP`
- **生产者相关**: `HEART_BEAT`, `SEND_BATCH_MESSAGE`
- **偏移量相关**: `QUERY_CONSUMER_OFFSET`, `UPDATE_CONSUMER_OFFSET`
- **主题相关**: `CREATE_TOPIC`, `GET_ROUTE_INFO_BY_TOPIC`
- **事务相关**: `END_TRANSACTION`, `CHECK_TRANSACTION_STATE`

## 🧪 运行测试

```bash
# 设置环境变量
export PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src

# 运行所有测试
python -m pytest tests/ -v

# 运行特定模块测试
python -m pytest tests/model/test_serializer.py -v

# 运行单个测试方法
python -m pytest tests/model/test_serializer.py::TestRemotingCommandSerializer::test_serialize_basic_command -v
```

## 🏗️ 项目架构

```
src/pyrocketmq/
├── model/              # RocketMQ协议模型层
│   ├── command.py      # 核心数据结构
│   ├── serializer.py   # 序列化器
│   ├── enums.py        # 协议枚举
│   ├── factory.py      # 工厂和构建器
│   ├── utils.py        # 工具函数
│   └── errors.py       # 异常定义
├── transport/          # 网络传输层
│   ├── abc.py          # 抽象接口
│   ├── tcp.py          # TCP实现
│   ├── config.py       # 配置管理
│   ├── states.py       # 状态机
│   └── errors.py       # 传输异常
└── logging/           # 日志模块
    ├── logger.py       # 日志记录器
    └── config.py       # 日志配置
```

## 🔬 协议规范

### 数据格式
```
| length(4) | header-length(4) | header-data(JSON) | body-data(bytes) |
```

### 大小限制
- 最大帧大小: 32MB
- 最大 Header 大小: 64KB

### Flag 类型
- `RPC_TYPE = 0`: 请求命令
- `RPC_ONEWAY = 1`: 单向消息
- `RESPONSE_TYPE = 1`: 响应命令

## 🤝 贡献

欢迎贡献代码！请遵循以下步骤：

1. Fork 本仓库
2. 创建特性分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 创建 Pull Request

## 📝 许可证

本项目采用 MIT 许可证 - 详见 [LICENSE](LICENSE) 文件。

## 🙏 致谢

- [RocketMQ](https://rocketmq.apache.org/) - 优秀的分布式消息队列
- Python 社区 - 提供了强大的生态系统

## 📞 联系方式

- 项目主页: [GitHub Repository](https://github.com/your-username/pyrocketmq)
- 问题反馈: [GitHub Issues](https://github.com/your-username/pyrocketmq/issues)
- 邮箱: your-email@example.com

---

**⭐ 如果这个项目对你有帮助，请给它一个星标！**
