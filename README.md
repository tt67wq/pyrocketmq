# pyrocketmq

[![Python](https://img.shields.io/badge/Python-3.11+-blue.svg)](https://www.python.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Development Status](https://img.shields.io/badge/Development-Alpha-orange.svg)](#)
[![Build Status](https://img.shields.io/badge/Build-Passing-brightgreen.svg)](#)

> **⚠️ 开发状态警告**: 本项目目前处于**早期开发阶段**，仅实现了 RocketMQ 协议的数据结构层。网络传输层尚未完成，**还不能用于生产环境**。

pyrocketmq 是一个正在开发中的高性能 Python RocketMQ 客户端库，旨在完全兼容 RocketMQ TCP 协议规范。

## 🎯 当前进展

### ✅ 已完成功能
- **协议模型层**: 完整的 RemotingCommand 数据结构实现
- **序列化器**: 基于 RocketMQ TCP 协议的二进制序列化/反序列化
- **协议兼容**: 与 Go 语言实现完全兼容的枚举定义
- **工具函数**: 丰富的命令创建、验证和处理工具
- **测试覆盖**: 完整的单元测试覆盖（16个测试用例全部通过）

### 🚧 正在开发中
- **网络传输层**: TCP 连接实现
- **连接管理**: 连接池和负载均衡
- **消息处理**: 生产者和消费者实现
- **性能优化**: 高并发场景优化

### 📋 待实现功能
- **完整客户端**: 生产者和消费者API
- **事务支持**: 分布式事务消息
- **监控指标**: 性能监控和统计
- **安全特性**: TLS 加密和认证

## 🏗️ 当前架构

虽然还在开发中，但项目已经具备了清晰的架构设计：

```
src/pyrocketmq/
├── model/              # ✅ 已完成的协议模型层
│   ├── command.py      # 核心数据结构 RemotingCommand
│   ├── serializer.py   # 二进制序列化器
│   ├── enums.py        # 协议枚举定义
│   ├── factory.py      # 工厂方法和构建器
│   ├── utils.py        # 工具函数
│   └── errors.py       # 模型层异常定义
├── transport/          # 🚧 开发中的网络传输层
│   ├── abc.py          # 传输层抽象接口
│   ├── tcp.py          # TCP连接实现（部分完成）
│   ├── config.py       # 传输配置管理
│   ├── states.py       # 连接状态机
│   └── errors.py       # 传输层异常定义
└── logging/           # ✅ 日志模块
    ├── logger.py       # 日志记录器
    └── config.py       # 日志配置
```

## 💡 当前的使用场景

虽然完整功能尚未完成，但当前的协议模型层可以用于：

### 学习和研究
- 理解 RocketMQ 协议的内部结构
- 学习协议数据结构的实现方式
- 作为实现其他语言客户端的参考

### 自定义实现
- 基于现有的协议模型实现自定义的网络层
- 扩展协议功能用于特殊场景
- 作为其他消息系统的参考实现

### 测试和验证
- 验证协议兼容性
- 测试消息序列化性能
- 开发自定义的RocketMQ工具

## 🔬 当前可用的API

### 基础数据结构操作
```python
from pyrocketmq.model import RemotingCommand, RequestCode, LanguageCode

# 创建命令对象
command = RemotingCommand(
    code=RequestCode.SEND_MESSAGE,
    language=LanguageCode.PYTHON,
    ext_fields={
        "topic": "test_topic",
        "producerGroup": "test_group"
    },
    body=b"message content"
)

# 使用工厂方法
from pyrocketmq.model import RemotingCommandFactory
command = RemotingCommandFactory.create_send_message_request(
    topic="test_topic",
    body=b"Hello, RocketMQ!",
    producer_group="test_group"
)

# 使用构建器
from pyrocketmq.model import RemotingCommandBuilder
command = (RemotingCommandBuilder(code=RequestCode.SEND_MESSAGE)
          .with_topic("test_topic")
          .with_body(b"Hello, RocketMQ!")
          .with_producer_group("test_group")
          .build())
```

### 序列化和反序列化
```python
from pyrocketmq.model import RemotingCommandSerializer

# 序列化命令为二进制数据
data = RemotingCommandSerializer.serialize(command)

# 从二进制数据反序列化命令
restored = RemotingCommandSerializer.deserialize(data)

# 验证数据帧格式
if RemotingCommandSerializer.validate_frame(data):
    total_length, header_length = RemotingCommandSerializer.get_frame_info(data)
```

### 工具函数
```python
from pyrocketmq.model.utils import (
    validate_command, generate_opaque, get_command_summary,
    is_success_response, get_topic_from_command
)

# 验证命令有效性
validate_command(command)

# 生成唯一消息ID
opaque = generate_opaque()

# 获取命令摘要信息
summary = get_command_summary(command)

# 从命令中提取主题信息
topic = get_topic_from_command(command)
```

## 🧪 运行测试

当前只实现了模型层的测试，可以验证协议实现的正确性：

```bash
# 设置环境变量（必需）
export PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src

# 运行所有测试
python -m pytest tests/ -v

# 运行序列化器测试
python -m pytest tests/model/test_serializer.py -v

# 运行单个测试方法
python -m pytest tests/model/test_serializer.py::TestRemotingCommandSerializer::test_serialize_basic_command -v
```

## 🤝 参与贡献

项目处于早期开发阶段，非常欢迎贡献代码！以下是急需帮助的领域：

1. **网络传输层**: 实现完整的TCP连接功能
2. **性能测试**: 进行大规模性能测试
3. **文档完善**: 补充API文档和使用示例
4. **社区建设**: 回答问题，帮助其他开发者

## 📋 系统要求

- Python 3.11+
- 网络传输层完成后需要 RocketMQ 4.x+

## 🔬 协议规范

### 数据格式
```
| length(4) | header-length(4) | header-data(JSON) | body-data(bytes) |
```

### 大小限制
- 最大帧大小: 32MB
- 最大 Header 大小: 64KB

### 支持的协议特性
- ✅ 所有标准请求代码和响应代码
- ✅ 完整的扩展字段支持
- ✅ 多语言客户端兼容
- ✅ Unicode 字符支持
- ✅ 错误处理机制

## 📝 许可证

本项目采用 MIT 许可证 - 详见 [LICENSE](LICENSE) 文件。

## 🙏 致谢

- [RocketMQ](https://rocketmq.apache.org/) - 优秀的分布式消息队列
- Python 社区 - 提供了强大的生态系统

## 📞 联系方式

- 项目主页: [GitHub Repository](https://github.com/your-username/pyrocketmq)
- 问题反馈: [GitHub Issues](https://github.com/your-username/pyrocketmq/issues)
- 开发讨论: [GitHub Discussions](https://github.com/your-username/pyrocketmq/discussions)

---

**⚠️ 请注意**: 这是一个**正在开发中的项目**，请勿在生产环境中使用。欢迎关注项目进展或参与贡献代码！
