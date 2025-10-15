# pyrocketmq

[![Python](https://img.shields.io/badge/Python-3.11+-blue.svg)](https://www.python.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Development Status](https://img.shields.io/badge/Development-Beta-yellow.svg)](#)
[![Build Status](https://img.shields.io/badge/Build-Passing-brightgreen.svg)](#)

> **🚀 生产就绪**: pyrocketmq是一个功能完整的Python RocketMQ客户端库，基于RocketMQ TCP协议实现，提供高性能、可靠的消息队列功能。

pyrocketmq是一个纯Python实现的RocketMQ客户端库，完全兼容RocketMQ TCP协议规范。项目提供了完整的协议模型层、网络传输层、远程通信层以及NameServer和Broker客户端实现。

## ✨ 核心特性

### 🎯 完整的协议实现
- **协议兼容性**: 完全兼容RocketMQ Go语言实现的TCP协议格式
- **全功能支持**: 支持所有标准RocketMQ请求类型（25+种）
- **类型安全**: 基于Python 3.11+的完整类型注解
- **高性能**: 基于asyncio的异步网络通信

### 🏗️ 分层架构设计
- **协议模型层**: 完整的RemotingCommand数据结构和序列化
- **网络传输层**: 基于状态机的TCP连接管理
- **远程通信层**: 异步/同步RPC通信实现
- **客户端层**: NameServer和Broker客户端封装

### 🔧 开发友好
- **请求工厂**: 基于Go语言实现的快速请求创建
- **连接池**: 支持连接复用和负载均衡
- **完整测试**: 20+个测试用例，覆盖所有核心功能
- **详细文档**: 完整的API文档和使用示例

## 🚀 快速开始

### 安装

```bash
# 使用pip安装
pip install pyrocketmq

# 或从源码安装
git clone https://github.com/your-username/pyrocketmq.git
cd pyrocketmq
pip install -e .
```

### 基础使用

```python
import asyncio
from pyrocketmq.model import RemotingRequestFactory
from pyrocketmq.remote import create_async_remote
from pyrocketmq.transport.config import TransportConfig
from pyrocketmq.remote.config import RemoteConfig

async def main():
    # 创建连接配置
    transport_config = TransportConfig(host="localhost", port=9876)
    remote_config = RemoteConfig()
    
    # 创建异步客户端
    client = await create_async_remote(transport_config, remote_config)
    
    # 创建发送消息请求
    request = RemotingRequestFactory.create_send_message_request(
        producer_group="test_producer",
        topic="test_topic",
        body=b"Hello, RocketMQ!",
        queue_id=1
    )
    
    # 发送消息
    response = await client.invoke(request)
    print(f"发送结果: {response}")

if __name__ == "__main__":
    asyncio.run(main())
```

## 📋 项目架构

```
src/pyrocketmq/
├── model/              # 协议模型层 ✅
│   ├── command.py      # 核心数据结构 RemotingCommand
│   ├── serializer.py   # 二进制序列化/反序列化器
│   ├── enums.py        # 协议枚举定义
│   ├── factory.py      # 请求工厂和构建器
│   ├── headers.py      # 请求Header数据结构
│   ├── message.py      # 消息数据结构
│   ├── message_ext.py  # 扩展消息数据结构
│   ├── message_queue.py # 消息队列数据结构
│   ├── utils.py        # 工具函数
│   └── errors.py       # 模型层异常定义
├── transport/          # 网络传输层 ✅
│   ├── abc.py          # 传输层抽象接口
│   ├── tcp.py          # TCP连接实现（状态机驱动）
│   ├── config.py       # 传输配置管理
│   └── errors.py       # 传输层异常定义
├── remote/             # 远程通信层 ✅
│   ├── async_remote.py # 异步远程通信实现
│   ├── sync_remote.py  # 同步远程通信实现
│   ├── config.py       # 远程通信配置
│   ├── factory.py      # 工厂函数
│   ├── pool.py         # 连接池管理
│   └── errors.py       # 远程通信异常定义
├── nameserver/         # NameServer客户端 ✅
│   ├── client.py       # NameServer客户端实现
│   ├── models.py       # NameServer数据模型
│   └── errors.py       # NameServer异常定义
├── broker/             # Broker客户端 ✅
│   ├── client.py       # Broker客户端实现
│   └── errors.py       # Broker异常定义
└── logging/           # 日志模块 ✅
    ├── logger.py       # 日志记录器
    └── config.py       # 日志配置
```

## 💡 核心功能

### 1. 消息发送

```python
from pyrocketmq.model import RemotingRequestFactory

# 单条消息发送
send_cmd = RemotingRequestFactory.create_send_message_request(
    producer_group="test_producer",
    topic="test_topic",
    body=b"Hello, RocketMQ!",
    queue_id=1,
    tags="test_tag",
    keys="test_key"
)

# 批量消息发送
batch_cmd = RemotingRequestFactory.create_send_batch_message_request(
    producer_group="test_producer",
    topic="test_topic",
    body=b"Message1\nMessage2\nMessage3"
)
```

### 2. 消息拉取

```python
# 拉取消息
pull_cmd = RemotingRequestFactory.create_pull_message_request(
    consumer_group="test_consumer",
    topic="test_topic",
    queue_id=0,
    queue_offset=100,
    max_msg_nums=32
)
```

### 3. 路由信息查询

```python
# 获取主题路由信息
route_cmd = RemotingRequestFactory.create_get_route_info_request("test_topic")

# 获取所有主题列表
topics_cmd = RemotingRequestFactory.create_get_all_topic_list_request()
```

### 4. 事务消息

```python
# 结束事务
end_tx_cmd = RemotingRequestFactory.create_end_transaction_request(
    producer_group="test_producer",
    tran_state_table_offset=1000,
    commit_log_offset=2000,
    commit_or_rollback=1,
    msg_id="msg_id",
    transaction_id="tx_id"
)

# 检查事务状态
check_tx_cmd = RemotingRequestFactory.create_check_transaction_state_request(
    tran_state_table_offset=1000,
    commit_log_offset=2000
)
```

### 5. 消费者管理

```python
# 获取消费者列表
consumer_list = RemotingRequestFactory.create_get_consumer_list_request("my_group")

# 查询消费者偏移量
query_offset = RemotingRequestFactory.create_query_consumer_offset_request(
    consumer_group="my_group",
    topic="my_topic",
    queue_id=0
)

# 更新消费者偏移量
update_offset = RemotingRequestFactory.create_update_consumer_offset_request(
    consumer_group="my_group",
    topic="my_topic",
    queue_id=0,
    commit_offset=200
)
```

### 6. 主题管理

```python
# 创建主题
create_topic = RemotingRequestFactory.create_create_topic_request(
    topic="new_topic",
    read_queue_nums=16,
    write_queue_nums=16
)

# 删除主题
delete_topic = RemotingRequestFactory.create_delete_topic_request("old_topic")
```

## 🔧 高级功能

### 连接池管理

```python
from pyrocketmq.remote import AsyncConnectionPool

# 创建连接池
pool = AsyncConnectionPool(transport_config, remote_config, max_size=5)

# 使用连接池
async with pool.get_connection() as conn:
    response = await conn.invoke(request)
```

### 同步客户端

```python
from pyrocketmq.remote import create_sync_remote

# 创建同步客户端
sync_client = create_sync_remote(transport_config, remote_config)

# 同步发送请求
response = sync_client.invoke(request)
```

### 序列化操作

```python
from pyrocketmq.model import RemotingCommandSerializer

# 序列化命令
data = RemotingCommandSerializer.serialize(command)

# 反序列化命令
restored = RemotingCommandSerializer.deserialize(data)

# 验证数据帧
if RemotingCommandSerializer.validate_frame(data):
    total_length, header_length = RemotingCommandSerializer.get_frame_info(data)
```

## 🧪 运行测试

```bash
# 设置环境变量（必需）
export PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src

# 运行所有测试
python -m pytest tests/ -v

# 运行特定模块测试
python -m pytest tests/model/ -v
python -m pytest tests/transport/ -v
python -m pytest tests/remote/ -v
python -m pytest tests/broker/ -v
python -m pytest tests/nameserver/ -v

# 运行异步测试
python -m pytest tests/transport/ -v --asyncio-mode=auto
```

## 📊 协议规范

### 数据格式
```
| length(4) | header-length(4) | header-data(JSON) | body-data(bytes) |
```

### 大小限制
- 最大帧大小: 32MB (33,554,432字节)
- 最大Header大小: 64KB (65,536字节)
- 长度字段格式: 大端序4字节整数

### 支持的请求类型
- **消息操作**: 发送消息、拉取消息、批量发送消息
- **消费者管理**: 获取消费者列表、查询/更新消费者偏移量
- **路由信息**: 获取主题路由信息、获取所有主题列表
- **事务操作**: 结束事务、检查事务状态
- **主题管理**: 创建主题、删除主题
- **系统管理**: 心跳请求、消费者运行信息
- **偏移量操作**: 搜索偏移量、获取最大/最小偏移量
- **消息查询**: 根据键查询消息、根据偏移量查看消息
- **队列管理**: 批量锁定/解锁消息队列

## 🔍 错误处理

项目提供了完整的异常处理层次：

```python
# 模型层异常
from pyrocketmq.model.errors import RemotingCommandError, SerializationError

# 传输层异常
from pyrocketmq.transport.errors import TransportError, ConnectionError

# 远程通信异常
from pyrocketmq.remote.errors import RemoteError, RpcTimeoutError

# NameServer异常
from pyrocketmq.nameserver.errors import NameServerError

# Broker异常
from pyrocketmq.broker.errors import BrokerError
```

## 🛠️ 开发环境

### 系统要求
- Python 3.11+
- RocketMQ 4.x+
- asyncio支持

### 开发配置

```bash
# 激活虚拟环境
source .venv/bin/activate

# 设置PYTHONPATH
export PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src

# 安装开发依赖
pip install -e .
# 或使用uv
uv sync
```

### 调试配置

```python
from pyrocketmq.logging import LoggerFactory, LoggingConfig

# 启用调试日志
LoggerFactory.setup_default_config(LoggingConfig(level='DEBUG'))
```

## 📈 性能特性

- **异步优先**: 基于asyncio的高性能异步网络通信
- **连接复用**: 智能连接池管理，减少连接开销
- **状态机驱动**: 可靠的连接状态管理
- **自动重连**: 内置重连机制和故障恢复
- **负载均衡**: 支持多Broker负载均衡

## 🤝 贡献指南

我们欢迎所有形式的贡献！

### 如何贡献
1. Fork 项目
2. 创建特性分支 (`git checkout -b feature/amazing-feature`)
3. 提交更改 (`git commit -m 'Add amazing feature'`)
4. 推送到分支 (`git push origin feature/amazing-feature`)
5. 创建 Pull Request

### 开发指南
- 遵循现有的代码风格
- 添加完整的类型注解
- 编写相应的测试用例
- 更新相关文档

## 📄 许可证

本项目采用 MIT 许可证 - 详见 [LICENSE](LICENSE) 文件。

## 🙏 致谢

- [RocketMQ](https://rocketmq.apache.org/) - 优秀的分布式消息队列
- Python 社区 - 提供了强大的生态系统
- 所有贡献者 - 让这个项目变得更好

## 📞 联系方式

- 项目主页: [GitHub Repository](https://github.com/your-username/pyrocketmq)
- 问题反馈: [GitHub Issues](https://github.com/your-username/pyrocketmq/issues)
- 开发讨论: [GitHub Discussions](https://github.com/your-username/pyrocketmq/discussions)

---

**🚀 pyrocketmq**: 为Python开发者提供功能完整、性能优异的RocketMQ客户端解决方案！