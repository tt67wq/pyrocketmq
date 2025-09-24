# pyrocketmq - Python RocketMQ Client

## 项目概述

pyrocketmq是一个Python实现的RocketMQ客户端库，目前处于初期开发阶段。项目旨在提供高性能、可靠的RocketMQ消息队列客户端功能。

## 项目结构

```
pyrocketmq/
├── src/pyrocketmq/           # 源代码目录
│   └── __init__.py          # 包初始化文件
├── main.py                  # 主入口文件
├── pyproject.toml          # 项目配置文件
├── README.md               # 项目说明文件
├── .python-version         # Python版本指定
├── .venv/                  # 虚拟环境目录
└── .claude/                # Claude相关配置和任务
    └── tasks/             # 开发任务和设计文档
        └── transport_interface.md
```

## 开发环境配置

### 前置要求
- Python >= 3.11
- uv 包管理器（已配置在.venv中）
- Git

### 环境设置
```bash
# 激活虚拟环境
source .venv/bin/activate

# 安装依赖（目前为空）
pip install -e .

# 运行主程序
python main.py
```

## 开发工具和命令

### 构建和打包
```bash
# 构建项目
python -m build

# 安装为可编辑模式
pip install -e .

# 检查包元数据
python -m check_manifest
```

### 代码质量工具
```bash
# 代码格式化（需要安装）
black src/
ruff check src/  # 或 flake8
isort src/      # 导入排序

# 类型检查（需要安装）
mypy src/

# 测试（需要安装）
pytest tests/
```

### 依赖管理
```bash
# 添加开发依赖
uv add --dev pytest black ruff mypy

# 添加运行时依赖
uv add some-package
```

## 项目架构和代码组织

### 当前状态
- **核心目标**: 实现Transport网络层接口
- **技术栈**: Python 3.11+, asyncio, ABC接口设计
- **开发阶段**: 初期设计和框架搭建

### 架构设计
项目基于Transport网络层接口设计，包含：
- 抽象接口定义（ABC）
- 同步和异步双模式支持
- TCP连接实现
- 状态管理和配置系统
- 异常处理机制

### 核心模块（规划中）
```
src/pyrocketmq/
├── transport/
│   ├── __init__.py
│   ├── abc.py           # 抽象接口定义
│   ├── states.py        # 状态管理
│   ├── config.py        # 配置管理
│   ├── errors.py        # 异常定义
│   ├── tcp.py           # TCP实现
│   └── utils.py         # 工具函数
```

## 开发模式和约定

### 代码风格
- 遵循PEP 8规范
- 使用类型注解（Python 3.11+）
- 文档字符串使用Google风格
- 优先使用asyncio进行异步编程

### 测试策略
- 单元测试覆盖核心功能
- 集成测试验证端到端功能
- 性能测试确保高并发场景

### 版本控制
- 使用Git进行版本控制
- 分支策略：main用于稳定版本，feature/*用于功能开发
- 提交消息规范：feat: 新功能，fix: 修复，docs: 文档更新

## 重要开发模式

### 接口设计模式
```python
from abc import ABC, abstractmethod

class Transport(ABC):
    @abstractmethod
    def output(self, msg: bytes) -> None:
        """发送二进制消息"""
        pass

    @abstractmethod
    def recv(self) -> bytes:
        """接收二进制消息"""
        pass
```

### 配置管理模式
```python
from dataclasses import dataclass

@dataclass
class TransportConfig:
    host: str = "localhost"
    port: int = 8080
    timeout: float = 30.0
    # ...其他配置项
```

### 异常处理模式
```python
class TransportError(Exception):
    """基础传输异常"""
    pass

class ConnectionError(TransportError):
    """连接异常"""
    pass
```

## 项目特定指导

### 开发里程碑
1. **M1**: 核心框架 - 接口定义、状态管理、配置管理
2. **M2**: TCP实现 - 同步和异步TCP传输实现
3. **M3**: 完善功能 - 连接池、性能优化、集成测试

### 注意事项
- **线程安全**: 异步实现需要确保线程安全
- **资源清理**: 确保连接正确关闭，避免资源泄漏
- **错误恢复**: 实现健壮的错误处理和恢复机制
- **性能考虑**: 考虑高并发场景下的性能优化
- **兼容性**: 保持同步和异步接口的一致性

### 使用示例
```python
# 同步使用
from pyrocketmq.transport.tcp import TcpTransport
from pyrocketmq.transport.config import TransportConfig

config = TransportConfig(host="localhost", port=8888)
transport = TcpTransport(config)

transport.connect()
transport.output(b"Hello, RocketMQ!")
response = transport.recv()
transport.close()
```

## 后续扩展计划

1. **协议支持**: UDP、WebSocket等协议
2. **连接池**: 连接池管理和负载均衡
3. **监控指标**: 性能监控和指标收集
4. **安全特性**: TLS加密支持
5. **消息协议**: 完整的RocketMQ协议实现

## 常见问题

### Q: 如何添加新的依赖？
A: 使用 `uv add package-name` 添加运行时依赖，`uv add --dev package-name` 添加开发依赖。

### Q: 如何运行测试？
A: 首先安装测试依赖 `uv add --dev pytest`，然后运行 `pytest tests/`。

### Q: 如何设置开发环境？
A: 项目已配置.venv虚拟环境，使用 `source .venv/bin/activate` 激活即可。

### Q: 代码风格检查？
A: 推荐使用black进行代码格式化，ruff进行代码检查。

## 联系和支持

- 项目仓库：[GitHub地址]
- 问题反馈：通过GitHub Issues
- 开发文档：参考.claude/tasks/下的设计文档