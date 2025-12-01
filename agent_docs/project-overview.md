# 项目概述

pyrocketmq是一个功能完整的Python实现的RocketMQ客户端库，基于RocketMQ TCP协议实现。项目提供高性能、可靠的RocketMQ消息队列客户端功能，完全兼容Go语言实现的协议规范。

## 开发环境配置

### 环境设置
```bash
# 激活虚拟环境（如果使用uv）
source .venv/bin/activate

# 设置PYTHONPATH（必需）
export PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src

# 安装依赖
pip install -e .
# 或使用uv
uv sync
```

### 测试运行
```bash
# 运行所有测试
export PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src && python -m pytest tests/

# 运行特定模块测试
export PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src && python -m pytest tests/model/ -v
export PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src && python -m pytest tests/transport/ -v
export PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src && python -m pytest tests/remote/ -v
export PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src && python -m pytest tests/broker/ -v
export PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src && python -m pytest tests/nameserver/ -v
export PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src && python -m pytest tests/producer/ -v
export PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src && python -m pytest tests/consumer/ -v

# 运行单个测试文件
export PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src && python -m pytest tests/model/test_serializer.py -v

# 运行单个测试方法
export PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src && python -m pytest tests/model/test_serializer.py::TestRemotingCommandSerializer::test_serialize_basic_command -v

# 运行异步测试
export PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src && python -m pytest tests/transport/ -v --asyncio-mode=auto
```