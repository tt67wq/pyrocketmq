# 依赖管理和注意事项

## 项目配置

### pyproject.toml配置
项目使用现代化的`pyproject.toml`进行项目配置，包含：
- **项目元数据**: 名称、版本、描述、作者信息
- **依赖管理**: 生产依赖和开发依赖
- **构建配置**: 构建后端和构建设置
- **代码质量**: linting、格式化、测试配置

### Python版本要求
- **最低版本**: Python 3.11+
- **推荐版本**: Python 3.11或更高版本
- **类型注解**: 完整的类型注解支持，提升代码质量和IDE体验

### 主要依赖
```toml
[project]
dependencies = [
    "asyncio-mqtt",          # 异步MQTT支持
    "aiofiles",              # 异步文件操作
    "typing-extensions",     # 类型注解扩展
    "structlog",             # 结构化日志
]

[project.optional-dependencies]
dev = [
    "pytest",                # 测试框架
    "pytest-asyncio",        # 异步测试支持
    "pytest-cov",            # 测试覆盖率
    "black",                 # 代码格式化
    "ruff",                  # 代码检查和格式化
    "mypy",                  # 类型检查
]
```

## 开发工具

### 使用pip安装依赖
```bash
# 安装项目依赖（开发模式）
pip install -e .

# 安装开发依赖
pip install -e ".[dev]"

# 更新依赖
pip install --upgrade -e ".[dev]"
```

### 使用uv进行快速依赖管理
```bash
# 安装uv（如果尚未安装）
curl -LsSf https://astral.sh/uv/install.sh | sh

# 同步依赖
uv sync

# 运行项目
uv run python -m pyrocketmq

# 运行测试
uv run pytest

# 添加新依赖
uv add requests

# 添加开发依赖
uv add --dev pytest-mock
```

### 虚拟环境管理
```bash
# 创建虚拟环境
python -m venv .venv

# 激活虚拟环境
source .venv/bin/activate  # Linux/Mac
.venv\Scripts\activate     # Windows

# 使用uv创建虚拟环境
uv venv

# 使用uv激活虚拟环境
source .venv/bin/activate
```

## 注意事项

### 1. 环境变量配置
**必须设置**: 开发时必须设置`PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src`

```bash
# 临时设置
export PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src

# 永久设置（添加到 ~/.bashrc 或 ~/.zshrc）
echo 'export PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src' >> ~/.bashrc
source ~/.bashrc

# 在Python中设置
import sys
sys.path.insert(0, '/Users/admin/Project/Python/pyrocketmq/src')
```

### 2. 分片键策略
**MessageHashSelector优先级**:
- 优先使用`SHARDING_KEY`属性
- 其次使用`KEYS`属性的第一个值
- 最后使用消息内容的哈希值

```python
message = Message(topic="test", body=b"content")

# 方式1：使用SHARDING_KEY
message.set_property("SHARDING_KEY", "user_123")

# 方式2：使用KEYS（如果没有SHARDING_KEY）
message.set_property("KEYS", "order_456,order_789")

# 方式3：基于消息内容哈希
# 自动使用message.body的哈希值
```

### 3. 选择器状态管理
**RoundRobinSelector计数器重置**:
- 路由更新时会重置计数器
- 确保负载均衡的公平性
- 避免计数器溢出问题

### 4. 性能优化策略
**TopicBrokerMapping预构建队列列表**:
- 路由更新时预先构建所有可用队列列表
- 避免运行时计算开销，显著提升性能
- 减少GC压力和内存分配

### 5. 线程安全保证
**所有映射管理器操作都是线程安全的**:
- 使用RLock保护共享状态
- 支持高并发访问
- 无数据竞争风险

### 6. 异步优先架构
**网络通信主要基于asyncio**:
- 同步模式是异步模式的封装
- 更好的资源利用率和性能
- 与现代异步生态无缝集成

### 7. 路由过期管理
**默认路由过期时间**:
- 30秒过期时间，可配置
- 定期清理过期路由信息
- 支持动态更新和失效检测

```python
# 配置路由过期时间
config = ProducerConfig(
    route_cache_expire_seconds=60,  # 60秒过期
    route_refresh_interval=30       # 30秒刷新间隔
)
```

### 8. 类型安全要求
**所有代码使用完整类型注解**:
- 提供完整的类型检查
- 改善IDE支持和代码补全
- 减少运行时类型错误

### 9. 心跳机制
**Producer心跳维护**:
- 定期向所有Broker发送心跳
- 确保连接活跃状态
- 支持故障检测和自动恢复

```python
# 配置心跳参数
config = ProducerConfig(
    heartbeat_interval=30000,        # 30秒心跳间隔
    heartbeat_timeout=60000,         # 60秒超时
    max_heartbeat_failures=3         # 最大失败次数
)
```

### 10. 事务消息完整支持
**事务消息模块特性**:
- 使用`TransactionListener`接口定义本地事务逻辑
- 支持三种事务状态：COMMIT_MESSAGE、ROLLBACK_MESSAGE、UNKNOWN
- 提供`SimpleTransactionListener`用于测试场景
- 包含完整的事务异常处理和超时管理
- 便利函数简化事务消息创建和结果处理

**便利函数**:
- `create_transaction_producer()` 创建事务Producer实例
- `create_transaction_message()` 创建事务消息
- `create_simple_transaction_listener()` 创建简单事务监听器
- `create_transaction_send_result()` 创建事务发送结果

### 11. Consumer模块完整实现
**消费者核心功能**:
- **配置管理**: 支持完整的Consumer配置参数，包括线程数、批量大小、消费模式等
- **消息监听器**: 支持并发消费(`MessageListenerConcurrently`)和顺序消费(`MessageListenerOrderly`)
- **偏移量存储**: 集群模式使用RemoteOffsetStore存储在Broker，广播模式使用LocalOffsetStore存储在本地
- **订阅管理**: 支持主题订阅、消息选择器和订阅冲突检测
- **队列分配**: AverageAllocateStrategy实现平均分配算法，支持大规模分配优化
- **消费起始位置**: 支持从最新、最早、指定时间戳三种起始位置开始消费
- **异常处理**: 20+种专用异常类型，精确处理各种消费错误场景
- **监控指标**: 全面的性能和状态监控，包括消费速率、成功率、延迟等

## 最佳实践

### 依赖管理最佳实践
1. **固定版本**: 生产环境中固定依赖版本
2. **定期更新**: 定期更新依赖包到最新稳定版
3. **安全扫描**: 使用工具扫描依赖的安全漏洞
4. **最小依赖**: 避免安装不必要的依赖包

### 开发环境最佳实践
1. **隔离环境**: 每个项目使用独立的虚拟环境
2. **环境变量**: 使用环境变量管理配置信息
3. **代码质量**: 配置代码格式化和检查工具
4. **测试覆盖**: 保持高测试覆盖率

### 性能优化最佳实践
1. **异步优先**: 优先使用异步API
2. **连接复用**: 合理使用连接池
3. **批量操作**: 使用批量API减少网络开销
4. **内存管理**: 及时释放不再使用的资源

### 监控和运维
1. **日志记录**: 完善的日志记录和监控
2. **性能指标**: 收集关键性能指标
3. **健康检查**: 实现健康检查机制
4. **告警机制**: 配置适当的告警规则