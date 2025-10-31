# Logging模块

## 概述

logging模块为pyrocketmq提供完整的日志记录功能，支持多种格式化器和灵活配置。模块包含JSON格式化器，支持结构化日志输出，便于日志分析和监控。

## 模块结构

```
logging/
├── __init__.py              # 模块导出和便捷接口
├── config.py                # 日志配置管理
├── logger.py                # Logger工厂类和彩色格式化器
├── json_formatter.py        # JSON格式化器实现
├── json_logging_demo.py     # JSON日志演示和示例
└── CLAUDE.md               # 本文档
```

## 核心组件

### 1. LoggingConfig (`config.py`)

日志配置数据类，提供完整的日志配置选项：

```python
from pyrocketmq.logging import LoggingConfig

# 创建默认配置
config = LoggingConfig()

# 自定义配置
config = LoggingConfig(
    level="DEBUG",
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    file_path="app.log",
    console_output=True,
    colored_output=True,
    max_file_size=10*1024*1024,  # 10MB
    backup_count=5
)

# 从环境变量创建配置
config = LoggingConfig.from_env()
```

**支持的环境变量：**
- `PYROCKETMQ_LOG_LEVEL`: 日志级别 (DEBUG/INFO/WARNING/ERROR/CRITICAL)
- `PYROCKETMQ_LOG_FILE`: 日志文件路径
- `PYROCKETMQ_LOG_CONSOLE`: 是否输出到控制台 (true/false/1/0/yes/no)

### 2. LoggerFactory (`logger.py`)

Logger工厂类，负责创建和管理logger实例：

```python
from pyrocketmq.logging import LoggerFactory

# 获取logger
logger = LoggerFactory.get_logger("my_module")

# 设置全局配置
config = LoggingConfig(level="DEBUG")
LoggerFactory.setup_default_config(config)

# 获取当前配置
current_config = LoggerFactory.get_current_config()
```

### 3. JsonFormatter (`json_formatter.py`)

JSON格式化器，将日志转换为JSON格式：

```python
from pyrocketmq.logging import JsonFormatter

# 创建JSON格式化器
formatter = JsonFormatter(
    include_extra=True,
    include_timestamp=True,
    include_level=True,
    service="my-service",
    version="1.0.0"
)

# 应用到logger
import logging
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logger = logging.getLogger("json_logger")
logger.addHandler(handler)
```

**配置选项：**
- `include_extra`: 是否包含extra字段
- `include_timestamp`: 是否包含时间戳
- `include_level`: 是否包含日志级别
- `include_logger`: 是否包含logger名称
- `include_module`: 是否包含模块名
- `include_function`: 是否包含函数名
- `include_line`: 是否包含行号
- `indent`: 是否缩进JSON输出
- `ensure_ascii`: 是否确保ASCII编码
- `**extra_fields`: 额外的固定字段

### 4. StructuredJsonFormatter (`json_formatter.py`)

结构化JSON格式化器，提供更结构化的日志字段：

```python
from pyrocketmq.logging import StructuredJsonFormatter

# 创建结构化JSON格式化器
formatter = StructuredJsonFormatter(
    include_extra=True,
    service="my-service",
    environment="production"
)

# 应用到logger
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logger = logging.getLogger("structured_logger")
logger.addHandler(handler)
```

**输出格式示例：**
```json
{
  "@timestamp": "2024-01-15T10:30:00.123456Z",
  "level": "info",
  "message": "用户登录成功",
  "source": {
    "logger": "auth_module",
    "module": "auth",
    "function": "login_user",
    "line": 45
  },
  "process": {"id": 12345, "name": "MainProcess"},
  "thread": {"id": 67890, "name": "MainThread"},
  "fields": {
    "user_id": 12345,
    "ip_address": "192.168.1.100"
  },
  "service": "my-service",
  "environment": "production"
}
```

## 便捷接口

### 基础使用

```python
from pyrocketmq.logging import get_logger, setup_logging, LoggingConfig

# 设置日志配置
config = LoggingConfig(level="INFO", colored_output=True)
setup_logging(config)

# 获取logger并使用
logger = get_logger("my_module")
logger.info("应用启动")
logger.warning("这是一个警告")
logger.error("发生错误")
```

### JSON日志使用

```python
from pyrocketmq.logging import get_logger, JsonFormatter
import logging

# 获取logger
logger = get_logger("json_app")

# 确保有处理器并设置JSON格式化器
if not logger.handlers:
    handler = logging.StreamHandler()
    logger.addHandler(handler)
    logger.propagate = False

# 设置JSON格式化器
for handler in logger.handlers:
    handler.setFormatter(JsonFormatter(
        include_extra=True,
        service="my-app",
        version="1.0.0"
    ))

# 记录结构化日志
logger.info("用户操作", extra={
    "user_id": 12345,
    "action": "purchase",
    "product_id": "prod_678",
    "amount": 99.99
})
```

## 在pyrocketmq中的使用

### Producer日志配置

```python
from pyrocketmq.producer import create_producer
from pyrocketmq.logging import setup_logging, JsonFormatter, get_logger
import logging

# 设置JSON日志
setup_logging()

# 配置producer的JSON日志
logger = get_logger("rocketmq.producer")
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    logger.addHandler(handler)
    logger.propagate = False

# 应用JSON格式化器
for handler in logger.handlers:
    handler.setFormatter(JsonFormatter(
        include_extra=True,
        service="rocketmq-producer",
        component="message-sending"
    ))

# 创建producer并记录日志
producer = create_producer("my_group")
producer.start()

logger.info("Producer启动成功", extra={
    "producer_group": "my_group",
    "operation": "startup"
})
```

### 异步Producer日志配置

```python
from pyrocketmq.producer import create_async_producer
from pyrocketmq.logging import get_logger, StructuredJsonFormatter
import logging

# 配置异步producer的JSON日志
logger = get_logger("rocketmq.async_producer")
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    logger.addHandler(handler)
    logger.propagate = False

# 应用结构化JSON格式化器
for handler in logger.handlers:
    handler.setFormatter(StructuredJsonFormatter(
        include_extra=True,
        service="async-rocketmq-producer",
        component="async-message-sending"
    ))

# 异步使用
async def async_send():
    producer = await create_async_producer("async_group")
    await producer.start()
    
    logger.info("异步Producer启动", extra={
        "producer_group": "async_group",
        "async_mode": True
    })
```

## 演示和示例

### 运行演示代码

```bash
# 设置PYTHONPATH
export PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src

# 运行JSON日志演示
python -m pyrocketmq.logging.json_logging_demo
```

### 演示功能

`json_logging_demo.py`包含9个完整的演示场景：

1. **基础JSON日志**: 演示基本的JSON日志输出
2. **结构化JSON日志**: 演示StructuredJsonFormatter的使用
3. **Producer JSON日志**: 演示Producer的JSON日志记录
4. **异步Producer JSON日志**: 演示异步Producer的JSON日志
5. **错误处理JSON日志**: 演示异常处理的JSON日志记录
6. **性能监控JSON日志**: 演示性能监控的JSON日志
7. **批量操作JSON日志**: 演示批量操作的JSON日志
8. **自定义字段JSON日志**: 演示自定义字段的JSON日志
9. **JsonFormatter对比**: 演示两种JSON格式化器的区别

### 演示代码示例

```python
# 基础JSON日志演示
def demo_basic_json_logging():
    """演示基础JSON日志功能"""
    from pyrocketmq.logging import get_logger, JsonFormatter
    
    logger = get_logger("basic_demo")
    
    # 确保有处理器
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setLevel(logging.INFO)
        logger.addHandler(handler)
        logger.propagate = False
    
    # 设置JSON格式化器
    json_formatter = JsonFormatter(
        include_timestamp=True,
        include_level=True,
        include_extra=True
    )
    
    for handler in logger.handlers:
        handler.setFormatter(json_formatter)
    
    # 记录日志
    logger.info("应用启动")
    logger.warning("内存使用率较高", extra={
        "memory_usage": 85.2,
        "threshold": 80.0
    })
```

## 最佳实践

### 1. 日志级别使用

- **DEBUG**: 调试信息，仅在开发环境使用
- **INFO**: 一般信息，记录应用正常运行状态
- **WARNING**: 警告信息，可能的问题但不影响正常运行
- **ERROR**: 错误信息，需要关注的问题
- **CRITICAL**: 严重错误，可能导致应用终止

### 2. 结构化日志字段

```python
# 推荐的字段命名规范
logger.info("操作完成", extra={
    "operation": "send_message",      # 操作类型
    "user_id": 12345,                 # 用户标识
    "resource_id": "msg_678",         # 资源标识
    "duration_ms": 150,               # 执行时间
    "success": True,                  # 操作结果
    "metadata": {                     # 元数据
        "topic": "test_topic",
        "partition": 0
    }
})
```

### 3. 异常处理

```python
try:
    # 业务逻辑
    result = risky_operation()
    logger.info("操作成功", extra={"result": result})
except Exception as e:
    logger.error(
        "操作失败",
        extra={
            "error_type": type(e).__name__,
            "error_message": str(e),
            "operation": "risky_operation"
        },
        exc_info=True  # 包含堆栈信息
    )
```

### 4. 性能监控

```python
import time

def performance_monitor(operation_name):
    def decorator(func):
        def wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = func(*args, **kwargs)
                success = True
                error = None
            except Exception as e:
                success = False
                error = str(e)
                raise
            finally:
                duration = (time.time() - start_time) * 1000
                logger.info(f"{operation_name}完成", extra={
                    "operation": operation_name,
                    "duration_ms": round(duration, 2),
                    "success": success,
                    "error": error
                })
            return result
        return wrapper
    return decorator

@performance_monitor("send_message")
def send_message(message):
    # 发送消息逻辑
    pass
```

## 配置管理

### 开发环境配置

```python
from pyrocketmq.logging import setup_logging, LoggingConfig

dev_config = LoggingConfig(
    level="DEBUG",
    console_output=True,
    colored_output=True,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
setup_logging(dev_config)
```

### 生产环境配置

```python
from pyrocketmq.logging import setup_logging, LoggingConfig

prod_config = LoggingConfig(
    level="INFO",
    file_path="/var/log/pyrocketmq/app.log",
    console_output=False,
    colored_output=False,
    max_file_size=100*1024*1024,  # 100MB
    backup_count=10,
    capture_exceptions=True
)
setup_logging(prod_config)
```

### 环境变量配置

```bash
# 设置环境变量
export PYROCKETMQ_LOG_LEVEL=INFO
export PYROCKETMQ_LOG_FILE=/var/log/pyrocketmq/app.log
export PYROCKETMQ_LOG_CONSOLE=false

# 应用启动时自动使用环境变量配置
python -m pyrocketmq.producer
```

## 注意事项

1. **Handler管理**: 使用自定义格式化器时，确保logger有正确的handler配置
2. **性能考虑**: JSON格式化比普通格式稍慢，生产环境建议异步日志
3. **字段命名**: 使用一致的字段命名规范，便于日志分析
4. **敏感信息**: 避免在日志中记录密码、密钥等敏感信息
5. **日志轮转**: 生产环境建议配置文件轮转，避免日志文件过大
6. **异常处理**: 使用`exc_info=True`记录完整的异常堆栈信息

## 故障排查

### 常见问题

1. **日志没有输出**: 检查logger是否有handler，检查日志级别配置
2. **JSON格式错误**: 检查extra字段中的数据是否可序列化
3. **颜色不显示**: 检查终端是否支持ANSI颜色，检查`colored_output`配置
4. **文件权限问题**: 确保应用有权限写入日志文件路径

### 调试技巧

```python
# 检查logger配置
logger = get_logger("debug")
print(f"Logger level: {logger.level}")
print(f"Logger handlers: {logger.handlers}")
print(f"Logger propagate: {logger.propagate}")

# 检查handler配置
for handler in logger.handlers:
    print(f"Handler level: {handler.level}")
    print(f"Handler formatter: {handler.formatter}")
```

## 扩展开发

### 自定义格式化器

```python
from pyrocketmq.logging import JsonFormatter
import logging

class CustomJsonFormatter(JsonFormatter):
    def format(self, record):
        # 自定义格式化逻辑
        log_entry = super().format(record)
        
        # 添加自定义字段
        if hasattr(record, 'custom_field'):
            # 处理自定义字段
            pass
            
        return log_entry

# 使用自定义格式化器
formatter = CustomJsonFormatter(
    include_extra=True,
    custom_field="value"
)
```

### 集成外部日志系统

```python
# 集成ELK Stack
from pyrocketmq.logging import JsonFormatter, get_logger
import logging

logger = get_logger("elk_integration")
handler = logging.StreamHandler()
handler.setFormatter(JsonFormatter(
    include_extra=True,
    service="pyrocketmq",
    environment="production"
))
logger.addHandler(handler)

# 现在日志可以被ELK Stack收集和分析
logger.info("业务日志", extra={
    "trace_id": "abc123",
    "span_id": "def456",
    "business_context": "order_processing"
})
```