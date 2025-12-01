# Logging层日志记录系统

**模块概述**: logging模块为pyrocketmq提供完整的日志记录功能，支持多种格式化器和灵活配置。包含JSON格式化器，支持结构化日志输出，便于日志分析和监控。

## 核心组件

### LoggingConfig
日志配置数据类，提供完整的日志配置选项：
- **级别控制**: DEBUG、INFO、WARNING、ERROR、CRITICAL
- **格式类型**: text、json、color等格式选择
- **输出目标**: 控制台、文件、网络等输出方式
- **文件配置**: 日志文件路径、轮转策略、大小限制

### LoggerFactory
Logger工厂类，统一创建和管理Logger实例：
- **实例管理**: 统一创建和缓存Logger实例
- **配置应用**: 将配置应用到Logger实例
- **命名规范**: 按模块名称自动命名Logger
- **性能优化**: 避免重复创建Logger实例

### JSONFormatter
JSON格式化器实现，支持结构化日志输出：
- **结构化输出**: 将日志转换为JSON格式
- **字段丰富**: 包含时间戳、级别、模块、消息、异常等
- **可定制字段**: 支持自定义字段和元数据
- **机器友好**: 便于日志分析和监控系统处理

## 日志格式

### 文本格式
```
2025-01-07 10:30:45,123 [INFO] pyrocketmq.producer - Message sent successfully: msg_id=12345
```

### JSON格式
```json
{
  "timestamp": "2025-01-07T10:30:45.123Z",
  "level": "INFO",
  "logger": "pyrocketmq.producer",
  "message": "Message sent successfully",
  "fields": {
    "msg_id": "12345",
    "topic": "test_topic",
    "producer_group": "test_group"
  }
}
```

### 彩色格式
```
2025-01-07 10:30:45,123 [INFO] pyrocketmq.producer - Message sent successfully
```
（带颜色高亮，不同级别显示不同颜色）

## 配置选项

### 基础配置
```python
config = LoggingConfig(
    level="INFO",                    # 日志级别
    format_type="text",              # 格式类型: text/json/color
    output_file="app.log",           # 输出文件路径
    max_file_size=10*1024*1024,      # 最大文件大小 (10MB)
    backup_count=5,                  # 备份文件数量
    console_output=True              # 是否输出到控制台
)
```

### 高级配置
```python
config = LoggingConfig(
    level="DEBUG",
    format_type="json",
    output_file="app.log",
    max_file_size=50*1024*1024,      # 50MB
    backup_count=10,
    console_output=False,
    json_fields=[                   # JSON格式的自定义字段
        "timestamp", "level", "logger", 
        "message", "module", "function", "line"
    ],
    async_logging=True,             # 异步日志
    buffer_size=1024*1024           # 缓冲区大小
)
```

## 关键特性

- **多格式支持**: 支持文本、JSON、彩色输出等多种格式
- **灵活配置**: 丰富的配置选项，满足不同场景需求
- **结构化日志**: JSON格式便于日志分析和监控
- **标准兼容**: 与Python标准logging完全兼容
- **性能优化**: 支持异步日志和缓冲机制
- **文件轮转**: 自动日志文件轮转和备份管理

## 使用示例

### 基础使用
```python
from pyrocketmq.logging import get_logger, LoggingConfig

# 获取Logger
logger = get_logger(__name__)

# 基础日志记录
logger.info("Producer started")
logger.debug("Sending message to broker")
logger.warning("Connection timeout, retrying...")
logger.error("Failed to send message")
logger.critical("System error, shutting down")

# 带参数的日志
logger.info("Message sent: %s", message_id)
logger.debug("Connection details: %s", connection_info)

# 异常日志
try:
    send_message(message)
except Exception as e:
    logger.error("Failed to send message: %s", e, exc_info=True)
```

### JSON格式日志
```python
from pyrocketmq.logging import configure_logging, LoggingConfig

# 配置JSON格式日志
config = LoggingConfig(
    level="INFO",
    format_type="json",
    output_file="app.log"
)

configure_logging(config)

# 记录结构化日志
logger = get_logger("pyrocketmq.producer")
logger.info(
    "Message sent successfully",
    extra={
        "msg_id": "12345",
        "topic": "test_topic",
        "producer_group": "test_group",
        "broker": "localhost:10911",
        "cost_time": 150
    }
)

# 异常信息会自动包含堆栈跟踪
try:
    risky_operation()
except Exception as e:
    logger.error("Operation failed", exc_info=True, extra={"operation": "risky_operation"})
```

### 自定义Logger
```python
from pyrocketmq.logging import create_logger, LoggingConfig

# 创建自定义配置的Logger
config = LoggingConfig(
    level="DEBUG",
    format_type="color",
    console_output=True,
    output_file="debug.log"
)

logger = create_logger("my_app.producer", config)

# 使用自定义Logger
logger.debug("Debug information")
logger.info("Information message")
logger.warning("Warning message")
logger.error("Error occurred")
```

### 异步日志
```python
from pyrocketmq.logging import configure_logging, LoggingConfig

# 配置异步日志
config = LoggingConfig(
    level="INFO",
    format_type="json",
    output_file="app.log",
    async_logging=True,
    buffer_size=1024*1024  # 1MB buffer
)

configure_logging(config)

# 在高并发场景中使用
logger = get_logger("high_throughput_app")
for i in range(10000):
    logger.info("Processing item %d", i)
```

## 最佳实践

### 日志级别使用
- **DEBUG**: 详细的调试信息，仅在开发和测试时使用
- **INFO**: 一般信息，记录重要的业务操作和状态变化
- **WARNING**: 警告信息，可能出现问题但不影响正常运行
- **ERROR**: 错误信息，记录异常和错误情况
- **CRITICAL**: 严重错误，系统级问题，可能导致服务不可用

### 结构化日志
```python
# 好的实践：包含足够的上下文信息
logger.info(
    "Message processed",
    extra={
        "message_id": msg.id,
        "topic": msg.topic,
        "processing_time": elapsed_time,
        "success": True
    }
)

# 避免的实践：信息不完整
logger.info("Message processed")
```

### 性能考虑
```python
# 好的实践：延迟字符串格式化
if logger.isEnabledFor(logging.DEBUG):
    logger.debug("Processing message: %s", expensive_to_string(message))

# 避免的实践：总是执行字符串格式化
logger.debug("Processing message: %s", expensive_to_string(message))
```

## 集成和监控

### 与日志系统集成
```python
# 可以与其他日志系统（如ELK、Splunk等）集成
import requests

def send_to_logstash(log_entry):
    requests.post("http://logstash:8080", json=log_entry)

# 自定义Handler将日志发送到外部系统
class LogstashHandler(logging.Handler):
    def emit(self, record):
        log_entry = self.format(record)
        send_to_logstash(log_entry)
```

### 日志监控
```python
# 监控错误日志频率
class ErrorMonitor:
    def __init__(self):
        self.error_count = 0
        self.error_threshold = 100  # 每分钟最多100个错误
    
    def check_errors(self, record):
        if record.levelno >= logging.ERROR:
            self.error_count += 1
            if self.error_count > self.error_threshold:
                # 发送告警
                send_alert("Too many errors detected")
```