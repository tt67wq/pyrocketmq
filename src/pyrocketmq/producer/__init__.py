"""
Producer模块

提供RocketMQ消息生产者的完整实现，包括：
- Producer核心实现 (MVP版本)
- Topic-Broker映射管理
- 消息路由和队列选择
- 生产者配置管理
- 异常处理

MVP版本特性:
- 简化的状态管理
- 核心消息发送功能
- 基础的队列选择策略
"""

from pyrocketmq.producer.config import (
    DEFAULT_CONFIG,
    DEVELOPMENT_CONFIG,
    HIGH_PERFORMANCE_CONFIG,
    PRODUCTION_CONFIG,
    TESTING_CONFIG,
    ProducerConfig,
    create_custom_config,
    get_config,
)
from pyrocketmq.producer.errors import (
    BrokerNotAvailableError,
    MessageSendError,
    ProducerError,
    ProducerStartError,
    ProducerStateError,
    QueueNotAvailableError,
    RouteNotFoundError,
    TimeoutError,
)
from pyrocketmq.producer.producer import Producer, SendResult, create_producer
from pyrocketmq.producer.queue_selectors import (
    MessageHashSelector,
    RandomSelector,
    RoundRobinSelector,
)
from pyrocketmq.producer.router import MessageRouter, RoutingStrategy
from pyrocketmq.producer.topic_broker_mapping import TopicBrokerMapping
from pyrocketmq.producer.utils import (
    calculate_message_size,
    generate_client_id,
    generate_message_id,
    parse_broker_address,
    validate_message,
)

__all__ = [
    # Core Producer (MVP)
    "Producer",
    "SendResult",
    "create_producer",
    # Config
    "ProducerConfig",
    "DEFAULT_CONFIG",
    "DEVELOPMENT_CONFIG",
    "PRODUCTION_CONFIG",
    "HIGH_PERFORMANCE_CONFIG",
    "TESTING_CONFIG",
    "get_config",
    "create_custom_config",
    # Core Classes
    "TopicBrokerMapping",
    "MessageRouter",
    # Queue Selectors
    "RoundRobinSelector",
    "RandomSelector",
    "MessageHashSelector",
    # Utils
    "generate_client_id",
    "validate_message",
    "calculate_message_size",
    "generate_message_id",
    "parse_broker_address",
    # Errors
    "ProducerError",
    "ProducerStartError",
    "ProducerStateError",
    "MessageSendError",
    "RouteNotFoundError",
    "BrokerNotAvailableError",
    "QueueNotAvailableError",
    "TimeoutError",
    # Constants
    "RoutingStrategy",
]
