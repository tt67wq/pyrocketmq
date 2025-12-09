"""
Producer模块

提供RocketMQ消息生产者的完整实现，包括：
- Producer核心实现 (MVP版本)
- TransactionProducer事务消息Producer
- Topic-Broker映射管理
- 消息路由和队列选择
- 生产者配置管理
- 异常处理

MVP版本特性:
- 简化的状态管理
- 核心消息发送功能
- 基础的队列选择策略
- 事务消息支持
"""

from pyrocketmq.producer.async_producer import (
    AsyncProducer,
    create_async_producer,
)
from pyrocketmq.producer.async_transactional_producer import (
    AsyncTransactionProducer,
    create_async_transaction_producer,
)
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
from pyrocketmq.producer.producer import Producer, create_producer
from pyrocketmq.producer.queue_selectors import (
    MessageHashSelector,
    RandomSelector,
    RoundRobinSelector,
)
from pyrocketmq.producer.router import MessageRouter, RoutingStrategy
from pyrocketmq.producer.transaction import (
    HalfMessageSendError,
    SimpleTransactionListener,
    TransactionCheckError,
    TransactionCommitError,
    TransactionError,
    TransactionListener,
    TransactionRollbackError,
    TransactionSendResult,
    TransactionState,
    TransactionStateError,
    TransactionTimeoutError,
    create_simple_transaction_listener,
    create_transaction_send_result,
    create_transaction_send_result_from_base,
)
from pyrocketmq.producer.transactional_producer import (
    TransactionProducer,
    create_transactional_producer,
)
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
    "AsyncProducer",
    "TransactionProducer",
    "create_producer",
    "create_async_producer",
    "create_transactional_producer",
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
    "MessageRouter",
    # Queue Selectors
    "RoundRobinSelector",
    "RandomSelector",
    "MessageHashSelector",
    # Transaction Support
    "TransactionListener",
    "SimpleTransactionListener",
    "TransactionSendResult",
    "TransactionState",
    "create_simple_transaction_listener",
    "create_transaction_send_result",
    "create_transaction_send_result_from_base",
    "AsyncTransactionProducer",
    "create_async_transaction_producer",
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
    # Transaction Errors
    "TransactionError",
    "TransactionTimeoutError",
    "TransactionCheckError",
    "TransactionStateError",
    "HalfMessageSendError",
    "TransactionCommitError",
    "TransactionRollbackError",
    # Constants
    "RoutingStrategy",
]
