"""
RocketMQ Consumer模块

提供完整的RocketMQ Consumer功能实现，支持集群消费、广播消费、
顺序消费等多种消费模式。采用MVP设计理念，从最简实现开始，
逐步扩展功能。

主要组件:
- ConsumerConfig: Consumer配置管理
- MessageListener: 消息监听器接口
- Consumer: 主Consumer类
- 异常体系: 完整的错误处理

使用示例:
    >>> from pyrocketmq.consumer import (
    ...     Consumer,
    ...     ConsumerConfig,
    ...     MessageListenerConcurrently,
    ...     ConsumeResult
    ... )
    >>>
    >>> # 创建配置
    >>> config = ConsumerConfig(
    ...     consumer_group="test_group",
    ...     namesrv_addr="localhost:9876"
    ... )
    >>>
    >>> # 创建监听器
    >>> class MyListener(MessageListenerConcurrently):
    ...     def consume_message_concurrently(self, messages, context):
    ...         for msg in messages:
    ...             print(f"收到消息: {msg.body.decode()}")
    ...         return ConsumeResult.SUCCESS
    >>>
    >>> # 创建并启动Consumer
    >>> consumer = Consumer(config)
    >>> consumer.register_message_listener(MyListener())
    >>> consumer.subscribe("test_topic", "*")
    >>> consumer.start()
    >>>
    >>> # 使用完成后关闭
    >>> consumer.shutdown()
"""

# 版本信息
__version__ = "1.0.0-MVP"
__author__ = "pyrocketmq团队"
__description__ = "RocketMQ Python Consumer实现 (MVP版本)"

# 核心类导入
from typing import Callable
from .config import (
    ConsumerConfig,
    MessageModel,
    ConsumeFromWhere,
    AllocateQueueStrategy,
    create_consumer_config,
    get_config,
    create_custom_config,
)
from pyrocketmq.model import (
    MessageExt,
)

from .listener import (
    MessageListener,
    MessageListenerOrderly,
    MessageListenerConcurrently,
    SimpleMessageListener,
    ConsumeResult,
    ConsumeContext,
    create_message_listener,
)

from .consumer import Consumer, create_consumer

# SubscriptionManager导入
from .subscription_manager import SubscriptionManager
from pyrocketmq.model import SubscriptionEntry, SubscriptionConflict
from .subscription_exceptions import (
    SubscriptionError,
    InvalidTopicError,
    InvalidSelectorError,
    TopicNotSubscribedError,
    SubscriptionConflictError,
    SubscriptionLimitExceededError,
    SubscriptionDataError,
)

# 异常类导入
from .errors import (
    # 基础异常
    ConsumerError,
    # 生命周期异常
    ConsumerStartError,
    ConsumerShutdownError,
    ConsumerStateError,
    # 订阅异常
    SubscribeError,
    UnsubscribeError,
    # 消息处理异常
    MessageConsumeError,
    MessagePullError,
    OffsetError,
    RebalanceError,
    # 网络和系统异常
    BrokerNotAvailableError,
    NameServerError,
    NetworkError,
    TimeoutError,
    # 配置和验证异常
    ConfigError,
    ValidationError,
    # 便利函数
    is_retriable_error,
    is_fatal_error,
)

# 公开接口
__all__ = [
    # 版本信息
    "__version__",
    "__author__",
    "__description__",
    # 配置相关
    "ConsumerConfig",
    "MessageModel",
    "ConsumeFromWhere",
    "AllocateQueueStrategy",
    "create_consumer_config",
    "get_config",
    "create_custom_config",
    # 监听器相关
    "MessageListener",
    "MessageListenerOrderly",
    "MessageListenerConcurrently",
    "SimpleMessageListener",
    "ConsumeResult",
    "ConsumeContext",
    "create_message_listener",
    # 核心Consumer
    "Consumer",
    "create_consumer",
    # SubscriptionManager
    "SubscriptionManager",
    "SubscriptionEntry",
    "SubscriptionConflict",
    # "SubscriptionMetrics",  # MVP版本暂不实现
    # 异常类
    "ConsumerError",
    "ConsumerStartError",
    "ConsumerShutdownError",
    "ConsumerStateError",
    "SubscribeError",
    "UnsubscribeError",
    "MessageConsumeError",
    "MessagePullError",
    "OffsetError",
    "RebalanceError",
    "BrokerNotAvailableError",
    "NameServerError",
    "NetworkError",
    "TimeoutError",
    "ConfigError",
    "ValidationError",
    "SubscriptionError",
    "InvalidTopicError",
    "InvalidSelectorError",
    "TopicNotSubscribedError",
    "SubscriptionConflictError",
    "SubscriptionLimitExceededError",
    "SubscriptionDataError",
    "is_retriable_error",
    "is_fatal_error",
]


def get_version() -> str:
    """获取Consumer模块版本

    Returns:
        版本字符串
    """
    return __version__


def create_simple_consumer(
    consumer_group: str,
    namesrv_addr: str,
    topic: str,
    message_handler: Callable[[list[MessageExt]], ConsumeResult],
    subscription: str = "*",
    **kwargs,
) -> Consumer:
    """
    创建简单Consumer的便利函数

    这个函数提供了最简化的Consumer创建方式，用户只需要提供
    基本参数和消息处理函数即可快速创建一个可用的Consumer。

    Args:
        consumer_group: 消费者组名称
        namesrv_addr: NameServer地址
        topic: 要订阅的Topic
        message_handler: 消息处理函数，签名为:
                        function(messages: list[MessageExt]) -> ConsumeResult
        subscription: 订阅表达式，默认"*"
        **kwargs: 其他配置参数

    Returns:
        已配置的Consumer实例

    Examples:
        >>> # 定义消息处理函数
        >>> def handle_messages(messages):
        ...     for msg in messages:
        ...         print(f"处理消息: {msg.body.decode()}")
        ...     return ConsumeResult.SUCCESS
        >>>
        >>> # 创建简单Consumer
        >>> consumer = create_simple_consumer(
        ...     "simple_group",
        ...     "localhost:9876",
        ...     "test_topic",
        ...     handle_messages
        ... )
        >>>
        >>> # 启动Consumer
        >>> consumer.start()
        >>>
        >>> # ... 使用完成后关闭
        >>> consumer.shutdown()
    """
    # 创建配置
    config = create_consumer_config(
        consumer_group=consumer_group, namesrv_addr=namesrv_addr, **kwargs
    )

    # 创建Consumer
    consumer = Consumer(config)

    # 创建简单监听器
    listener = create_message_listener(message_handler)

    # 注册监听器并订阅
    consumer.register_message_listener(listener)
    consumer.subscribe(topic, subscription)

    return consumer
