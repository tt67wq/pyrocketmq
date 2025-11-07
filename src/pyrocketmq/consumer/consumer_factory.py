"""
Consumer便利函数模块

提供创建和配置消费者的便利函数，简化用户使用。

主要功能:
- create_consumer: 创建并发消费者
- create_consumer_with_config: 使用自定义配置创建消费者
- 便利函数用于快速创建常见配置的消费者

作者: pyrocketmq开发团队
"""

from typing import Any, Optional

from pyrocketmq.consumer import ConcurrentConsumer
from pyrocketmq.consumer.config import ConsumerConfig, create_consumer_config
from pyrocketmq.consumer.listener import MessageListenerConcurrently
from pyrocketmq.logging import get_logger

logger = get_logger(__name__)


def create_consumer(
    consumer_group: str,
    namesrv_addr: str,
    message_listener: Optional[MessageListenerConcurrently] = None,
    **kwargs: dict[str, Any],
) -> ConcurrentConsumer:
    """
    创建并发消费者的便利函数

    使用默认配置创建一个并发消费者实例，可以通过kwargs覆盖默认参数。

    Args:
        consumer_group: 消费者组名称
        namesrv_addr: NameServer地址
        message_listener: 可选的消息监听器
        **kwargs: 其他配置参数

    Returns:
        ConcurrentConsumer: 创建的消费者实例

    Examples:
        >>> # 基本使用
        >>> from pyrocketmq.consumer import create_consumer, MessageListenerConcurrently, ConsumeResult
        >>>
        >>> class MyListener(MessageListenerConcurrently):
        ...     def consume_message(self, messages, context):
        ...         for msg in messages:
        ...             print(f"Processing: {msg.body.decode()}")
        ...         return ConsumeResult.SUCCESS
        >>>
        >>> consumer = create_consumer("my_group", "localhost:9876", MyListener())
        >>> consumer.start()
        >>> consumer.subscribe("test_topic", create_tag_selector("*"))

        >>> # 使用自定义配置
        >>> consumer = create_consumer(
        ...     "my_group",
        ...     "localhost:9876",
        ...     consume_thread_max=20,
        ...     pull_batch_size=64,
        ...     message_listener=MyListener()
        ... )
    """
    try:
        # 创建配置
        config = create_consumer_config(consumer_group, namesrv_addr, **kwargs)

        # 创建消费者
        consumer = ConcurrentConsumer(config)

        # 注册消息监听器（如果提供）
        if message_listener:
            consumer.register_message_listener(message_listener)

        logger.info(
            "Consumer created successfully",
            extra={
                "consumer_group": consumer_group,
                "namesrv_addr": namesrv_addr,
                "has_listener": message_listener is not None,
                "config_overrides": list(kwargs.keys()),
            },
        )

        return consumer

    except Exception as e:
        logger.error(
            f"Failed to create consumer: {e}",
            extra={
                "consumer_group": consumer_group,
                "namesrv_addr": namesrv_addr,
                "error": str(e),
            },
            exc_info=True,
        )
        raise


def create_consumer_with_config(
    config: ConsumerConfig,
    message_listener: Optional[MessageListenerConcurrently] = None,
) -> ConcurrentConsumer:
    """
    使用现有配置创建消费者

    Args:
        config: 消费者配置
        message_listener: 可选的消息监听器

    Returns:
        ConcurrentConsumer: 创建的消费者实例

    Examples:
        >>> from pyrocketmq.consumer import create_consumer_config, create_consumer_with_config
        >>> from pyrocketmq.model import MessageModel
        >>>
        >>> config = create_consumer_config(
        ...     consumer_group="my_group",
        ...     namesrv_addr="localhost:9876",
        ...     message_model=MessageModel.CLUSTERING,
        ...     consume_thread_max=30
        ... )
        >>>
        >>> consumer = create_consumer_with_config(config, my_listener)
        >>> consumer.start()
    """
    try:
        if not config:
            raise ValueError("ConsumerConfig cannot be None")

        # 创建消费者
        consumer = ConcurrentConsumer(config)

        # 注册消息监听器（如果提供）
        if message_listener:
            consumer.register_message_listener(message_listener)

        logger.info(
            "Consumer created with custom config",
            extra={
                "consumer_group": config.consumer_group,
                "namesrv_addr": config.namesrv_addr,
                "message_model": config.message_model,
                "has_listener": message_listener is not None,
            },
        )

        return consumer

    except Exception as e:
        logger.error(
            f"Failed to create consumer with config: {e}",
            extra={
                "error": str(e),
            },
            exc_info=True,
        )
        raise


# 便利函数别名
def create_concurrent_consumer(
    consumer_group: str,
    namesrv_addr: str,
    message_listener: Optional[MessageListenerConcurrently] = None,
    **kwargs,
) -> ConcurrentConsumer:
    """
    创建并发消费者的别名函数

    这是为了向后兼容性和更明确的命名而提供的别名函数。
    功能与create_consumer完全相同。
    """
    return create_consumer(consumer_group, namesrv_addr, message_listener, **kwargs)
