"""
Consumer便利函数模块

提供创建和配置消费者的便利函数，简化用户使用。

主要功能:
- create_consumer: 创建并发消费者
- create_consumer_with_config: 使用自定义配置创建消费者
- create_message_selector: 创建消息选择器
- 便利函数用于快速创建常见配置的消费者

作者: pyrocketmq开发团队
"""

from typing import Any

from pyrocketmq.consumer import AsyncConcurrentConsumer, ConcurrentConsumer
from pyrocketmq.consumer.config import ConsumerConfig, create_consumer_config
from pyrocketmq.consumer.oredrly_consumer import OrderlyConsumer
from pyrocketmq.logging import get_logger

logger: Any = get_logger(__name__)


def create_consumer(
    consumer_group: str,
    namesrv_addr: str,
    **kwargs: Any,
) -> ConcurrentConsumer:
    """创建并发消费者的便利函数。

    使用默认配置创建一个并发消费者实例，可以通过kwargs覆盖默认参数。

    Args:
        consumer_group (str): 消费者组名称，用于标识属于同一组的消费者实例
        namesrv_addr (str): NameServer地址，格式为"host:port"，用于获取路由信息
        **kwargs (Any): 其他可选配置参数，包括consume_thread_max、pull_batch_size等，
                       具体参数参考ConsumerConfig类定义

    Returns:
        ConcurrentConsumer: 创建的并发消费者实例，用于消息消费

    Raises:
        ValueError: 当consumer_group或namesrv_addr为空或格式不正确时
        ConfigurationError: 当消费者配置参数无效时
        ConnectionError: 当无法连接到NameServer时

    Examples:
        >>> # 基本使用
        >>> from pyrocketmq.consumer import create_consumer
        >>> from pyrocketmq.consumer.listener import MessageListenerConcurrently, ConsumeResult
        >>>
        >>> class MyListener(MessageListenerConcurrently):
        ...     def consume_message_concurrently(self, messages, context):
        ...         for msg in messages:
        ...             print(f"Processing: {msg.body.decode()}")
        ...         return ConsumeResult.CONSUME_SUCCESS
        >>>
        >>> consumer = create_consumer("my_group", "localhost:9876", message_listener=MyListener())
        >>> consumer.start()
        >>> consumer.subscribe("test_topic", "*")

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
        config: ConsumerConfig = create_consumer_config(
            consumer_group, namesrv_addr, **kwargs
        )

        # 创建消费者
        consumer: ConcurrentConsumer = ConcurrentConsumer(config)

        logger.info(
            "Consumer created successfully",
            extra={
                "consumer_group": consumer_group,
                "namesrv_addr": namesrv_addr,
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


def create_orderly_consumer(
    consumer_group: str,
    namesrv_addr: str,
    **kwargs: Any,
) -> OrderlyConsumer:
    """创建顺序消费者的便利函数。

    使用默认配置创建一个顺序消费者实例，可以通过kwargs覆盖默认参数。
    顺序消费者保证同一个消息队列中的消息按照顺序被消费。

    Args:
        consumer_group (str): 消费者组名称，用于标识属于同一组的消费者实例
        namesrv_addr (str): NameServer地址，格式为"host:port"，用于获取路由信息
        **kwargs (Any): 其他可选配置参数，包括consume_thread_max、pull_batch_size等，
                       具体参数参考ConsumerConfig类定义

    Returns:
        OrderlyConsumer: 创建的顺序消费者实例，用于顺序消息消费

    Raises:
        ValueError: 当consumer_group或namesrv_addr为空或格式不正确时
        ConfigurationError: 当消费者配置参数无效时
        ConnectionError: 当无法连接到NameServer时

    Examples:
        >>> # 基本使用
        >>> from pyrocketmq.consumer import create_orderly_consumer
        >>> from pyrocketmq.consumer.listener import MessageListenerOrderly, ConsumeResult
        >>>
        >>> class MyOrderlyListener(MessageListenerOrderly):
        ...     def consume_message_orderly(self, messages, context):
        ...         for msg in messages:
        ...             print(f"Processing: {msg.body.decode()}")
        ...             # 处理用户相关的消息，保证同一用户的消息顺序性
        ...         return ConsumeResult.CONSUME_SUCCESS
        >>>
        >>> consumer = create_orderly_consumer("my_orderly_group", "localhost:9876",
        ...                                    message_listener=MyOrderlyListener())
        >>> consumer.start()
        >>> consumer.subscribe("user_topic", "*")

        >>> # 使用自定义配置
        >>> consumer = create_orderly_consumer(
        ...     "my_orderly_group",
        ...     "localhost:9876",
        ...     consume_thread_max=10,
        ...     pull_batch_size=32,
        ...     message_listener=MyOrderlyListener()
        ... )
    """
    try:
        # 创建配置
        config: ConsumerConfig = create_consumer_config(
            consumer_group, namesrv_addr, **kwargs
        )

        # 创建顺序消费者
        consumer: OrderlyConsumer = OrderlyConsumer(config)

        logger.info(
            "Orderly consumer created successfully",
            extra={
                "consumer_group": consumer_group,
                "namesrv_addr": namesrv_addr,
                "config_overrides": list(kwargs.keys()),
            },
        )

        return consumer

    except Exception as e:
        logger.error(
            f"Failed to create orderly consumer: {e}",
            extra={
                "consumer_group": consumer_group,
                "namesrv_addr": namesrv_addr,
                "error": str(e),
            },
            exc_info=True,
        )
        raise


def create_concurrent_consumer(
    consumer_group: str,
    namesrv_addr: str,
    **kwargs: Any,
) -> ConcurrentConsumer:
    """创建并发消费者的别名函数。

    这是为了向后兼容性和更明确的命名而提供的别名函数，功能与create_consumer完全相同。

    Args:
        consumer_group (str): 消费者组名称，用于标识属于同一组的消费者实例
        namesrv_addr (str): NameServer地址，格式为"host:port"，用于获取路由信息
        **kwargs (Any): 其他可选配置参数，包括consume_thread_max、pull_batch_size等，
                       具体参数参考ConsumerConfig类定义

    Returns:
        ConcurrentConsumer: 创建的并发消费者实例，用于消息消费

    Raises:
        ValueError: 当consumer_group或namesrv_addr为空或格式不正确时
        ConfigurationError: 当消费者配置参数无效时
        ConnectionError: 当无法连接到NameServer时
    """
    return create_consumer(consumer_group, namesrv_addr, **kwargs)


def create_async_consumer(
    consumer_group: str,
    namesrv_addr: str,
    **kwargs: Any,
) -> AsyncConcurrentConsumer:
    """创建异步并发消费者的便利函数。

    使用默认配置创建一个异步并发消费者实例，可以通过kwargs覆盖默认参数。

    Args:
        consumer_group (str): 消费者组名称，用于标识属于同一组的消费者实例
        namesrv_addr (str): NameServer地址，格式为"host:port"，用于获取路由信息
        **kwargs (Any): 其他可选配置参数，包括consume_thread_max、pull_batch_size等，
                       具体参数参考ConsumerConfig类定义

    Returns:
        AsyncConcurrentConsumer: 创建的异步并发消费者实例，用于异步消息消费

    Raises:
        ValueError: 当consumer_group或namesrv_addr为空或格式不正确时
        ConfigurationError: 当消费者配置参数无效时
        ConnectionError: 当无法连接到NameServer时

    Examples:
        >>> # 基本使用
        >>> from pyrocketmq.consumer import create_async_consumer
        >>> from pyrocketmq.consumer.listener import AsyncMessageListenerConcurrently, ConsumeResult
        >>> import asyncio
        >>>
        >>> class MyAsyncListener(AsyncMessageListenerConcurrently):
        ...     async def consume_message_concurrently(self, messages, context):
        ...         for msg in messages:
        ...             print(f"Processing: {msg.body.decode()}")
        ...         return ConsumeResult.CONSUME_SUCCESS
        >>>
        >>> async def main():
        ...     consumer = await create_async_consumer("my_group", "localhost:9876", message_listener=MyAsyncListener())
        ...     await consumer.start()
        ...     await consumer.subscribe("test_topic", "*")
        ...     # 保持消费者运行
        ...     await asyncio.sleep(60)
        ...     await consumer.shutdown()
        >>>
        >>> asyncio.run(main())

        >>> # 使用自定义配置
        >>> consumer = await create_async_consumer(
        ...     "my_group",
        ...     "localhost:9876",
        ...     message_listener=MyAsyncListener(),
        ...     consume_thread_max=20,
        ...     pull_batch_size=64
        ... )
    """
    try:
        # 创建配置
        config: ConsumerConfig = create_consumer_config(
            consumer_group, namesrv_addr, **kwargs
        )

        # 创建异步消费者
        consumer: AsyncConcurrentConsumer = AsyncConcurrentConsumer(config)

        logger.info(
            "Async consumer created successfully",
            extra={
                "consumer_group": consumer_group,
                "namesrv_addr": namesrv_addr,
                "config_overrides": list(kwargs.keys()),
            },
        )

        return consumer

    except Exception as e:
        logger.error(
            f"Failed to create async consumer: {e}",
            extra={
                "consumer_group": consumer_group,
                "namesrv_addr": namesrv_addr,
                "error": str(e),
            },
            exc_info=True,
        )
        raise
