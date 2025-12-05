"""
统一消费者工厂模块

提供便利函数来创建各种类型的消费者实例，简化消费者的配置和创建过程。

主要功能：
- 创建同步/异步并发消费者
- 创建同步/异步顺序消费者
- 提供常用的配置模板（高性能、内存优化、环境相关）
- 自动注册监听器和订阅主题
- 提供启动便利函数

作者: pyrocketmq开发团队
"""

from typing import Any

from pyrocketmq.consumer import (
    AsyncConcurrentConsumer,
    AsyncMessageListener,
    ConcurrentConsumer,
)
from pyrocketmq.consumer.async_orderly_consumer import AsyncOrderlyConsumer
from pyrocketmq.consumer.config import ConsumerConfig, create_consumer_config
from pyrocketmq.consumer.orderly_consumer import OrderlyConsumer
from pyrocketmq.logging import get_logger
from pyrocketmq.model.client_data import create_tag_selector

logger = get_logger(__name__)


# ==================== 同步并发消费者工厂函数 ====================


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


# ==================== 同步顺序消费者工厂函数 ====================


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


# ==================== 异步并发消费者工厂函数 ====================


def create_async_concurrent_consumer(
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
        >>> from pyrocketmq.consumer import create_async_concurrent_consumer
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
        ...     consumer = await create_async_concurrent_consumer("my_group", "localhost:9876", message_listener=MyAsyncListener())
        ...     await consumer.start()
        ...     await consumer.subscribe("test_topic", "*")
        ...     # 保持消费者运行
        ...     await asyncio.sleep(60)
        ...     await consumer.shutdown()
        >>>
        >>> asyncio.run(main())

        >>> # 使用自定义配置
        >>> consumer = await create_async_concurrent_consumer(
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


async def create_async_consumer(
    consumer_group: str,
    namesrv_addr: str,
    message_listener: AsyncMessageListener,
    topic: str | None = None,
    selector: str = "*",
    **kwargs: Any,
) -> AsyncConcurrentConsumer:
    """创建异步并发消费者的便利函数（高级版本）。

    Args:
        consumer_group (str): 消费者组名称
        namesrv_addr (str): NameServer地址，格式如"localhost:9876"或"broker1:9876;broker2:9876"
        message_listener (AsyncMessageListener): 异步消息监听器
        topic (str, optional): 要订阅的主题名称，如果提供则会自动订阅
        selector (str, optional): 消息选择器表达式，默认为"*"表示订阅所有消息
        **kwargs: 其他配置参数，会传递给ConsumerConfig

    Returns:
        AsyncConcurrentConsumer: 配置完成的异步并发消费者实例

    Example:
        >>> from pyrocketmq.consumer.factory import create_async_consumer
        >>> from pyrocketmq.consumer.async_listener import AsyncMessageListener, ConsumeResult
        >>>
        >>> class MyListener(AsyncMessageListener):
        ...     async def consume_message(self, messages, context):
        ...         for msg in messages:
        ...             print(f"Processing: {msg.body.decode()}")
        ...         return ConsumeResult.SUCCESS
        >>>
        >>> # 创建并启动消费者
        >>> async def main():
        ...     consumer = await create_async_consumer(
        ...         consumer_group="my_group",
        ...         namesrv_addr="localhost:9876",
        ...         message_listener=MyListener(),
        ...         topic="test_topic"
        ...     )
        ...
        ...     await consumer.start()
        ...     # 运行一段时间...
        ...     await consumer.shutdown()

    Note:
        - 此函数是协程，需要在async上下文中调用
        - 如果提供了topic参数，会自动调用subscribe方法
        - 所有kwargs参数会传递给ConsumerConfig构造函数
    """
    try:
        # 创建消费者配置
        config = create_consumer_config(
            consumer_group=consumer_group, namesrv_addr=namesrv_addr, **kwargs
        )

        # 创建异步并发消费者
        consumer = AsyncConcurrentConsumer(config)

        # 如果提供了主题，自动订阅
        if topic:
            await consumer.subscribe(
                topic, create_tag_selector(selector), message_listener
            )
            logger.info(
                f"Auto-subscribed to topic: {topic}",
                extra={
                    "consumer_group": consumer_group,
                    "topic": topic,
                    "selector": selector,
                },
            )

        logger.info(
            "AsyncConcurrentConsumer created successfully",
            extra={
                "consumer_group": consumer_group,
                "namesrv_addr": namesrv_addr,
                "auto_subscribed_topic": topic if topic else None,
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


# ==================== 异步顺序消费者工厂函数 ====================


def create_async_orderly_consumer(
    consumer_group: str,
    namesrv_addr: str,
    **kwargs: Any,
) -> AsyncOrderlyConsumer:
    """创建异步顺序消费者的便利函数。

    使用默认配置创建一个异步顺序消费者实例，可以通过kwargs覆盖默认参数。
    异步顺序消费者保证同一个消息队列中的消息按照顺序被消费，并且所有操作都是异步的。

    Args:
        consumer_group (str): 消费者组名称，用于标识属于同一组的消费者实例
        namesrv_addr (str): NameServer地址，格式为"host:port"，用于获取路由信息
        **kwargs (Any): 其他可选配置参数，包括consume_thread_max、pull_batch_size等，
                       具体参数参考ConsumerConfig类定义

    Returns:
        AsyncOrderlyConsumer: 创建的异步顺序消费者实例，用于异步顺序消息消费

    Raises:
        ValueError: 当consumer_group或namesrv_addr为空或格式不正确时
        ConfigurationError: 当消费者配置参数无效时
        ConnectionError: 当无法连接到NameServer时

    Examples:
        >>> # 基本使用
        >>> from pyrocketmq.consumer import create_async_orderly_consumer
        >>> from pyrocketmq.consumer.listener import AsyncMessageListenerOrderly, ConsumeResult
        >>> import asyncio
        >>>
        >>> class MyAsyncOrderlyListener(AsyncMessageListenerOrderly):
        ...     async def consume_message_orderly(self, messages, context):
        ...         for msg in messages:
        ...             print(f"Processing: {msg.body.decode()}")
        ...             # 处理用户相关的消息，保证同一用户的消息顺序性
        ...         return ConsumeResult.CONSUME_SUCCESS
        >>>
        >>> async def main():
        ...     consumer = await create_async_orderly_consumer(
        ...         "my_async_orderly_group",
        ...         "localhost:9876",
        ...         message_listener=MyAsyncOrderlyListener()
        ...     )
        ...     await consumer.start()
        ...     await consumer.subscribe("user_topic", "*")
        ...     # 保持消费者运行
        ...     await asyncio.sleep(60)
        ...     await consumer.shutdown()
        >>>
        >>> asyncio.run(main())

        >>> # 使用自定义配置
        >>> consumer = await create_async_orderly_consumer(
        ...     "my_async_orderly_group",
        ...     "localhost:9876",
        ...     consume_thread_max=10,
        ...     pull_batch_size=32,
        ...     message_listener=MyAsyncOrderlyListener()
        ... )
    """
    try:
        # 创建配置
        config: ConsumerConfig = create_consumer_config(
            consumer_group, namesrv_addr, **kwargs
        )

        # 创建异步顺序消费者
        consumer: AsyncOrderlyConsumer = AsyncOrderlyConsumer(config)

        logger.info(
            "Async orderly consumer created successfully",
            extra={
                "consumer_group": consumer_group,
                "namesrv_addr": namesrv_addr,
                "config_overrides": list(kwargs.keys()),
            },
        )

        return consumer

    except Exception as e:
        logger.error(
            f"Failed to create async orderly consumer: {e}",
            extra={
                "consumer_group": consumer_group,
                "namesrv_addr": namesrv_addr,
                "error": str(e),
            },
            exc_info=True,
        )
        raise


async def create_async_orderly_consumer_with_subscription(
    consumer_group: str,
    namesrv_addr: str,
    message_listener: AsyncMessageListener,
    topic: str | None = None,
    selector: str = "*",
    **kwargs: Any,
) -> AsyncOrderlyConsumer:
    """创建异步顺序消费者的便利函数（高级版本，自动订阅）。

    Args:
        consumer_group (str): 消费者组名称
        namesrv_addr (str): NameServer地址，格式如"localhost:9876"或"broker1:9876;broker2:9876"
        message_listener (AsyncMessageListener): 异步消息监听器
        topic (str, optional): 要订阅的主题名称，如果提供则会自动订阅
        selector (str, optional): 消息选择器表达式，默认为"*"表示订阅所有消息
        **kwargs: 其他配置参数，会传递给ConsumerConfig

    Returns:
        AsyncOrderlyConsumer: 配置完成的异步顺序消费者实例

    Example:
        >>> from pyrocketmq.consumer.factory import create_async_orderly_consumer_with_subscription
        >>> from pyrocketmq.consumer.async_listener import AsyncMessageListener, ConsumeResult
        >>>
        >>> class MyOrderlyListener(AsyncMessageListener):
        ...     async def consume_message(self, messages, context):
        ...         for msg in messages:
        ...             print(f"Processing orderly: {msg.body.decode()}")
        ...         return ConsumeResult.SUCCESS
        >>>
        >>> # 创建并启动消费者
        >>> async def main():
        ...     consumer = await create_async_orderly_consumer_with_subscription(
        ...         consumer_group="orderly_group",
        ...         namesrv_addr="localhost:9876",
        ...         message_listener=MyOrderlyListener(),
        ...         topic="orderly_topic"
        ...     )
        ...
        ...     await consumer.start()
        ...     # 运行一段时间...
        ...     await consumer.shutdown()

    Note:
        - 此函数是协程，需要在async上下文中调用
        - 顺序消费者保证同一消息队列的消息按顺序处理
        - 如果提供了topic参数，会自动调用subscribe方法
        - 所有kwargs参数会传递给ConsumerConfig构造函数
    """
    try:
        # 创建消费者配置
        config = create_consumer_config(
            consumer_group=consumer_group, namesrv_addr=namesrv_addr, **kwargs
        )

        # 创建异步顺序消费者
        consumer = AsyncOrderlyConsumer(config)

        # 如果提供了主题，自动订阅并注册消息监听器
        if topic:
            await consumer.subscribe(
                topic, create_tag_selector(selector), message_listener
            )
            logger.info(
                f"Auto-subscribed to topic: {topic}",
                extra={
                    "consumer_group": consumer_group,
                    "topic": topic,
                    "selector": selector,
                    "consumer_type": "AsyncOrderlyConsumer",
                },
            )

        logger.info(
            "AsyncOrderlyConsumer created successfully",
            extra={
                "consumer_group": consumer_group,
                "namesrv_addr": namesrv_addr,
                "auto_subscribed_topic": topic if topic else None,
                "consumer_type": "AsyncOrderlyConsumer",
            },
        )

        return consumer

    except Exception as e:
        logger.error(
            f"Failed to create async orderly consumer: {e}",
            extra={
                "consumer_group": consumer_group,
                "namesrv_addr": namesrv_addr,
                "error": str(e),
                "consumer_type": "AsyncOrderlyConsumer",
            },
            exc_info=True,
        )
        raise


# ==================== 高性能异步消费者 ====================


async def create_high_performance_async_consumer(
    consumer_group: str,
    namesrv_addr: str,
    message_listener: AsyncMessageListener,
    topic: str | None = None,
    selector: str = "*",
    **kwargs: Any,
) -> AsyncConcurrentConsumer:
    """创建高性能异步并发消费者。

    使用优化的配置参数，提供更高的吞吐量和更低的延迟。

    Args:
        consumer_group (str): 消费者组名称
        namesrv_addr (str): NameServer地址
        message_listener (AsyncMessageListener): 异步消息监听器
        topic (str, optional): 要订阅的主题名称
        selector (str, optional): 消息选择器表达式
        **kwargs: 其他配置参数

    Returns:
        AsyncConcurrentConsumer: 高性能配置的异步并发消费者

    Example:
        >>> consumer = await create_high_performance_async_consumer(
        ...     consumer_group="high_performance_group",
        ...     namesrv_addr="localhost:9876",
        ...     message_listener=MyListener(),
        ...     topic="high_volume_topic"
        ... )
    """
    # 高性能配置参数
    high_performance_config = {
        "consume_thread_max": 50,  # 更高的并发处理线程数
        "pull_batch_size": 64,  # 更大的拉取批次
        "consume_batch_size": 16,  # 更���的消费批次
        "pull_interval": 0,  # 无间隔拉取，最大化吞吐量
        "max_cache_count_per_queue": 5000,  # 更大的缓存容量
        "max_cache_size_per_queue": 256,  # 更大的内存缓存
        "persist_interval": 10000,  # 较长的持久化间隔，减少IO
        **kwargs,  # 允许覆盖默认配置
    }

    return await create_async_consumer(
        consumer_group=consumer_group,
        namesrv_addr=namesrv_addr,
        message_listener=message_listener,
        topic=topic,
        selector=selector,
        **high_performance_config,
    )


async def create_high_performance_async_orderly_consumer(
    consumer_group: str,
    namesrv_addr: str,
    message_listener: AsyncMessageListener,
    topic: str | None = None,
    selector: str = "*",
    **kwargs: Any,
) -> AsyncOrderlyConsumer:
    """创建高性能异步顺序消费者。

    使用优化的配置参数，在保证消息顺序性的同时提供更高的吞吐量和更低的延迟。

    Args:
        consumer_group (str): 消费者组名称
        namesrv_addr (str): NameServer地址
        message_listener (AsyncMessageListener): 异步消息监听器
        topic (str, optional): 要订阅的主题名称
        selector (str, optional): 消息选择器表达式
        **kwargs: 其他配置参数

    Returns:
        AsyncOrderlyConsumer: 高性能配置的异步顺序消费者

    Example:
        >>> consumer = await create_high_performance_async_orderly_consumer(
        ...     consumer_group="high_performance_orderly_group",
        ...     namesrv_addr="localhost:9876",
        ...     message_listener=MyOrderlyListener(),
        ...     topic="orderly_high_volume_topic"
        ... )
    """
    # 高性能配置参数
    high_performance_config = {
        "consume_thread_min": 10,  # 较高的并发处理线程数，但考虑顺序性约束
        "consume_thread_max": 40,  # 较高的并发处理线程数，但考虑顺序性约束
        "pull_batch_size": 32,  # 适中的拉取批次，保证顺序性
        "consume_batch_size": 8,  # 适中的消费批次
        "pull_interval": 0,  # 无间隔拉取，最大化吞吐量
        "max_cache_count_per_queue": 2000,  # 较大的缓存容量
        "max_cache_size_per_queue": 128,  # 较大的内存缓存
        "persist_interval": 10000,  # 较长的持久化间隔，减少IO
        **kwargs,  # 允许覆盖默认配置
    }

    return await create_async_orderly_consumer_with_subscription(
        consumer_group=consumer_group,
        namesrv_addr=namesrv_addr,
        message_listener=message_listener,
        topic=topic,
        selector=selector,
        **high_performance_config,
    )


# ==================== 内存优化异步消费者 ====================


async def create_memory_optimized_async_consumer(
    consumer_group: str,
    namesrv_addr: str,
    message_listener: AsyncMessageListener,
    topic: str | None = None,
    selector: str = "*",
    **kwargs: Any,
) -> AsyncConcurrentConsumer:
    """创建内存优化的异步并发消费者。

    使用较小的内存占用配置，适合内存受限的环境。

    Args:
        consumer_group (str): 消费者组名称
        namesrv_addr (str): NameServer地址
        message_listener (AsyncMessageListener): 异步消息监听器
        topic (str, optional): 要订阅的主题名称
        selector (str, optional): 消息选择器表达式
        **kwargs: 其他配置参数

    Returns:
        AsyncConcurrentConsumer: 内存优化配置的异步并发消费者
    """
    # 内存优化配置参数
    memory_optimized_config = {
        "consume_thread_max": 5,  # 较少的并发线程
        "pull_batch_size": 8,  # 较小的拉取批次
        "consume_batch_size": 2,  # 较小的消费批次
        "pull_interval": 2000,  # 较长的拉取间隔
        "max_cache_count_per_queue": 100,  # 较小的缓存容量
        "max_cache_size_per_queue": 16,  # 较小的内存缓存
        "persist_interval": 5000,  # 较短的持久化间隔
        **kwargs,  # 允许覆盖默认配置
    }

    return await create_async_consumer(
        consumer_group=consumer_group,
        namesrv_addr=namesrv_addr,
        message_listener=message_listener,
        topic=topic,
        selector=selector,
        **memory_optimized_config,
    )


async def create_memory_optimized_async_orderly_consumer(
    consumer_group: str,
    namesrv_addr: str,
    message_listener: AsyncMessageListener,
    topic: str | None = None,
    selector: str = "*",
    **kwargs: Any,
) -> AsyncOrderlyConsumer:
    """创建内存优化的异步顺序消费者。

    使用较小的内存占用配置，在保证消息顺序性的同时适合内存受限的环境。

    Args:
        consumer_group (str): 消费者组名称
        namesrv_addr (str): NameServer地址
        message_listener (AsyncMessageListener): 异步消息监听器
        topic (str, optional): 要订阅的主题名称
        selector (str, optional): 消息选择器表达式
        **kwargs: 其他配置参数

    Returns:
        AsyncOrderlyConsumer: 内存优化配置的异步顺序消费者
    """
    # 内存优化配置参数
    memory_optimized_config = {
        "consume_thread_min": 1,  # 较少的并发线程，保证顺序性
        "consume_thread_max": 3,  # 较少的并发线程，保证顺序性
        "pull_batch_size": 4,  # 较小的拉取批次
        "consume_batch_size": 1,  # 单条消费，确保顺序处理
        "pull_interval": 3000,  # 较长的拉取间隔
        "max_cache_count_per_queue": 50,  # 较小的缓存容量
        "max_cache_size_per_queue": 8,  # 较小的内存缓存
        "persist_interval": 3000,  # 较短的持久化间隔
        **kwargs,  # 允许覆盖默认配置
    }

    return await create_async_orderly_consumer_with_subscription(
        consumer_group=consumer_group,
        namesrv_addr=namesrv_addr,
        message_listener=message_listener,
        topic=topic,
        selector=selector,
        **memory_optimized_config,
    )


# ==================== 便利启动函数 ====================


async def create_and_start_async_consumer(
    consumer_group: str,
    namesrv_addr: str,
    message_listener: AsyncMessageListener,
    topic: str | None = None,
    selector: str = "*",
    **kwargs: Any,
) -> AsyncConcurrentConsumer:
    """创建并启动异步并发消费者。

    这是一个便利函数，集成了创建和启动操作。

    Args:
        consumer_group (str): 消费者组名称
        namesrv_addr (str): NameServer地址
        message_listener (AsyncMessageListener): 异步消息监听器
        topic (str, optional): 要订阅的主题名称
        selector (str, optional): 消息选择器表达式
        **kwargs: 其他配置参数

    Returns:
        AsyncConcurrentConsumer: 已启动的异步并发消费者

    Raises:
        Exception: 当创建或启动失败时抛出

    Example:
        >>> # 创建并启动消费者，一行代码搞定
        >>> consumer = await create_and_start_async_consumer(
        ...     consumer_group="quick_group",
        ...     namesrv_addr="localhost:9876",
        ...     message_listener=MyListener(),
        ...     topic="test_topic"
        ... )
        >>>
        >>> # 消费者已经启动，可以立即消费消息
        >>> await asyncio.sleep(60)  # 运行60秒
        >>> await consumer.shutdown()
    """
    consumer = await create_async_consumer(
        consumer_group=consumer_group,
        namesrv_addr=namesrv_addr,
        message_listener=message_listener,
        topic=topic,
        selector=selector,
        **kwargs,
    )

    # 启动消费者
    await consumer.start()

    logger.info(
        "AsyncConcurrentConsumer created and started successfully",
        extra={
            "consumer_group": consumer_group,
            "auto_subscribed_topic": topic if topic else None,
        },
    )

    return consumer


async def create_and_start_async_orderly_consumer(
    consumer_group: str,
    namesrv_addr: str,
    message_listener: AsyncMessageListener,
    topic: str | None = None,
    selector: str = "*",
    **kwargs: Any,
) -> AsyncOrderlyConsumer:
    """创建并启动异步顺序消费者。

    这是一个便利函数，集成了创建和启动操作。

    Args:
        consumer_group (str): 消费者组名称
        namesrv_addr (str): NameServer地址
        message_listener (AsyncMessageListener): 异步消息监听器
        topic (str, optional): 要订阅的主题名称
        selector (str, optional): 消息选择器表达式
        **kwargs: 其他配置参数

    Returns:
        AsyncOrderlyConsumer: 已启动的异步顺序消费者

    Raises:
        Exception: 当创建或启动失败时抛出

    Example:
        >>> # 创建并启动顺序消费者，一行代码搞定
        >>> consumer = await create_and_start_async_orderly_consumer(
        ...     consumer_group="quick_orderly_group",
        ...     namesrv_addr="localhost:9876",
        ...     message_listener=MyOrderlyListener(),
        ...     topic="orderly_topic"
        ... )
        >>>
        >>> # 消费者已经启动，可以立即按顺序消费消息
        >>> await asyncio.sleep(60)  # 运行60秒
        >>> await consumer.shutdown()
    """
    consumer = await create_async_orderly_consumer_with_subscription(
        consumer_group=consumer_group,
        namesrv_addr=namesrv_addr,
        message_listener=message_listener,
        topic=topic,
        selector=selector,
        **kwargs,
    )

    # 启动消费者
    await consumer.start()

    logger.info(
        "AsyncOrderlyConsumer created and started successfully",
        extra={
            "consumer_group": consumer_group,
            "auto_subscribed_topic": topic if topic else None,
            "consumer_type": "AsyncOrderlyConsumer",
        },
    )

    return consumer


# ==================== 环境相关函数 ====================


async def create_environment_based_async_consumer(
    consumer_group: str,
    message_listener: AsyncMessageListener,
    topic: str | None = None,
    selector: str = "*",
    environment: str = "development",
    **kwargs: Any,
) -> AsyncConcurrentConsumer:
    """基于环境创建异步并发消费者。

    根据环境变量或指定环境自动选择合适的配置模板。

    Args:
        consumer_group (str): 消费者组名称
        message_listener (AsyncMessageListener): 异步消息监听器
        topic (str, optional): 要订阅的主题名称
        selector (str, optional): 消息选择器表达式
        environment (str): 环境名称，支持"development", "production", "testing"
        **kwargs: 其他配置参数

    Returns:
        AsyncConcurrentConsumer: 基于环境配置的异步并发消费者
    """
    import os

    # 从环境变量获取NameServer地址
    namesrv_addr = os.environ.get("ROCKETMQ_NAMESRV_ADDR", "localhost:9876")

    # 根据环境选择配置
    if environment == "production":
        return await create_high_performance_async_consumer(
            consumer_group=consumer_group,
            namesrv_addr=namesrv_addr,
            message_listener=message_listener,
            topic=topic,
            selector=selector,
            **kwargs,
        )
    elif environment == "testing":
        return await create_memory_optimized_async_consumer(
            consumer_group=consumer_group,
            namesrv_addr=namesrv_addr,
            message_listener=message_listener,
            topic=topic,
            selector=selector,
            **kwargs,
        )
    else:  # development
        return await create_async_consumer(
            consumer_group=consumer_group,
            namesrv_addr=namesrv_addr,
            message_listener=message_listener,
            topic=topic,
            selector=selector,
            **kwargs,
        )


async def create_environment_based_async_orderly_consumer(
    consumer_group: str,
    message_listener: AsyncMessageListener,
    topic: str | None = None,
    selector: str = "*",
    environment: str = "development",
    **kwargs: Any,
) -> AsyncOrderlyConsumer:
    """基于环境创建异步顺序消费者。

    根据环境变量或指定环境自动选择合适的配置模板。

    Args:
        consumer_group (str): 消费者组名称
        message_listener (AsyncMessageListener): 异步消息监听器
        topic (str, optional): 要订阅的主题名称
        selector (str, optional): 消息选择器表达式
        environment (str): 环境名称，支持"development", "production", "testing"
        **kwargs: 其他配置参数

    Returns:
        AsyncOrderlyConsumer: 基于环境配置的异步顺序消费者
    """
    import os

    # 从环境变量获取NameServer地址
    namesrv_addr = os.environ.get("ROCKETMQ_NAMESRV_ADDR", "localhost:9876")

    # 根据环境选择配置
    if environment == "production":
        return await create_high_performance_async_orderly_consumer(
            consumer_group=consumer_group,
            namesrv_addr=namesrv_addr,
            message_listener=message_listener,
            topic=topic,
            selector=selector,
            **kwargs,
        )
    elif environment == "testing":
        return await create_memory_optimized_async_orderly_consumer(
            consumer_group=consumer_group,
            namesrv_addr=namesrv_addr,
            message_listener=message_listener,
            topic=topic,
            selector=selector,
            **kwargs,
        )
    else:  # development
        return await create_async_orderly_consumer_with_subscription(
            consumer_group=consumer_group,
            namesrv_addr=namesrv_addr,
            message_listener=message_listener,
            topic=topic,
            selector=selector,
            **kwargs,
        )


# ==================== 废弃的函数（向后兼容） ====================


def create_async_consumer_deprecated(
    consumer_group: str,
    namesrv_addr: str,
    **kwargs: Any,
) -> AsyncConcurrentConsumer:
    """创建异步并发消费者的别名函数（已废弃，请使用create_async_concurrent_consumer）。

    这是为了向后兼容性而提供的别名函数，功能与create_async_concurrent_consumer完全相同。
    建议使用新的函数名create_async_concurrent_consumer。

    Args:
        consumer_group (str): 消费者组名称，用于标识属于同一组的消费者实例
        namesrv_addr (str): NameServer地址，格式为"host:port"，用于获取路由信息
        **kwargs (Any): 其他可选配置参数，包括consume_thread_max、pull_batch_size等，
                       具体参数参考ConsumerConfig类定义

    Returns:
        AsyncConcurrentConsumer: 创建的异步并发消费者实例，用于异步消息消费

    Deprecated:
        此函数已被废弃，请使用create_async_concurrent_consumer替代。
    """
    import warnings

    warnings.warn(
        "create_async_consumer_deprecated is deprecated, use create_async_concurrent_consumer instead",
        DeprecationWarning,
        stacklevel=2,
    )
    return create_async_concurrent_consumer(consumer_group, namesrv_addr, **kwargs)


# ==================== 便利别名 ====================

# 异步并发消费者便利别名
create_async_consumer_simple = create_async_consumer
create_async_consumer_fast = create_high_performance_async_consumer
create_async_consumer_light = create_memory_optimized_async_consumer
quick_start_async_consumer = create_and_start_async_consumer

# 异步顺序消费者便利别名
create_async_orderly_consumer_simple = create_async_orderly_consumer
create_async_orderly_consumer_fast = create_high_performance_async_orderly_consumer
create_async_orderly_consumer_light = create_memory_optimized_async_orderly_consumer
quick_start_async_orderly_consumer = create_and_start_async_orderly_consumer
