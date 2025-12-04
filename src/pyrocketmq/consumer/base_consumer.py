"""
BaseConsumer - 消费者抽象基类

BaseConsumer是pyrocketmq消费者模块的核心抽象基类，定义了所有消费者的
通用接口和基础功能。它提供统一的配置管理、消息监听器注册、订阅管理等
核心功能，为具体消费者实现（如ConcurrentConsumer、OrderlyConsumer等）
提供坚实的基础。

文件结构说明:
本文件按照功能模块进行了重新组织，将实现相同或相似功能的函数集中到同一模块中，
确保代码逻辑清晰、结构合理，便于阅读和理解。

作者: pyrocketmq开发团队
"""

# 标准库导入
import threading
import time

# pyrocketmq导入
from pyrocketmq.broker import BrokerClient, BrokerManager
from pyrocketmq.consumer.allocate_queue_strategy import (
    AllocateQueueStrategyBase,
    AllocateQueueStrategyFactory,
)
from pyrocketmq.consumer.consume_from_where_manager import ConsumeFromWhereManager
from pyrocketmq.consumer.offset_store import OffsetStore
from pyrocketmq.consumer.offset_store_factory import OffsetStoreFactory
from pyrocketmq.consumer.topic_broker_mapping import ConsumerTopicBrokerMapping
from pyrocketmq.logging import get_logger
from pyrocketmq.model import (
    BrokerData,
    ConsumerData,
    ConsumeResult,
    ConsumerRunningInfo,
    HeartbeatData,
    MessageExt,
    MessageModel,
    MessageProperty,
    MessageQueue,
    MessageSelector,
    RemotingCommand,
)
from pyrocketmq.nameserver import NameServerManager, create_nameserver_manager
from pyrocketmq.remote import ConnectionPool, RemoteConfig

# 本地模块导入
from .config import ConsumerConfig
from .errors import ConsumerError, InvalidConsumeResultError
from .listener import ConsumeContext, MessageListener
from .stats_manager import StatsManager
from .subscription_manager import SubscriptionManager

logger = get_logger(__name__)


class BaseConsumer:
    """
    消费者抽象基类.

    定义了所有RocketMQ消费者的核心接口和基础功能。具体消费者实现
    （如并发消费者、顺序消费者等）需要继承这个类并实现其抽象方法.

    核心功能:
        - 配置管理: 统一的Consumer配置管理
        - 订阅管理: Topic订阅和消息选择器管理
        - 消息监听: 消息处理回调机制
        - 生命周期: 启动、停止等生命周期管理
        - 错误处理: 统一的异常处理和错误恢复

    设计原则:
        - 接口分离: 清晰定义各层职责
        - 可扩展性: 便于添加新的消费者类型
        - 线程安全: 所有公共操作保证线程安全
        - 资源管理: 完善的资源清理机制

    Attributes:
        _config: 消费者配置实例
        _subscription_manager: 订阅关系管理器
        _message_listeners: 消息监听器字典，支持每个topic对应一个监听器
        _is_running: 消费者运行状态标志
        _lock: 线程安全锁

    Example:
        >>> class MyConsumer(BaseConsumer):
        ...     def __init__(self, config):
        ...         super().__init__(config)
        ...         # 初始化特定资源
        ...
        ...     def start(self):
        ...         # 实现具体的启动逻辑
        ...         pass
        ...
        ...     def shutdown(self):
        ...         # 实现具体的停止逻辑
        ...         pass
    """

    def __init__(self, config: ConsumerConfig) -> None:
        """
        初始化BaseConsumer实例.

        创建消费者实例并初始化核心组件，包括订阅管理器、消息监听器存储等。
        验证配置的有效性并进行必要的初始化设置.

        Args:
            config: 消费者配置，包含所有必要的配置参数

        Raises:
            ValueError: 当config为None时抛出

        Note:
            初始化完成后，消费者处于停止状态，需要调用start()方法启动消费。
        """
        if not config:
            raise ValueError("ConsumerConfig cannot be None")

        # 验证消息模型
        if config.message_model not in [
            MessageModel.CLUSTERING,
            MessageModel.BROADCASTING,
        ]:
            raise ValueError(f"Unsupported message model: {self._config.message_model}")

        self._config: ConsumerConfig = config
        self._subscription_manager: SubscriptionManager = SubscriptionManager()
        # 改为支持多个listener，每个topic对应一个listener
        self._message_listeners: dict[str, MessageListener] = {}
        self._topic_broker_mapping: ConsumerTopicBrokerMapping = (
            ConsumerTopicBrokerMapping()
        )

        # 初始化核心组件
        self._name_server_manager: NameServerManager = create_nameserver_manager(
            self._config.namesrv_addr
        )
        self._broker_manager: BrokerManager = BrokerManager(
            RemoteConfig(connection_pool_size=32, connection_max_lifetime=30)
        )
        # 创建偏移量存储
        self._offset_store: OffsetStore = OffsetStoreFactory.create_offset_store(
            consumer_group=self._config.consumer_group,
            message_model=self._config.message_model,
            namesrv_manager=self._name_server_manager,
            broker_manager=self._broker_manager,
            persist_interval=self._config.persist_interval,
        )

        # 创建消费起始位置管理器
        self._consume_from_where_manager: ConsumeFromWhereManager = (
            ConsumeFromWhereManager(
                consume_group=self._config.consumer_group,
                namesrv_manager=self._name_server_manager,
                broker_manager=self._broker_manager,
            )
        )

        # 创建队列分配策略
        self._allocate_strategy: AllocateQueueStrategyBase = (
            AllocateQueueStrategyFactory.create_strategy(
                self._config.allocate_queue_strategy
            )
        )

        self._is_running: bool = False
        self._lock: threading.RLock = threading.RLock()

        # 统计管理器
        self._stats_manager: StatsManager = StatsManager()

        # 路由刷新定时任务
        self._route_refresh_interval: int = 30000  # 30秒刷新一次路由信息
        self._route_refresh_event: threading.Event = (
            threading.Event()
        )  # 用于路由刷新循环的事件
        self._route_refresh_thread: threading.Thread | None = None  # 路由刷新线程

        # 心跳任务管理
        self._heartbeat_interval: float = getattr(
            config, "heartbeat_interval", 10.0
        )  # 心跳间隔(秒)
        self._last_heartbeat_time: float = 0.0
        self._heartbeat_thread: threading.Thread | None = None
        self._heartbeat_event: threading.Event = threading.Event()  # 用于心跳循环的事件

        logger.info(
            "Initializing BaseConsumer",
            extra={
                "consumer_group": self._config.consumer_group,
                "client_id": self._config.client_id,
                "namesrv_addr": self._config.namesrv_addr,
            },
        )

    # ==============================================================================
    # 1. 核心生命周期管理模块
    # 功能：管理消费者的启动、停止和运行状态
    # 包含函数：start, shutdown, is_running, get_config
    # ==============================================================================

    def start(self) -> None:
        """
        启动消费者.

        启动消息消费的各个组件，包括：
        - 设置运行状态
        - 启动核心组件（NameServer、BrokerManager、OffsetStore）
        - 启动路由刷新任务
        - 启动心跳任务
        - 订阅重试主题（集群模式下）

        Raises:
            ConsumerError: 启动失败时抛出

        Note:
            启动前需要确保已注册消息监听器和订阅了必要的Topic。
        """
        self._is_running = True

        # 启动background任务
        self._name_server_manager.start()
        self._broker_manager.start()
        self._offset_store.start()

        # 启动路由刷新任务
        self._start_route_refresh_task()

        # 启动心跳任务
        self._start_heartbeat_task()

        # 订阅重试主题
        if self._config.message_model == MessageModel.CLUSTERING:
            self._subscribe_retry_topic()

    def shutdown(self) -> None:
        """
        停止消费者.

        优雅地停止消费者，包括：
        - 触发后台任务停止事件
        - 关闭线程池和专用线程
        - 清理所有资源

        Raises:
            ConsumerError: 停止过程中发生错误时抛出

        Note:
            这是一个阻塞操作，会等待所有正在处理的消息完成。
        """
        # 设置停止事件，通知后台线程退出
        self._route_refresh_event.set()
        self._heartbeat_event.set()

        # 关闭线程池和清理资源
        self._shutdown_thread_pools()
        self._cleanup_resources()

        # 关闭统计管理器
        self._stats_manager.shutdown()

    def is_running(self) -> bool:
        """
        检查消费者是否正在运行

        Returns:
            bool: 如果消费者正在运行返回True，否则返回False
        """
        return self._is_running

    def get_config(self) -> ConsumerConfig:
        """
        获取消费者配置

        Returns:
            ConsumerConfig: 消费者配置实例
        """
        return self._config

    # ==============================================================================
    # 2. 订阅管理模块
    # 功能：管理Topic订阅关系，包括订阅、取消订阅和查询订阅状态
    # 包含函数：subscribe, unsubscribe, get_subscribed_topics, is_subscribed
    # ==============================================================================

    def subscribe(
        self, topic: str, selector: MessageSelector, listener: MessageListener
    ) -> None:
        """
        订阅Topic并注册对应的消息监听器。

        将订阅topic和注册listener合并为一个方法。每个topic可以有独立的message listener，
        支持不同业务逻辑处理。

        Args:
            topic: 要订阅的Topic名称，不能为空字符串
            selector: 消息选择器，用于过滤消息，不能为None
            listener: 消息监听器，用于处理该topic的消息，不能为None

        Raises:
            ConsumerError: 当订阅失败时抛出
            ValueError: 当参数为空时抛出

        Example:
            >>> # 为订单topic注册专门的处理逻辑
            >>> from pyrocketmq.model.client_data import create_tag_selector
            >>> order_listener = OrderMessageListener()
            >>> consumer.subscribe_with_listener("order_topic", create_tag_selector("pay||ship"), order_listener)

        Note:
            - 每个topic只能有一个listener，重复注册会覆盖之前的listener
            - 支持为不同topic注册不同类型的listener（并发、顺序等）
        """
        if not topic or not topic.strip():
            raise ValueError("Topic cannot be empty")

        if not selector:
            raise ValueError("Message selector cannot be None")

        if not isinstance(listener, MessageListener):
            raise ValueError("Listener must be an instance of MessageListener")

        try:
            # 1. 注册到订阅管理器
            success = self._subscription_manager.subscribe(topic, selector)
            if not success:
                raise ConsumerError(
                    f"Failed to subscribe to topic: {topic}",
                    context={
                        "consumer_group": self._config.consumer_group,
                        "topic": topic,
                    },
                )

            # 2. 注册消息监听器
            self._message_listeners[topic] = listener

            logger.info(
                f"Successfully subscribed to topic with listener: {topic}",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "topic": topic,
                    "selector_type": type(selector).__name__,
                    "listener_type": type(listener).__name__,
                },
            )

        except Exception as e:
            logger.error(
                f"Failed to subscribe with listener for topic {topic}: {e}",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "topic": topic,
                    "error": str(e),
                },
                exc_info=True,
            )
            raise ConsumerError(
                f"Failed to subscribe to topic: {topic}",
                context={"consumer_group": self._config.consumer_group, "topic": topic},
                cause=e,
            ) from e

    def unsubscribe(self, topic: str) -> None:
        """
        取消订阅Topic.

        移除对指定Topic的订阅关系。取消订阅后，消费者将不再接收该Topic的消息。

        Args:
            topic: 要取消订阅的Topic名称，不能为空字符串

        Raises:
            ConsumerError: 当取消订阅失败时抛出
            ValueError: 当topic为空时抛出

        Example:
            >>> consumer.unsubscribe("order_topic")

        Note:
            如果Topic未订阅，此方法不会抛出异常。
        """
        if not topic:
            raise ValueError("Topic must be a non-empty string")

        try:
            logger.info(
                f"Unsubscribing from topic: {topic}",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "topic": topic,
                },
            )

            success: bool = self._subscription_manager.unsubscribe(topic)
            if success:
                # 同时清理消息监听器
                removed_listener = self._message_listeners.pop(topic, None)

                logger.info(
                    f"Successfully unsubscribed from topic: {topic}",
                    extra={
                        "consumer_group": self._config.consumer_group,
                        "topic": topic,
                        "total_subscriptions": self._subscription_manager.get_subscription_count(),
                        "listener_removed": removed_listener is not None,
                    },
                )
            else:
                logger.warning(
                    f"Topic not subscribed: {topic}",
                    extra={
                        "consumer_group": self._config.consumer_group,
                        "topic": topic,
                    },
                )

        except Exception as e:
            logger.error(
                f"Failed to unsubscribe from topic {topic}: {e}",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "topic": topic,
                    "error": str(e),
                },
                exc_info=True,
            )
            raise ConsumerError(
                "Unsubscription failed for topic",
                cause=e,
                context={
                    "consumer_group": self._config.consumer_group,
                    "topic": topic,
                },
            ) from e

    def get_subscribed_topics(self) -> set[str]:
        """
        获取已订阅的Topic列表.

        Returns:
            list[str]: 已订阅的Topic名称列表，如果没有订阅则返回空列表

        Example:
            >>> topics = consumer.get_subscribed_topics()
            >>> print(f"Subscribed topics: {topics}")
        """
        return self._subscription_manager.get_topics()

    def is_subscribed(self, topic: str) -> bool:
        """
        检查是否已订阅指定Topic.

        Args:
            topic: 要检查的Topic名称

        Returns:
            bool: 如果已订阅返回True，否则返回False

        Example:
            >>> if consumer.is_subscribed("order_topic"):
            ...     print("Already subscribed to order_topic")
        """
        return self._subscription_manager.is_subscribed(topic)

    # ==============================================================================
    # 3. 消息消费处理模块
    # 功能：处理消息消费的核心逻辑，包括并发消费、顺序消费和重试机制
    # 包含函数：_prepare_message_consumption, _concurrent_consume_message,
    #          _orderly_consume_message, check_reconsume_times,
    #          _filter_messages_by_tags, _send_back_message, _reset_retry
    # ==============================================================================

    def _prepare_message_consumption(
        self,
        messages: list[MessageExt],
        message_queue: MessageQueue,
        consumption_type: str = "concurrent",
    ) -> tuple[MessageListener | None, ConsumeContext | None]:
        """
        为消息消费做准备的通用方法。

        这个方法封装了消息验证、监听器选择和上下文创建的通用逻辑，
        被并发消费和顺序消费共同使用，避免代码重复。

        Args:
            messages (list[MessageExt]): 要处理的消息列表
            message_queue (MessageQueue): 消息来自的队列信息
            consumption_type (str): 消费类型，用于日志记录，可选值为 "concurrent" 或 "orderly"

        Returns:
            tuple[MessageListener | None, ConsumeContext]:
                - MessageListener: 找到的监听器，如果没有找到则为None
                - ConsumeContext: 创建的消费上下文

        Examples:
            >>> listener, context = self._prepare_message_consumption(messages, queue)
            >>> if listener:
            >>>     result = listener.consume_message(messages, context)
            >>> else:
            >>>     return False
        """
        # 消息验证
        if not messages:
            logger.warning(
                f"Empty message list received for {consumption_type} consumption",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "topic": message_queue.topic,
                    "queue_id": message_queue.queue_id,
                },
            )
            return None, None

        # 获取消息的topic
        topic = messages[0].topic if messages else message_queue.topic

        # 根据topic获取对应的监听器
        listener: MessageListener | None = self._message_listeners.get(topic)
        if not listener and self._is_retry_topic(topic):
            origin_topic: str | None = messages[0].get_property(
                MessageProperty.RETRY_TOPIC
            )
            listener = (
                self._message_listeners.get(origin_topic) if origin_topic else None
            )

        # 如果没有找到监听器，记录错误日志
        if not listener:
            logger.error(
                f"No message listener registered for topic: {topic} in {consumption_type} consumption",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "topic": topic,
                    "message_count": len(messages),
                    "queue_id": message_queue.queue_id,
                    "available_topics": list(self._message_listeners.keys()),
                },
            )

        # 创建消费上下文
        reconsume_times: int = messages[0].reconsume_times if messages else 0
        context: ConsumeContext = ConsumeContext(
            consumer_group=self._config.consumer_group,
            message_queue=message_queue,
            reconsume_times=reconsume_times,
        )

        return listener, context

    def _concurrent_consume_message(
        self, messages: list[MessageExt], message_queue: MessageQueue
    ) -> bool:
        """
        并发消费消息的核心处理方法。

        这是并发消费者消息处理的核心方法，根据消息的topic选择对应的监听器来处理消息。
        支持为不同topic配置不同的监听器，实现灵活的业务逻辑处理。同时支持重试主题的智能路由。

        Args:
            messages (list[MessageExt]): 要处理的消息列表，可以包含多条消息
            message_queue (MessageQueue): 消息来自的队列信息，包含topic、broker名称和队列ID

        Returns:
            bool: 消息处理是否成功
                - True: 消息处理成功（ConsumeResult.SUCCESS），可以更新偏移量
                - False: 消息处理失败或发生异常，消息将进入重试流程

        核心处理流程:
            1. **消息验证**: 检查消息列表是否为空（空列表视为处理成功）
            2. **监听器选择**: 通过通用方法获取对应的监听器和消费上下文
            3. **消息处理**: 调用监听器的consume_message方法处理消息
            4. **结果验证**: 验证消费结果的有效性（并发消费者只支持SUCCESS/RECONSUME_LATER）
            5. **异常处理**: 捕获处理异常并调用监听器的异常回调
            6. **日志记录**: 记录详细的处理过程和结果日志

        消费结果处理:
            - ConsumeResult.SUCCESS: 返回True，消息处理成功，可以更新消费偏移量
            - ConsumeResult.RECONSUME_LATER: 返回False，消息处理失败，将触发重试机制
            - 无效结果（COMMIT、ROLLBACK、SUSPEND_CURRENT_QUEUE_A_MOMENT）:
              记录错误日志，返回False，触发消息重试

        Examples:
            >>> # 该方法由消费者内部调用，通常不需要外部直接使用
            >>> success = self._concurrent_consume_message(messages, queue)
            >>> if success:
            >>>     # 消息处理成功，可以更新偏移量
            >>>     offset_store.update_offset(queue, next_offset)
            >>> else:
            >>>     # 消息处理失败，将触发重试机制
            >>>     failed_messages.extend(messages)

        Important Notes:
            - 这是ConcurrentConsumer的核心消息处理逻辑
            - 支持多监听器模式，实现不同topic的独立业务逻辑
            - 与OrderlyConsumer不同，不支持事务性的COMMIT/ROLLBACK操作
            - 适用于高并发、无序消息处理场景
            - 每次调用可能处理多条消息，监听器需要支持批量处理
            - 重试机制是自动的，无需手动管理重试队列
        """
        # 使用通用方法进行消息验证和监听器选择
        listener, context = self._prepare_message_consumption(
            messages, message_queue, "concurrent"
        )

        # 如果没有找到监听器或消息列表为空，抛出异常
        if listener is None or context is None:
            raise ValueError("Listener or context is None")

        # 获取topic用于日志记录
        topic = messages[0].topic if messages else message_queue.topic

        try:
            logger.debug(
                "Processing messages",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "topic": topic,
                    "message_count": len(messages),
                    "queue_id": context.queue_id,
                    "reconsume_times": context.reconsume_times,
                },
            )

            result: ConsumeResult = listener.consume_message(messages, context)
            if result in [
                ConsumeResult.COMMIT,
                ConsumeResult.ROLLBACK,
                ConsumeResult.SUSPEND_CURRENT_QUEUE_A_MOMENT,
            ]:
                logger.error(
                    "Invalid result for concurrent consumer",
                    extra={
                        "consumer_group": self._config.consumer_group,
                        "topic": topic,
                        "queue_id": context.queue_id,
                        "result": result.value,
                    },
                )
                # 抛出无效消费结果异常
                raise InvalidConsumeResultError(
                    consumer_type="concurrent",
                    invalid_result=result.value,
                    valid_results=[
                        ConsumeResult.SUCCESS.value,
                        ConsumeResult.RECONSUME_LATER.value,
                    ],
                    topic=topic,
                    queue_id=context.queue_id,
                )

            consume_duration = context.get_consume_duration()

            logger.info(
                "Message processing completed",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "topic": topic,
                    "message_count": len(messages),
                    "queue_id": context.queue_id,
                    "result": result.value,
                    "duration": consume_duration,
                },
            )

            return result == ConsumeResult.SUCCESS

        except InvalidConsumeResultError:
            raise

        except Exception as e:
            logger.error(
                f"Failed to process messages: {e}",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "topic": topic,
                    "message_count": len(messages),
                    "queue_id": context.queue_id,
                    "error": str(e),
                },
                exc_info=True,
            )

            # 调用异常回调（如果监听器实现了）
            if hasattr(listener, "on_exception"):
                try:
                    listener.on_exception(messages, context, e)
                except Exception as callback_error:
                    logger.error(
                        f"Exception callback failed: {callback_error}",
                        extra={
                            "consumer_group": self._config.consumer_group,
                            "topic": topic,
                            "error": str(callback_error),
                        },
                        exc_info=True,
                    )

            return False

    def _orderly_consume_message(
        self, messages: list[MessageExt], message_queue: MessageQueue
    ) -> tuple[bool, ConsumeResult]:
        """
        顺序消费消息的核心方法

        此方法负责按消息的队列偏移量顺序进行消费处理，保证同一消息队列中的消息
        按照严格的顺序被处理。这是RocketMQ顺序消费的核心实现，支持多种消费结果
        和异常处理机制。

        消费结果处理规则:
        - ConsumeResult.SUCCESS: 消费成功，提交偏移量
        - ConsumeResult.COMMIT: 消费成功，明确提交偏移量
        - ConsumeResult.ROLLBACK: 消费失败，回滚消息重新处理
        - ConsumeResult.SUSPEND_CURRENT_QUEUE_A_MOMENT: 消费失败，暂停当前队列一段时间
        - ConsumeResult.RECONSUME_LATER: 无效结果，抛出异常（顺序消费不支持）

        Args:
            messages (list[MessageExt]): 要消费的消息列表，保证按queue_offset排序
            message_queue (MessageQueue): 消息所属的队列信息，包含topic、brokerName、queueId

        Returns:
            tuple[bool, ConsumeResult]:
                - bool: 消费是否成功（True表示成功，False表示失败）
                - ConsumeResult: 具体的消费结果，用于后续处理决策

        Raises:
            ValueError: 当监听器或上下文为None时
            InvalidConsumeResultError: 当返回不支持的消费结果时

        处理流程:
        1. 通过_prepare_message_consumption准备消息消费上下文
        2. 调用监听器的consume_message方法进行实际消费
        3. 验证消费结果的有效性
        4. 处理消费过程中的异常情况
        5. 返回处理结果供上层调用者决策

        注意事项:
        - 顺序消费不支持RECONSUME_LATER结果，会抛出InvalidConsumeResultError
        - 异常情况下会尝试调用监听器的on_exception回调方法
        - 所有日志都包含结构化信息，便于问题诊断和监控
        - 返回的bool值表示整体处理成功与否，ConsumeResult提供具体结果信息

        Example:
            >>> success, result = consumer._orderly_consume_message(
            ...     messages, message_queue
            ... )
            >>> if success and result == ConsumeResult.SUCCESS:
            ...     # 消费成功，提交偏移量
            ...     pass
            >>> elif result == ConsumeResult.ROLLBACK:
            ...     # 需要回滚消息重新处理
            ...     pass
        """
        # 使用通用方法进行消息验证和监听器选择
        listener, context = self._prepare_message_consumption(
            messages, message_queue, "orderly"
        )

        # 如果没有找到监听器或消息列表为空，抛出异常
        if listener is None or context is None:
            raise ValueError("Listener or context is None")

        # 获取topic用于日志记录
        topic = messages[0].topic if messages else message_queue.topic

        try:
            logger.debug(
                "Processing messages orderly",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "topic": topic,
                    "message_count": len(messages),
                    "queue_id": context.queue_id,
                    "reconsume_times": context.reconsume_times,
                },
            )

            result: ConsumeResult = listener.consume_message(messages, context)

            if result == ConsumeResult.RECONSUME_LATER:
                logger.error(
                    "Invalid result for orderly consumer",
                    extra={
                        "consumer_group": self._config.consumer_group,
                        "topic": topic,
                        "queue_id": context.queue_id,
                        "result": result.value,
                    },
                )
                # 抛出无效消费结果异常
                raise InvalidConsumeResultError(
                    consumer_type="orderly",
                    invalid_result=result.value,
                    valid_results=[
                        ConsumeResult.SUCCESS.value,
                        ConsumeResult.COMMIT.value,
                        ConsumeResult.ROLLBACK.value,
                        ConsumeResult.SUSPEND_CURRENT_QUEUE_A_MOMENT.value,
                    ],
                    topic=topic,
                    queue_id=context.queue_id,
                )

            success = result in [
                ConsumeResult.SUCCESS,
                ConsumeResult.COMMIT,
            ]

            return success, result

        except InvalidConsumeResultError:
            raise

        except Exception as e:
            logger.error(
                f"Failed to process messages orderly: {e}",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "topic": topic,
                    "message_count": len(messages),
                    "queue_id": context.queue_id,
                    "error": str(e),
                },
                exc_info=True,
            )

            # 调用异常回调（如果监听器实现了）
            if hasattr(listener, "on_exception"):
                try:
                    listener.on_exception(messages, context, e)
                except Exception as callback_error:
                    logger.error(
                        f"Exception callback failed: {callback_error}",
                        extra={
                            "consumer_group": self._config.consumer_group,
                            "topic": topic,
                            "error": str(callback_error),
                        },
                        exc_info=True,
                    )

            return False, ConsumeResult.SUSPEND_CURRENT_QUEUE_A_MOMENT

    def check_reconsume_times(
        self, message_queue: MessageQueue, msgs: list[MessageExt]
    ) -> bool:
        """
        检查消息的重试次数并决定是否继续重试。

        这个方法是消息重试机制的核心，根据配置的最大重试次数来决定消息的处理方式：
        - 当消息重试次数超过最大限制时，尝试将消息发送到死信队列
        - 当消息重试次数未超过最大限制时，标记为需要继续重试
        - 无论如何都会增加消息的重试计数器

        Args:
            message_queue (MessageQueue): 消息队列对象，用于发送死信消息
            msgs (list[MessageExt]): 需要检查重试次数的消息列表

        Returns:
            bool: 返回是否需要暂停消费，True表示需要暂停重试，False表示可以继续消费

        Note:
            重试策略逻辑：
            1. 如果消息重试次数 > 最大重试次数：
               - 记录警告日志，包含消费者组、主题、消息ID等信息
               - 在消息属性中设置 RECONSUME_TIME 属性
               - 尝试将消息发送到死信队列（通过 _send_back_message）
               - 如果发送失败，标记为需要暂停重试
               - 无论成功与否，都增加重试计数器

            2. 如果消息重试次数 <= 最大重试次数：
               - 直接标记为需要暂停重试，等待下次重新消费
               - 增加重试计数器

        Warning:
            这个方法会修改消息对象的状态（msg.reconsume_times 和消息属性），
            调用者需要注意这个副作用。

        Example:
            >>> # 检查一批消息的重试状态
            >>> should_suspend = consumer.check_reconsume_times(message_queue, messages)
            >>> if should_suspend:
            >>>     logger.info("消息需要暂停消费，等待重试")
            >>> else:
            >>>     logger.info("消息处理成功，可以继续消费")
        """
        suspend: bool = False
        for msg in msgs:
            if msg.reconsume_times > self._config.max_reconsume_times:
                logger.warning(
                    "Message has exceeded max reconsume times",
                    extra={
                        "consumer_group": self._config.consumer_group,
                        "topic": msg.topic,
                        "msg_id": msg.msg_id,
                    },
                )
                msg.set_property(
                    MessageProperty.RECONSUME_TIME, str(msg.reconsume_times)
                )
                if not self._send_back_message(message_queue, msg):
                    suspend = True
                    msg.reconsume_times += 1
            else:
                suspend = True
                msg.reconsume_times += 1

        return suspend

    def _filter_messages_by_tags(
        self, messages: list[MessageExt], tags_set: list[str]
    ) -> list[MessageExt]:
        """
        根据标签过滤消息。

        Args:
            messages: 待过滤的消息列表
            tags_set: 允许的标签集合

        Returns:
            list[MessageExt]: 过滤后的消息列表
        """
        filtered_messages: list[MessageExt] = []
        for message in messages:
            if message.get_tags() in tags_set:
                filtered_messages.append(message)

        return filtered_messages

    def _send_back_message(
        self, message_queue: MessageQueue, message: MessageExt
    ) -> bool:
        """
        将消费失败的消息发送回broker重新消费。

        当消息消费失败时，此方法负责将消息发送回原始broker，
        以便后续重新消费。这是RocketMQ消息重试机制的重要组成部分。

        Args:
            message_queue (MessageQueue): 消息来自的队列信息
            message (MessageExt): 需要发送回的消息对象

        Returns:
            bool: 发送成功返回True，发送失败返回False

        处理流程:
            1. 根据队列信息获取目标broker地址
            2. 验证broker地址有效性
            3. 建立与broker的连接池
            4. 设置消息重试相关属性：
               - RETRY_TOPIC: 设置重试主题名
               - CONSUME_START_TIME: 记录消费开始时间
               - reconsume_times: 递增重试次数
            5. 调用broker的consumer_send_msg_back接口
            6. 记录处理结果和统计信息

        错误处理:
            - 如果无法获取broker地址，记录错误日志并返回False
            - 如果连接或发送失败，记录错误日志但不抛出异常
            - 确保消费循环的连续性，避免单个消息失败影响整体消费

        Examples:
            >>> # 在消费循环中处理失败消息
            >>> result = self._consume_message(messages, context)
            >>> if result == ConsumeResult.RECONSUME_LATER:
            >>>     for msg in messages:
            >>>         if not self._send_back_message(msg.queue, msg):
            >>>             logger.error(f"Failed to send back message: {msg.msg_id}")

        Note:
            - 该方法在消费失败时被调用，用于实现消息重试机制
            - 消息会被重新放入重试队列等待重新消费
            - 重试次数受max_reconsume_times配置限制，默认16次
            - 超过最大重试次数后，消息会进入死信队列(%DLQ%{consumer_group})
            - reconsume_times属性会递增，用于跟踪消息重试次数
            - 方法不会抛出异常，确保消费循环的稳定性
        """
        broker_addr = self._name_server_manager.get_broker_address(
            message_queue.broker_name
        )
        if not broker_addr:
            logger.error(
                "Failed to get broker address for message send back",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "broker_name": message_queue.broker_name,
                    "message_id": message.msg_id,
                    "topic": message.topic,
                    "queue_id": message.queue.queue_id if message.queue else 0,
                },
            )
            return False

        try:
            pool: ConnectionPool = self._broker_manager.must_connection_pool(
                broker_addr
            )
            with pool.get_connection(usage="发送消息回broker") as conn:
                self._reset_retry(message)
                message.reconsume_times += 1
                BrokerClient(conn).consumer_send_msg_back(
                    message,
                    message.reconsume_times,
                    self._config.consumer_group,
                    self._config.max_reconsume_times,
                )

                logger.debug(
                    "Message sent back to broker for reconsume",
                    extra={
                        "consumer_group": self._config.consumer_group,
                        "message_id": message.msg_id,
                        "topic": message.topic,
                        "queue_id": message.queue.queue_id if message.queue else 0,
                        "broker_name": message_queue.broker_name,
                        "reconsume_times": message.reconsume_times,
                        "max_reconsume_times": self._config.max_reconsume_times,
                    },
                )

        except Exception as e:
            logger.error(
                f"Failed to send message back to broker: {e}",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "message_id": message.msg_id,
                    "topic": message.topic,
                    "queue_id": message.queue.queue_id if message.queue else 0,
                    "broker_name": message_queue.broker_name,
                    "error": str(e),
                },
                exc_info=True,
            )
            return False
        else:
            return True

    def _reset_retry(self, msg: MessageExt) -> None:
        """
        重置消息的重试相关属性。

        当消息需要重新消费时，此方法负责重置或设置消息的重试相关属性，
        确保消息能够正确地参与重试机制。这通常在消息处理前或需要重新处理时调用。

        Args:
            msg (MessageExt): 需要重置重试属性的消息对象

        设置的属性:
            - RETRY_TOPIC: 检查并设置重试主题名（如果存在）
            - CONSUME_START_TIME: 设置消费开始时间，使用当前时间戳

        属性说明:
            RETRY_TOPIC:
                - 指示消息在消费失败时应该发送到的重试主题
                - 由消费者组名唯一确定，确保重试消息的隔离性
                - RocketMQ会根据该属性将失败消息投递到正确的重试主题
                - 格式为：%RETRY%{consumer_group}

            CONSUME_START_TIME:
                - 记录消息开始消费的时间戳（毫秒）
                - 用于监控消费延迟和性能分析
                - 帮助判断消息处理的耗时情况
                - 格式为Unix时间戳的毫秒表示

        执行逻辑:
            1. 检查消息是否包含RETRY_TOPIC属性
            2. 如果存在，将该属性值设置为消息的topic字段
            3. 设置当前时间戳作为CONSUME_START_TIME属性

        使用场景:
            - 消息处理前的属性初始化
            - 消息重新消费前的属性重置
            - 重试机制中的属性设置
            - 从重试队列中消费的消息处理

        Examples:
            >>> # 在消息处理前调用
            >>> message = MessageExt()
            >>> message.set_property("RETRY_TOPIC", "%RETRY%my_group")
            >>> self._reset_retry(message)
            >>> # 现在消息topic已更新为重试主题，并具备消费时间戳
            >>> print(f"Topic: {message.topic}")  # %RETRY%my_group
            >>> print(f"Start time: {message.get_property('CONSUME_START_TIME')}")

        重要注意事项:
            - 该方法只处理已有的RETRY_TOPIC属性，不会创建新的重试主题
            - 时间戳使用当前时刻，确保每次调用都更新为最新的消费开始时间
            - 重试主题的切换是RocketMQ重试机制的关键环节
            - 确保所有重试消息都具有一致的属性格式

        RocketMQ重试流程:
            1. 消费失败的消息调用_send_back_message发送回broker
            2. Broker将消息投递到对应的重试主题
            3. 消费者从重试主题拉取消息
            4. 调用_reset_retry将消息topic重置为重试主题
            5. 设置消费开始时间戳
            6. 再次尝试消费处理

        监控和调试:
            - CONSUME_START_TIME用于计算消息处理延迟
            - RETRY_TOPIC属性帮助追踪消息的重试路径
            - 时间戳精度为毫秒级，支持细粒度性能分析
            - 可通过消息属性查询重试次数和处理时长
        """
        retry_topic: str | None = msg.get_property(MessageProperty.RETRY_TOPIC)
        if retry_topic:
            msg.topic = retry_topic
        msg.set_property(
            MessageProperty.CONSUME_START_TIME, str(int(time.time() * 1000))
        )

    # ==============================================================================
    # 4. 监听器管理模块
    # 功能：管理消息监听器，包括获取监听器、监听器查询等
    # 包含函数：get_message_listener, get_all_listeners, get_subscription_manager
    # ==============================================================================

    def get_message_listener(self, topic: str | None = None) -> MessageListener | None:
        """
        获取指定topic或默认的消息监听器。

        Args:
            topic (str | None): 指定的topic名称，如果为None则返回默认监听器

        Returns:
            MessageListener | None: 对应的监听器，如果未找到则返回None

        Examples:
            >>> # 获取特定topic的监听器
            >>> order_listener = consumer.get_message_listener("order_topic")
            >>> # 获取默认监听器
            >>> default_listener = consumer.get_message_listener()
        """
        if topic:
            return self._message_listeners.get(topic)
        else:
            return getattr(self, "_message_listener", None)

    def get_all_listeners(self) -> dict[str, MessageListener]:
        """
        获取所有注册的消息监听器。

        Returns:
            dict[str, MessageListener]: topic到监听器的映射字典

        Examples:
            >>> listeners = consumer.get_all_listeners()
            >>> for topic, listener in listeners.items():
            >>>     print(f"Topic: {topic}, Listener: {type(listener).__name__}")
        """
        return self._message_listeners.copy()

    def get_subscription_manager(self) -> SubscriptionManager:
        """
        获取订阅管理器（主要用于内部使用和测试）

        Returns:
            SubscriptionManager: 订阅管理器实例
        """
        return self._subscription_manager

    def get_stats_manager(self) -> StatsManager:
        """
        获取统计管理器

        Returns:
            StatsManager: 统计管理器实例
        """
        return self._stats_manager

    def get_consume_status(self, topic: str):
        """
        获取指定主题的消费状态

        Args:
            topic: 主题名称

        Returns:
            ConsumeStatus: 消费状态信息
        """
        return self._stats_manager.get_consume_status(
            self._config.consumer_group, topic
        )

    def _on_notify_get_consumer_info(
        self, command: RemotingCommand, _addr: tuple[str, int]
    ) -> RemotingCommand:
        running_info: ConsumerRunningInfo = ConsumerRunningInfo()
        for sub in self._subscription_manager.get_all_subscriptions():
            running_info.add_subscription(sub.subscription_data)
            status = self._stats_manager.get_consume_status(
                self._config.consumer_group, sub.topic
            )
            running_info.add_status(sub.topic, status)

        return RemotingCommand(0)

    # ==============================================================================
    # 5. 路由信息管理模块
    # 功能：管理Topic路由信息，包括路由刷新、更新等
    # 包含函数：_start_route_refresh_task, _route_refresh_loop, _refresh_all_routes,
    #          _update_route_info
    # ==============================================================================

    def _start_route_refresh_task(self) -> None:
        """启动路由刷新任务"""
        self._route_refresh_thread = threading.Thread(
            target=self._route_refresh_loop,
            name=f"{self._config.consumer_group}-route-refresh-thread",
            daemon=True,
        )
        self._route_refresh_thread.start()

    def _route_refresh_loop(self) -> None:
        """路由刷新循环

        定期刷新所有订阅Topic的路由信息，确保消费者能够感知到集群拓扑的变化。
        """
        logger.info(
            "Route refresh loop started",
            extra={
                "consumer_group": self._config.consumer_group,
                "refresh_interval": self._route_refresh_interval,
            },
        )

        self._refresh_all_routes()

        while self._is_running:
            try:
                # 等待指定间隔或关闭事件
                if self._route_refresh_event.wait(
                    timeout=self._route_refresh_interval / 1000
                ):
                    # 收到事件信号，退出循环
                    break

                # 检查是否正在关闭
                if self._route_refresh_event.is_set():
                    break

                # 执行路由刷新
                self._refresh_all_routes()
                _ = self._topic_broker_mapping.clear_expired_routes()

                logger.debug(
                    "Route refresh completed",
                    extra={
                        "consumer_group": self._config.consumer_group,
                        "topics_count": len(
                            self._topic_broker_mapping.get_all_topics()
                        ),
                    },
                )

            except Exception as e:
                logger.warning(
                    f"Error in route refresh loop: {e}",
                    extra={
                        "consumer_group": self._config.consumer_group,
                        "error": str(e),
                    },
                    exc_info=True,
                )

                # 发生异常时，等待较短时间后重试
                self._route_refresh_event.wait(timeout=5.0)

        logger.info(
            "Route refresh loop stopped",
            extra={
                "consumer_group": self._config.consumer_group,
            },
        )

    def _refresh_all_routes(self) -> None:
        """刷新所有Topic的路由信息"""
        topics: set[str] = self._topic_broker_mapping.get_all_topics().union(
            self._subscription_manager.get_topics()
        )

        for topic in topics:
            try:
                if self._topic_broker_mapping.get_route_info(topic) is None:
                    _ = self._update_route_info(topic)
            except Exception as e:
                logger.debug(
                    "Failed to refresh route",
                    extra={
                        "topic": topic,
                        "error": str(e),
                    },
                )

    def _update_route_info(self, topic: str) -> bool:
        """更新Topic路由信息

        手动触发Topic路由信息的更新。通常情况下，路由信息会自动更新，
        但在某些特殊场景下可能需要手动触发更新。

        Args:
            topic: 要更新的Topic名称

        Returns:
            bool: 更新是否成功
        """
        logger.info(
            "Updating route info for topic",
            extra={
                "topic": topic,
            },
        )
        topic_route_data = self._name_server_manager.get_topic_route(topic)
        if not topic_route_data:
            logger.error(
                "Failed to get topic route data",
                extra={
                    "topic": topic,
                },
            )
            return False

        # 维护broker连接
        for broker_data in topic_route_data.broker_data_list:
            for _idx, broker_addr in broker_data.broker_addresses.items():
                self._broker_manager.add_broker(
                    broker_addr,
                    broker_data.broker_name,
                )

        # 更新本地缓存
        success = self._topic_broker_mapping.update_route_info(topic, topic_route_data)
        if success:
            logger.info(
                "Route info updated for topic",
                extra={
                    "topic": topic,
                    "brokers_count": len(topic_route_data.broker_data_list),
                },
            )
            return True
        else:
            logger.warning(
                "Failed to update route cache for topic",
                extra={"topic": topic},
            )

        # 如果所有NameServer都失败，强制刷新缓存
        return self._topic_broker_mapping.force_refresh(topic)

    # ==============================================================================
    # 6. 心跳管理模块
    # 功能：管理与Broker的心跳连接，维持消费者在线状态
    # 包含函数：_start_heartbeat_task, _heartbeat_loop, _collect_broker_addresses,
    #          _build_heartbeat_data, _send_heartbeat_to_broker,
    #          _update_heartbeat_statistics, _send_heartbeat_to_all_brokers
    # ==============================================================================

    def _start_heartbeat_task(self) -> None:
        """启动心跳任务"""
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop,
            name=f"{self._config.consumer_group}-heartbeat-thread",
            daemon=True,
        )
        self._heartbeat_thread.start()

    def _heartbeat_loop(self) -> None:
        """消费者心跳发送循环。

        定期向所有相关的Broker发送心跳包，维持消费者与Broker的连接状态。
        心跳机制确保Broker能够感知消费者的在线状态，及时进行重平衡操作。

        执行流程：
        1. 计算距离下次心跳的等待时间
        2. 使用事件等待机制，支持优雅停止
        3. 到达心跳时间时，向所有Broker发送心跳
        4. 记录心跳统计信息

        Args:
            None

        Returns:
            None

        Raises:
            None: 此方法会捕获所有异常并记录日志

        Note:
            - 默认心跳间隔为30秒，可通过配置调整
            - 使用Event.wait()替代time.sleep()，支持快速响应停止请求
            - 心跳失败不会影响消费者的正常运行
            - 统计心跳成功/失败次数用于监控连接健康状态
            - 消费者停止时会立即退出心跳循环
        """
        logger.info("Heartbeat loop started")

        self._send_heartbeat_to_all_brokers()
        self._last_heartbeat_time = time.time()
        while self._is_running:
            try:
                current_time = time.time()

                # 计算到下一次心跳的等待时间
                time_until_next_heartbeat: float = self._heartbeat_interval - (
                    current_time - self._last_heartbeat_time
                )

                # 如果还没到心跳时间，等待一小段时间或直到被唤醒
                if time_until_next_heartbeat > 0:
                    # 使用Event.wait()替代time.sleep()
                    wait_timeout: float = min(
                        time_until_next_heartbeat, 1.0
                    )  # 最多等待1秒
                    if self._heartbeat_event.wait(timeout=wait_timeout):
                        # Event被触发，检查是否需要退出
                        if not self._is_running:
                            break
                        # 重置事件状态
                        self._heartbeat_event.clear()
                        continue  # 重新计算等待时间

                # 检查是否需要发送心跳
                current_time = time.time()  # 重新获取当前时间
                if current_time - self._last_heartbeat_time >= self._heartbeat_interval:
                    self._send_heartbeat_to_all_brokers()
                    self._last_heartbeat_time = current_time

            except Exception as e:
                logger.error(
                    f"Error in heartbeat loop: {e}",
                    extra={
                        "consumer_group": self._config.consumer_group,
                        "error": str(e),
                    },
                    exc_info=True,
                )
                # 等待一段时间再重试，使用Event.wait()
                if self._heartbeat_event.wait(timeout=5.0):
                    if not self._is_running:
                        break
                    self._heartbeat_event.clear()

        logger.info("Heartbeat loop stopped")

    def _collect_broker_addresses(self) -> set[str]:
        """收集所有Broker地址

        Returns:
            Broker地址集合
        """
        broker_addrs: set[str] = set()
        all_topics: set[str] = self._topic_broker_mapping.get_all_topics().union(
            self._subscription_manager.get_topics()
        )

        if not all_topics:
            logger.warning("No topics found for heartbeat")
            return broker_addrs

        for topic in all_topics:
            brokers: list[BrokerData] = (
                self._topic_broker_mapping.get_available_brokers(topic)
            )
            for broker_data in brokers:
                # 获取主从地址
                if broker_data.broker_addresses:
                    for addr in broker_data.broker_addresses.values():
                        if addr:  # 过滤空地址
                            broker_addrs.add(addr)

        if not broker_addrs:
            logger.warning("No broker addresses found for heartbeat")

        return broker_addrs

    def _build_heartbeat_data(self) -> HeartbeatData:
        """构建心跳数据

        Returns:
            心跳数据对象
        """
        return HeartbeatData(
            client_id=self._config.client_id,
            consumer_data_set=[
                ConsumerData(
                    group_name=self._config.consumer_group,
                    consume_type="CONSUME_PASSIVELY",
                    message_model=self._config.message_model,
                    consume_from_where=self._config.consume_from_where,
                    subscription_data=[
                        e.subscription_data
                        for e in self._subscription_manager.get_all_subscriptions()
                    ],
                )
            ],
        )

    def _send_heartbeat_to_broker(
        self, broker_addr: str, heartbeat_data: HeartbeatData
    ) -> bool:
        """向单个Broker发送心跳

        Args:
            broker_addr: Broker地址
            heartbeat_data: 心跳数据

        Returns:
            是否发送成功
        """
        try:
            # 创建Broker客户端连接
            pool: ConnectionPool = self._broker_manager.must_connection_pool(
                broker_addr
            )
            with pool.get_connection(usage="发送心跳") as conn:
                # 发送心跳请求
                BrokerClient(conn).send_heartbeat(heartbeat_data)
            return True
        except Exception as e:
            logger.warning(
                f"Failed to send heartbeat to broker {broker_addr}: {e}",
                extra={
                    "broker_addr": broker_addr,
                    "consumer_group": self._config.consumer_group,
                    "error": str(e),
                },
            )
            return False

    def _update_heartbeat_statistics(
        self, success_count: int, failure_count: int, total_brokers: int
    ) -> None:
        """更新心跳统计信息

        Args:
            success_count: 成功次数
            failure_count: 失败次数
            total_brokers: 总Broker数量
        """
        if success_count > 0 or failure_count > 0:
            logger.info(
                f"Heartbeat summary: {success_count} success, {failure_count} failure",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "heartbeat_success_count": success_count,
                    "heartbeat_failure_count": failure_count,
                    "total_brokers": total_brokers,
                },
            )

    def _send_heartbeat_to_all_brokers(self) -> None:
        """向所有Broker发送心跳"""
        logger.debug("Sending heartbeat to all brokers...")

        try:
            # 收集所有Broker地址
            broker_addrs = self._collect_broker_addresses()
            if not broker_addrs:
                return

            # 构建心跳数据
            heartbeat_data = self._build_heartbeat_data()

            # 向每个Broker发送心跳
            heartbeat_success_count: int = 0
            heartbeat_failure_count: int = 0

            for broker_addr in broker_addrs:
                if self._send_heartbeat_to_broker(broker_addr, heartbeat_data):
                    heartbeat_success_count += 1
                else:
                    heartbeat_failure_count += 1

            # 更新统计信息
            self._update_heartbeat_statistics(
                heartbeat_success_count, heartbeat_failure_count, len(broker_addrs)
            )

        except Exception as e:
            logger.error(
                f"Error sending heartbeat to brokers: {e}",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "error": str(e),
                },
                exc_info=True,
            )

    # ==============================================================================
    # 7. 重试主题管理模块
    # 功能：管理重试主题的订阅和相关逻辑
    # 包含函数：_get_retry_topic, _is_retry_topic, _subscribe_retry_topic
    # ==============================================================================

    def _get_retry_topic(self) -> str:
        """
        获取消费者组对应的重试主题名称。

        在RocketMQ中，当消息消费失败时，系统会按照重试主题将消息重新投递给消费者。
        重试主题的命名规则为：%RETRY%{consumer_group}。

        Returns:
            str: 重试主题名称，格式为 %RETRY%{consumer_group}

        Examples:
            >>> retry_topic = self._get_retry_topic()
            >>> print(retry_topic)
            '%RETRY%order_consumer_group'

        Note:
            - 重试主题名前缀是固定的 %RETRY%
            - 重试主题使用消费者组名而不是原始主题名
            - 重试机制的消息会根据重试次数延迟投递
            - 默认重试次数为16次，超过后��息会进入死信队列
            - 每个消费者组都有自己独立的重试主题
        """
        return f"%RETRY%{self._config.consumer_group}"

    def _is_retry_topic(self, topic: str) -> bool:
        """
        判断指定主题是否是重试主题

        检查给定的主题名是否符合重试主题的命名规范。
        重试主题的格式为：%RETRY%+consumer_group

        Args:
            topic (str): 要检查的主题名

        Returns:
            bool: 如果是重试主题返回True，否则返回False

        Examples:
            >>> self._is_retry_topic("%RETRY%my_consumer_group")
            True
            >>> self._is_retry_topic("normal_topic")
            False
            >>> self._is_retry_topic("%DLQ%my_consumer_group")
            False
        """
        if not topic or not isinstance(topic, str):
            return False

        retry_topic_prefix = f"%RETRY%{self._config.consumer_group}"
        return topic == retry_topic_prefix

    def _subscribe_retry_topic(self) -> None:
        """
        订阅重试主题

        自动订阅该消费者组的重试主题，格式为：%RETRY%+consumer_group。
        重试主题用于接收消费失败需要重试的消息。

        Note:
            重试主题使用TAG选择器订阅所有消息（"*"），因为重试消息不需要额外过滤
        """
        retry_topic = self._get_retry_topic()

        try:
            from pyrocketmq.model.client_data import create_tag_selector

            # 创建订阅所有消息的选择器
            retry_selector = create_tag_selector("*")

            # 检查是否已经订阅了重试主题，避免重复订阅
            if not self._subscription_manager.is_subscribed(retry_topic):
                success = self._subscription_manager.subscribe(
                    retry_topic, retry_selector
                )

                if success:
                    logger.info(
                        f"Successfully subscribed to retry topic: {retry_topic}",
                        extra={
                            "consumer_group": self._config.consumer_group,
                            "retry_topic": retry_topic,
                            "max_retry_times": self._config.max_retry_times,
                        },
                    )
                else:
                    logger.warning(
                        f"Failed to subscribe to retry topic: {retry_topic}",
                        extra={
                            "consumer_group": self._config.consumer_group,
                            "retry_topic": retry_topic,
                        },
                    )
            else:
                logger.debug(
                    f"Retry topic already subscribed: {retry_topic}",
                    extra={
                        "consumer_group": self._config.consumer_group,
                        "retry_topic": retry_topic,
                    },
                )

        except Exception as e:
            logger.error(
                f"Error subscribing to retry topic {retry_topic}: {e}",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "retry_topic": retry_topic,
                    "error": str(e),
                },
                exc_info=True,
            )
            # 不抛出异常，重试主题订阅失败不应该影响消费者正常启动

    # ==============================================================================
    # 8. 资源清理模块
    # 功能：清理消费者资源，包括线程池关闭、组件清理等
    # 包含函数：_shutdown_thread_pools, _cleanup_resources
    # ==============================================================================

    def _shutdown_thread_pools(self) -> None:
        """
        关闭线程池和专用线程
        """
        try:
            # 等待专用线程结束
            self._route_refresh_event.set()  # 唤醒重平衡线程
            self._heartbeat_event.set()  # 唤醒心跳线程

            # 等待线程结束
            threads_to_join: list[threading.Thread] = []

            if self._heartbeat_thread and self._heartbeat_thread.is_alive():
                threads_to_join.append(self._heartbeat_thread)
            if self._route_refresh_thread and self._route_refresh_thread.is_alive():
                threads_to_join.append(self._route_refresh_thread)

            # 并发等待所有线程结束
            for thread in threads_to_join:
                try:
                    thread.join(timeout=5.0)
                    if thread.is_alive():
                        logger.warning(
                            f"Thread did not stop gracefully: {thread.name}",
                            extra={
                                "consumer_group": self._config.consumer_group,
                                "thread_name": thread.name,
                            },
                        )
                except Exception as e:
                    logger.error(
                        f"Error joining thread {thread.name}: {e}",
                        extra={
                            "consumer_group": self._config.consumer_group,
                            "thread_name": thread.name,
                            "error": str(e),
                        },
                    )

        except Exception as e:
            logger.warning(
                f"Error shutting down thread pools and threads: {e}",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "error": str(e),
                },
            )

    def _cleanup_resources(self) -> None:
        """
        清理资源（内部方法）

        由shutdown方法调用，负责清理消费者使用的所有资源。
        子类可以重写这个方法来添加特定的资源清理逻辑。

        注意:
            - 必须调用super()._cleanup_resources()来确保基类资源也被清理
            - 应该处理所有可能的异常，避免资源清理失败

        清理顺序：
        1. 偏移量存储 - 优先清理，确保持久化完成
        2. 订阅管理器 - 在偏移量存储之后清理
        3. Broker管理器 - 在依赖组件之后清理
        4. NameServer管理器 - 最后清理网络连接
        5. 重置运行状态
        """
        try:
            logger.info(
                "Cleaning up BaseConsumer resources",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "client_id": self._config.client_id,
                },
            )

            # 清理核心组件
            try:
                # 1. 清理偏移量存储 - 优先清理，确保持久化完成
                if hasattr(self, "_offset_store") and self._offset_store:
                    try:
                        # 尝试持久化未提交的偏移量
                        if hasattr(self._offset_store, "persist_all"):
                            self._offset_store.persist_all()
                        # 关闭偏移量存储
                        if hasattr(self._offset_store, "stop"):
                            self._offset_store.stop()

                        logger.info("OffsetStore cleaned up successfully")
                    except Exception as e:
                        logger.warning(f"Error cleaning up offset_store: {e}")

                # 2. 清理订阅管理器 - 在偏移量存储之后清理
                if (
                    hasattr(self, "_subscription_manager")
                    and self._subscription_manager
                ):
                    try:
                        # 保存订阅状态和清理非活跃订阅
                        if hasattr(
                            self._subscription_manager, "cleanup_inactive_subscriptions"
                        ):
                            self._subscription_manager.cleanup_inactive_subscriptions()

                        # 清理订阅数据
                        if hasattr(self._subscription_manager, "clear_all"):
                            self._subscription_manager.clear_all()

                        logger.info("SubscriptionManager cleaned up successfully")
                    except Exception as e:
                        logger.warning(f"Error cleaning up subscription_manager: {e}")

                # 3. 清理Topic到Broker的映射(Need not to)

                # 4. 清理Broker管理器 - 在依赖组件之后清理
                if hasattr(self, "_broker_manager") and self._broker_manager:
                    try:
                        self._broker_manager.shutdown()
                        logger.info("BrokerManager shutdown successfully")
                    except Exception as e:
                        logger.warning(f"Error shutting down broker_manager: {e}")

                # 5. 清理NameServer管理器 - 最后清理网络连接
                if hasattr(self, "_name_server_manager") and self._name_server_manager:
                    try:
                        self._name_server_manager.stop()
                        logger.info("NameServerManager stopped successfully")
                    except Exception as e:
                        logger.warning(f"Error stopping name_server_manager: {e}")

                # 6. 重置运行状态
                self._is_running = False

                logger.info("All core components cleaned up successfully")

            except Exception as cleanup_error:
                logger.error(f"Error during core components cleanup: {cleanup_error}")
                raise

            logger.info(
                "BaseConsumer resources cleaned up successfully",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "client_id": self._config.client_id,
                },
            )

        except Exception as e:
            logger.error(
                f"Error during BaseConsumer resource cleanup: {e}",
                extra={
                    "consumer_group": self._config.consumer_group,
                    "error": str(e),
                },
                exc_info=True,
            )

    # ==============================================================================
    # 9. 状态查询和监控模块（已移除统计相关方法）
    # 功能：该模块的统计相关方法已被移除
    # ==============================================================================

    # ==============================================================================
    # 10. 工具和辅助方法模块
    # 功能：提供字符串表示等辅助功能
    # 包含函数：__str__, __repr__
    # ==============================================================================

    def __str__(self) -> str:
        """
        返回消费者的字符串表示

        Returns:
            str: 消费者的简洁字符串表示
        """
        return (
            f"BaseConsumer["
            f"group={self._config.consumer_group}, "
            f"running={self._is_running}, "
            f"subscriptions={self._subscription_manager.get_subscription_count()}"
            f"]"
        )

    def __repr__(self) -> str:
        """
        返回消费者的详细字符串表示

        Returns:
            str: 消费者的详细字符串表示
        """
        status = "RUNNING" if self._is_running else "STOPPED"
        return (
            f"BaseConsumer("
            f"group='{self._config.consumer_group}', "
            f"client_id='{self._config.client_id}', "
            f"namesrv='{self._config.namesrv_addr}', "
            f"model='{self._config.message_model}', "
            f"status='{status}', "
            f"subscriptions={self._subscription_manager.get_subscription_count()}, "
            f"topic_listeners={len(self._message_listeners)}, "
            f"default_listener={'Registered' if getattr(self, '_message_listener', None) else 'None'}"
            f")"
        )
