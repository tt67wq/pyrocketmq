#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import threading
from collections import deque
from threading import Lock
from typing import Deque

from pyrocketmq.broker import BrokerClient, BrokerManager, TopicBrokerMapping
from pyrocketmq.logging import get_logger
from pyrocketmq.model import (
    CONTENT_SPLITTER,
    Message,
    MessageQueue,
    SendMessageResult,
    TraceContext,
    TraceTransferBean,
)
from pyrocketmq.nameserver import NameServerManager, create_nameserver_manager
from pyrocketmq.queue_helper import (
    BrokerNotAvailableError,
    MessageRouter,
    QueueNotAvailableError,
    RouteNotFoundError,
)
from pyrocketmq.remote import ConnectionPool, RemoteConfig
from pyrocketmq.trace import TraceConfig
from pyrocketmq.transport import TransportConfig

RmqSysTraceTopic = "RMQ_SYS_TRACE_TOPIC"
TraceTopicPrefix = "rmq_sys_TRACE_DATA_"
TraceGroupName = "_INNER_TRACE_PRODUCER"
contentSplitter = CONTENT_SPLITTER  # 内容分隔符，对应Go版本的分隔符

logger = get_logger(__name__)


class TraceDispatcher:
    # ========================================================================
    # LIFECYCLE MANAGEMENT MODULE
    # 功能：管理 TraceDispatcher 的生命周期，包括初始化、启动和停止分发器及其后台线程
    # 包含函数：__init__, start, stop
    # ========================================================================

    def __init__(self, config: TraceConfig):
        """初始化跟踪分发器 (TraceDispatcher)。

        Args:
            config (TraceConfig): 跟踪配置对象，包含跟踪相关的配置参数
                如 NameServer 地址、批量大小、消息大小限制等
        """
        # 配置管理
        self._config: TraceConfig = config

        # 简化的状态管理
        self._running: bool = False

        # 核心组件
        self._topic_mapping: TopicBrokerMapping = TopicBrokerMapping()

        # 消息路由器
        self._message_router: MessageRouter = MessageRouter(
            topic_mapping=self._topic_mapping,
        )

        # NameServer连接管理
        self._nameserver_manager: NameServerManager = create_nameserver_manager(
            self._config.namesrv_addr
        )

        # Broker管理器（使用现有的连接池管理）
        transport_config = TransportConfig(
            host="localhost",  # 默认值，会被Broker覆盖
            port=10911,  # 默认值，会被Broker覆盖
        )
        remote_config = RemoteConfig()
        self._broker_manager: BrokerManager = BrokerManager(
            remote_config=remote_config,
            transport_config=transport_config,
        )

        # 消息队列管理
        self._queue: Deque[TraceContext] = deque()
        self._lock = Lock()  # 用于线程安全

        # 线程同步
        self._flush_thread = None
        self._stop_event = threading.Event()  # 停止事件
        self._flush_event = threading.Event()  # 刷新触发事件

    def start(self):
        """启动跟踪分发器。

        启动后台刷新线程，开始处理跟踪数据的分发。如果分发器已经在运行，
        则不会重复启动。刷新线程将以守护进程方式运行。

        Raises:
            RuntimeError: 当启动线程失败时可能抛出
        """
        if not self._running:
            self._running = True

            self._stop_event.clear()
            # 启动刷新线程
            self._flush_thread = threading.Thread(
                target=self._flush_worker,
                name="trace-dispatcher-flush-worker",
                daemon=True,
            )
            self._flush_thread.start()

    def stop(self):
        """停止跟踪分发器。

        停止后台刷新线程并清理资源。调用此方法后，分发器将不再处理
        新的跟踪数据。会等待刷新线程完成当前任务，最多等待5秒。

        Raises:
            RuntimeError: 当停止线程失败时可能抛出
        """
        if self._running:
            self._running = False
            # 设置停止事件
            self._stop_event.set()
            # 设置刷新事件以唤醒工作线程
            self._flush_event.set()

            # 等待刷新线程完成
            if self._flush_thread and self._flush_thread.is_alive():
                self._flush_thread.join(timeout=5.0)  # 最多等待5秒

    # ========================================================================
    # DATA PROCESSING MODULE
    # 功能：处理核心数据逻辑，包括队列管理、批处理和提交跟踪数据
    # 数据流：dispatch -> 队列 -> _flush_worker -> _commit -> 消息路由模块
    # 包含函数：dispatch, _flush_worker, _commit
    # ========================================================================

    def dispatch(self, ctx: TraceContext):
        """分发跟踪上下文到队列 (Queue)。

        将跟踪上下文添加到内部队列中等待处理。当队列中的数据量达到
        配置的最大批量大小时，会自动触发刷新操作。

        Args:
            ctx (TraceContext): 跟踪上下文对象，包含需要分发的跟踪数据

        Raises:
            RuntimeError: 当队列操作失败时可能抛出
        """
        with self._lock:
            # 将跟踪上下文添加到队列
            self._queue.append(ctx)

            # 检查队列大小，如果超过最大批量大小则触发刷新
            if len(self._queue) >= self._config.max_batch_size:
                # 触发刷新事件
                self._flush_event.set()

    def _flush_worker(self):
        """刷新工作线程的主循环。

        持续运行的工作线程，等待刷新事件或停止事件。当收到刷新事件时，
        执行数据提交操作；当收到停止事件时，退出线程。
        线程以守护进程模式运行。

        Raises:
            RuntimeError: 当线程操作异常时可能抛出
        """
        while not self._stop_event.is_set():
            # 等待刷新事件或停止事件
            self._flush_event.wait()

            # 如果收到停止信号，退出循环
            if self._stop_event.is_set():
                break

            # 清除刷新事件
            self._flush_event.clear()

            # 执行刷新操作
            self._commit()

    def _commit(self):
        """提交队列中的所有跟踪数据。

        从队列中取出所有待处理的跟踪上下文，按主题和区域ID进行分组，
        然后将分组后的数据批量发送到对应的 Broker。此操作是线程安全的。

        数据处理流程：
        1. 从队列取出所有数据并清空队列
        2. 按 topic 和 regionID 分组
        3. 对每个分组调用 _flush 方法（委托给消息路由模块）

        Raises:
            RouteNotFoundError: 当找不到消息路由时抛出
            QueueNotAvailableError: 当没有可用队列时抛出
            BrokerNotAvailableError: 当没有可用 Broker 时抛出
        """
        with self._lock:
            # 获取当前队列中的所有数据
            batch_ctx_list: list[TraceContext] = list(self._queue)
            # 清空队列
            self._queue.clear()

        # 预处理：按topic和regionID分组
        keyed_ctx: dict[str, list[TraceTransferBean]] = {}
        for ctx in batch_ctx_list:
            # 跳过没有trace beans的上下文
            if not ctx.trace_beans:
                continue

            # 获取topic和region ID
            topic = ctx.trace_beans[0].topic
            region_id = getattr(ctx, "region_id", "")

            # 构建key
            key: str = topic
            if region_id:
                key = f"{topic}{contentSplitter}{region_id}"

            # 将上下文转换为bean并添加到对应分组
            keyed_ctx.setdefault(key, []).append(ctx.marshal2bean())

        # 发送分组后的数据到远程 broker
        # 每个key对应一个独立的消息
        for key, ctx_list in keyed_ctx.items():
            arr: list[str] = key.split(contentSplitter)
            topic: str = key
            regionID: str = ""
            if len(arr) > 1:
                topic = arr[0]
                regionID = arr[1]

            self._flush(topic, regionID, ctx_list)

    # ========================================================================
    # MESSAGE ROUTING & SENDING MODULE
    # 功能：管理消息路由、Broker 选择和实际的消息传输
    # 依赖关系：依赖数据处理模块的 _commit 调用
    # 函数交互：_flush -> _send_trace_data_by_mq -> _send_message_to_broker
    #           update_route_info 被调用以维护路由信息
    # 包含函数：_flush, _send_trace_data_by_mq, _send_message_to_broker, update_route_info
    # ========================================================================

    def _flush(self, topic: str, regionID: str, data: list[TraceTransferBean]) -> None:
        """批量刷新跟踪数据到消息队列 (Message Queue)。

        将批量跟踪数据按照消息大小限制进行分割，确保每个消息不超过
        配置的最大消息大小。将分割后的数据发送到指定的主题和区域。

        处理流程：
        1. 遍历所有数据，收集传输键并拼接消息内容
        2. 当消息大小超过限制时分批发送
        3. 将最终的数据委托给 _send_trace_data_by_mq 处理

        Args:
            topic (str): 目标主题名称
            regionID (str): 区域标识符，可为空字符串
            data (list[TraceTransferBean]): 要发送的跟踪数据传输对象列表

        Raises:
            RouteNotFoundError: 当找不到消息路由时抛出
            QueueNotAvailableError: 当没有可用队列时抛出
            BrokerNotAvailableError: 当没有可用 Broker 时抛出
        """
        if not data:
            return

        # 使用set来存储所有传输键（对应Go版本的Keyset）
        keyset: set[str] = set()
        # 使用list来拼接字符串（Python中list拼接比+=更高效）
        builder: list[str] = []
        # 维护当前字符串总长度，避免重复计算
        current_size = 0
        flushed = True

        for bean in data:
            # 将所有传输键添加到keyset
            for key in bean.trans_key:
                keyset.add(key)

            # 计算新数据的大小
            new_data_size = len(bean.trans_data)

            # 如果添加新数据会超过限制，先发送已有数据
            if current_size + new_data_size > self._config.max_msg_size and not flushed:
                # 发送数据
                self._send_trace_data_by_mq(topic, keyset, regionID, "".join(builder))
                # 重置
                builder = []
                keyset = set()
                current_size = 0
                flushed = True

            # 将传输数据追加到builder
            builder.append(bean.trans_data)
            current_size += new_data_size
            flushed = False

        # 如果还有未发送的数据，发送它
        if not flushed:
            self._send_trace_data_by_mq(topic, keyset, regionID, "".join(builder))

    def _send_trace_data_by_mq(
        self, topic: str, keyset: set[str], region_id: str, data: str
    ):
        """通过消息队列 (MQ) 发送跟踪数据。

        构造消息对象并设置传输键，通过路由器选择目标队列和 Broker，
        然后将跟踪数据发送到选定的 Broker。

        处理流程：
        1. 构造 Message 对象并设置传输键
        2. 确保路由信息是最新的（调用 update_route_info）
        3. 通过消息路由器选择目标队列
        4. 委托给 _send_message_to_broker 实际发送消息

        Args:
            topic (str): 消息主题名称
            keyset (set[str]): 传输键集合，用于消息索引和检索
            region_id (str): 区域标识符，可为空字符串
            data (str): 要发送的跟踪数据内容

        Returns:
            None: 此方法不返回值，发送结果通过日志记录

        Raises:
            RouteNotFoundError: 当找不到消息路由时抛出
            QueueNotAvailableError: 当没有可用队列时抛出
            BrokerNotAvailableError: 当没有可用 Broker 地址时抛出
            ConnectionError: 当与 Broker 连接失败时抛出
        """
        message: Message = Message(topic=topic, body=data.encode())
        message.set_keys("".join(list(keyset)))

        if message.topic not in self._topic_mapping.get_all_topics():
            _ = self.update_route_info(message.topic)

        routing_result = self._message_router.route_message(message.topic, message)
        if not routing_result.success:
            raise RouteNotFoundError(
                f"Route not found for topic: {message.topic}, error: {routing_result.error}"
            )

        message_queue = routing_result.message_queue
        broker_data = routing_result.broker_data

        if not message_queue:
            raise QueueNotAvailableError(
                f"No available queue for topic: {message.topic}"
            )
        if not broker_data:
            raise BrokerNotAvailableError(
                f"No available broker data for topic: {message.topic}"
            )

        target_broker_addr = routing_result.broker_address
        if not target_broker_addr:
            raise BrokerNotAvailableError(
                f"No available broker address for topic: {message.topic}"
            )
        logger.debug(
            "Sending trace message to broker",
            extra={
                "broker_address": target_broker_addr,
                "queue": message_queue.full_name,
                "topic": message.topic,
            },
        )

        send_result: SendMessageResult = self._send_message_to_broker(
            message, target_broker_addr, message_queue
        )
        if not send_result.is_success:
            logger.error(
                "Failed to send trace message",
                extra={
                    "broker_address": target_broker_addr,
                    "queue": message_queue.full_name,
                    "topic": message.topic,
                },
            )

    def update_route_info(self, topic: str) -> bool:
        """更新指定主题的路由信息。

        从 NameServer 获取主题的路由数据，更新 Broker 连接池，
        并刷新本地路由缓存。如果更新失败，会尝试强制刷新缓存。

        此方法被 _send_trace_data_by_mq 调用以确保路由信息是最新的。

        Args:
            topic (str): 需要更新路由信息的主题名称

        Returns:
            bool: 路由信息更新是否成功
                - True: 更新成功或强制刷新成功
                - False: 更新失败且强制刷新也失败

        Raises:
            ConnectionError: 当无法连接到 NameServer 时抛出
        """
        topic_route_data = self._nameserver_manager.get_topic_route(topic)
        if not topic_route_data:
            logger.error(
                "Failed to get topic route data",
                extra={"topic": topic},
            )
            return False

        # 维护broker连接
        for broker_data in topic_route_data.broker_data_list:
            for idx, broker_addr in broker_data.broker_addresses.items():
                logger.info(
                    "Adding broker",
                    extra={
                        "broker_id": idx,
                        "broker_address": broker_addr,
                        "broker_name": broker_data.broker_name,
                    },
                )
                self._broker_manager.add_broker(
                    broker_addr,
                    broker_data.broker_name,
                )

        # 更新本地缓存
        success = self._topic_mapping.update_route_info(topic, topic_route_data)
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
        return self._topic_mapping.force_refresh(topic)

    def _send_message_to_broker(
        self, message: Message, broker_addr: str, message_queue: MessageQueue
    ) -> SendMessageResult:
        """发送消息到指定的 Broker。

        使用连接池获取与目标 Broker 的连接，通过 BrokerClient
        同步发送消息到指定的队列。

        此方法是实际的网络传输层，被 _send_trace_data_by_mq 调用。

        Args:
            message (Message): 要发送的消息对象，包含主题、内容和属性
            broker_addr (str): 目标 Broker 的网络地址
            message_queue (MessageQueue): 目标消息队列信息

        Returns:
            SendMessageResult: 消息发送结果对象，包含发送状态和相关信息

        Raises:
            ConnectionError: 当无法连接到 Broker 时抛出
            TimeoutError: 当发送消息超时时抛出
            BrokerNotAvailableError: 当 Broker 不可用时抛出
        """

        pool: ConnectionPool = self._broker_manager.must_connection_pool(broker_addr)
        with pool.get_connection() as broker_remote:
            return BrokerClient(broker_remote).sync_send_message(
                TraceGroupName,
                message.body,
                message_queue,
                message.properties,
                flag=message.flag,
            )
