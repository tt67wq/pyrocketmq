#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
from collections import deque
from types import CoroutineType
from typing import Any, Deque

from pyrocketmq.broker import (
    AsyncBrokerClient,
    AsyncBrokerManager,
    AsyncTopicBrokerMapping,
)
from pyrocketmq.logging import get_logger
from pyrocketmq.model import (
    CONTENT_SPLITTER,
    Message,
    MessageQueue,
    SendMessageResult,
    TraceContext,
    TraceTransferBean,
)
from pyrocketmq.nameserver import (
    AsyncNameServerManager,
    create_async_nameserver_manager,
)
from pyrocketmq.queue_helper import (
    AsyncMessageRouter,
    BrokerNotAvailableError,
    QueueNotAvailableError,
    RouteNotFoundError,
)
from pyrocketmq.remote import AsyncConnectionPool, RemoteConfig
from pyrocketmq.trace.config import TraceConfig
from pyrocketmq.transport import TransportConfig

RmqSysTraceTopic = "RMQ_SYS_TRACE_TOPIC"
TraceTopicPrefix = "rmq_sys_TRACE_DATA_"
TraceGroupName = "_INNER_TRACE_PRODUCER"
contentSplitter = CONTENT_SPLITTER  # 内容分隔符，对应Go版本的分隔符

logger = get_logger(__name__)


class AsyncTraceDispatcher:
    # ========================================================================
    # LIFECYCLE MANAGEMENT MODULE
    # 功能：管理 AsyncTraceDispatcher 的生命周期，包括初始化、启动和停止分发器及其后台任务
    # 包含函数：__init__, start, stop
    # ========================================================================

    def __init__(self, config: TraceConfig):
        """初始化异步跟踪分发器 (AsyncTraceDispatcher)。

        Args:
            config (TraceConfig): 跟踪配置对象，包含跟踪相关的配置参数
                如 NameServer 地址、批量大小、消息大小限制等
        """
        # 配置管理
        self._config: TraceConfig = config

        # 简化的状态管理
        self._running: bool = False

        # 核心组件
        self._topic_mapping: AsyncTopicBrokerMapping = AsyncTopicBrokerMapping()

        # 消息路由器
        self._message_router: AsyncMessageRouter = AsyncMessageRouter(
            topic_mapping=self._topic_mapping,
        )

        # NameServer连接管理
        self._nameserver_manager: AsyncNameServerManager = (
            create_async_nameserver_manager(self._config.namesrv_addr)
        )

        # Broker管理器（使用异步连接池管理）
        transport_config = TransportConfig(
            host="localhost",  # 默认值，会被Broker覆盖
            port=10911,  # 默认值，会被Broker覆盖
        )
        remote_config = RemoteConfig()
        self._broker_manager: AsyncBrokerManager = AsyncBrokerManager(
            remote_config=remote_config,
            transport_config=transport_config,
        )

        # 消息队列管理 - 使用线程安全的队列
        self._queue: Deque[TraceContext] = deque()
        self._queue_lock = asyncio.Lock()  # 用于异步队列安全

        # 异步任务管理
        self._flush_task = None
        self._stop_event = asyncio.Event()  # 停止事件
        self._flush_event = asyncio.Event()  # 刷新触发事件

        # 路由刷新定时任务
        self._route_refresh_interval: int = 30000  # 30秒刷新一次路由信息
        self._route_refresh_task: asyncio.Task[None] | None = None  # 路由刷新任务

    async def start(self):
        """启动异步跟踪分发器。

        启动 NameServer 管理器和 Broker 管理器，然后启动后台刷新任务，
        开始处理跟踪数据的分发。如果分发器已经在运行，则不会重复启动。
        刷新任务将以异步任务方式运行。

        Raises:
            RuntimeError: 当启动组件失败时可能抛出
        """
        if not self._running:
            self._running = True

            # 启动 NameServer 管理器
            await self._nameserver_manager.start()

            # 启动 Broker 管理器
            await self._broker_manager.start()

            self._stop_event.clear()
            # 启动刷新任务
            self._flush_task = asyncio.create_task(
                self._flush_worker(),
                name="trace-dispatcher-flush-worker",
            )

            # 启动路由刷新任务
            self._route_refresh_task = asyncio.create_task(
                self._route_refresh_loop(),
                name="trace-dispatcher-route-refresh-loop",
            )

    async def stop(self):
        """停止异步跟踪分发器并回收资源。

        停止后台刷新任务、NameServer 管理器和 Broker 管理器，
        并清理所有相关资源。调用此方法后，分发器将不再处理
        新的跟踪数据。会等待刷新任务完成当前任务，最多等待5秒。

        Raises:
            RuntimeError: 当停止组件失败时可能抛出
        """
        if self._running:
            self._running = False
            # 设置停止事件
            self._stop_event.set()
            # 设置刷新事件以唤醒工作任务
            self._flush_event.set()

            # 等待刷新任务完成
            if self._flush_task and not self._flush_task.done():
                try:
                    await asyncio.wait_for(self._flush_task, timeout=5.0)  # 最多等待5秒
                except asyncio.TimeoutError:
                    self._flush_task.cancel()
                    try:
                        await self._flush_task
                    except asyncio.CancelledError:
                        pass

            # 等待路由刷新任务完成
            if self._route_refresh_task and not self._route_refresh_task.done():
                try:
                    await asyncio.wait_for(
                        self._route_refresh_task, timeout=5.0
                    )  # 最多等待5秒
                except asyncio.TimeoutError:
                    self._route_refresh_task.cancel()
                    try:
                        await self._route_refresh_task
                    except asyncio.CancelledError:
                        pass

            # 清空队列中的剩余数据
            async with self._queue_lock:
                self._queue.clear()

            # 停止 Broker 管理器并回收连接资源
            await self._broker_manager.shutdown()

            # 停止 NameServer 管理器并回收连接资源
            await self._nameserver_manager.stop()

            # 清空任务引用
            self._flush_task = None
            self._route_refresh_task = None

    # ========================================================================
    # DATA PROCESSING MODULE
    # 功能：处理核心数据逻辑，包括队列管理、批处理和提交跟踪数据
    # 数据流：dispatch -> 队列 -> _flush_worker -> _commit -> 消息路由模块
    # 包含函数：dispatch, _flush_worker, _commit
    # ========================================================================

    async def dispatch(self, ctx: TraceContext):
        """分发跟踪上下文到异步队列 (Queue)。

        将跟踪上下文添加到内部队列中等待处理。当队列中的数据量达到
        配置的最大批量大小时，会自动触发刷新操作。

        Args:
            ctx (TraceContext): 跟踪上下文对象，包含需要分发的跟踪数据

        Raises:
            RuntimeError: 当队列操作失败时可能抛出
        """
        async with self._queue_lock:
            # 将跟踪上下文添加到队列
            self._queue.append(ctx)

            # 检查队列大小，如果超过最大批量大小则触发刷新
            if len(self._queue) >= self._config.max_batch_size:
                # 触发刷新事件
                self._flush_event.set()

    async def _flush_worker(self):
        """刷新工作异步任务的主循环。

        持续运行的异步任务，等待刷新事件或停止事件。当收到刷新事件时，
        执行数据提交操作；当收到停止事件时，退出任务。
        任务以异步方式运行。

        Raises:
            RuntimeError: 当任务操作异常时可能抛出
        """
        while not self._stop_event.is_set():
            # 等待刷新事件或停止事件
            await asyncio.wait(
                [
                    asyncio.create_task(self._flush_event.wait()),
                    asyncio.create_task(self._stop_event.wait()),
                ],
                return_when=asyncio.FIRST_COMPLETED,
            )

            # 如果收到停止信号，退出循环
            if self._stop_event.is_set():
                break

            # 清除刷新事件
            self._flush_event.clear()

            # 执行刷新操作
            await self._commit()

    async def _commit(self):
        """异步提交队列中的所有跟踪数据。

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
        async with self._queue_lock:
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

        # 并发发送分组后的数据到远程 broker
        # 使用 asyncio.gather 实现并发发送
        tasks: list[CoroutineType[Any, Any, None]] = []
        for key, ctx_list in keyed_ctx.items():
            arr: list[str] = key.split(contentSplitter)
            topic: str = key
            regionID: str = ""
            if len(arr) > 1:
                topic = arr[0]
                regionID = arr[1]

            tasks.append(self._flush(topic, regionID, ctx_list))

        # 并发执行所有刷新任务
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    # ========================================================================
    # MESSAGE ROUTING & SENDING MODULE
    # 功能：管理消息路由、Broker 选择和实际的消息传输
    # 依赖关系：依赖数据处理模块的 _commit 调用
    # 函数交互：_flush -> _send_trace_data_by_mq -> _send_message_to_broker
    #           update_route_info 被调用以维护路由信息
    # 包含函数：_flush, _send_trace_data_by_mq, _send_message_to_broker, update_route_info
    # ========================================================================

    async def _flush(
        self, topic: str, regionID: str, data: list[TraceTransferBean]
    ) -> None:
        """异步批量刷新跟踪数据到消息队列 (Message Queue)。

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
                # 异步发送数据
                try:
                    await self._send_trace_data_by_mq(
                        keyset, regionID, "".join(builder)
                    )
                except Exception as e:
                    logger.error(f"Failed to send trace data: {e}")
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
        if not flushed and keyset:
            try:
                await self._send_trace_data_by_mq(keyset, regionID, "".join(builder))
            except Exception as e:
                logger.error(f"Failed to send trace data: {e}")

    async def _send_trace_data_by_mq(self, keyset: set[str], region_id: str, data: str):
        """通过消息队列 (MQ) 异步发送跟踪数据。

        构造消息对象并设置传输键，通过路由器选择目标队列和 Broker，
        然后将跟踪数据发送到选定的 Broker。

        处理流程：
        1. 构造 Message 对象并设置传输键
        2. 确保路由信息是最新的（调用 update_route_info）
        3. 通过消息路由器选择目标队列
        4. 委托给 _send_message_to_broker 实际发送消息

        Args:
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
        trace_topic = self._config.trace_topic

        message: Message = Message(topic=trace_topic, body=data.encode())
        message.set_keys(list(keyset))

        all_topics = await self._topic_mapping.aget_all_topics()
        if trace_topic not in all_topics:
            _ = await self.update_route_info(trace_topic)

        routing_result = await self._message_router.aroute_message(trace_topic, message)
        if not routing_result.success:
            raise RouteNotFoundError(
                f"Route not found for topic: {trace_topic}, error: {routing_result.error}"
            )

        message_queue = routing_result.message_queue
        broker_data = routing_result.broker_data

        if not message_queue:
            raise QueueNotAvailableError(f"No available queue for topic: {trace_topic}")
        if not broker_data:
            raise BrokerNotAvailableError(
                f"No available broker data for topic: {trace_topic}"
            )

        target_broker_addr = routing_result.broker_address
        if not target_broker_addr:
            raise BrokerNotAvailableError(
                f"No available broker address for topic: {trace_topic}"
            )
        logger.debug(
            "Sending trace message to broker",
            extra={
                "broker_address": target_broker_addr,
                "queue": message_queue.full_name,
                "topic": trace_topic,
            },
        )

        send_result: SendMessageResult = await self._send_message_to_broker(
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

    async def update_route_info(self, topic: str) -> bool:
        """异步更新指定主题的路由信息。

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
        # 直接使用异步的 get_topic_route 操作
        topic_route_data = await self._nameserver_manager.get_topic_route(topic)

        if not topic_route_data:
            logger.error(
                "Failed to get topic route data",
                extra={"topic": topic},
            )
            return False

        # 并发维护broker连接
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
                # 异步执行 add_broker 操作
                await self._broker_manager.add_broker(
                    broker_addr,
                    broker_data.broker_name,
                )

        # 更新本地缓存
        success = await self._topic_mapping.aupdate_route_info(topic, topic_route_data)
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
        return await self._topic_mapping.aforce_refresh(topic)

    async def _send_message_to_broker(
        self, message: Message, broker_addr: str, message_queue: MessageQueue
    ) -> SendMessageResult:
        """异步发送消息到指定的 Broker。

        使用连接池获取与目标 Broker 的连接，通过 BrokerClient
        异步发送消息到指定的队列。

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
        # 使用异步连接池

        pool: AsyncConnectionPool = await self._broker_manager.must_connection_pool(
            broker_addr
        )
        async with pool.get_connection() as broker_remote:
            return await AsyncBrokerClient(broker_remote).async_send_message(
                TraceGroupName,
                message.body,
                message_queue,
                message.properties,
                flag=message.flag,
            )

    async def _route_refresh_loop(self) -> None:
        """异步路由刷新循环

        定期刷新所有Topic的路由信息，确保跟踪数据能够正确发送到最新的Broker。
        """
        logger.info(
            "Async trace route refresh loop started",
            extra={
                "refresh_interval": self._route_refresh_interval,
            },
        )

        # 首次执行立即刷新一次
        await self._refresh_all_routes()

        while self._running:
            try:
                # 使用 asyncio.wait_for 实现可中断的等待
                try:
                    await asyncio.wait_for(
                        self._stop_event.wait(),
                        timeout=self._route_refresh_interval / 1000,
                    )
                    # 收到停止信号，退出循环
                    break
                except asyncio.TimeoutError:
                    # 超时，继续执行路由刷新
                    pass

                # 检查是否仍在运行
                if not self._running:
                    break

                # 执行路由刷新
                await self._refresh_all_routes()

            except Exception as e:
                logger.error(
                    "Error in async route refresh loop",
                    extra={
                        "error": str(e),
                        "error_type": type(e).__name__,
                    },
                )
                # 发生错误时等待较短时间后重试
                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=5.0)
                    break
                except asyncio.TimeoutError:
                    continue

        logger.info("Async trace route refresh loop stopped")

    async def _refresh_all_routes(self) -> None:
        """异步刷新所有Topic的路由信息"""
        topics = list(await self._topic_mapping.aget_all_topics())

        for topic in topics:
            try:
                success = await self.update_route_info(topic)
                if not success:
                    logger.debug(
                        "Failed to refresh route for topic",
                        extra={"topic": topic},
                    )
            except Exception as e:
                logger.debug(
                    "Failed to refresh route",
                    extra={
                        "topic": topic,
                        "error": str(e),
                    },
                )
