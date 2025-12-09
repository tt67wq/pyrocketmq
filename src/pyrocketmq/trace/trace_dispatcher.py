#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import threading
from collections import deque
from threading import Lock
from typing import Deque

from pyrocketmq.broker import BrokerManager, TopicBrokerMapping
from pyrocketmq.model import CONTENT_SPLITTER, TraceContext, TraceTransferBean
from pyrocketmq.nameserver import NameServerManager, create_nameserver_manager
from pyrocketmq.queue_helper import MessageRouter
from pyrocketmq.remote import RemoteConfig
from pyrocketmq.trace import TraceConfig
from pyrocketmq.transport import TransportConfig

RmqSysTraceTopic = "RMQ_SYS_TRACE_TOPIC"
TraceTopicPrefix = "rmq_sys_TRACE_DATA_"
TraceGroupName = "_INNER_TRACE_PRODUCER"
contentSplitter = CONTENT_SPLITTER  # 内容分隔符，对应Go版本的分隔符


class TraceDispatcher:
    def __init__(self, config: TraceConfig):
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
        """启动跟踪分发器"""
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
        """停止跟踪分发器"""
        if self._running:
            self._running = False
            # 设置停止事件
            self._stop_event.set()
            # 设置刷新事件以唤醒工作线程
            self._flush_event.set()

            # 等待刷新线程完成
            if self._flush_thread and self._flush_thread.is_alive():
                self._flush_thread.join(timeout=5.0)  # 最多等待5秒

    def dispatch(self, ctx: TraceContext):
        """分发跟踪上下文到队列

        Args:
            ctx: 跟踪上下文对象
        """
        with self._lock:
            # 将跟踪上下文添加到队列
            self._queue.append(ctx)

            # 检查队列大小，如果超过最大批量大小则触发刷新
            if len(self._queue) >= self._config.max_batch_size:
                # 触发刷新事件
                self._flush_event.set()

    def _flush_worker(self):
        """刷新工作线程，持续运行等待刷新事件"""
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
        """刷新队列中的跟踪数据到远程"""
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

    def _flush(self, topic: str, regionID: str, data: list[TraceTransferBean]) -> None:
        """批量刷新跟踪数据到MQ

        Args:
            topic: 主题名称
            regionID: 区域ID
            data: 要发送的跟踪数据列表
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
                self._send_trace_data_by_mq(keyset, regionID, "".join(builder))
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
            self._send_trace_data_by_mq(keyset, regionID, "".join(builder))

    def _send_trace_data_by_mq(self, keyset: set[str], region_id: str, data: str):
        """通过MQ发送跟踪数据

        Args:
            keyset: 传输键集合
            region_id: 区域ID
            data: 要发送的数据
        """
        # TODO: 实现通过MQ发送跟踪数据的逻辑
        pass
