#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import threading
from collections import deque
from threading import Lock
from typing import Deque

from pyrocketmq.broker import BrokerManager, TopicBrokerMapping
from pyrocketmq.model import TraceContext
from pyrocketmq.nameserver import NameServerManager, create_nameserver_manager
from pyrocketmq.queue_helper import MessageRouter
from pyrocketmq.remote import RemoteConfig
from pyrocketmq.trace import TraceConfig
from pyrocketmq.transport import TransportConfig

RmqSysTraceTopic = "RMQ_SYS_TRACE_TOPIC"
TraceTopicPrefix = "rmq_sys_TRACE_DATA_"
TraceGroupName = "_INNER_TRACE_PRODUCER"


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
            self._flush()

    def _flush(self):
        """刷新队列中的跟踪数据到远程"""
        # TODO: 实现刷新逻辑，将队列中的批量数据发送到远程
        # 暂时只清空队列
        with self._lock:
            # 获取当前队列中的所有数据
            batch_ctx_list: list[TraceContext] = list(self._queue)
            # 清空队列
            self._queue.clear()

            # TODO: 发送 batch_ctx_list 到远程 broker
            # 这里需要将 TraceContext 转换为 Message 对象并发送
            pass
