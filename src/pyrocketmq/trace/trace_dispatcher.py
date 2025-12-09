#! /usr/bin/env python3
# -*- coding: utf-8 -*-


from pyrocketmq.broker import BrokerManager, TopicBrokerMapping
from pyrocketmq.model import TraceContext
from pyrocketmq.nameserver import NameServerManager, create_nameserver_manager
from pyrocketmq.producer import MessageRouter
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

        # NameServer连接管理）
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

    def start(self):
        pass

    def stop(self):
        pass

    def dispatch(self, ctx: TraceContext):
        # add ctx to queue
        pass

    def _flush(self):
        # flush queue to remote
        pass
