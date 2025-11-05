#! /usr/bin/env python3
# -*- coding: utf-8 -*-


# 消息模式枚举
class MessageModel:
    """消息模式"""

    BROADCASTING: str = "BROADCASTING"  # 广播模式
    CLUSTERING: str = "CLUSTERING"  # 集群模式


# 消费起始位置枚举
class ConsumeFromWhere:
    """消费起始位置"""

    CONSUME_FROM_LAST_OFFSET: str = "CONSUME_FROM_LAST_OFFSET"  # 从最后偏移量开始消费
    CONSUME_FROM_FIRST_OFFSET: str = (
        "CONSUME_FROM_FIRST_OFFSET"  # 从第一个偏移量开始消费
    )
    CONSUME_FROM_TIMESTAMP: str = "CONSUME_FROM_TIMESTAMP"  # 从指定时间戳开始消费


class AllocateQueueStrategy:
    """队列负载均衡策略

    定义在多消费者场景下如何分配Topic下的队列给不同的Consumer实例。
    不同的策略适用于不同的业务场景和部署架构。
    """

    AVERAGE: str = "AVG"  # 平均分配策略(默认，MVP实现)
    HASH: str = "HASH"  # 哈希分配策略(后续版本)
