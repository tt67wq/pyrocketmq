"""
队列选择器模块 - Consumer版本

从producer.queue_selectors中导入所有QueueSelector，方便consumer使用。
提供各种队列选择策略，支持同步和异步两种模式。

作者: pyrocketmq团队
版本: Consumer Import 1.0
"""

# 从producer.queue_selectors中导入所有队列选择器
from pyrocketmq.producer.queue_selectors import (
    AsyncMessageHashSelector,
    # 异步队列选择器
    AsyncQueueSelector,
    AsyncRandomSelector,
    AsyncRoundRobinSelector,
    MessageHashSelector,
    # 同步队列选择器
    QueueSelector,
    RandomSelector,
    RoundRobinSelector,
)

# 导出的所有类，方便消费者使用
__all__ = [
    # 同步队列选择器
    "QueueSelector",
    "RoundRobinSelector",
    "RandomSelector",
    "MessageHashSelector",
    # 异步队列选择器
    "AsyncQueueSelector",
    "AsyncRoundRobinSelector",
    "AsyncRandomSelector",
    "AsyncMessageHashSelector",
]
