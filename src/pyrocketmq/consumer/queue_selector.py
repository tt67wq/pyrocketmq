"""
队列选择器模块 - Consumer版本

从producer.queue_selectors中导入所有QueueSelector，方便consumer使用。
提供各种队列选择策略，用于队列分配和选择场景。

作者: pyrocketmq团队
版本: Consumer Import 1.0
"""

# 从producer.queue_selectors中导入所有队列选择器
from pyrocketmq.producer.queue_selectors import (
    AsyncMessageHashSelector,
    AsyncQueueSelector,
    AsyncRandomSelector,
    AsyncRoundRobinSelector,
    MessageHashSelector,
    QueueSelector,
    RandomSelector,
    RoundRobinSelector,
    create_async_message_hash_selector,
    create_async_random_selector,
    create_async_round_robin_selector,
    create_message_hash_selector,
    create_random_selector,
    create_round_robin_selector,
    get_async_message_hash_selector,
    get_async_random_selector,
    get_async_round_robin_selector,
)

# 导出的所有类，方便消费者使用
__all__ = [
    # 队列选择器
    "QueueSelector",
    "AsyncQueueSelector",
    "RoundRobinSelector",
    "RandomSelector",
    "MessageHashSelector",
    "AsyncRoundRobinSelector",
    "AsyncRandomSelector",
    "AsyncMessageHashSelector",
    # 便利函数
    "create_round_robin_selector",
    "create_random_selector",
    "create_message_hash_selector",
    "create_async_round_robin_selector",
    "create_async_random_selector",
    "create_async_message_hash_selector",
    "get_async_round_robin_selector",
    "get_async_random_selector",
    "get_async_message_hash_selector",
]
