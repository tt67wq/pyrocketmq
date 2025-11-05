"""
OffsetStore - 偏移量存储模块

负责管理和持久化消息消费偏移量，确保Consumer能够记录每个队列的消费进度，
并在重启后恢复消费位置。

支持两种存储模式：
1. 集群模式(Clustering) - 远程存储：偏移量存储在Broker服务器上
2. 广播模式(Broadcasting) - 本地存储：偏移量存储在本地文件中
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any
import threading
from datetime import datetime

from pyrocketmq.model import MessageQueue
from pyrocketmq.logging import get_logger

logger = get_logger(__name__)


class ReadOffsetType(Enum):
    """偏移量读取类型"""

    MEMORY_FIRST_THEN_STORE = (
        "MEMORY_FIRST_THEN_STORE"  # 优先从内存读取，失败则从存储读取
    )
    READ_FROM_MEMORY = "READ_FROM_MEMORY"  # 仅从内存读取
    READ_FROM_STORE = "READ_FROM_STORE"  # 仅从存储读取


@dataclass
class OffsetEntry:
    """偏移量条目"""

    queue: MessageQueue
    offset: int
    last_update_timestamp: int = 0

    def __post_init__(self) -> None:
        if self.last_update_timestamp == 0:
            self.last_update_timestamp = int(datetime.now().timestamp() * 1000)

    def to_dict(self) -> dict[str, Any]:
        """转换为字典格式"""
        return {
            "topic": self.queue.topic,
            "broker_name": self.queue.broker_name,
            "queue_id": self.queue.queue_id,
            "offset": self.offset,
            "last_update_timestamp": self.last_update_timestamp,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "OffsetEntry":
        """从字典创建OffsetEntry"""
        queue: MessageQueue = MessageQueue(
            topic=data["topic"],
            broker_name=data["broker_name"],
            queue_id=data["queue_id"],
        )
        return cls(
            queue=queue,
            offset=data["offset"],
            last_update_timestamp=data.get("last_update_timestamp", 0),
        )


class OffsetStore(ABC):
    """偏移量存储抽象基类"""

    def __init__(self, consumer_group: str) -> None:
        """
        初始化偏移量存储

        Args:
            consumer_group: 消费者组名称
        """
        self.consumer_group: str = consumer_group
        self.offset_table: dict[MessageQueue, int] = {}
        self._lock: threading.RLock = threading.RLock()

    @abstractmethod
    def load(self) -> None:
        """加载偏移量数据"""
        pass

    @abstractmethod
    def update_offset(self, queue: MessageQueue, offset: int) -> None:
        """
        更新偏移量到本地缓存

        Args:
            queue: 消息队列
            offset: 偏移量
        """
        pass

    @abstractmethod
    def persist(self, queue: MessageQueue) -> None:
        """
        持久化单个队列的偏移量

        Args:
            queue: 消息队列
        """
        pass

    @abstractmethod
    def persist_all(self) -> None:
        """持久化所有偏移量"""
        pass

    @abstractmethod
    def read_offset(self, queue: MessageQueue, read_type: ReadOffsetType) -> int:
        """
        读取偏移量

        Args:
            queue: 消息队列
            read_type: 读取类型

        Returns:
            int: 偏移量，如果不存在返回-1
        """
        pass

    @abstractmethod
    def remove_offset(self, queue: MessageQueue) -> None:
        """
        移除队列的偏移量

        Args:
            queue: 消息队列
        """
        pass

    def clone_offset_table(self, topic: str | None = None) -> dict[MessageQueue, int]:
        """
        克隆偏移量表

        Args:
            topic: 指定Topic，如果为None则克隆所有

        Returns:
            dict[MessageQueue, int]: 偏移量表的副本
        """
        with self._lock:
            if topic:
                return {
                    queue: offset
                    for queue, offset in self.offset_table.items()
                    if queue.topic == topic
                }
            return self.offset_table.copy()

    def get_all_cached_offsets(self) -> dict[MessageQueue, int]:
        """获取所有缓存的偏移量"""
        with self._lock:
            return self.offset_table.copy()

    def clear_cached_offsets(self) -> None:
        """清空缓存的偏移量"""
        with self._lock:
            self.offset_table.clear()

    def _update_local_cache(self, queue: MessageQueue, offset: int) -> None:
        """更新本地缓存（内部方法，需要在锁保护下调用）"""
        self.offset_table[queue] = offset
        logger.info(
            "local offset cache updated", extra={"queue": str(queue), "offset": offset}
        )

    def _get_from_local_cache(self, queue: MessageQueue) -> int | None:
        """从本地缓存获取偏移量（内部方法，需要在锁保护下调用）"""
        return self.offset_table.get(queue)

    def _remove_from_local_cache(self, queue: MessageQueue) -> bool:
        """从本地缓存移除偏移量（内部方法，需要在锁保护下调用）"""
        if queue in self.offset_table:
            del self.offset_table[queue]
            logger.info("local offset cache entry removed", extra={"queue": str(queue)})
            return True
        return False


class OffsetStoreMetrics:
    """偏移量存储指标"""

    def __init__(self) -> None:
        self.persist_success_count: int = 0
        self.persist_failure_count: int = 0
        self.load_success_count: int = 0
        self.load_failure_count: int = 0
        self.cache_hit_count: int = 0
        self.cache_miss_count: int = 0
        self.total_persist_latency: float = 0.0
        self.persist_operation_count: int = 0

    def record_persist_success(self, latency_ms: float) -> None:
        """记录持久化成功"""
        self.persist_success_count += 1
        self.total_persist_latency += latency_ms
        self.persist_operation_count += 1

    def record_persist_failure(self) -> None:
        """记录持久化失败"""
        self.persist_failure_count += 1

    def record_load_success(self) -> None:
        """记录加载成功"""
        self.load_success_count += 1

    def record_load_failure(self) -> None:
        """记录加载失败"""
        self.load_failure_count += 1

    def record_cache_hit(self) -> None:
        """记录缓存命中"""
        self.cache_hit_count += 1

    def record_cache_miss(self) -> None:
        """记录缓存未命中"""
        self.cache_miss_count += 1

    @property
    def avg_persist_latency(self) -> float:
        """平均持久化延迟（毫秒）"""
        if self.persist_operation_count == 0:
            return 0.0
        return self.total_persist_latency / self.persist_operation_count

    def to_dict(self) -> dict[str, Any]:
        """转换为字典格式"""
        return {
            "persist_success_count": self.persist_success_count,
            "persist_failure_count": self.persist_failure_count,
            "load_success_count": self.load_success_count,
            "load_failure_count": self.load_failure_count,
            "cache_hit_count": self.cache_hit_count,
            "cache_miss_count": self.cache_miss_count,
            "avg_persist_latency": self.avg_persist_latency,
        }
