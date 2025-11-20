"""
AsyncOffsetStore - 异步偏移量存储模块

异步版本的偏移量存储实现，基于asyncio提供高性能的非阻塞IO操作。
支持并发偏移量管理，适用于高吞吐量的异步消费者场景。

主要特性：
- 完全异步的IO操作，避免阻塞事件循环
- 线程安全的并发访问控制
- 批量操作优化，减少IO开销
- 详细的性能指标收集
- 支持异步持久化和加载

使用场景：
- 高并发异步消费者
- 需要非阻塞IO操作的场景
- 大量队列的偏移量管理
- 对性能要求极高的消息处理

设计原则：
- 异步优先：所有IO操作都是异步的
- 线程安全：支持多协程并发访问
- 性能优化：批量操作、缓存优化
- 接口兼容：与同步版本API保持一致
"""

import asyncio
import threading
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from pyrocketmq.logging import get_logger
from pyrocketmq.model import MessageQueue

from .offset_store import OffsetEntry, ReadOffsetType

logger = get_logger(__name__)


@dataclass
class AsyncOffsetEntry:
    """异步偏移量条目数据类

    在OffsetEntry基础上增加异步相关的元数据，
    支持异步操作的状态跟踪和时间统计。

    Attributes:
        queue (MessageQueue): 消息队列对象
        offset (int): 队列的消费偏移量
        last_update_timestamp (int): 最后更新时间戳（毫秒）
        persist_pending (bool): 是否有待持久化的更新
        last_persist_timestamp (int): 最后持久化时间戳（毫秒）
        persist_attempts (int): 持久化尝试次数
    """

    queue: MessageQueue
    offset: int
    last_update_timestamp: int = 0
    persist_pending: bool = False
    last_persist_timestamp: int = 0
    persist_attempts: int = 0

    def __post_init__(self) -> None:
        """初始化后处理"""
        if self.last_update_timestamp == 0:
            self.last_update_timestamp = int(datetime.now().timestamp() * 1000)

    def to_dict(self) -> dict[str, Any]:
        """转换为字典格式"""
        return {
            "queue": {
                "topic": self.queue.topic,
                "broker_name": self.queue.broker_name,
                "queue_id": self.queue.queue_id,
            },
            "offset": self.offset,
            "last_update_timestamp": self.last_update_timestamp,
            "persist_pending": self.persist_pending,
            "last_persist_timestamp": self.last_persist_timestamp,
            "persist_attempts": self.persist_attempts,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "AsyncOffsetEntry":
        """从字典创建对象"""
        queue_data = data["queue"]
        queue = MessageQueue(
            topic=queue_data["topic"],
            broker_name=queue_data["broker_name"],
            queue_id=queue_data["queue_id"],
        )

        return cls(
            queue=queue,
            offset=data["offset"],
            last_update_timestamp=data.get("last_update_timestamp", 0),
            persist_pending=data.get("persist_pending", False),
            last_persist_timestamp=data.get("last_persist_timestamp", 0),
            persist_attempts=data.get("persist_attempts", 0),
        )

    def mark_persist_pending(self) -> None:
        """标记为待持久化状态"""
        self.persist_pending = True
        self.persist_attempts += 1

    def mark_persisted(self) -> None:
        """标记为已持久化状态"""
        self.persist_pending = False
        self.last_persist_timestamp = int(datetime.now().timestamp() * 1000)
        self.persist_attempts = 0

    def to_sync_entry(self) -> OffsetEntry:
        """转换为同步版本的OffsetEntry"""
        return OffsetEntry(
            queue=self.queue,
            offset=self.offset,
            last_update_timestamp=self.last_update_timestamp,
        )


class AsyncOffsetStore(ABC):
    """异步偏移量存储抽象基类

    提供异步的偏移量存储接口，支持高并发场景下的非阻塞操作。
    所有IO操作都是异步的，不会阻塞事件循环。

    主要特性：
    - 异步IO操作，避免阻塞
    - 线程安全的并发访问
    - 批量操作优化
    - 详细的性能指标

    生命周期管理：
    - start(): 异步启动存储服务
    - stop(): 异步停止存储服务
    - load(): 异步加载历史数据
    - persist(): 异步持久化偏移量
    """

    def __init__(self, consumer_group: str) -> None:
        """
        初始化异步偏移量存储

        Args:
            consumer_group: 消费者组名称
        """
        self.consumer_group: str = consumer_group
        self.offset_table: dict[MessageQueue, int] = {}
        self._lock = asyncio.Lock()  # 异步锁
        self._thread_lock = threading.RLock()  # 线程锁（用于同步兼容）
        self.metrics = AsyncOffsetStoreMetrics()
        self._started: bool = False
        self._persist_batch_size: int = 100
        self._persist_interval: float = 1.0  # 秒
        self._persist_task: asyncio.Task[None] | None = None
        self._pending_persist: set[MessageQueue] = set()

    @abstractmethod
    async def start(self) -> None:
        """异步启动偏移量存储服务

        初始化存储服务所需的资源，如：
        - 启动后台持久化任务
        - 建立异步网络连接
        - 异步加载初始化数据

        Raises:
            OffsetStoreError: 启动失败时抛出
        """
        pass

    @abstractmethod
    async def stop(self) -> None:
        """异步停止偏移量存储服务

        清理存储服务占用的资源，如：
        - 停止后台持久化任务
        - 关闭异步网络连接
        - 异步持久化剩余数据

        Raises:
            OffsetStoreError: 停止失败时抛出
        """
        pass

    @abstractmethod
    async def load(self) -> None:
        """异步加载偏移量数据

        从持久化存储中异步加载历史偏移量数据到内存缓存。
        不会阻塞事件循环，适合高并发场景。

        Raises:
            OffsetStoreError: 加载失败时抛出
        """
        pass

    async def update_offset(self, queue: MessageQueue, offset: int) -> None:
        """异步更新偏移量到本地缓存

        将指定队列的消费偏移量异步更新到内存缓存中。
        使用异步锁确保并发安全。

        Args:
            queue (MessageQueue): 要更新偏移量的消息队列
            offset (int): 新的消费偏移量

        Raises:
            ValueError: 当offset值为负数时抛出

        Note:
            此操作仅更新内存缓存，不会立即触发持久化。
            实际持久化由后台任务定期执行。
        """
        if offset < 0:
            raise ValueError(f"Offset cannot be negative: {offset}")

        start_time = time.time()

        async with self._lock:
            old_offset = self.offset_table.get(queue, -1)
            self.offset_table[queue] = offset
            self._pending_persist.add(queue)

            logger.debug(
                "offset cache updated",
                extra={
                    "queue": str(queue),
                    "old_offset": old_offset,
                    "new_offset": offset,
                    "update_latency_ms": (time.time() - start_time) * 1000,
                },
            )

    @abstractmethod
    async def persist(self, queue: MessageQueue) -> None:
        """异步持久化单个队列的偏移量

        将指定队列的偏移量异步持久化到存储介质。
        不会阻塞事件循环，支持高并发操作。

        Args:
            queue (MessageQueue): 要持久化的消息队列

        Raises:
            OffsetStoreError: 持久化失败时抛出
        """
        pass

    async def persist_all(self) -> None:
        """异步持久化所有偏移量

        将内存缓存中的所有偏移量批量异步持久化到存储介质。
        使用批量操作优化性能，减少IO次数。

        Raises:
            OffsetStoreError: 批量持久化失败时抛出

        Performance:
            - 通过批量操作减少网络IO或文件IO次数
            - 异步并发处理，不阻塞事件循环
            - 自动分批处理大量数据
        """
        if not self.offset_table:
            return

        start_time = time.time()

        # 分批处理以避免内存压力
        items = list(self.offset_table.items())
        batch_size = self._persist_batch_size

        for i in range(0, len(items), batch_size):
            batch = dict(items[i : i + batch_size])

            # 并发持久化批次中的队列
            persist_tasks: list[asyncio.Task[None]] = []
            for queue in batch:
                task = asyncio.create_task(self.persist(queue))
                persist_tasks.append(task)

            # 等待当前批次完成
            try:
                await asyncio.gather(*persist_tasks, return_exceptions=True)
            except Exception as e:
                logger.error(f"Batch persist error: {e}")
                await self.metrics.record_batch_persist_failure()
                continue

        # 清除待持久化标记
        async with self._lock:
            self._pending_persist.clear()

        latency_ms = (time.time() - start_time) * 1000
        await self.metrics.record_batch_persist_success(latency_ms)

        logger.info(
            "all offsets persisted",
            extra={
                "total_queues": len(items),
                "batch_count": (len(items) + batch_size - 1) // batch_size,
                "latency_ms": latency_ms,
            },
        )

    async def read_offset(self, queue: MessageQueue, read_type: ReadOffsetType) -> int:
        """异步读取偏移量

        根据指定的读取类型异步获取队列的消费偏移量。
        支持多种读取策略，适应不同场景需求。

        Args:
            queue (MessageQueue): 要读取偏移量的消息队列
            read_type (ReadOffsetType): 读取策略

        Returns:
            int: 队列的消费偏移量，如果不存在则返回-1

        Raises:
            OffsetStoreError: 读取失败时抛出
            ValueError: read_type参数无效时抛出
        """
        start_time = time.time()

        try:
            if read_type == ReadOffsetType.READ_FROM_MEMORY:
                # 仅从内存读取
                async with self._lock:
                    offset = self.offset_table.get(queue, -1)
                    if offset != -1:
                        await self.metrics.record_cache_hit()
                    else:
                        await self.metrics.record_cache_miss()

                return offset

            elif read_type == ReadOffsetType.MEMORY_FIRST_THEN_STORE:
                # 优先内存，失败则从存储读取
                async with self._lock:
                    offset = self.offset_table.get(queue)

                if offset is not None:
                    await self.metrics.record_cache_hit()
                    return offset
                else:
                    await self.metrics.record_cache_miss()
                    # 从存储加载
                    return await self._load_from_store(queue)

            elif read_type == ReadOffsetType.READ_FROM_STORE:
                # 仅从存储读取
                return await self._load_from_store(queue)

            else:
                raise ValueError(f"Invalid read_type: {read_type}")

        except Exception as e:
            logger.error(f"Read offset error: {e}", exc_info=True)
            raise

        finally:
            latency_ms = (time.time() - start_time) * 1000
            logger.debug(
                "offset read completed",
                extra={
                    "queue": str(queue),
                    "read_type": read_type.value,
                    "latency_ms": latency_ms,
                },
            )

    @abstractmethod
    async def remove_offset(self, queue: MessageQueue) -> None:
        """异步移除队列的偏移量

        从内存缓存和持久化存储中异步移除指定队列的偏移量信息。

        Args:
            queue (MessageQueue): 要移除偏移量的消息队列

        Raises:
            OffsetStoreError: 移除操作失败时抛出
        """
        pass

    async def get_metrics(self) -> dict[str, Any]:
        """异步获取偏移量存储的性能指标

        返回包含操作统计和性能指标的字典，用于监控和调优。

        Returns:
            dict[str, Any]: 包含详细性能指标的字典
        """
        async with self._lock:
            cache_size = len(self.offset_table)
            pending_count = len(self._pending_persist)

        metrics_dict = await self.metrics.to_dict()
        metrics_dict.update(
            {
                "cache_size": cache_size,
                "pending_persist_count": pending_count,
                "started": self._started,
                "persist_batch_size": self._persist_batch_size,
                "persist_interval": self._persist_interval,
            }
        )

        return metrics_dict

    async def clone_offset_table(
        self, topic: str | None = None
    ) -> dict[MessageQueue, int]:
        """异步克隆偏移量表

        创建当前偏移量表的深拷贝，用于安全的并发访问。

        Args:
            topic (str | None): 可选的主题过滤条件

        Returns:
            dict[MessageQueue, int]: 偏移量表的副本
        """
        async with self._lock:
            if topic:
                return {
                    queue: offset
                    for queue, offset in self.offset_table.items()
                    if queue.topic == topic
                }
            return self.offset_table.copy()

    async def get_all_cached_offsets(self) -> dict[MessageQueue, int]:
        """异步获取所有缓存的偏移量

        获取当前内存中缓存的所有队列偏移量的快照。

        Returns:
            dict[MessageQueue, int]: 包含所有队列偏移量的字典副本
        """
        async with self._lock:
            return self.offset_table.copy()

    async def clear_cached_offsets(self) -> None:
        """异步清空缓存的偏移量

        清空内存中的所有偏移量缓存。此操作不会影响持久化存储。

        Warning:
            此操作会丢失所有未持久化的偏移量数据。
        """
        async with self._lock:
            self.offset_table.clear()
            self._pending_persist.clear()

    # 内部辅助方法
    def _remove_from_local_cache(self, queue: MessageQueue) -> bool:
        """从本地缓存移除偏移量（内部方法，需要在锁保护下调用）

        从内存缓存中移除指定队列的偏移量。这是一个内部方法，
        调用者必须已经获取了self._lock锁（同步锁或异步锁）。

        Args:
            queue (MessageQueue): 要移除的消息队列

        Returns:
            bool: 如果成功移除返回True，如果队列不存在返回False

        Note:
            此方法不是线程安全的，必须在锁保护下调用。
            仅从内存缓存中移除，不会影响持久化存储。

            对于AsyncOffsetStore，调用者可以使用：
            - async with self._lock: (异步锁)
            - 或者：with self._thread_lock: (同步锁)
        """
        if queue in self.offset_table:
            del self.offset_table[queue]
            logger.info("local offset cache entry removed", extra={"queue": str(queue)})
            return True
        return False

    def _update_local_cache(self, queue: MessageQueue, offset: int) -> None:
        """更新本地缓存中的偏移量（内部方法，需要在锁保护下调用）

        将指定队列的消费偏移量更新到内存缓存中。这是一个内部方法，
        调用者必须已经获取了self._lock锁（异步锁）。

        Args:
            queue (MessageQueue): 要更新偏移量的消息队列
            offset (int): 新的消费偏移量，必须大于等于0

        Raises:
            ValueError: 当offset值为负数时抛出

        Note:
            此方法不是线程安全的，必须在异步锁保护下调用：
            async with self._lock:
                await self._update_local_cache(queue, offset)

            此操作仅更新内存缓存，不会自动触发持久化。
            如果需要持久化，调用者应该手动添加到待持久化集合：
            self._pending_persist.add(queue)
        """
        if offset < 0:
            raise ValueError(f"Offset cannot be negative: {offset}")

        old_offset = self.offset_table.get(queue, -1)
        self.offset_table[queue] = offset

        logger.debug(
            "local offset cache updated",
            extra={
                "queue": str(queue),
                "old_offset": old_offset,
                "new_offset": offset,
            },
        )

    def _get_from_local_cache(self, queue: MessageQueue) -> int | None:
        """从本地缓存获取偏移量（内部方法，需要在锁保护下调用）

        从内存缓存中获取指定队列的消费偏移量。这是一个内部方法，
        调用者必须已经获取了self._lock锁（异步锁）。

        Args:
            queue (MessageQueue): 要获取偏移量的消息队列

        Returns:
            int | None: 队列的消费偏移量，如果不存在返回None

        Note:
            此方法不是线程安全的，必须在异步锁保护下调用：
            async with self._lock:
                offset = self._get_from_local_cache(queue)
                if offset is not None:
                    # 处理缓存命中
                else:
                    # 处理缓存未命中
        """
        return self.offset_table.get(queue)

    async def _load_from_store(self, queue: MessageQueue) -> int:
        """从持久化存储加载偏移量（内部方法）

        Args:
            queue: 要加载的队列

        Returns:
            int: 加载到的偏移量，如果不存在则返回-1
        """
        # 子类需要实现具体的存储加载逻辑
        return -1

    async def _start_background_persist(self) -> None:
        """启动后台持久化任务"""
        if self._persist_task and not self._persist_task.done():
            return

        self._persist_task = asyncio.create_task(self._background_persist_loop())
        logger.info("background persist task started")

    async def _stop_background_persist(self) -> None:
        """停止后台持久化任务"""
        if self._persist_task and not self._persist_task.done():
            self._persist_task.cancel()
            try:
                await self._persist_task
            except asyncio.CancelledError:
                pass
            logger.info("background persist task stopped")

    async def _background_persist_loop(self) -> None:
        """后台持久化循环"""
        while self._started:
            try:
                await asyncio.sleep(self._persist_interval)

                if self._pending_persist and self._started:
                    await self._persist_pending_batch()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Background persist error: {e}", exc_info=True)

    async def _persist_pending_batch(self) -> None:
        """持久化待处理的偏移量批次"""
        if not self._pending_persist:
            return

        # 获取待持久化的队列列表
        async with self._lock:
            pending_queues = list(self._pending_persist)

        if not pending_queues:
            return

        # 限制批次大小
        batch = pending_queues[: self._persist_batch_size]

        # 并发持久化
        persist_tasks = [self.persist(queue) for queue in batch]
        results = await asyncio.gather(*persist_tasks, return_exceptions=True)

        # 处理结果
        success_count = 0
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Persist failed for queue {batch[i]}: {result}")
                await self.metrics.record_persist_failure()
            else:
                success_count += 1
                async with self._lock:
                    self._pending_persist.discard(batch[i])

        logger.debug(
            "batch persist completed",
            extra={
                "batch_size": len(batch),
                "success_count": success_count,
                "pending_remaining": len(self._pending_persist),
            },
        )


class AsyncOffsetStoreMetrics:
    """异步偏移量存储指标收集器

    扩展同步版本的指标收集功能，增加异步操作特有的指标。
    线程安全，支持高并发访问。

    新增指标：
    - 批量操作统计
    - 并发度统计
    - 异步任务状态
    - 背压指标
    """

    def __init__(self) -> None:
        """初始化异步指标收集器"""
        # 基础指标（继承自同步版本）
        self.persist_success_count: int = 0
        self.persist_failure_count: int = 0
        self.load_success_count: int = 0
        self.load_failure_count: int = 0
        self.cache_hit_count: int = 0
        self.cache_miss_count: int = 0
        self.total_persist_latency: float = 0.0
        self.persist_operation_count: int = 0

        # 异步特有指标
        self.batch_persist_count: int = 0
        self.batch_persist_success_count: int = 0
        self.batch_persist_failure_count: int = 0
        self.total_batch_persist_latency: float = 0.0
        self.concurrent_operations: int = 0
        self.max_concurrent_operations: int = 0
        self.background_persist_count: int = 0

        # 时间戳
        self._start_time: float = time.time()
        self._last_persist_time: float = 0.0

        # 锁保护
        self._metrics_lock = asyncio.Lock()

    async def record_persist_success(self, latency_ms: float) -> None:
        """异步记录持久化成功"""
        async with self._metrics_lock:
            self.persist_success_count += 1
            self.total_persist_latency += latency_ms
            self.persist_operation_count += 1
            self._last_persist_time = time.time()

    async def record_persist_failure(self) -> None:
        """异步记录持久化失败"""
        async with self._metrics_lock:
            self.persist_failure_count += 1

    async def record_batch_persist_success(self, latency_ms: float) -> None:
        """异步记录批量持久化成功"""
        async with self._metrics_lock:
            self.batch_persist_count += 1
            self.batch_persist_success_count += 1
            self.total_batch_persist_latency += latency_ms

    async def record_batch_persist_failure(self) -> None:
        """异步记录批量持久化失败"""
        async with self._metrics_lock:
            self.batch_persist_count += 1
            self.batch_persist_failure_count += 1

    async def record_load_success(self) -> None:
        """异步记录加载成功"""
        async with self._metrics_lock:
            self.load_success_count += 1

    async def record_load_failure(self) -> None:
        """异步记录加载失败"""
        async with self._metrics_lock:
            self.load_failure_count += 1

    async def record_cache_hit(self) -> None:
        """异步记录缓存命中"""
        async with self._metrics_lock:
            self.cache_hit_count += 1

    async def record_cache_miss(self) -> None:
        """异步记录缓存未命中"""
        async with self._metrics_lock:
            self.cache_miss_count += 1

    async def increment_concurrent_operations(self) -> None:
        """增加并发操作计数"""
        async with self._metrics_lock:
            self.concurrent_operations += 1
            if self.concurrent_operations > self.max_concurrent_operations:
                self.max_concurrent_operations = self.concurrent_operations

    async def decrement_concurrent_operations(self) -> None:
        """减少并发操作计数"""
        async with self._metrics_lock:
            if self.concurrent_operations > 0:
                self.concurrent_operations -= 1

    async def record_background_persist(self) -> None:
        """记录后台持久化操作"""
        async with self._metrics_lock:
            self.background_persist_count += 1

    async def get_avg_persist_latency(self) -> float:
        """获取平均持久化延迟"""
        async with self._metrics_lock:
            if self.persist_operation_count == 0:
                return 0.0
            return self.total_persist_latency / self.persist_operation_count

    async def get_avg_batch_persist_latency(self) -> float:
        """获取平均批量持久化延迟"""
        async with self._metrics_lock:
            if self.batch_persist_success_count == 0:
                return 0.0
            return self.total_batch_persist_latency / self.batch_persist_success_count

    async def get_persist_success_rate(self) -> float:
        """获取持久化成功率"""
        async with self._metrics_lock:
            total = self.persist_success_count + self.persist_failure_count
            if total == 0:
                return 100.0
            return (self.persist_success_count / total) * 100.0

    async def get_batch_persist_success_rate(self) -> float:
        """获取批量持久化成功率"""
        async with self._metrics_lock:
            total = self.batch_persist_success_count + self.batch_persist_failure_count
            if total == 0:
                return 100.0
            return (self.batch_persist_success_count / total) * 100.0

    async def get_cache_hit_rate(self) -> float:
        """获取缓存命中率"""
        async with self._metrics_lock:
            total = self.cache_hit_count + self.cache_miss_count
            if total == 0:
                return 0.0
            return (self.cache_hit_count / total) * 100.0

    async def to_dict(self) -> dict[str, Any]:
        """异步转换为字典格式"""
        async with self._metrics_lock:
            uptime = time.time() - self._start_time
            time_since_last_persist = time.time() - self._last_persist_time

            return {
                # 基础指标
                "persist_success_count": self.persist_success_count,
                "persist_failure_count": self.persist_failure_count,
                "load_success_count": self.load_success_count,
                "load_failure_count": self.load_failure_count,
                "cache_hit_count": self.cache_hit_count,
                "cache_miss_count": self.cache_miss_count,
                # 延迟指标
                "avg_persist_latency": await self.get_avg_persist_latency(),
                "avg_batch_persist_latency": await self.get_avg_batch_persist_latency(),
                # 成功率指标
                "persist_success_rate": await self.get_persist_success_rate(),
                "batch_persist_success_rate": await self.get_batch_persist_success_rate(),
                "cache_hit_rate": await self.get_cache_hit_rate(),
                # 异步特有指标
                "batch_persist_count": self.batch_persist_count,
                "batch_persist_success_count": self.batch_persist_success_count,
                "batch_persist_failure_count": self.batch_persist_failure_count,
                "concurrent_operations": self.concurrent_operations,
                "max_concurrent_operations": self.max_concurrent_operations,
                "background_persist_count": self.background_persist_count,
                # 时间相关指标
                "uptime_seconds": uptime,
                "time_since_last_persist_seconds": time_since_last_persist,
            }

    async def reset(self) -> None:
        """异步重置所有指标"""
        async with self._metrics_lock:
            # 重置基础指标
            self.persist_success_count = 0
            self.persist_failure_count = 0
            self.load_success_count = 0
            self.load_failure_count = 0
            self.cache_hit_count = 0
            self.cache_miss_count = 0
            self.total_persist_latency = 0.0
            self.persist_operation_count = 0

            # 重置异步特有指标
            self.batch_persist_count = 0
            self.batch_persist_success_count = 0
            self.batch_persist_failure_count = 0
            self.total_batch_persist_latency = 0.0
            self.concurrent_operations = 0
            self.max_concurrent_operations = 0
            self.background_persist_count = 0

            # 重置时间戳
            self._start_time = time.time()
            self._last_persist_time = 0.0


# 便利函数
async def create_async_offset_store(
    consumer_group: str,
    store_type: str = "local",  # "local" 或 "remote"
    **kwargs: Any,
) -> AsyncOffsetStore:
    """创建异步偏移量存储实例的便利函数

    Args:
        consumer_group: 消费者组名称
        store_type: 存储类型，"local"或"remote"
        **kwargs: 其他配置参数

    Returns:
        AsyncOffsetStore: 异步偏移量存储实例

    Raises:
        ValueError: 不支持的存储类型
    """
    if store_type == "local":
        from .async_local_offset_store import AsyncLocalOffsetStore

        return AsyncLocalOffsetStore(consumer_group, **kwargs)
    elif store_type == "remote":
        from .async_remote_offset_store import AsyncRemoteOffsetStore

        return AsyncRemoteOffsetStore(consumer_group, **kwargs)
    else:
        raise ValueError(f"Unsupported store type: {store_type}")


async def get_async_offset_store_metrics(
    consumer_group: str, store_type: str = "local"
) -> dict[str, Any]:
    """获取异步偏移量存储指标的便利函数

    Args:
        consumer_group: 消费者组名称
        store_type: 存储类型

    Returns:
        dict[str, Any]: 性能指标字典
    """
    store = await create_async_offset_store(consumer_group, store_type)
    return await store.get_metrics()
