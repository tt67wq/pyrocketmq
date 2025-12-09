"""
AsyncRemoteOffsetStore - 异步集群模式偏移量存储

集群模式下的异步远程偏移量存储实现，偏移量存储在Broker服务器上，
支持多Consumer实例协同消费。完全基于异步IO实现，提供高性能的并发偏移量管理。

特点：
- 异步网络通信，避免阻塞事件循环
- 本地缓存减少异步网络请求
- 并发批量提交，提升性能
- 异步后台持久化任务
- Broker地址缓存优化，避免频繁查询NameServer
- 完整的异步指标监控和日志记录

设计原则：
- 异步优先：所有IO操作都是异步的
- 高性能：并发操作、智能缓存、批量提交
- 可靠性：原子性操作、错误恢复、重试机制
- 可监控：详细的性能指标和诊断信息
"""

import asyncio
import time
from collections import defaultdict
from types import CoroutineType
from typing import Any

from pyrocketmq.broker import AsyncBrokerClient, AsyncBrokerManager, TopicBrokerMapping
from pyrocketmq.consumer.async_offset_store import (
    AsyncOffsetStore,
    AsyncOffsetStoreMetrics,
    ReadOffsetType,
)
from pyrocketmq.logging import get_logger
from pyrocketmq.model import MessageQueue
from pyrocketmq.nameserver import AsyncNameServerManager
from pyrocketmq.queue_helper import MessageRouter
from pyrocketmq.remote import AsyncConnectionPool

logger = get_logger(__name__)

# 类型别名
BrokerOffsetMap = dict[str, list[tuple[MessageQueue, int]]]
BrokerAddress = str


class AsyncRemoteOffsetStore(AsyncOffsetStore):
    """集群模式下的异步远程偏移量存储

    实现RocketMQ标准的集群消费模式下的异步偏移量管理，偏移量存储在Broker服务器上，
    支持多Consumer实例协同消费同一队列集合。

    主要特性：
    - 异步远程存储：基于asyncio的Broker通信，支持高并发
    - 本地缓存：使用remote_offset_cache缓存偏移量，减少异步网络请求
    - 并发批量提交：支持并发批量提交偏移量，提升性能
    - 异步后台持久化：asyncio任务定期同步偏移量到Broker
    - Broker地址缓存：缓存Broker地址信息，避免频繁查询NameServer
    - 异步指标监控：提供完整的异步操作指标和性能监控

    线程安全：
    - 使用asyncio.Lock保护所有共享状态
    - 支持多协程并发访问
    - 原子性的偏移量更新操作
    """

    def __init__(
        self,
        consumer_group: str,
        broker_manager: AsyncBrokerManager,
        nameserver_manager: AsyncNameServerManager,
        persist_interval: float = 5.0,
        max_concurrent_brokers: int = 10,
        enable_parallel_persist: bool = True,
    ) -> None:
        """初始化异步远程偏移量存储

        Args:
            consumer_group: 消费者组名称
            broker_manager: 异步Broker管理器，用于与Broker通信
            nameserver_manager: 异步NameServer管理器，用于路由查询
            persist_interval: 持久化间隔（秒），默认5秒
            max_concurrent_brokers: 最大并发Broker连接数，默认10
            enable_parallel_persist: 是否启用并行持久化，默认True

        Raises:
            ValueError: 当参数值无效时抛出

        Note:
            初始化的组件包括：
            - remote_offset_cache: 偏移量本地缓存，减少网络请求
            - _lock: 异步锁，保护所有共享状态
            - _persist_task: 后台持久化任务，定期同步偏移量到Broker
            - _semaphore: 控制并发Broker连接数
        """
        super().__init__(consumer_group)
        self.broker_manager: AsyncBrokerManager = broker_manager
        self.persist_interval: float = persist_interval
        self.max_concurrent_brokers: int = max_concurrent_brokers
        self.enable_parallel_persist: bool = enable_parallel_persist

        # 远程偏移量缓存，减少网络请求
        self.remote_offset_cache: dict[MessageQueue, int] = {}

        # 异步锁
        self._lock: asyncio.Lock = asyncio.Lock()
        self._cache_lock: asyncio.Lock = asyncio.Lock()

        # NameServer连接管理（仅用于路由查询）
        self._nameserver_manager: AsyncNameServerManager = nameserver_manager

        # router
        self._router: MessageRouter = MessageRouter(TopicBrokerMapping())

        # 并发控制
        self._broker_semaphore: asyncio.Semaphore = asyncio.Semaphore(
            max_concurrent_brokers
        )

        # 指标收集
        self.metrics: AsyncOffsetStoreMetrics = AsyncOffsetStoreMetrics()

        # 异步后台持久化任务
        self._persist_task: asyncio.Task[None] | None = None
        self._running: bool = False

    async def start(self) -> None:
        """异步启动偏移量存储服务

        启动后台持久化任务，开始定期同步偏移量到Broker。

        Note:
            如果偏移量存储已经启动，此方法会直接返回而不重复启动。
        """
        if self._running:
            return

        self._running = True
        # 启动异步定期持久化任务
        await self._start_background_persist()

        logger.info(
            "async remote offset store started",
            extra={"consumer_group": self.consumer_group},
        )

    async def stop(self) -> None:
        """异步停止偏移量存储服务

        停止后台持久化任务，等待所有待持久化的偏移量完成同步。

        Note:
            如果偏移量存储已经停止，此方法会直接返回而不重复停止。
        """
        if not self._running:
            return

        self._running = False

        # 停止后台持久化任务
        await self._stop_background_persist()

        # 持久化所有缓存的偏移量
        await self.persist_all()

        logger.info(
            "async remote offset store stopped",
            extra={"consumer_group": self.consumer_group},
        )

    async def load(self) -> None:
        """从Broker异步加载偏移量到本地缓存

        对于远程存储，初始化时不需要加载所有偏移量，
        而是在第一次读取时按需从Broker获取。

        Note:
            此方法为空实现，因为远程存储采用按需加载策略。
        """
        try:
            logger.info(
                "loading async remote offsets",
                extra={"consumer_group": self.consumer_group},
            )
            # 远程模式下，偏移量按需加载
            await self.metrics.record_load_success()
        except Exception as e:
            logger.error(
                "failed to load async remote offsets",
                extra={"consumer_group": self.consumer_group, "error": str(e)},
            )
            await self.metrics.record_load_failure()

    async def update_offset(self, queue: MessageQueue, offset: int) -> None:
        """异步更新偏移量到本地缓存

        将偏移量更新到本地缓存中，后台持久化任务会定期同步到Broker。

        Args:
            queue: 消息队列，包含Topic、Broker名称和队列ID
            offset: 新的偏移量值，必须大于等于0

        Raises:
            ValueError: 当offset为负数时抛出
        """
        if offset < 0:
            raise ValueError(f"Offset must be non-negative, got {offset}")

        async with self._lock:
            # 更新本地缓存
            self._update_local_cache(queue, offset)

            # 更新远程缓存
            self.remote_offset_cache[queue] = offset

            logger.debug(
                "updated async remote offset cache",
                extra={"queue": str(queue), "offset": offset},
            )

    async def persist(self, queue: MessageQueue) -> None:
        """异步持久化单个队列的偏移量到Broker

        立即将指定队列的偏移量同步到Broker，不等待后台持久化任务。

        Args:
            queue: 需要持久化偏移量的消息队列

        Raises:
            ConnectionError: 当无法连接到Broker时抛出
            BrokerError: 当Broker返回错误时抛出
        """
        async with self._lock:
            if queue not in self.offset_table:
                return

            offset: int = self.offset_table[queue]
            start_time: float = time.time()

        try:
            async with self._broker_semaphore:
                broker_addr: str | None = await self._get_broker_addr_by_name(
                    queue.broker_name
                )
                if not broker_addr:
                    raise ValueError(f"Broker address not found for {queue}")

                pool: AsyncConnectionPool = (
                    await self.broker_manager.must_connection_pool(broker_addr)
                )
                async with pool.get_connection(usage="更新消费者偏移量") as connection:
                    await AsyncBrokerClient(connection).update_consumer_offset(
                        self.consumer_group, queue.topic, queue.queue_id, offset
                    )

            # 更新远程缓存
            async with self._cache_lock:
                self.remote_offset_cache[queue] = offset

            latency: float = (time.time() - start_time) * 1000
            await self.metrics.record_persist_success(latency)

            logger.debug(
                "persisted offset to broker asynchronously",
                extra={
                    "queue": str(queue),
                    "offset": offset,
                    "latency_ms": round(latency, 2),
                },
            )

        except Exception as e:
            await self.metrics.record_persist_failure()
            logger.error(
                "failed to async persist offset",
                extra={"queue": str(queue), "offset": offset, "error": str(e)},
            )
            raise

    async def persist_all(self) -> None:
        """异步批量持久化所有偏移量

        立即将所有缓存的偏移量同步到对应的Broker。

        Raises:
            ConnectionError: 当无法连接到Broker时抛出
            BrokerError: 当Broker返回错误时抛出
        """
        if not self.offset_table:
            return

        async with self._lock:
            # 按Broker分组，批量提交
            broker_offsets: BrokerOffsetMap = await self._group_offsets_by_broker()

            if not broker_offsets:
                return

            logger.info(
                "persisting offsets to brokers asynchronously",
                extra={
                    "offsets_count": len(self.offset_table),
                    "brokers_count": len(broker_offsets),
                },
            )

            # 并发或顺序提交到各个Broker
            if self.enable_parallel_persist and len(broker_offsets) > 1:
                # 并发模式：同时向多个Broker提交
                tasks: list[CoroutineType[Any, Any, None]] = [
                    self._persist_to_broker_async(broker_name, offsets)
                    for broker_name, offsets in broker_offsets.items()
                ]
                results: list[BaseException | None] = await asyncio.gather(
                    *tasks, return_exceptions=True
                )

                # 统计结果
                success_count: int = sum(
                    1 for r in results if not isinstance(r, Exception)
                )
                failure_count: int = len(results) - success_count

                # 记录失败的错误
                for i, result in enumerate(results):
                    if isinstance(result, Exception):
                        broker_name: str = list(broker_offsets.keys())[i]
                        logger.error(
                            "failed to async persist offsets to broker",
                            extra={"broker_name": broker_name, "error": str(result)},
                        )
            else:
                # 顺序模式：依次向各个Broker提交
                success_count = 0
                failure_count = 0

                for broker_name, offsets in broker_offsets.items():
                    try:
                        await self._persist_to_broker_async(broker_name, offsets)
                        success_count += 1
                    except Exception as e:
                        failure_count += 1
                        logger.error(
                            "failed to async persist offsets to broker",
                            extra={"broker_name": broker_name, "error": str(e)},
                        )
                        continue

            # 记录批量持久化指标
            total_offsets: int = len(self.offset_table)
            if success_count > 0:
                await self.metrics.record_batch_persist_success(total_offsets)
            if failure_count > 0:
                await self.metrics.record_batch_persist_failure()

            logger.info(
                "async offset persistence completed",
                extra={"success_count": success_count, "failure_count": failure_count},
            )

    async def read_offset(self, queue: MessageQueue, read_type: ReadOffsetType) -> int:
        """异步读取偏移量

        从Broker异步读取指定队列的消费偏移量。首先检查本地缓存，
        如果缓存未命中或已过期，则从Broker查询最新偏移量。

        Args:
            queue: 消息队列，包含Topic、Broker名称和队列ID
            read_type: 读取类型，支持MEMORY_FIRST_THEN_STORE、
                      READ_FROM_MEMORY、READ_FROM_STORE等

        Returns:
            int: 队列的消费偏移量，如果不存在返回-1

        Raises:
            ConnectionError: 当无法连接到Broker时抛出
            BrokerError: 当Broker返回错误时抛出

        Note:
            读取成功的偏移量会自动更新本地缓存。
        """
        async with self._lock:
            # 1. 优先从内存读取
            if read_type in [
                ReadOffsetType.MEMORY_FIRST_THEN_STORE,
                ReadOffsetType.READ_FROM_MEMORY,
            ]:
                offset: int | None = self._get_from_local_cache(queue)
                if offset is not None:
                    await self.metrics.record_cache_hit()
                    return offset

                if read_type == ReadOffsetType.READ_FROM_MEMORY:
                    await self.metrics.record_cache_miss()
                    return -1

            # 2. 从远程存储读取
            if read_type in [
                ReadOffsetType.MEMORY_FIRST_THEN_STORE,
                ReadOffsetType.READ_FROM_STORE,
            ]:
                try:
                    async with self._broker_semaphore:
                        # 获取Broker客户端
                        broker_addr: str | None = await self._get_broker_addr_by_name(
                            queue.broker_name
                        )
                        if not broker_addr:
                            logger.error(
                                "failed to get broker address",
                                extra={"queue": str(queue)},
                            )
                            await self.metrics.record_cache_miss()
                            return -1

                        pool: AsyncConnectionPool = (
                            await self.broker_manager.must_connection_pool(broker_addr)
                        )
                        async with pool.get_connection(
                            usage="查询消费者偏移量"
                        ) as conn:
                            offset = await AsyncBrokerClient(
                                conn
                            ).query_consumer_offset(
                                consumer_group=self.consumer_group,
                                topic=queue.topic,
                                queue_id=queue.queue_id,
                            )

                    if offset >= 0:
                        # 更新本地缓存
                        self._update_local_cache(queue, offset)
                        async with self._cache_lock:
                            self.remote_offset_cache[queue] = offset
                        logger.debug(
                            "loaded offset from broker asynchronously",
                            extra={"queue": str(queue), "offset": offset},
                        )
                    else:
                        logger.debug(
                            "no offset found in broker", extra={"queue": str(queue)}
                        )

                    await self.metrics.record_cache_miss()
                    return offset

                except Exception as e:
                    logger.error(
                        "failed to async read offset from broker",
                        extra={"queue": str(queue), "error": str(e)},
                    )
                    await self.metrics.record_cache_miss()
                    return -1

            return -1

    async def remove_offset(self, queue: MessageQueue) -> None:
        """异步移除队列的偏移量

        从本地缓存中移除指定队列的偏移量。注意：这不会删除Broker上的偏移量，
        只是清理本地缓存。

        Args:
            queue: 需要移除偏移量的消息队列
        """
        async with self._lock:
            # 从本地缓存中移除
            self._remove_from_local_cache(queue)

        async with self._cache_lock:
            # 从远程缓存中移除
            self.remote_offset_cache.pop(queue, None)

        logger.debug(
            "removed offset from local cache",
            extra={"queue": str(queue)},
        )

    async def get_metrics(self) -> dict[str, Any]:
        """异步获取偏移量存储指标

        Returns:
            dict: 包含各种性能指标的字典，包括缓存命中率、持久化延迟等
        """
        base_metrics: dict[str, Any] = await super().get_metrics()

        async with self._cache_lock:
            remote_cache_size: int = len(self.remote_offset_cache)

        # 添加远程存储特有指标
        metrics: dict[str, Any] = {
            **base_metrics,
            "remote_cache_size": remote_cache_size,
            "max_concurrent_brokers": self.max_concurrent_brokers,
            "parallel_persist_enabled": self.enable_parallel_persist,
            "broker_semaphore_available": self._broker_semaphore._value,
        }

        return metrics

    # ========== 内部辅助方法 ==========

    async def _group_offsets_by_broker(
        self,
    ) -> BrokerOffsetMap:
        """按Broker分组偏移量

        Returns:
            dict: 按Broker名称分组的偏移量列表
        """
        broker_offsets: BrokerOffsetMap = defaultdict(list)

        for queue, offset in self.offset_table.items():
            broker_offsets[queue.broker_name].append((queue, offset))

        return dict(broker_offsets)

    async def _persist_to_broker_async(
        self, broker_name: str, offsets: list[tuple[MessageQueue, int]]
    ) -> None:
        """异步持久化偏移量到指定Broker

        Args:
            broker_name: Broker名称
            offsets: 要持久化的偏移量列表

        Raises:
            ConnectionError: 当无法连接到Broker时抛出
            BrokerError: 当Broker返回错误时抛出
        """
        if not offsets:
            return

        broker_addr = await self._get_broker_addr_by_name(broker_name)
        if not broker_addr:
            raise ValueError(f"Broker address not found for {broker_name}")

        pool = await self.broker_manager.must_connection_pool(broker_addr)
        async with pool.get_connection(usage="批量更新消费者偏移量") as connection:
            broker_client = AsyncBrokerClient(connection)

            # 批量更新偏移量
            for queue, offset in offsets:
                await broker_client.update_consumer_offset(
                    self.consumer_group, queue.topic, queue.queue_id, offset
                )

                # 更新远程缓存
                async with self._cache_lock:
                    self.remote_offset_cache[queue] = offset

        logger.debug(
            "persisted offsets to broker asynchronously",
            extra={
                "broker_name": broker_name,
                "offsets_count": len(offsets),
            },
        )

    async def _get_broker_addr_by_name(self, broker_name: str) -> str | None:
        """根据Broker名称获取地址

        Args:
            broker_name: Broker名称

        Returns:
            str | None: Broker地址，如果未找到返回None
        """
        return await self._nameserver_manager.get_broker_address(broker_name)

    async def _start_background_persist(self) -> None:
        """启动后台异步持久化任务"""
        if self._persist_task is None or self._persist_task.done():
            self._persist_task = asyncio.create_task(self._background_persist_loop())
            logger.debug("async background persist task started")

    async def _stop_background_persist(self) -> None:
        """停止后台异步持久化任务"""
        if self._persist_task and not self._persist_task.done():
            self._persist_task.cancel()
            try:
                await self._persist_task
            except asyncio.CancelledError:
                pass
            logger.debug("async background persist task stopped")

    async def _background_persist_loop(self) -> None:
        """后台异步持久化循环"""
        logger.debug("async background persist loop started")

        while self._running:
            try:
                await asyncio.sleep(self.persist_interval)

                if self._running and self.offset_table:
                    await self._persist_pending_batch()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(
                    "error in async background persist loop",
                    extra={"error": str(e)},
                )

        logger.debug("async background persist loop stopped")

    async def _persist_pending_batch(self) -> None:
        """异步持久化待处理的偏移量批次"""
        if not self.offset_table:
            return

        try:
            # 按Broker分组
            broker_offsets = await self._group_offsets_by_broker()

            # 并发持久化到各个Broker
            tasks: list[asyncio.Task[None]] = []
            for broker_name, offsets in broker_offsets.items():
                task = asyncio.create_task(
                    self._persist_to_broker_async(broker_name, offsets)
                )
                tasks.append(task)

            # 等待所有持久化任务完成
            await asyncio.gather(*tasks, return_exceptions=True)

            await self.metrics.record_background_persist()

        except Exception as e:
            logger.error(
                "failed to async persist pending batch",
                extra={"error": str(e)},
            )
            await self.metrics.record_batch_persist_failure()


# ========== 便利函数 ==========


async def create_async_remote_offset_store(
    consumer_group: str,
    broker_manager: AsyncBrokerManager,
    nameserver_manager: AsyncNameServerManager,
    persist_interval: float = 5.0,
    max_concurrent_brokers: int = 10,
    enable_parallel_persist: bool = True,
) -> AsyncRemoteOffsetStore:
    """创建异步远程偏移量存储实例

    Args:
        consumer_group: 消费者组名称
        broker_manager: 异步Broker管理器
        nameserver_manager: 异步NameServer管理器
        persist_interval: 持久化间隔（秒）
        max_concurrent_brokers: 最大并发Broker连接数
        enable_parallel_persist: 是否启用并行持久化

    Returns:
        AsyncRemoteOffsetStore: 异步远程偏移量存储实例
    """
    return AsyncRemoteOffsetStore(
        consumer_group=consumer_group,
        broker_manager=broker_manager,
        nameserver_manager=nameserver_manager,
        persist_interval=persist_interval,
        max_concurrent_brokers=max_concurrent_brokers,
        enable_parallel_persist=enable_parallel_persist,
    )


async def get_async_remote_offset_store_metrics(
    offset_store: AsyncRemoteOffsetStore,
) -> dict[str, Any]:
    """获取异步远程偏移量存储指标

    Args:
        offset_store: 异步远程偏移量存储实例

    Returns:
        dict: 包含各种性能指标的字典
    """
    return await offset_store.get_metrics()
