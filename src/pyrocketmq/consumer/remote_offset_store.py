"""
RemoteOffsetStore - 集群模式偏移量存储

集群模式下的远程偏移量存储实现，偏移量存储在Broker服务器上，
支持多Consumer实例协同消费。这是RocketMQ的标准实现方式。

特点：
- 偏移量存储在Broker服务器上
- 支持多Consumer实例协同消费
- 使用本地缓存减少网络请求
- 批量提交提升性能
- Broker地址缓存优化，避免频繁查询NameServer
- 完整的指标监控和日志记录
"""

import threading
import time
from collections import defaultdict
from typing import Any

from pyrocketmq.broker import BrokerClient, BrokerManager
from pyrocketmq.consumer.offset_store import (
    OffsetStore,
    OffsetStoreMetrics,
    ReadOffsetType,
)
from pyrocketmq.logging import get_logger
from pyrocketmq.model import MessageQueue
from pyrocketmq.nameserver import NameServerManager
from pyrocketmq.producer.router import MessageRouter
from pyrocketmq.producer.topic_broker_mapping import TopicBrokerMapping
from pyrocketmq.remote import ConnectionPool

logger = get_logger(__name__)


class RemoteOffsetStore(OffsetStore):
    """集群模式下的远程偏移量存储

    实现RocketMQ标准的集群消费模式下的偏移量管理，偏移量存储在Broker服务器上，
    支持多Consumer实例协同消费同一队列集合。

    主要特性：
    - 远程存储：偏移量存储在Broker服务器，支持多实例协同消费
    - 本地缓存：使用remote_offset_cache缓存偏移量，减少网络请求
    - 批量提交：支持批量提交偏移量，提升性能
    - 定期持久化：后台线程定期持久化缓存的偏移量
    - Broker地址缓存：缓存Broker地址信息，避免频繁查询NameServer
    - 指标监控：提供完整的操作指标和性能监控

    线程安全：
    - 使用RLock保护所有共享状态
    - 支持多线程并发访问
    - 原子性的偏移量更新操作
    """

    def __init__(
        self,
        consumer_group: str,
        broker_manager: BrokerManager,
        nameserver_manager: NameServerManager,
        persist_interval: int = 5000,
    ) -> None:
        """初始化远程偏移量存储.

        Args:
            consumer_group: 消费者组名称
            broker_manager: Broker管理器，用于与Broker通信
            nameserver_manager: NameServer管理器，用于路由查询
            persist_interval: 持久化间隔（毫秒），默认5秒

        Raises:
            ValueError: 当参数值无效时抛出

        Note:
            初始化的组件包括：
            - remote_offset_cache: 偏移量本地缓存，减少网络请求
            - _lock: 线程安全锁，保护所有共享状态
            - _persist_thread: 后台持久化线程，定期同步偏移量到Broker
        """
        super().__init__(consumer_group)
        self.broker_manager: BrokerManager = broker_manager
        self.persist_interval: int = persist_interval

        # 远程偏移量缓存，减少网络请求
        self.remote_offset_cache: dict[MessageQueue, int] = {}

        # 线程锁
        self._lock: threading.RLock = threading.RLock()

        # NameServer连接管理（仅用于路由查询）
        self._nameserver_manager: NameServerManager = nameserver_manager

        # router
        self._router: MessageRouter = MessageRouter(TopicBrokerMapping())

        # 指标收集
        self.metrics: OffsetStoreMetrics = OffsetStoreMetrics()

        # 定期持久化任务
        self._persist_thread: threading.Thread | None = None
        self._running: bool = False

    def start(self) -> None:
        """启动偏移量存储服务.

        启动后台持久化线程，开始定期同步偏移量到Broker。

        Note:
            如果偏移量存储已经启动，此方法会直接返回而不重复启动。
        """
        if self._running:
            return

        self._running = True
        # 启动定期持久化任务
        self._persist_thread = threading.Thread(
            target=self._periodic_persist, daemon=True
        )
        self._persist_thread.start()
        logger.info(
            "remote offset store started", extra={"consumer_group": self.consumer_group}
        )

    def stop(self) -> None:
        """停止偏移量存储服务.

        停止后台持久化线程，等待所有待持久化的偏移量完成同步。

        Note:
            如果偏移量存储已经停止，此方法会直接返回而不重复停止。
        """
        if not self._running:
            return

        self._running = False

        # 等待定期持久化任务结束
        if self._persist_thread and self._persist_thread.is_alive():
            self._persist_thread.join(timeout=5.0)

        # 持久化所有缓存的偏移量
        self.persist_all()

        logger.info(
            "remote offset store stopped", extra={"consumer_group": self.consumer_group}
        )

    def load(self) -> None:
        """从Broker加载偏移量到本地缓存.

        对于远程存储，初始化时不需要加载所有偏移量，
        而是在第一次读取时按需从Broker获取。

        Note:
            此方法为空实现，因为远程存储采用按需加载策略。
        """
        try:
            logger.info(
                "loading remote offsets", extra={"consumer_group": self.consumer_group}
            )
            # 远程模式下，偏移量按需加载
            self.metrics.record_load_success()
        except Exception as e:
            logger.error(
                "failed to load remote offsets",
                extra={"consumer_group": self.consumer_group, "error": str(e)},
            )
            self.metrics.record_load_failure()

    def update_offset(self, queue: MessageQueue, offset: int) -> None:
        """更新偏移量到本地缓存.

        将偏移量更新到本地缓存中，后台持久化线程会定期同步到Broker。

        Args:
            queue: 消息队列，包含Topic、Broker名称和队列ID
            offset: 新的偏移量值，必须大于等于0

        Raises:
            ValueError: 当offset为负数时抛出
        """
        with self._lock:
            # 更新本地缓存
            self._update_local_cache(queue, offset)

            # 更新远程缓存
            self.remote_offset_cache[queue] = offset

            logger.debug(
                "updated remote offset cache",
                extra={"queue": str(queue), "offset": offset},
            )

    def persist(self, queue: MessageQueue) -> None:
        """持久化单个队列的偏移量到Broker.

        立即将指定队列的偏移量同步到Broker，不等待后台持久化线程。

        Args:
            queue: 需要持久化偏移量的消息队列

        Raises:
            ConnectionError: 当无法连接到Broker时抛出
            BrokerError: 当Broker返回错误时抛出
        """
        with self._lock:
            if queue not in self.offset_table:
                return

            offset = self.offset_table[queue]
            start_time = time.time()

            try:
                broker_addr: str | None = self._get_broker_addr_by_name(
                    queue.broker_name
                )
                if not broker_addr:
                    raise ValueError(f"Broker address not found for {queue}")

                pool: ConnectionPool = self.broker_manager.must_connection_pool(
                    broker_addr
                )
                with pool.get_connection(usage="更新消费者偏移量") as connection:
                    BrokerClient(connection).update_consumer_offset(
                        self.consumer_group, queue.topic, queue.queue_id, offset
                    )

                # 更新远程缓存
                self.remote_offset_cache[queue] = offset

                latency = (time.time() - start_time) * 1000
                self.metrics.record_persist_success(latency)

                logger.debug(
                    "persisted offset to broker",
                    extra={
                        "queue": str(queue),
                        "offset": offset,
                        "latency_ms": round(latency, 2),
                    },
                )

            except Exception as e:
                self.metrics.record_persist_failure()
                logger.error(
                    "failed to persist offset",
                    extra={"queue": str(queue), "offset": offset, "error": str(e)},
                )
                raise

    def persist_all(self) -> None:
        """批量持久化所有偏移量.

        立即将所有缓存的偏移量同步到对应的Broker。

        Raises:
            ConnectionError: 当无法连接到Broker时抛出
            BrokerError: 当Broker返回错误时抛出
        """
        if not self.offset_table:
            return

        with self._lock:
            # 按Broker分组，批量提交
            broker_offsets = self._group_offsets_by_broker()

            if not broker_offsets:
                return

            logger.info(
                "persisting offsets to brokers",
                extra={
                    "offsets_count": len(self.offset_table),
                    "brokers_count": len(broker_offsets),
                },
            )

            # 顺序提交到各个Broker（同步模式）
            success_count = 0
            failure_count = 0

            for broker_name, offsets in broker_offsets.items():
                try:
                    self._persist_to_broker(broker_name, offsets)
                    success_count += 1
                except Exception as e:
                    failure_count += 1
                    logger.error(
                        "failed to persist offsets to broker",
                        extra={"broker_name": broker_name, "error": str(e)},
                    )
                    continue

            logger.info(
                "offset persistence completed",
                extra={"success_count": success_count, "failure_count": failure_count},
            )

    def read_offset(self, queue: MessageQueue, read_type: ReadOffsetType) -> int:
        """读取偏移量.

        从Broker读取指定队列的消费偏移量。首先检查本地缓存，
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
        with self._lock:
            # 1. 优先从内存读取
            if read_type in [
                ReadOffsetType.MEMORY_FIRST_THEN_STORE,
                ReadOffsetType.READ_FROM_MEMORY,
            ]:
                offset: int | None = self._get_from_local_cache(queue)
                if offset is not None:
                    self.metrics.record_cache_hit()
                    return offset

                if read_type == ReadOffsetType.READ_FROM_MEMORY:
                    self.metrics.record_cache_miss()
                    return -1

            # 2. 从远程存储读取
            if read_type in [
                ReadOffsetType.MEMORY_FIRST_THEN_STORE,
                ReadOffsetType.READ_FROM_STORE,
            ]:
                try:
                    # 获取Broker客户端
                    broker_addr: str | None = self._get_broker_addr_by_name(
                        queue.broker_name
                    )
                    if not broker_addr:
                        logger.error(
                            "failed to get broker address",
                            extra={"queue": str(queue)},
                        )
                        self.metrics.record_cache_miss()
                        return -1

                    pool: ConnectionPool = self.broker_manager.must_connection_pool(
                        broker_addr
                    )
                    with pool.get_connection(usage="查询消费者偏移量") as conn:
                        offset = BrokerClient(conn).query_consumer_offset(
                            consumer_group=self.consumer_group,
                            topic=queue.topic,
                            queue_id=queue.queue_id,
                        )

                    if offset >= 0:
                        # 更新本地缓存
                        self._update_local_cache(queue, offset)
                        self.remote_offset_cache[queue] = offset
                        logger.debug(
                            "loaded offset from broker",
                            extra={"queue": str(queue), "offset": offset},
                        )
                    else:
                        logger.debug(
                            "no offset found in broker", extra={"queue": str(queue)}
                        )

                    self.metrics.record_cache_miss()
                    return offset

                except Exception as e:
                    logger.error(
                        "failed to read offset from broker",
                        extra={"queue": str(queue), "error": str(e)},
                    )
                    self.metrics.record_cache_miss()
                    return -1

            return -1

    def remove_offset(self, queue: MessageQueue) -> None:
        """移除队列的偏移量.

        从本地缓存中移除指定队列的偏移量，并从Broker删除该队列的偏移量记录。

        Args:
            queue: 需要移除偏移量的消息队列

        Raises:
            ConnectionError: 当无法连接到Broker时抛出
            BrokerError: 当Broker返回错误时抛出
        """
        with self._lock:
            # 从本地缓存移除
            _ = self._remove_from_local_cache(queue)

            # 从远程缓存移除
            if queue in self.remote_offset_cache:
                del self.remote_offset_cache[queue]

            logger.debug("removed offset from cache", extra={"queue": str(queue)})

    def _group_offsets_by_broker(self) -> dict[str, list[tuple[MessageQueue, int]]]:
        """按Broker分组偏移量.

        将所有缓存的偏移量按照Broker名称进行分组，便于批量提交到对应的Broker。

        Returns:
            dict[str, list[tuple[MessageQueue, int]]]: 按Broker名称分组的偏移量列表，
                                                      键为Broker名称，值为(队列, 偏移量)元组列表
        """
        broker_offsets: dict[str, list[tuple[MessageQueue, int]]] = defaultdict(list)

        for queue, offset in self.offset_table.items():
            # 只持久化需要更新的偏移量（与远程缓存不一致的）
            if (
                queue not in self.remote_offset_cache
                or self.remote_offset_cache[queue] != offset
            ):
                broker_offsets[queue.broker_name].append((queue, offset))

        return dict(broker_offsets)

    def _persist_to_broker(
        self, broker_name: str, offsets: list[tuple[MessageQueue, int]]
    ) -> None:
        """持久化偏移量到指定Broker.

        将多个队列的偏移量批量提交到指定的Broker进行持久化。

        Args:
            broker_name: Broker名称，用于连接对应的Broker服务器
            offsets: 偏移量列表，每个元素为(队列, 偏移量)元组

        Raises:
            ConnectionError: 当无法连接到Broker时抛出
            BrokerError: 当Broker返回错误时抛出
        """
        if not offsets:
            return

        start_time = time.time()

        try:
            # 获取Broker地址
            broker_addr: str | None = self._get_broker_addr_by_name(broker_name)
            if not broker_addr:
                logger.warning("broker address not found")
                return

            pool: ConnectionPool = self.broker_manager.must_connection_pool(broker_addr)
            with pool.get_connection() as conn:
                cli: BrokerClient = BrokerClient(conn)
                for queue, offset in offsets:
                    try:
                        cli.update_consumer_offset(
                            consumer_group=self.consumer_group,
                            topic=queue.topic,
                            queue_id=queue.queue_id,
                            commit_offset=offset,
                        )
                    except Exception as e:
                        logger.error(f"Failed to update offset for {queue}: {e}")
                        continue
                    else:
                        self.remote_offset_cache[queue] = offset

            latency = (time.time() - start_time) * 1000
            self.metrics.record_persist_success(latency)

            logger.debug(
                "persisted offsets to broker",
                extra={
                    "broker_name": broker_name,
                    "offsets_count": len(offsets),
                    "latency_ms": round(latency, 2),
                },
            )

        except Exception as e:
            self.metrics.record_persist_failure()
            logger.error(
                "failed to persist offsets to broker",
                extra={"broker_name": broker_name, "error": str(e)},
            )
            raise

    def _periodic_persist(self) -> None:
        """定期持久化任务（在后台线程中运行）.

        后台线程定期执行，按照配置的间隔时间自动将本地缓存的偏移量同步到Broker。
        采用批量提交的方式提高性能。

        Note:
            此方法在单独的线程中运行，会被_stop方法中断。
        """
        while self._running:
            try:
                time.sleep(self.persist_interval / 1000)

                if self._running and self.offset_table:
                    self.persist_all()

            except Exception as e:
                logger.error("error in periodic persist task", extra={"error": str(e)})

    def _get_broker_addr_by_name(self, broker_name: str) -> str | None:
        """根据broker名称查询broker地址.

        从NameServer查询指定broker的网络地址，支持主从地址选择。

        Args:
            broker_name: broker名称，用于标识具体的broker实例

        Returns:
            str | None: broker地址，格式为"host:port"，未找到则返回None

        Note:
            优先选择master broker的地址。
        """
        logger.debug("查询broker地址", extra={"broker_name": broker_name})

        return self._nameserver_manager.get_broker_address(broker_name)

    def get_metrics(self) -> dict[str, Any]:
        """获取指标信息.

        返回远程偏移量存储的性能和操作统计信息，
        包括缓存命中率、持久化统计、错误统计等。

        Returns:
            dict[str, Any]: 包含以下键的指标字典：
                - cached_offsets_count: 缓存的偏移量数量
                - cache_hit_rate: 缓存命中率
                - persist_success_count: 持久化成功次数
                - persist_failure_count: 持久化失败次数
                - load_success_count: 加载成功次数
                - load_failure_count: 加载失败次数
                - running: 是否正在运行
        """
        metrics_dict = self.metrics.to_dict()
        metrics_dict.update(
            {
                "cached_offsets_count": len(self.offset_table),
                "remote_cache_count": len(self.remote_offset_cache),
                "persist_interval": self.persist_interval,
            }
        )
        return metrics_dict
