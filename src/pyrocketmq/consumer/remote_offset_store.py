"""
RemoteOffsetStore - 集群模式偏移量存储

集群模式下的远程偏移量存储实现，偏移量存储在Broker服务器上，
支持多Consumer实例协同消费。这是RocketMQ的标准实现方式。

特点：
- 偏移量存储在Broker服务器上
- 支持多Consumer实例协同消费
- 使用本地缓存减少网络请求
- 批量提交提升性能
"""

import threading
import time
from collections import defaultdict
from typing import Any

from typing_extensions import override

from pyrocketmq.broker import BrokerClient, BrokerManager
from pyrocketmq.consumer.errors import NameServerError
from pyrocketmq.consumer.offset_store import (
    OffsetStore,
    OffsetStoreMetrics,
    ReadOffsetType,
)
from pyrocketmq.logging import get_logger
from pyrocketmq.model import MessageQueue
from pyrocketmq.nameserver.client import SyncNameServerClient
from pyrocketmq.nameserver.models import BrokerClusterInfo
from pyrocketmq.producer.router import MessageRouter
from pyrocketmq.producer.topic_broker_mapping import TopicBrokerMapping
from pyrocketmq.remote.sync_remote import Remote

logger = get_logger(__name__)


class RemoteOffsetStore(OffsetStore):
    """集群模式下的远程偏移量存储"""

    def __init__(
        self,
        namesrv_addr: str,
        consumer_group: str,
        broker_manager: BrokerManager,
        persist_interval: int = 5000,
        persist_batch_size: int = 10,
    ) -> None:
        """
        初始化远程偏移量存储

        Args:
            consumer_group: 消费者组名称
            broker_manager: Broker管理器
            persist_interval: 持久化间隔（毫秒）
            persist_batch_size: 批量提交大小
        """
        super().__init__(consumer_group)
        self.broker_manager: BrokerManager = broker_manager
        self.persist_interval: int = persist_interval
        self.persist_batch_size: int = persist_batch_size

        # 远程偏移量缓存，减少网络请求
        self.remote_offset_cache: dict[MessageQueue, int] = {}

        # 线程锁
        self._lock: threading.RLock = threading.RLock()

        # NameServer连接管理（仅用于路由查询）
        self._nameserver_connections: dict[str, Remote] = {}
        self._nameserver_addrs: dict[str, str] = self._parse_nameserver_addrs(
            namesrv_addr
        )

        # router
        self._router: MessageRouter = MessageRouter(TopicBrokerMapping())

        # 指标收集
        self.metrics: OffsetStoreMetrics = OffsetStoreMetrics()

        # 定期持久化任务
        self._persist_thread: threading.Thread | None = None
        self._running: bool = False

        # broker地址缓存，避免频繁查询NameServer
        self._broker_addr_cache: dict[
            str, tuple[str, float]
        ] = {}  # broker_name -> (address, timestamp)
        self._broker_cache_ttl: int = 300  # 缓存TTL：5分钟

    def start(self) -> None:
        """启动偏移量存储服务"""
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
        """停止偏移量存储服务"""
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

    @override
    def load(self) -> None:
        """
        从Broker加载偏移量到本地缓存

        对于远程存储，初始化时不需要加载所有偏移量，
        而是在第一次读取时按需从Broker获取。
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

    @override
    def update_offset(self, queue: MessageQueue, offset: int) -> None:
        """
        更新偏移量到本地缓存

        Args:
            queue: 消息队列
            offset: 偏移量
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

    @override
    def persist(self, queue: MessageQueue) -> None:
        """
        持久化单个队列的偏移量到Broker

        Args:
            queue: 消息队列
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

                with self.broker_manager.connection(broker_addr) as connection:
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

    @override
    def persist_all(self) -> None:
        """批量持久化所有偏移量"""
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

    @override
    def read_offset(self, queue: MessageQueue, read_type: ReadOffsetType) -> int:
        """
        读取偏移量

        Args:
            queue: 消息队列
            read_type: 读取类型

        Returns:
            int: 偏移量，如果不存在返回-1
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

                    with self.broker_manager.connection(broker_addr) as conn:
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

    @override
    def remove_offset(self, queue: MessageQueue) -> None:
        """
        移除队列的偏移量

        Args:
            queue: 消息队列
        """
        with self._lock:
            # 从本地缓存移除
            _ = self._remove_from_local_cache(queue)

            # 从远程缓存移除
            if queue in self.remote_offset_cache:
                del self.remote_offset_cache[queue]

            logger.debug("removed offset from cache", extra={"queue": str(queue)})

    def _group_offsets_by_broker(self) -> dict[str, list[tuple[MessageQueue, int]]]:
        """
        按Broker分组偏移量

        Returns:
            dict[str, list[tuple[MessageQueue, int]]]: 按Broker名称分组的偏移量列表
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
        """
        持久化偏移量到指定Broker

        Args:
            broker_name: Broker名称
            offsets: 偏移量列表
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

            with self.broker_manager.connection(broker_addr) as conn:
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
        """定期持久化任务（在后台线程中运行）"""
        while self._running:
            try:
                time.sleep(self.persist_interval / 1000)

                if self._running and self.offset_table:
                    self.persist_all()

            except Exception as e:
                logger.error("error in periodic persist task", extra={"error": str(e)})

    def _parse_nameserver_addrs(self, namesrv_addr: str) -> dict[str, str]:
        """解析NameServer地址列表

        Args:
            namesrv_addr: NameServer地址，格式为"host1:port1;host2:port2"

        Returns:
            dict[str, str]: 地址字典 {addr: host:port}
        """
        addrs: dict[str, str] = {}
        for addr in namesrv_addr.split(";"):
            addr = addr.strip()
            if addr:
                addrs[addr] = addr
        return addrs

    def _get_broker_addr_by_name(self, broker_name: str) -> str | None:
        """根据broker名称查询broker地址

        通过查询NameServer获取指定broker名称的地址信息。

        Args:
            broker_name: 要查询的broker名称

        Returns:
            str | None: 找到的broker地址，格式为"host:port"，未找到则返回None

        Raises:
            NameServerError: 当NameServer连接不可用或查询失败时抛出
        """
        logger.debug("查询broker地址", extra={"broker_name": broker_name})

        # 先检查缓存
        with self._lock:
            if broker_name in self._broker_addr_cache:
                address, timestamp = self._broker_addr_cache[broker_name]
                # 检查缓存是否过期
                if time.time() - timestamp < self._broker_cache_ttl:
                    logger.debug(
                        "使用缓存的broker地址",
                        extra={"broker_name": broker_name, "address": address},
                    )
                    return address
                else:
                    # 缓存过期，删除
                    del self._broker_addr_cache[broker_name]
                    logger.debug(
                        "broker地址缓存过期", extra={"broker_name": broker_name}
                    )

        if not self._nameserver_connections:
            raise NameServerError("", "NameServer连接不可用，无法查询broker地址")

        for addr, remote in self._nameserver_connections.items():
            try:
                # 使用NameServer客户端查询路由信息
                client: SyncNameServerClient = SyncNameServerClient(remote)

                # 查询Topic路由信息
                cluster_info: BrokerClusterInfo = client.get_broker_cluster_info()

                # 在路由数据中查找目标broker
                for name, broker_data in cluster_info.broker_addr_table.items():
                    if name == broker_name:
                        address = self._router.select_broker_address(broker_data)

                        if not address:
                            continue

                        # 更新缓存
                        with self._lock:
                            self._broker_addr_cache[broker_name] = (
                                address,
                                time.time(),
                            )
                            logger.debug(
                                "缓存broker地址",
                                extra={"broker_name": broker_name, "address": address},
                            )

                        return address

            except Exception as e:
                logger.warning(
                    "从NameServer查询失败", extra={"addr": addr, "error": str(e)}
                )
                continue

        logger.warning("未找到broker地址信息", extra={"broker_name": broker_name})
        return None

    def get_metrics(self) -> dict[str, Any]:
        """获取指标信息"""
        metrics_dict = self.metrics.to_dict()
        metrics_dict.update(
            {
                "cached_offsets_count": len(self.offset_table),
                "remote_cache_count": len(self.remote_offset_cache),
                "persist_interval": self.persist_interval,
                "persist_batch_size": self.persist_batch_size,
            }
        )
        return metrics_dict
