"""
LocalOffsetStore - 广播模式偏移量存储

广播模式下的本地偏移量存储实现，偏移量存储在本地文件中，
每个Consumer实例独立消费所有消息。

特点：
- 偏移量存储在本地文件中
- 每个Consumer实例独立消费所有消息
- 文件格式：topic@brokerName:queueId=offset
- 线程安全的文件读写操作
"""

import json
import os
import threading
import time
from pathlib import Path
from typing import Any, override

from pyrocketmq.consumer.offset_store import (
    OffsetEntry,
    OffsetStore,
    OffsetStoreMetrics,
    ReadOffsetType,
)
from pyrocketmq.logging import get_logger
from pyrocketmq.model import MessageQueue

logger = get_logger(__name__)


class LocalOffsetStore(OffsetStore):
    """广播模式下的本地偏移量存储"""

    def __init__(
        self,
        consumer_group: str,
        store_path: str = "~/.rocketmq/offsets",
        persist_interval: int = 5000,
        persist_batch_size: int = 10,
    ) -> None:
        """
        初始化本地偏移量存储

        Args:
            consumer_group: 消费者组名称
            store_path: 存储路径
            persist_interval: 持久化间隔（毫秒）
            persist_batch_size: 批量提交大小
        """
        super().__init__(consumer_group)

        # 本地存储路径
        self.store_path: Path = Path(os.path.expanduser(store_path))
        self.store_path.mkdir(parents=True, exist_ok=True)

        # 偏移量文件路径
        self.offset_file_path: Path = (
            self.store_path / f"{self.consumer_group}.offsets.json"
        )

        # 配置参数
        self.persist_interval: int = persist_interval
        self.persist_batch_size: int = persist_batch_size

        # 指标收集
        self.metrics: OffsetStoreMetrics = OffsetStoreMetrics()

        # 定期持久化任务
        self._persist_thread: threading.Thread | None = None
        self._running: bool = False
        self._stop_event: threading.Event = threading.Event()

    @override
    def start(self) -> None:
        """启动偏移量存储服务"""
        if self._running:
            return

        self._running = True
        self._stop_event.clear()

        # 启动定期持久化线程
        self._persist_thread = threading.Thread(
            target=self._periodic_persist, daemon=True
        )
        self._persist_thread.start()

        logger.info(
            "LocalOffsetStore started",
            extra={
                "consumer_group": self.consumer_group,
                "store_path": str(self.offset_file_path),
            },
        )

    @override
    def stop(self) -> None:
        """停止偏移量存储服务"""
        if not self._running:
            return

        self._running = False
        self._stop_event.set()

        # 等待持久化线程结束
        if self._persist_thread and self._persist_thread.is_alive():
            self._persist_thread.join(timeout=5)

        # 持久化所有缓存的偏移量
        self.persist_all()

        logger.info(
            "LocalOffsetStore stopped", extra={"consumer_group": self.consumer_group}
        )

    @override
    def load(self) -> None:
        """从本地文件加载偏移量"""
        try:
            logger.info(
                "Loading local offsets",
                extra={"offset_file_path": str(self.offset_file_path)},
            )

            if not self.offset_file_path.exists():
                logger.info("Offset file does not exist, starting with empty offsets")
                self.metrics.record_load_success()
                return

            start_time: float = time.time()

            with self._lock:
                with open(self.offset_file_path, "r", encoding="utf-8") as f:
                    try:
                        data: dict[str, Any] = json.load(f)

                        # 加载偏移量条目
                        loaded_count: int = 0
                        for entry_data in data.get("offsets", []):
                            try:
                                offset_entry: OffsetEntry = OffsetEntry.from_dict(
                                    entry_data
                                )
                                self.offset_table[offset_entry.queue] = (
                                    offset_entry.offset
                                )
                                loaded_count += 1
                            except Exception as e:
                                logger.warning(
                                    f"Failed to load offset entry: {entry_data}, error: {e}"
                                )

                        load_time: float = (time.time() - start_time) * 1000
                        logger.info(
                            "Offsets loaded",
                            extra={
                                "loaded_count": loaded_count,
                                "load_time": load_time,
                            },
                        )
                        self.metrics.record_load_success()

                    except json.JSONDecodeError as e:
                        logger.error(f"Invalid JSON in offset file: {e}")
                        self.metrics.record_load_failure()
                        raise

        except Exception as e:
            logger.error(f"Failed to load offset file: {e}")
            self.metrics.record_load_failure()
            raise

    @override
    def update_offset(self, queue: MessageQueue, offset: int) -> None:
        """
        更新偏移量到内存缓存

        Args:
            queue: 消息队列
            offset: 偏移量
        """
        with self._lock:
            self._update_local_cache(queue, offset)
            logger.debug(
                "Local offset cache updated",
                extra={"queue": str(queue), "offset": offset},
            )

    @override
    def persist(self, queue: MessageQueue) -> None:
        """
        持久化单个队列的偏移量到本地文件

        Args:
            queue: 消息队列
        """
        with self._lock:
            if queue not in self.offset_table:
                return

            offset: int = self.offset_table[queue]
            start_time: float = time.time()

            try:
                # 创建偏移量条目
                offset_entry: OffsetEntry = OffsetEntry(queue=queue, offset=offset)

                # 构建文件数据
                data: dict[str, Any] = self._build_file_data()

                # 查找并更新对应条目
                updated: bool = False
                for entry_data in data["offsets"]:
                    if (
                        entry_data["topic"] == queue.topic
                        and entry_data["broker_name"] == queue.broker_name
                        and entry_data["queue_id"] == queue.queue_id
                    ):
                        entry_data.update(offset_entry.to_dict())
                        updated = True
                        break

                # 如果不存在，添加新条目
                if not updated:
                    data["offsets"].append(offset_entry.to_dict())

                # 写入文件
                self._write_to_file(data)

                latency: float = (time.time() - start_time) * 1000
                self.metrics.record_persist_success(latency)

                logger.debug(
                    "Offset persisted to file",
                    extra={"queue": str(queue), "offset": offset, "latency": latency},
                )

            except Exception as e:
                self.metrics.record_persist_failure()
                logger.error(f"Failed to persist offset {queue} -> {offset}: {e}")
                raise

    @override
    def persist_all(self) -> None:
        """持久化所有偏移量到本地文件"""
        if not self.offset_table:
            return

        with self._lock:
            start_time: float = time.time()

            try:
                # 构建文件数据
                data: dict[str, Any] = self._build_file_data()

                # 写入文件
                self._write_to_file(data)

                latency: float = (time.time() - start_time) * 1000
                self.metrics.record_persist_success(latency)

                logger.info(
                    "All offsets persisted to file",
                    extra={"offset_count": len(self.offset_table), "latency": latency},
                )

            except Exception as e:
                self.metrics.record_persist_failure()
                logger.error(f"Failed to persist all offsets: {e}")
                raise

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
                _offset: int | None = self._get_from_local_cache(queue)
                if _offset is not None:
                    self.metrics.record_cache_hit()
                    return _offset

                if read_type == ReadOffsetType.READ_FROM_MEMORY:
                    self.metrics.record_cache_miss()
                    return -1

            # 2. 从文件读取
            if read_type in [
                ReadOffsetType.MEMORY_FIRST_THEN_STORE,
                ReadOffsetType.READ_FROM_STORE,
            ]:
                try:
                    offset: int = self._read_from_file(queue)
                    if offset >= 0:
                        # 更新本地缓存
                        self._update_local_cache(queue, offset)
                        logger.debug(
                            "Offset loaded from file",
                            extra={"queue": str(queue), "offset": offset},
                        )
                    else:
                        logger.debug(
                            "No offset found in file", extra={"queue": str(queue)}
                        )

                    self.metrics.record_cache_miss()
                    return offset

                except Exception as e:
                    logger.error(f"Failed to read offset from file {queue}: {e}")
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
            # 从内存缓存移除
            _ = self._remove_from_local_cache(queue)

            try:
                # 从文件中移除
                if self.offset_file_path.exists():
                    data: dict[str, Any] = self._read_file()

                    # 移除对应条目
                    data["offsets"] = [
                        entry
                        for entry in data["offsets"]
                        if not (
                            entry["topic"] == queue.topic
                            and entry["broker_name"] == queue.broker_name
                            and entry["queue_id"] == queue.queue_id
                        )
                    ]

                    # 写回文件
                    self._write_to_file(data)

                logger.debug("Offset removed from file", extra={"queue": str(queue)})

            except Exception as e:
                logger.error(f"Failed to remove offset from file {queue}: {e}")

    def _read_from_file(self, queue: MessageQueue) -> int:
        """从文件读取指定队列的偏移量"""
        if not self.offset_file_path.exists():
            return -1

        try:
            data: dict[str, Any] = self._read_file()

            for entry_data in data.get("offsets", []):
                if (
                    entry_data["topic"] == queue.topic
                    and entry_data["broker_name"] == queue.broker_name
                    and entry_data["queue_id"] == queue.queue_id
                ):
                    return entry_data["offset"]

            return -1

        except Exception as e:
            logger.error(f"Failed to read offset from file for {queue}: {e}")
            return -1

    def _read_file(self) -> dict[str, Any]:
        """读取偏移量文件"""
        with open(self.offset_file_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def _write_to_file(self, data: dict[str, Any]) -> None:
        """写入偏移量文件"""
        # 先写入临时文件，然后原子性重命名
        temp_file: Path = self.offset_file_path.with_suffix(".tmp")

        try:
            with open(temp_file, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)

            # 原子性重命名
            _ = temp_file.replace(self.offset_file_path)

        except Exception as _:
            # 清理临时文件
            if temp_file.exists():
                temp_file.unlink()
            raise

    def _build_file_data(self) -> dict[str, Any]:
        """构建文件数据结构"""
        offsets: list[dict[str, Any]] = []

        for queue, offset in self.offset_table.items():
            offset_entry: OffsetEntry = OffsetEntry(queue=queue, offset=offset)
            offsets.append(offset_entry.to_dict())

        return {
            "consumer_group": self.consumer_group,
            "offsets": offsets,
            "last_update_time": int(time.time() * 1000),
            "version": "1.0",
        }

    def _periodic_persist(self) -> None:
        """定期持久化线程函数"""
        while self._running and not self._stop_event.wait(self.persist_interval / 1000):
            try:
                if self._running and self.offset_table:
                    # 直接调用同步方法
                    self.persist_all()

            except Exception as e:
                logger.error(f"Error in periodic persist thread: {e}")

    def get_metrics(self) -> dict[str, Any]:
        """获取指标信息"""
        metrics_dict: dict[str, Any] = self.metrics.to_dict()
        metrics_dict.update(
            {
                "cached_offsets_count": len(self.offset_table),
                "store_path": str(self.offset_file_path),
                "persist_interval": self.persist_interval,
                "persist_batch_size": self.persist_batch_size,
            }
        )
        return metrics_dict

    def get_file_info(self) -> dict[str, Any]:
        """获取文件信息"""
        info: dict[str, Any] = {
            "file_path": str(self.offset_file_path),
            "file_exists": self.offset_file_path.exists(),
            "file_size": 0,
            "last_modified": None,
        }

        if self.offset_file_path.exists():
            stat = self.offset_file_path.stat()
            info.update({"file_size": stat.st_size, "last_modified": stat.st_mtime})

        return info
