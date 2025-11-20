"""
AsyncLocalOffsetStore - 异步本地偏移量存储

广播模式下的异步本地偏移量存储实现，偏移量存储在本地文件中，
每个Consumer实例独立消费所有消息。完全基于异步IO实现，
避免阻塞事件循环。

特点：
- 完全异步的文件读写操作，基于aiofiles
- 每个Consumer实例独立消费所有消息
- 支持高并发场景下的非阻塞IO
- 智能文件缓冲和批量写入优化
- 线程安全的异步文件操作
- 支持文件轮转和压缩
- 详细的性能监控和错误处理

设计原则：
- 异步优先：所有IO操作都是异步的
- 高性能：批量操作、文件缓冲、智能压缩
- 可靠性：原子性文件操作、错误恢复
- 可监控：详细的性能指标和诊断信息
"""

import asyncio
import json
import os
import time
from pathlib import Path
from typing import Any

import aiofiles
import aiofiles.os

from pyrocketmq.consumer.async_offset_store import (
    AsyncOffsetEntry,
    AsyncOffsetStore,
    ReadOffsetType,
)
from pyrocketmq.consumer.errors import OffsetError
from pyrocketmq.logging import get_logger
from pyrocketmq.model import MessageQueue

logger = get_logger(__name__)


class AsyncLocalOffsetStore(AsyncOffsetStore):
    """广播模式下的异步本地偏移量存储

    基于aiofiles提供完全异步的本地文件存储实现，
    支持高并发场景下的非阻塞偏移量管理。

    主要特性：
    - 异步文件读写，避免阻塞事件循环
    - 智能文件缓冲，减少磁盘IO
    - 原子性文件操作，保证数据一致性
    - 支持文件压缩和轮转
    - 详细的性能监控和错误统计

    配置选项：
    - store_path: 存储路径，默认"~/.rocketmq/async_offsets"
    - persist_interval: 后台持久化间隔，默认5秒
    - file_rotation: 是否启用文件轮转，默认True
    - max_file_size: 单文件最大大小，默认100MB
    - compression: 是否启用压缩，默认False
    - buffer_size: 文件缓冲区大小，默认8KB
    """

    def __init__(
        self,
        consumer_group: str,
        store_path: str = "~/.rocketmq/async_offsets",
        persist_interval: float = 5.0,
        file_rotation: bool = True,
        max_file_size: int = 100 * 1024 * 1024,  # 100MB
        compression: bool = False,
        buffer_size: int = 8192,  # 8KB
        enable_file_cache: bool = True,
    ) -> None:
        """初始化异步本地偏移量存储

        Args:
            consumer_group: 消费者组名称
            store_path: 存储路径
            persist_interval: 后台持久化间隔（秒）
            file_rotation: 是否启用文件轮转
            max_file_size: 单文件最大大小（字节）
            compression: 是否启用数据压缩
            buffer_size: 文件缓冲区大小（字节）
            enable_file_cache: 是否启用文件缓存
        """
        super().__init__(consumer_group)

        # 本地存储路径配置
        self.store_path: Path = Path(os.path.expanduser(store_path))
        self.store_path.mkdir(parents=True, exist_ok=True)

        # 偏移量文件路径
        self.offset_file_path: Path = (
            self.store_path / f"{self.consumer_group}.offsets.json"
        )
        self.lock_file_path: Path = (
            self.store_path / f"{self.consumer_group}.offsets.lock"
        )

        # 备份和临时文件路径
        self.backup_file_path: Path = (
            self.store_path / f"{self.consumer_group}.offsets.bak"
        )
        self.temp_file_path: Path = (
            self.store_path / f"{self.consumer_group}.offsets.tmp"
        )

        # 配置参数
        self.persist_interval: float = persist_interval
        self.file_rotation: bool = file_rotation
        self.max_file_size: int = max_file_size
        self.compression: bool = compression
        self.buffer_size: int = buffer_size
        self.enable_file_cache: bool = enable_file_cache

        # 异步文件缓存
        self._file_cache: dict[str, Any] | None = None
        self._cache_dirty: bool = False
        self._cache_lock: asyncio.Lock = asyncio.Lock()

        # 文件轮转计数
        self._rotation_count: int = 0
        self._current_file_index: int = 0

        # 文件操作信号量（限制并发文件操作）
        self._file_semaphore: asyncio.Semaphore = asyncio.Semaphore(10)

        logger.info(
            "AsyncLocalOffsetStore initialized",
            extra={
                "consumer_group": self.consumer_group,
                "store_path": str(self.store_path),
                "persist_interval": self.persist_interval,
                "file_rotation": self.file_rotation,
                "compression": self.compression,
                "buffer_size": self.buffer_size,
            },
        )

    async def start(self) -> None:
        """异步启动偏移量存储服务"""
        if self._started:
            logger.warning("AsyncLocalOffsetStore already started")
            return

        try:
            # 确保目录存在
            await aiofiles.os.makedirs(self.store_path, exist_ok=True)

            # 加载现有数据
            await self.load()

            # 启动后台持久化任务
            await self._start_background_persist()

            self._started = True

            logger.info(
                "AsyncLocalOffsetStore started successfully",
                extra={
                    "consumer_group": self.consumer_group,
                    "offset_file": str(self.offset_file_path),
                    "cache_enabled": self.enable_file_cache,
                },
            )

        except Exception as e:
            logger.error(f"Failed to start AsyncLocalOffsetStore: {e}")
            await self.metrics.record_load_failure()
            raise OffsetError("", 0, 0, f"Start failed: {e}", e) from e

    async def stop(self) -> None:
        """异步停止偏移量存储服务"""
        if not self._started:
            return

        try:
            self._started = False

            # 停止后台持久化任务
            await self._stop_background_persist()

            # 持久化所有缓存的偏移量
            if self._pending_persist or self._cache_dirty:
                await self.persist_all()

            # 清理文件缓存
            async with self._cache_lock:
                self._file_cache = None
                self._cache_dirty = False

            logger.info(
                "AsyncLocalOffsetStore stopped successfully",
                extra={"consumer_group": self.consumer_group},
            )

        except Exception as e:
            logger.error(f"Error during AsyncLocalOffsetStore shutdown: {e}")
            raise OffsetError("", 0, 0, f"Stop failed: {e}", e) from e

    async def load(self) -> None:
        """异步从本地文件加载偏移量"""
        start_time = time.time()

        async with self._file_semaphore:
            try:
                logger.info(
                    "Loading async local offsets",
                    extra={"offset_file_path": str(self.offset_file_path)},
                )

                # 检查文件是否存在
                if not await aiofiles.os.path.exists(self.offset_file_path):
                    logger.info(
                        "Offset file does not exist, starting with empty offsets"
                    )
                    await self.metrics.record_load_success()
                    return

                # 异步读取文件
                async with aiofiles.open(
                    self.offset_file_path,
                    "r",
                    encoding="utf-8",
                    buffering=self.buffer_size,
                ) as f:
                    content = await f.read()

                if not content.strip():
                    logger.info("Offset file is empty, starting with empty offsets")
                    await self.metrics.record_load_success()
                    return

                # 解析JSON数据
                try:
                    data: dict[str, Any] = json.loads(content)
                except json.JSONDecodeError as e:
                    logger.error(f"Invalid JSON in offset file: {e}")
                    # 尝试从备份文件恢复
                    if await self._try_load_from_backup():
                        await self.metrics.record_load_success()
                        return
                    else:
                        await self.metrics.record_load_failure()
                        raise OffsetError(
                            "", 0, 0, f"Invalid JSON in offset file: {e}", e
                        ) from e

                # 加载偏移量条目
                loaded_count: int = 0
                async with self._lock:
                    for entry_data in data.get("offsets", []):
                        try:
                            # 使用AsyncOffsetEntry解析
                            offset_entry: AsyncOffsetEntry = AsyncOffsetEntry.from_dict(
                                entry_data
                            )
                            self.offset_table[offset_entry.queue] = offset_entry.offset
                            loaded_count += 1
                        except Exception as e:
                            logger.warning(
                                f"Failed to load offset entry: {entry_data}, error: {e}"
                            )

                    # 更新文件缓存
                    if self.enable_file_cache:
                        self._file_cache = data
                        self._cache_dirty = False

                load_time = (time.time() - start_time) * 1000
                await self.metrics.record_load_success()

                logger.info(
                    "Async offsets loaded successfully",
                    extra={
                        "loaded_count": loaded_count,
                        "load_time": load_time,
                        "file_size": len(content.encode("utf-8")),
                    },
                )

            except Exception as e:
                logger.error(f"Failed to load async offset file: {e}")
                await self.metrics.record_load_failure()
                raise OffsetError("", 0, 0, f"Load failed: {e}", e) from e

    async def persist(self, queue: MessageQueue) -> None:
        """异步持久化单个队列的偏移量到本地文件"""
        if not await self._should_persist():
            return

        async with self._file_semaphore:
            start_time: float = time.time()
            offset: int = 0

            try:
                async with self._lock:
                    if queue not in self.offset_table:
                        return

                    offset = self.offset_table[queue]

                    # 创建异步偏移量条目
                    offset_entry: AsyncOffsetEntry = AsyncOffsetEntry(
                        queue=queue, offset=offset
                    )

                    # 获取或创建文件数据
                    file_data: dict[str, Any] = await self._get_or_create_file_data()

                    # 查找并更新对应条目
                    updated: bool = False
                    for entry_data in file_data["offsets"]:
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
                        file_data["offsets"].append(offset_entry.to_dict())

                    # 异步写入文件
                    await self._write_to_file_async(file_data)

                    # 更新缓存
                    if self.enable_file_cache:
                        async with self._cache_lock:
                            self._file_cache = file_data
                            self._cache_dirty = False

                    latency: float = (time.time() - start_time) * 1000
                    await self.metrics.record_persist_success(latency)

                    logger.debug(
                        "Single offset persisted to async file",
                        extra={
                            "queue": str(queue),
                            "offset": offset,
                            "latency": latency,
                        },
                    )

            except Exception as e:
                await self.metrics.record_persist_failure()
                logger.error(f"Failed to async persist offset {queue} -> {offset}: {e}")
                raise OffsetError(
                    queue.topic, queue.queue_id, offset, f"Persist failed: {e}", e
                ) from e

    async def persist_all(self) -> None:
        """异步持久化所有偏移量到本地文件"""
        if not self.offset_table:
            return

        async with self._file_semaphore:
            start_time: float = time.time()

            try:
                # 构建文件数据
                file_data: dict[str, Any] = await self._build_file_data()

                # 检查是否需要文件轮转
                if self.file_rotation:
                    await self._check_file_rotation(file_data)

                # 异步写入文件
                await self._write_to_file_async(file_data)

                # 更新缓存和标记
                async with self._cache_lock:
                    self._file_cache = file_data
                    self._cache_dirty = False

                # 清除待持久化标记
                async with self._lock:
                    self._pending_persist.clear()

                latency: float = (time.time() - start_time) * 1000
                await self.metrics.record_persist_success(latency)

                logger.info(
                    "All async offsets persisted to file",
                    extra={
                        "offset_count": len(self.offset_table),
                        "latency": latency,
                        "file_rotation": self.file_rotation,
                    },
                )

            except Exception as e:
                await self.metrics.record_persist_failure()
                logger.error(f"Failed to async persist all offsets: {e}")
                raise OffsetError("", 0, 0, f"Persist all failed: {e}", e) from e

    async def read_offset(self, queue: MessageQueue, read_type: ReadOffsetType) -> int:
        """异步读取偏移量"""
        start_time = time.time()

        try:
            # 1. 优先从内存读取
            if read_type in [
                ReadOffsetType.MEMORY_FIRST_THEN_STORE,
                ReadOffsetType.READ_FROM_MEMORY,
            ]:
                async with self._lock:
                    offset: int | None = self.offset_table.get(queue)

                if offset is not None:
                    await self.metrics.record_cache_hit()
                    return offset

                if read_type == ReadOffsetType.READ_FROM_MEMORY:
                    await self.metrics.record_cache_miss()
                    return -1

            # 2. 从文件读取
            if read_type in [
                ReadOffsetType.MEMORY_FIRST_THEN_STORE,
                ReadOffsetType.READ_FROM_STORE,
            ]:
                offset = await self._read_from_file_async(queue)
                if offset >= 0:
                    # 更新本地缓存
                    await self.update_offset(queue, offset)

                await self.metrics.record_cache_miss()
                return offset

            return -1

        except Exception as e:
            logger.error(f"Failed to async read offset from file {queue}: {e}")
            await self.metrics.record_cache_miss()
            return -1

        finally:
            latency = (time.time() - start_time) * 1000
            logger.debug(
                "Async offset read completed",
                extra={
                    "queue": str(queue),
                    "read_type": read_type.value,
                    "latency": latency,
                },
            )

    async def remove_offset(self, queue: MessageQueue) -> None:
        """异步移除队列的偏移量"""
        try:
            # 从内存缓存移除
            async with self._lock:
                _ = self._remove_from_local_cache(queue)
                if queue in self._pending_persist:
                    self._pending_persist.discard(queue)

            # 从文件中移除
            if await aiofiles.os.path.exists(self.offset_file_path):
                file_data = await self._get_or_create_file_data()

                # 移除对应条目
                original_count = len(file_data["offsets"])
                file_data["offsets"] = [
                    entry
                    for entry in file_data["offsets"]
                    if not (
                        entry["topic"] == queue.topic
                        and entry["broker_name"] == queue.broker_name
                        and entry["queue_id"] == queue.queue_id
                    )
                ]

                # 如果有变化，写入文件
                if len(file_data["offsets"]) < original_count:
                    await self._write_to_file_async(file_data)

                    # 更新缓存
                    async with self._cache_lock:
                        self._file_cache = file_data
                        self._cache_dirty = False

            logger.debug("Async offset removed from file", extra={"queue": str(queue)})

        except Exception as e:
            logger.error(f"Failed to async remove offset from file {queue}: {e}")
            raise OffsetError(
                queue.topic, queue.queue_id, 0, f"Remove offset failed: {e}", e
            ) from e

    async def _load_from_store(self, queue: MessageQueue) -> int:
        """从持久化存储异步加载偏移量（重写基类方法）"""
        return await self._read_from_file_async(queue)

    # 异步文件操作辅助方法
    async def _read_from_file_async(self, queue: MessageQueue) -> int:
        """异步从文件读取指定队列的偏移量"""
        try:
            # 检查文件是否存在
            if not await aiofiles.os.path.exists(self.offset_file_path):
                return -1

            # 尝试从缓存读取
            if self.enable_file_cache:
                async with self._cache_lock:
                    if self._file_cache:
                        for entry_data in self._file_cache.get("offsets", []):
                            if (
                                entry_data["topic"] == queue.topic
                                and entry_data["broker_name"] == queue.broker_name
                                and entry_data["queue_id"] == queue.queue_id
                            ):
                                return entry_data["offset"]

            # 从文件读取
            file_data: dict[str, Any] = await self._read_file_async()

            for entry_data in file_data.get("offsets", []):
                if (
                    entry_data["topic"] == queue.topic
                    and entry_data["broker_name"] == queue.broker_name
                    and entry_data["queue_id"] == queue.queue_id
                ):
                    return entry_data["offset"]

            return -1

        except Exception as e:
            logger.error(f"Failed to async read offset from file for {queue}: {e}")
            return -1

    async def _read_file_async(self) -> dict[str, Any]:
        """异步读取偏移量文件"""
        async with aiofiles.open(
            self.offset_file_path, "r", encoding="utf-8", buffering=self.buffer_size
        ) as f:
            content = await f.read()
            return json.loads(content)

    async def _write_to_file_async(self, data: dict[str, Any]) -> None:
        """异步写入偏移量文件（原子性操作）"""
        temp_file: Path = self.temp_file_path.with_suffix(".tmp.gz")
        try:
            # 序列化数据
            json_content: str = json.dumps(
                data,
                indent=2,
                ensure_ascii=False,
                separators=(",", ": ") if not self.compression else (",", ":"),
            )

            # 如果启用压缩
            if self.compression:
                import gzip

                json_content = gzip.compress(json_content.encode("utf-8")).decode()
                temp_file = self.temp_file_path.with_suffix(".tmp.gz")
                final_file = self.offset_file_path.with_suffix(".json.gz")
            else:
                temp_file = self.temp_file_path
                final_file = self.offset_file_path

            # 写入临时文件
            async with aiofiles.open(
                temp_file, "w", encoding="utf-8", buffering=self.buffer_size
            ) as f:
                await f.write(json_content)

            # 原子性重命名
            await aiofiles.os.replace(temp_file, final_file)

            # 创建备份（可选）
            if await self._should_create_backup():
                await self._create_backup_async()

            logger.debug(
                "Async file written successfully",
                extra={"file_size": len(json_content), "compressed": self.compression},
            )

        except Exception as _:
            # 清理临时文件
            if await aiofiles.os.path.exists(temp_file):
                await aiofiles.os.remove(temp_file)

    async def _get_or_create_file_data(self) -> dict[str, Any]:
        """获取或创建文件数据结构"""
        if self.enable_file_cache:
            async with self._cache_lock:
                if self._file_cache and not self._cache_dirty:
                    return self._file_cache.copy()

        # 从文件读取或创建新数据
        try:
            if await aiofiles.os.path.exists(self.offset_file_path):
                return await self._read_file_async()
            else:
                return await self._build_file_data()
        except Exception:
            # 如果读取失败，返回空的数据结构
            return await self._build_file_data()

    async def _build_file_data(self) -> dict[str, Any]:
        """构建文件数据结构"""
        offsets: list[dict[str, Any]] = []

        async with self._lock:
            for queue, offset in self.offset_table.items():
                offset_entry: AsyncOffsetEntry = AsyncOffsetEntry(
                    queue=queue, offset=offset
                )
                offsets.append(offset_entry.to_dict())

        return {
            "consumer_group": self.consumer_group,
            "offsets": offsets,
            "last_update_time": int(time.time() * 1000),
            "version": "2.0",
            "compression": self.compression,
            "rotation_count": self._rotation_count,
        }

    async def _check_file_rotation(self, file_data: dict[str, Any]) -> None:
        """检查是否需要进行文件轮转"""
        try:
            if not await aiofiles.os.path.exists(self.offset_file_path):
                return

            file_size = await aiofiles.os.path.getsize(self.offset_file_path)

            if file_size > self.max_file_size:
                await self._rotate_file_async(file_data)

        except Exception as e:
            logger.warning(f"File rotation check failed: {e}")

    async def _rotate_file_async(self, file_data: dict[str, Any]) -> None:
        """异步执行文件轮转"""
        try:
            # 生成轮转文件名
            timestamp = int(time.time())
            rotated_file = (
                self.store_path / f"{self.consumer_group}.offsets.{timestamp}.json"
            )

            # 备份当前文件
            await aiofiles.os.replace(self.offset_file_path, rotated_file)

            # 更新轮转计数
            self._rotation_count += 1
            file_data["rotation_count"] = self._rotation_count
            file_data["previous_rotation"] = timestamp

            # 重新创建主文件
            await self._write_to_file_async(file_data)

            logger.info(
                "File rotation completed",
                extra={
                    "rotated_file": str(rotated_file),
                    "rotation_count": self._rotation_count,
                },
            )

        except Exception as e:
            logger.error(f"File rotation failed: {e}")
            raise

    async def _should_create_backup(self) -> bool:
        """判断是否应该创建备份文件"""
        try:
            if not await aiofiles.os.path.exists(self.offset_file_path):
                return False

            # 每小时创建一次备份
            if await aiofiles.os.path.exists(self.backup_file_path):
                backup_stat: os.stat_result = await aiofiles.os.stat(
                    self.backup_file_path
                )
                backup_age = time.time() - backup_stat.st_mtime
                return backup_age > 3600  # 1小时

            return True

        except Exception:
            return False

    async def _create_backup_async(self) -> None:
        """异步创建备份文件"""
        try:
            await aiofiles.os.replace(self.offset_file_path, self.backup_file_path)
            logger.debug(
                "Backup file created", extra={"backup_file": str(self.backup_file_path)}
            )

        except Exception as e:
            logger.warning(f"Failed to create backup file: {e}")

    async def _try_load_from_backup(self) -> bool:
        """尝试从备份文件加载"""
        try:
            if await aiofiles.os.path.exists(self.backup_file_path):
                logger.info("Attempting to load from backup file")

                async with aiofiles.open(
                    self.backup_file_path,
                    "r",
                    encoding="utf-8",
                    buffering=self.buffer_size,
                ) as f:
                    content = await f.read()

                if content.strip():
                    data = json.loads(content)
                    async with self._lock:
                        for entry_data in data.get("offsets", []):
                            offset_entry = AsyncOffsetEntry.from_dict(entry_data)
                            self.offset_table[offset_entry.queue] = offset_entry.offset

                    logger.info("Successfully loaded from backup file")
                    return True

        except Exception as e:
            logger.error(f"Failed to load from backup: {e}")

        return False

    async def _should_persist(self) -> bool:
        """判断是否应该进行持久化"""
        return self._started and (len(self._pending_persist) > 0 or self._cache_dirty)

    async def get_metrics(self) -> dict[str, Any]:
        """获取异步本地偏移量存储的性能指标"""
        # 获取基础指标
        metrics_dict: dict[str, Any] = await super().get_metrics()

        # 添加本地存储特有的指标
        file_info: dict[str, Any] = await self._get_file_info_async()
        metrics_dict.update(
            {
                "store_path": str(self.store_path),
                "file_rotation": self.file_rotation,
                "max_file_size": self.max_file_size,
                "compression": self.compression,
                "buffer_size": self.buffer_size,
                "enable_file_cache": self.enable_file_cache,
                "rotation_count": self._rotation_count,
                "file_cache_enabled": self._file_cache is not None,
                "file_cache_dirty": self._cache_dirty,
            }
        )
        metrics_dict.update(file_info)

        return metrics_dict

    async def _get_file_info_async(self) -> dict[str, Any]:
        """异步获取文件信息"""
        info: dict[str, Any] = {
            "file_path": str(self.offset_file_path),
            "file_exists": False,
            "file_size": 0,
            "last_modified": None,
            "backup_file_exists": False,
            "backup_file_size": 0,
        }

        try:
            if await aiofiles.os.path.exists(self.offset_file_path):
                stat: os.stat_result = await aiofiles.os.stat(self.offset_file_path)
                info.update(
                    {
                        "file_exists": True,
                        "file_size": stat.st_size,
                        "last_modified": stat.st_mtime,
                    }
                )

            if await aiofiles.os.path.exists(self.backup_file_path):
                backup_stat = await aiofiles.os.stat(self.backup_file_path)
                info.update(
                    {
                        "backup_file_exists": True,
                        "backup_file_size": backup_stat.st_size,
                    }
                )

        except Exception as e:
            logger.warning(f"Failed to get file info: {e}")

        return info

    async def cleanup_old_files(self, max_files: int = 10) -> int:
        """清理旧的轮转文件"""
        try:
            files: list[str] = []

            # 查找轮转文件
            entries = await aiofiles.os.scandir(self.store_path)
            for entry in entries:
                if entry.is_file() and entry.name.startswith(
                    f"{self.consumer_group}.offsets."
                ):
                    files.append(entry.name)

            # 按修改时间排序，保留最新的max_files个
            files.sort(reverse=True)
            files_to_remove = files[max_files:]

            removed_count = 0
            for filename in files_to_remove:
                file_path: Path = self.store_path / filename
                await aiofiles.os.remove(file_path)
                removed_count += 1
                logger.debug(f"Removed old offset file: {filename}")

            if removed_count > 0:
                logger.info(
                    "Cleaned up old offset files",
                    extra={
                        "removed_count": removed_count,
                        "remaining_files": len(files) - removed_count,
                    },
                )

            return removed_count

        except Exception as e:
            logger.error(f"Failed to cleanup old files: {e}")
            return 0

    async def validate_file_integrity(self) -> dict[str, Any]:
        """验证文件完整性"""
        result: dict[str, Any] = {
            "main_file_valid": False,
            "backup_file_valid": False,
            "can_load_main": False,
            "can_load_backup": False,
            "file_size_reasonable": True,
            "json_format_valid": False,
            "issues": [],
        }

        try:
            # 检查主文件
            if await aiofiles.os.path.exists(self.offset_file_path):
                result["main_file_valid"] = True
                file_size = await aiofiles.os.path.getsize(self.offset_file_path)

                # 检查文件大小
                if file_size > self.max_file_size * 2:  # 超过限制2倍认为异常
                    result["file_size_reasonable"] = False
                    result["issues"].append(
                        f"File size {file_size} exceeds reasonable limit"
                    )

                # 尝试解析JSON
                try:
                    async with aiofiles.open(
                        self.offset_file_path, "r", encoding="utf-8"
                    ) as f:
                        content = await f.read()

                    if content.strip():
                        json.loads(content)
                        result["json_format_valid"] = True
                        result["can_load_main"] = True
                    else:
                        result["issues"].append("File is empty")

                except json.JSONDecodeError as e:
                    result["issues"].append(f"JSON decode error: {e}")

            # 检查备份文件
            if await aiofiles.os.path.exists(self.backup_file_path):
                result["backup_file_valid"] = True

                try:
                    async with aiofiles.open(
                        self.backup_file_path, "r", encoding="utf-8"
                    ) as f:
                        async with aiofiles.open(
                            self.backup_file_path, "r", encoding="utf-8"
                        ) as f:
                            content = await f.read()

                    if content.strip():
                        json.loads(content)
                        result["can_load_backup"] = True

                except json.JSONDecodeError as e:
                    result["issues"].append(f"Backup JSON decode error: {e}")

        except Exception as e:
            result["issues"].append(f"Validation error: {e}")

        return result
