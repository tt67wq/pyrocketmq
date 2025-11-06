"""
OffsetStore工厂 - 偏移量存储工厂

根据消费模式创建相应的偏移量存储实例：
- 集群模式：RemoteOffsetStore
- 广播模式：LocalOffsetStore
"""

import threading
from pathlib import Path
from typing import Any

from pyrocketmq.broker import BrokerManager
from pyrocketmq.consumer.config import MessageModel
from pyrocketmq.consumer.local_offset_store import LocalOffsetStore
from pyrocketmq.consumer.offset_store import OffsetStore
from pyrocketmq.consumer.remote_offset_store import RemoteOffsetStore
from pyrocketmq.logging import get_logger

logger = get_logger(__name__)


class OffsetStoreFactory:
    """偏移量存储工厂类"""

    @staticmethod
    def create_offset_store(
        consumer_group: str,
        message_model: MessageModel,
        broker_manager: BrokerManager | None = None,
        store_path: str = "~/.rocketmq/offsets",
        persist_interval: int = 5000,
        persist_batch_size: int = 10,
        auto_start: bool = True,
        **kwargs,
    ) -> OffsetStore:
        """
        创建偏移量存储实例

        Args:
            consumer_group: 消费者组名称
            message_model: 消息模式（集群或广播）
            broker_manager: Broker管理器（集群模式必需）
            store_path: 本地存储路径（广播模式使用）
            persist_interval: 持久化间隔（毫秒）
            persist_batch_size: 批量提交大小
            auto_start: 是否自动启动OffsetStore
            **kwargs: 其他配置参数

        Returns:
            OffsetStore: 偏移量存储实例

        Raises:
            ValueError: 当参数不合法时抛出
        """
        if not consumer_group:
            raise ValueError("Consumer group cannot be empty")

        if message_model == MessageModel.CLUSTERING:
            # 集群模式：使用远程存储
            if not broker_manager:
                raise ValueError("Broker manager is required for clustering mode")

            logger.info(
                f"Creating RemoteOffsetStore for consumer group: {consumer_group}"
            )

            offset_store = RemoteOffsetStore(
                consumer_group=consumer_group,
                broker_manager=broker_manager,
                persist_interval=persist_interval,
                persist_batch_size=persist_batch_size,
                **kwargs,
            )

        elif message_model == MessageModel.BROADCASTING:
            # 广播模式：使用本地存储
            logger.info(
                f"Creating LocalOffsetStore for consumer group: {consumer_group}, path: {store_path}"
            )

            offset_store = LocalOffsetStore(
                consumer_group=consumer_group,
                store_path=store_path,
                persist_interval=persist_interval,
                persist_batch_size=persist_batch_size,
                **kwargs,
            )

        else:
            raise ValueError(f"Unsupported message model: {message_model}")

        # 自动启动
        if auto_start:
            offset_store.start()

        return offset_store


class OffsetStoreManager:
    """偏移量存储管理器（同步版本）"""

    def __init__(self):
        self._offset_stores: dict[str, OffsetStore] = {}
        self._lock: threading.RLock = threading.RLock()

    def get_or_create_offset_store(
        self,
        consumer_group: str,
        message_model: MessageModel,
        broker_manager: BrokerManager | None = None,
        store_path: str = "~/.rocketmq/offsets",
        persist_interval: int = 5000,
        persist_batch_size: int = 10,
        **kwargs,
    ) -> OffsetStore:
        """
        获取或创建偏移量存储实例

        Args:
            consumer_group: 消费者组名称
            message_model: 消息模式
            broker_manager: Broker管理器
            store_path: 本地存储路径
            persist_interval: 持久化间隔
            persist_batch_size: 批量提交大小
            **kwargs: 其他配置参数

        Returns:
            OffsetStore: 偏移量存储实例
        """
        with self._lock:
            key = f"{consumer_group}:{message_model}"

            if key not in self._offset_stores:
                # 创建新的OffsetStore实例
                offset_store = OffsetStoreFactory.create_offset_store(
                    consumer_group=consumer_group,
                    message_model=message_model,
                    broker_manager=broker_manager,
                    store_path=store_path,
                    persist_interval=persist_interval,
                    persist_batch_size=persist_batch_size,
                    auto_start=True,
                    **kwargs,
                )

                # 缓存实例
                self._offset_stores[key] = offset_store

                logger.info(f"Created and started OffsetStore for {key}")

            return self._offset_stores[key]

    def close_offset_store(
        self, consumer_group: str, message_model: MessageModel
    ) -> None:
        """
        关闭指定的偏移量存储实例

        Args:
            consumer_group: 消费者组名称
            message_model: 消息模式
        """
        with self._lock:
            key = f"{consumer_group}:{message_model}"

            if key in self._offset_stores:
                offset_store = self._offset_stores[key]
                try:
                    offset_store.stop()
                except Exception as e:
                    logger.error(f"Failed to stop OffsetStore for {key}: {e}")
                finally:
                    del self._offset_stores[key]
                    logger.info(f"Closed OffsetStore for {key}")

    def close_all(self) -> None:
        """关闭所有偏移量存储实例"""
        with self._lock:
            for key, offset_store in list(self._offset_stores.items()):
                try:
                    offset_store.stop()
                    logger.info(f"Closed OffsetStore for {key}")
                except Exception as e:
                    logger.error(f"Failed to close OffsetStore for {key}: {e}")

            self._offset_stores.clear()

    def get_all_metrics(self) -> dict[str, dict[str, Any]]:
        """获取所有OffsetStore的指标"""
        with self._lock:
            metrics = {}

            for key, offset_store in self._offset_stores.items():
                try:
                    if hasattr(offset_store, "get_metrics"):
                        metrics[key] = offset_store.get_metrics()
                    else:
                        metrics[key] = {"error": "Metrics not supported"}
                except Exception as e:
                    metrics[key] = {"error": str(e)}

            return metrics

    def get_managed_stores_info(self) -> dict[str, dict[str, Any]]:
        """获取管理的OffsetStore信息"""
        with self._lock:
            info = {}

            for key, offset_store in self._offset_stores.items():
                consumer_group, message_model = key.split(":", 1)

                store_info = {
                    "consumer_group": consumer_group,
                    "message_model": message_model,
                    "store_type": type(offset_store).__name__,
                    "is_running": getattr(offset_store, "_running", False),
                }

                # 添加特定类型的信息
                if isinstance(offset_store, LocalOffsetStore):
                    if hasattr(offset_store, "get_file_info"):
                        store_info.update(offset_store.get_file_info())
                elif isinstance(offset_store, RemoteOffsetStore):
                    store_info.update(
                        {
                            "cached_offsets_count": len(offset_store.offset_table),
                            "remote_cache_count": len(
                                getattr(offset_store, "remote_offset_cache", {})
                            ),
                        }
                    )

                info[key] = store_info

            return info

    def get_offset_store(
        self, consumer_group: str, message_model: MessageModel
    ) -> OffsetStore | None:
        """
        获取已创建的偏移量存储实例

        Args:
            consumer_group: 消费者组名称
            message_model: 消息模式

        Returns:
            OffsetStore | None: 偏移量存储实例，如果不存在返回None
        """
        with self._lock:
            key = f"{consumer_group}:{message_model}"
            return self._offset_stores.get(key)


# 全局OffsetStore管理器实例
_offset_store_manager = OffsetStoreManager()


def get_offset_store_manager() -> OffsetStoreManager:
    """获取全局OffsetStore管理器实例"""
    return _offset_store_manager


def create_offset_store(
    consumer_group: str,
    message_model: MessageModel,
    broker_manager: BrokerManager | None = None,
    store_path: str = "~/.rocketmq/offsets",
    persist_interval: int = 5000,
    persist_batch_size: int = 10,
    **kwargs,
) -> OffsetStore:
    """
    便捷函数：创建偏移量存储实例

    Args:
        consumer_group: 消费者组名称
        message_model: 消息模式
        broker_manager: Broker管理器
        store_path: 本地存储路径
        persist_interval: 持久化间隔
        persist_batch_size: 批量提交大小
        **kwargs: 其他配置参数

    Returns:
        OffsetStore: 偏移量存储实例
    """
    manager = get_offset_store_manager()
    return manager.get_or_create_offset_store(
        consumer_group=consumer_group,
        message_model=message_model,
        broker_manager=broker_manager,
        store_path=store_path,
        persist_interval=persist_interval,
        persist_batch_size=persist_batch_size,
        **kwargs,
    )


def close_offset_store(consumer_group: str, message_model: MessageModel) -> None:
    """
    便捷函数：关闭偏移量存储实例

    Args:
        consumer_group: 消费者组名称
        message_model: 消息模式
    """
    manager = get_offset_store_manager()
    manager.close_offset_store(consumer_group, message_model)


def close_all_offset_stores() -> None:
    """便捷函数：关闭所有偏移量存储实例"""
    manager = get_offset_store_manager()
    manager.close_all()


def get_offset_store_metrics() -> dict[str, dict[str, Any]]:
    """
    便捷函数：获取所有OffsetStore的指标

    Returns:
        dict[str, dict[str, Any]]: 指标信息
    """
    manager = get_offset_store_manager()
    return manager.get_all_metrics()


def validate_offset_store_config(
    consumer_group: str,
    message_model: MessageModel,
    store_path: str = "~/.rocketmq/offsets",
) -> bool:
    """
    验证OffsetStore配置

    Args:
        consumer_group: 消费者组名称
        message_model: 消息模式
        store_path: 本地存储路径

    Returns:
        bool: 配置是否有效
    """
    # 验证消费者组名称
    if not consumer_group or not isinstance(consumer_group, str):
        logger.error("Consumer group must be a non-empty string")
        return False

    # 验证消息模式
    if message_model not in [MessageModel.CLUSTERING, MessageModel.BROADCASTING]:
        logger.error(f"Invalid message model: {message_model}")
        return False

    # 验证存储路径
    if message_model == MessageModel.BROADCASTING:
        try:
            path = Path(store_path).expanduser()
            # 尝试创建目录验证权限
            path.mkdir(parents=True, exist_ok=True)

            # 测试写入权限
            test_file = path / ".rocketmq_test"
            test_file.touch()
            test_file.unlink()

        except Exception as e:
            logger.error(
                f"Invalid store path or no write permission: {store_path}, error: {e}"
            )
            return False

    return True
