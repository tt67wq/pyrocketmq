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
from pyrocketmq.nameserver import NameServerManager

logger = get_logger(__name__)


class OffsetStoreFactory:
    """偏移量存储工厂类

    工厂模式实现，根据消费模式创建相应的偏移量存储实例。
    支持两种消息模式：
    - 集群模式(CLUSTERING): 创建RemoteOffsetStore，偏移量存储在Broker
    - 广播模式(BROADCASTING): 创建LocalOffsetStore，偏移量存储在本地文件

    设计优势：
    - 封装创建逻辑，客户端无需了解具体实现细节
    - 统一的创建接口，便于扩展新的存储类型
    - 参数验证和错误处理，确保创建的实例有效
    - 支持自动启动，简化使用流程

    Thread Safety:
        所有方法都是线程安全的，支持并发创建实例。
    """

    @staticmethod
    def create_offset_store(
        consumer_group: str,
        message_model: str,
        namesrv_manager: NameServerManager | None = None,
        broker_manager: BrokerManager | None = None,
        store_path: str = "~/.rocketmq/offsets",
        persist_interval: int = 5000,
        persist_batch_size: int = 10,
        auto_start: bool = True,
        **kwargs: dict[str, Any],
    ) -> OffsetStore:
        """创建偏移量存储实例

        根据消息模型类型创建相应的偏移量存储实例，支持集群和广播两种模式。
        创建完成后可选择性地自动启动存储服务。

        Args:
            consumer_group (str): 消费者组名称，用于标识消费者实例
                - 不能为空字符串
                - 在同一应用中应该保持唯一性
                - 用于持久化文件命名或Broker通信

            namesrv_addr (str): NameServer地址列表，格式："host1:port1;host2:port2"
                - 集群模式必需，用于路由查询
                - 广播模式下某些实现也可能需要
                - 支持多个地址以实现高可用

            message_model (MessageModel): 消息消费模式枚举
                - MessageModel.CLUSTERING: 集群消费，偏移量存储在Broker
                - MessageModel.BROADCASTING: 广播消费，偏移量存储在本地

            broker_manager (BrokerManager | None, optional): Broker连接管理器
                - 集群模式下的必需参数
                - 用于与Broker建立连接和通信
                - 广播模式下可以为None

            store_path (str, optional): 本地存储路径，默认"~/.rocketmq/offsets"
                - 广播模式下的存储目录
                - 支持波浪号展开为用户主目录
                - 自动创建不存在的目录

            persist_interval (int, optional): 持久化间隔（毫秒），默认5000ms
                - 定期持久化缓存中的偏移量
                - 间隔越短数据安全性越高，但性能开销越大
                - 建议范围：1000ms-30000ms

            persist_batch_size (int, optional): 批量提交大小，默认10
                - 批量持久化的偏移量数量阈值
                - 较大的值可以提高性能，但增加内存占用
                - 建议范围：5-50

            auto_start (bool, optional): 是否自动启动存储服务，默认True
                - True: 创建后自动调用start()方法
                - False: 仅创建实例，需要手动启动
                - 便于在启动前进行额外配置

            **kwargs: 其他特定实现的配置参数
                - 传递给具体OffsetStore实现类的额外参数
                - 允许扩展配置而不修改接口
                - 示例：timeout=30000, max_retries=3

        Returns:
            OffsetStore: 配置完成的偏移量存储实例
                - 集群模式返回RemoteOffsetStore实例
                - 广播模式返回LocalOffsetStore实例
                - 如果auto_start=True，实例已启动并可使用

        Raises:
            ValueError: 当参数配置不合法时抛出：
                - consumer_group为空字符串
                - 集群模式下缺少broker_manager参数
                - message_model参数无效

            OSError: 当存储路径创建失败时抛出（广播模式）

            RuntimeError: 当存储实例启动失败时抛出（auto_start=True时）

        Examples:
            >>> # 创建集群模式存储
            >>> remote_store = OffsetStoreFactory.create_offset_store(
            ...     consumer_group="test_group",
            ...     namesrv_addr="localhost:9876",
            ...     message_model=MessageModel.CLUSTERING,
            ...     broker_manager=broker_manager,
            ...     persist_interval=3000
            ... )
            >>>
            >>> # 创建广播模式存储
            >>> local_store = OffsetStoreFactory.create_offset_store(
            ...     consumer_group="broadcast_group",
            ...     namesrv_addr="localhost:9876",
            ...     message_model=MessageModel.BROADCASTING,
            ...     store_path="/tmp/rocketmq_offsets",
            ...     auto_start=False  # 手动控制启动时机
            ... )
            >>> local_store.start()
        """
        if not consumer_group:
            raise ValueError("Consumer group cannot be empty")

        if message_model == MessageModel.CLUSTERING:
            # 集群模式：使用远程存储
            if not broker_manager:
                raise ValueError("Broker manager is required for clustering mode")
            if not namesrv_manager:
                raise ValueError("Nameserver manager is required for clustering mode")

            logger.info(
                f"Creating RemoteOffsetStore for consumer group: {consumer_group}"
            )

            offset_store = RemoteOffsetStore(
                consumer_group=consumer_group,
                nameserver_manager=namesrv_manager,
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
    """偏移量存储管理器（同步版本）

    单例模式管理多个OffsetStore实例的生命周期，支持实例复用和统一管理。
    通过消费者组和消息模式的组合来唯一标识和管理OffsetStore实例。

    主要功能：
    - 实例缓存：避免重复创建相同的OffsetStore
    - 生命周期管理：统一的启动和停止控制
    - 线程安全：支持多线程并发访问
    - 监控支持：提供性能指标和运行状态查询

    设计模式：
    - 单例模式：全局唯一的管理器实例
    - 工厂模式：通过工厂方法创建OffsetStore
    - 缓存模式：缓存创建的实例以供复用

    Thread Safety:
        使用RLock保护所有操作，确保线程安全。
        支持高并发的创建、查询和关闭操作。
    """

    def __init__(self) -> None:
        """初始化偏移量存储管理器

        创建空的实例缓存和线程安全锁。
        通常通过全局函数get_offset_store_manager()获取实例。
        """
        self._offset_stores: dict[str, OffsetStore] = {}
        self._lock: threading.RLock = threading.RLock()

    def get_or_create_offset_store(
        self,
        consumer_group: str,
        message_model: str,
        namesrv_manager: NameServerManager | None = None,
        broker_manager: BrokerManager | None = None,
        store_path: str = "~/.rocketmq/offsets",
        persist_interval: int = 5000,
        persist_batch_size: int = 10,
        **kwargs: dict[str, Any],
    ) -> OffsetStore:
        """获取或创建偏移量存储实例

        采用单例模式，确保相同的消费者组和消息模式组合只创建一个实例。
        如果实例已存在，直接返回缓存的实例；否则创建新实例并缓存。

        Args:
            consumer_group (str): 消费者组名称，用于标识消费者实例
            namesrv_addr (str): NameServer地址，格式："host1:port1;host2:port2"
            message_model (MessageModel): 消息消费模式枚举值
            broker_manager (BrokerManager | None): Broker连接管理器，集群模式必需
            store_path (str): 本地存储路径，广播模式使用
            persist_interval (int): 持久化间隔（毫秒）
            persist_batch_size (int): 批量提交大小
            **kwargs: 其他配置参数

        Returns:
            OffsetStore: 偏移量存储实例，保证已启动状态
                - 实例会被自动缓存以供后续复用
                - 调用者无需关心实例的生命周期管理

        Raises:
            ValueError: 当参数配置不合法时抛出
            RuntimeError: 当OffsetStore创建或启动失败时抛出

        Thread Safety:
            方法是线程安全的，支持并发调用。
            使用双重检查锁定模式优化性能。

        Note:
            实例缓存键格式："{consumer_group}:{message_model}"
            相同键的请求会返回同一个实例。
        """
        with self._lock:
            key = f"{consumer_group}:{message_model}"

            if key not in self._offset_stores:
                # 创建新的OffsetStore实例
                offset_store = OffsetStoreFactory.create_offset_store(
                    consumer_group=consumer_group,
                    message_model=message_model,
                    namesrv_manager=namesrv_manager,
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
        """关闭指定的偏移量存储实例

        优雅地关闭指定的OffsetStore实例，包括停止后台线程、
        持久化剩余数据和清理资源。关闭后实例将从缓存中移除。

        Args:
            consumer_group (str): 消费者组名称
            message_model (MessageModel): 消息消费模式

        Raises:
            KeyError: 当指定的OffsetStore实例不存在时抛出
            RuntimeError: 当OffsetStore停止过程中发生错误时抛出

        Thread Safety:
            方法是线程安全的，与其他操作互斥。

        Note:
            即使stop()方法调用失败，实例仍会从缓存中移除，
            避免后续访问已损坏的实例。
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
        """关闭所有偏移量存储实例

        优雅地关闭所有管理的OffsetStore实例。通常在应用关闭时调用，
        确保所有偏移量数据被正确持久化。

        安全保证：
            - 遍历现有实例的副本，避免并发修改
            - 每个实例的关闭失败不影响其他实例
            - 即使部分实例关闭失败，最终也会清空缓存

        Thread Safety:
            方法是线程安全的，会阻塞所有其他操作直到完成。

        Example:
            >>> # 应用关闭时清理资源
            >>> manager = get_offset_store_manager()
            >>> manager.close_all()
            >>> print("所有OffsetStore已关闭")
        """
        with self._lock:
            for key, offset_store in list(self._offset_stores.items()):
                try:
                    offset_store.stop()
                    logger.info(f"Closed OffsetStore for {key}")
                except Exception as e:
                    logger.error(f"Failed to close OffsetStore for {key}: {e}")

            self._offset_stores.clear()

    def get_all_metrics(self) -> dict[str, dict[str, Any]]:
        """获取所有OffsetStore的性能指标

        收集所有管理的OffsetStore实例的性能指标，包括持久化统计、
        缓存命中率、延迟信息等。用于系统监控和性能分析。

        Returns:
            dict[str, dict[str, Any]]: 指标字典，键为实例标识，值为指标数据
                - 键格式："{consumer_group}:{message_model}"
                - 值包含具体的性能指标，取决于具体实现
                - 如果指标获取失败，包含错误信息

        Thread Safety:
            读取操作持有锁，但尽量减少锁持有时间。
            指标获取失败不会影响其他实例的指标收集。

        Example:
            >>> metrics = manager.get_all_metrics()
            >>> for store_key, store_metrics in metrics.items():
            ...     if "error" in store_metrics:
            ...         print(f"{store_key}: {store_metrics['error']}")
            ...     else:
            ...         print(f"{store_key}: {store_metrics['persist_success_count']} 次成功持久化")
        """
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
        """获取管理的OffsetStore基本信息

        返回所有管理的OffsetStore实例的运行状态和基本信息，
        用于监控和诊断目的。

        Returns:
            dict[str, dict[str, Any]]: 信息字典，包含每个实例的：
                - consumer_group (str): 消费者组名称
                - message_model (str): 消息模式
                - store_type (str): 存储类型名称
                - is_running (bool): 运行状态
                - 其他特定实现的信息

        Thread Safety:
            读取操作线程安全，不影响实例的正常运行。

        Example:
            >>> info = manager.get_managed_stores_info()
            >>> for store_key, store_info in info.items():
            ...     print(f"{store_key}: {store_info['store_type']} - "
            ...           f"{'运行中' if store_info['is_running'] else '已停止'}")
        """
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
        """获取已创建的偏移量存储实例

        从缓存中获取指定的OffsetStore实例，如果不存在则返回None。
        不会创建新实例，仅查询已存在的实例。

        Args:
            consumer_group (str): 消费者组名称
            message_model (MessageModel): 消息消费模式

        Returns:
            OffsetStore | None: 偏移量存储实例，如果不存在返回None
                - 返回的实例已经启动且可用
                - 调用者应确保实例的生命周期管理

        Thread Safety:
            读取操作线程安全，不影响实例的正常运行。

        Example:
            >>> store = manager.get_offset_store("test_group", MessageModel.CLUSTERING)
            >>> if store:
            ...     offset = store.read_offset(queue, ReadOffsetType.MEMORY_FIRST_THEN_STORE)
            >>> else:
            ...     print("OffsetStore实例不存在")
        """
        with self._lock:
            key = f"{consumer_group}:{message_model}"
            return self._offset_stores.get(key)


# 全局OffsetStore管理器实例
_offset_store_manager = OffsetStoreManager()


def get_offset_store_manager() -> OffsetStoreManager:
    """获取全局OffsetStore管理器实例

    返回全局唯一的OffsetStoreManager实例，采用单例模式确保
    整个应用中只有一个管理器实例。

    Returns:
        OffsetStoreManager: 全局偏移量存储管理器实例
            - 实例已初始化并可用
            - 支持管理多个OffsetStore实例
            - 提供统一的创建、查询、关闭接口

    Thread Safety:
        全局实例在模块加载时创建，线程安全。
        返回的实例本身是线程安全的。

    Example:
        >>> manager = get_offset_store_manager()
        >>> store = manager.get_or_create_offset_store(
        ...     consumer_group="test_group",
        ...     namesrv_addr="localhost:9876",
        ...     message_model=MessageModel.CLUSTERING
        ... )
    """
    return _offset_store_manager


def create_offset_store(
    consumer_group: str,
    message_model: str,
    namesrv_manager: NameServerManager | None = None,
    broker_manager: BrokerManager | None = None,
    store_path: str = "~/.rocketmq/offsets",
    persist_interval: int = 5000,
    persist_batch_size: int = 10,
    **kwargs: dict[str, Any],
) -> OffsetStore:
    """便捷函数：创建偏移量存储实例

    通过全局管理器创建并获取OffsetStore实例。如果相同配置的实例
    已存在，则返回缓存的实例；否则创建新实例。

    Args:
        consumer_group (str): 消费者组名称，用于标识消费者实例
        namesrv_addr (str): NameServer地址，格式："host1:port1;host2:port2"
        message_model (MessageModel): 消息消费模式枚举
        broker_manager (BrokerManager | None): Broker连接管理器，集群模式必需
        store_path (str): 本地存储路径，广播模式使用
        persist_interval (int): 持久化间隔（毫秒）
        persist_batch_size (int): 批量提交大小
        **kwargs: 其他配置参数

    Returns:
        OffsetStore: 偏移量存储实例，保证已启动状态
            - 实例会被全局管理器缓存
            - 相同配置的请求会返回同一个实例

    Raises:
        ValueError: 当参数配置不合法时抛出
        RuntimeError: 当OffsetStore创建或启动失败时抛出

    Example:
        >>> # 快速创建集群模式存储
        >>> store = create_offset_store(
        ...     consumer_group="quick_group",
        ...     namesrv_addr="localhost:9876",
        ...     message_model=MessageModel.CLUSTERING,
        ...     broker_manager=broker_mgr
        ... )
        >>>
        >>> # 再次调用会返回相同实例
        >>> same_store = create_offset_store(
        ...     consumer_group="quick_group",
        ...     namesrv_addr="localhost:9876",
        ...     message_model=MessageModel.CLUSTERING,
        ...     broker_manager=broker_mgr
        ... )
        >>> assert store is same_store  # True
    """
    manager = get_offset_store_manager()
    return manager.get_or_create_offset_store(
        consumer_group=consumer_group,
        namesrv_manager=namesrv_manager,
        message_model=message_model,
        broker_manager=broker_manager,
        store_path=store_path,
        persist_interval=persist_interval,
        persist_batch_size=persist_batch_size,
        **kwargs,
    )


def close_offset_store(consumer_group: str, message_model: MessageModel) -> None:
    """便捷函数：关闭偏移量存储实例

    通过全局管理器关闭指定的OffsetStore实例。关闭后实例会被
    从缓存中移除，后续访问会创建新实例。

    Args:
        consumer_group (str): 消费者组名称
        message_model (MessageModel): 消息消费模式

    Raises:
        KeyError: 当指定的OffsetStore实例不存在时抛出
        RuntimeError: 当关闭过程中发生错误时抛出

    Example:
        >>> # 使用完成后关闭存储
        >>> close_offset_store(
        ...     consumer_group="test_group",
        ...     message_model=MessageModel.CLUSTERING
        ... )
        >>> print("OffsetStore已关闭")
    """
    manager = get_offset_store_manager()
    manager.close_offset_store(consumer_group, message_model)


def close_all_offset_stores() -> None:
    """便捷函数：关闭所有偏移量存储实例

    关闭全局管理器中的所有OffsetStore实例。通常在应用关闭时
    调用，确保所有资源被正确释放。

    作用：
    - 停止所有后台线程
    - 持久化剩余的偏移量数据
    - 清理网络连接和文件资源
    - 清空全局缓存

    Thread Safety:
        操作是线程安全的，会阻塞直到所有实例关闭完成。

    Example:
        >>> import atexit
        >>>
        >>> # 注册应用退出时的清理函数
        >>> atexit.register(close_all_offset_stores)
        >>>
        >>> # 或者手动调用
        >>> close_all_offset_stores()
        >>> print("所有OffsetStore已关闭")
    """
    manager = get_offset_store_manager()
    manager.close_all()


def get_offset_store_metrics() -> dict[str, dict[str, Any]]:
    """便捷函数：获取所有OffsetStore的性能指标

    收集全局管理器中所有OffsetStore实例的性能指标，
    用于系统监控和性能分析。

    Returns:
        dict[str, dict[str, Any]]: 指标字典，包含：
            - 键：实例标识格式"{consumer_group}:{message_model}"
            - 值：具体指标数据或错误信息

        指标内容包括：
        - 持久化成功/失败次数
        - 缓存命中率
        - 平均延迟
        - 其他实现特定的指标

    Example:
        >>> metrics = get_offset_store_metrics()
        >>> for store_key, store_metrics in metrics.items():
        ...     if "error" in store_metrics:
        ...         print(f"{store_key}: 错误 - {store_metrics['error']}")
        ...     else:
        ...     success_rate = store_metrics.get('persist_success_rate', 0)
        ...     print(f"{store_key}: 持久化成功率 {success_rate:.1%}")
    """
    manager = get_offset_store_manager()
    return manager.get_all_metrics()


def validate_offset_store_config(
    consumer_group: str,
    message_model: str,
    store_path: str = "~/.rocketmq/offsets",
) -> bool:
    """验证OffsetStore配置的有效性

    在创建OffsetStore之前验证配置参数的有效性，避免在运行时
    出现配置错误。验证失败时会记录详细的错误日志。

    Args:
        consumer_group (str): 消费者组名称
            - 不能为空或None
            - 必须是字符串类型

        message_model (MessageModel): 消息消费模式
            - 必须是有效的MessageModel枚举值
            - 支持CLUSTERING和BROADCASTING

        store_path (str): 本地存储路径，广播模式使用
            - 默认值："~/.rocketmq/offsets"
            - 支持波浪号展开
            - 必须具有写入权限

    Returns:
        bool: 配置是否有效
            - True: 所有参数验证通过
            - False: 存在无效参数，错误日志已记录

    验证项目：
    1. consumer_group: 非空字符串检查
    2. message_model: 枚举值有效性检查
    3. store_path: 广播模式下的路径权限检查

    Example:
        >>> # 验证集群模式配置
        >>> if validate_offset_store_config(
        ...     consumer_group="test_group",
        ...     message_model=MessageModel.CLUSTERING
        ... ):
        ...     store = create_offset_store(...)
        ... else:
        ...     print("配置验证失败")
        >>>
        >>> # 验证广播模式配置
        >>> if validate_offset_store_config(
        ...     consumer_group="broadcast_group",
        ...     message_model=MessageModel.BROADCASTING,
        ...     store_path="/tmp/offsets"
        ... ):
        ...     store = create_offset_store(...)
    """
    # 验证消费者组名称
    if not consumer_group:
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
