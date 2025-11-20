"""
AsyncOffsetStore工厂 - 异步偏移量存储工厂

根据消费模式创建相应的异步偏移量存储实例：
- 集群模式：AsyncRemoteOffsetStore
- 广播模式：AsyncLocalOffsetStore
"""

from pathlib import Path
from typing import Any

from pyrocketmq.broker.async_broker_manager import AsyncBrokerManager
from pyrocketmq.consumer.async_local_offset_store import AsyncLocalOffsetStore
from pyrocketmq.consumer.async_offset_store import AsyncOffsetStore
from pyrocketmq.consumer.async_remote_offset_store import AsyncRemoteOffsetStore
from pyrocketmq.consumer.config import MessageModel
from pyrocketmq.logging import get_logger
from pyrocketmq.nameserver.async_manager import AsyncNameServerManager

logger = get_logger(__name__)


class AsyncOffsetStoreFactory:
    """异步偏移量存储工厂类

    异步工厂模式实现，根据消费模式创建相应的异步偏移量存储实例。
    支持两种消息模式：
    - 集群模式(CLUSTERING): 创建AsyncRemoteOffsetStore，偏移量存储在Broker
    - 广播模式(BROADCASTING): 创建LocalOffsetStore，偏移量存储在本地文件

    设计优势：
    - 异步优先：所有操作都采用async/await模式
    - 封装创建逻辑，客户端无需了解具体实现细节
    - 统一的创建接口，便于扩展新的存储类型
    - 参数验证和错误处理，确保创建的实例有效
    - 支持自动启动，简化使用流程

    Thread Safety:
        所有方法都是线程安全的，支持并发创建实例。
    """

    @staticmethod
    async def create_offset_store(
        consumer_group: str,
        message_model: str,
        namesrv_manager: AsyncNameServerManager | None = None,
        broker_manager: AsyncBrokerManager | None = None,
        store_path: str = "~/.rocketmq/offsets",
        persist_interval: int = 5000,
        auto_start: bool = True,
        **kwargs: Any,
    ) -> AsyncOffsetStore:
        """异步创建偏移量存储实例

        根据消息模型类型异步创建相应的偏移量存储实例，支持集群和广播两种模式。
        创建完成后可选择性地自动启动存储服务。

        Args:
            consumer_group (str): 消费者组名称，用于标识消费者实例
                - 不能为空字符串
                - 在同一应用中应该保持唯一性
                - 用于持久化文件命名或Broker通信

            message_model (MessageModel): 消息消费模式枚举
                - MessageModel.CLUSTERING: 集群消费，偏移量存储在Broker
                - MessageModel.BROADCASTING: 广播消费，偏移量存储在本地

            namesrv_manager (AsyncNameServerManager | None, optional): 异步NameServer管理器
                - 集群模式必需，用于路由查询
                - 广播模式下某些实现也可能需要
                - 支持异步操作

            broker_manager (AsyncBrokerManager | None, optional): 异步Broker连接管理器
                - 集群模式下的必需参数
                - 用于与Broker建立连接和通信
                - 支持异步操作
                - 广播模式下可以为None

            store_path (str, optional): 本地存储路径，默认"~/.rocketmq/offsets"
                - 广播模式下的存储目录
                - 支持波浪号展开为用户主目录
                - 自动创建不存在的目录

            persist_interval (int, optional): 持久化间隔（毫秒），默认5000ms
                - 定期持久化缓存中的偏移量
                - 间隔越短数据安全性越高，但性能开销越大
                - 建议范围：1000ms-30000ms

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
                - 集群模式返回AsyncRemoteOffsetStore实例
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
            >>> # 异步创建集群模式存储
            >>> remote_store = await AsyncOffsetStoreFactory.create_offset_store(
            ...     consumer_group="test_group",
            ...     namesrv_addr="localhost:9876",
            ...     message_model=MessageModel.CLUSTERING,
            ...     broker_manager=async_broker_manager,
            ...     persist_interval=3000
            ... )
            >>>
            >>> # 异步创建广播模式存储
            >>> local_store = await AsyncOffsetStoreFactory.create_offset_store(
            ...     consumer_group="broadcast_group",
            ...     namesrv_addr="localhost:9876",
            ...     message_model=MessageModel.BROADCASTING,
            ...     store_path="/tmp/rocketmq_offsets",
            ...     auto_start=False  # 手动控制启动时机
            ... )
            >>> await local_store.start()
        """
        if not consumer_group:
            raise ValueError("Consumer group cannot be empty")

        if message_model == MessageModel.CLUSTERING:
            # 集群模式：使用异步远程存储
            if not broker_manager:
                raise ValueError("Broker manager is required for clustering mode")
            if not namesrv_manager:
                raise ValueError("Nameserver manager is required for clustering mode")

            logger.info(
                f"Creating AsyncRemoteOffsetStore for consumer group: {consumer_group}"
            )

            offset_store = AsyncRemoteOffsetStore(
                consumer_group=consumer_group,
                nameserver_manager=namesrv_manager,
                broker_manager=broker_manager,
                persist_interval=persist_interval,
                **kwargs,
            )

        elif message_model == MessageModel.BROADCASTING:
            # 广播模式：使用本地存储（LocalOffsetStore是同步的）
            logger.info(
                f"Creating LocalOffsetStore for consumer group: {consumer_group}, path: {store_path}"
            )

            offset_store = AsyncLocalOffsetStore(
                consumer_group=consumer_group,
                store_path=store_path,
                persist_interval=persist_interval,
                **kwargs,
            )

        else:
            raise ValueError(f"Unsupported message model: {message_model}")

        # 自动启动
        if auto_start:
            await offset_store.start()

        return offset_store


async def create_offset_store(
    consumer_group: str,
    message_model: str,
    namesrv_manager: AsyncNameServerManager | None = None,
    broker_manager: AsyncBrokerManager | None = None,
    store_path: str = "~/.rocketmq/offsets",
    persist_interval: int = 5000,
    **kwargs: Any,
) -> AsyncOffsetStore:
    """异步便捷函数：创建偏移量存储实例

    通过异步工厂方法创建OffsetStore实例。这是一个便利函数，
    提供了简单的异步接口来创建不同类型的偏移量存储。

    Args:
        consumer_group (str): 消费者组名称，用于标识消费者实例
        message_model (MessageModel): 消费消费模式枚举
        namesrv_manager (AsyncNameServerManager | None): 异步NameServer管理器
        broker_manager (AsyncBrokerManager | None): 异步Broker连接管理器，集群模式必需
        store_path (str): 本地存储路径，广播模式使用
        persist_interval (int): 持久化间隔（毫秒）
        **kwargs: 其他配置参数

    Returns:
        OffsetStore: 偏移量存储实例，保证已启动状态
            - 集群模式返回AsyncRemoteOffsetStore实例
            - 广播模式返回LocalOffsetStore实例
            - 实例已启动并可用

    Raises:
        ValueError: 当参数配置不合法时抛出
        RuntimeError: 当OffsetStore创建或启动失败时抛出

    Example:
        >>> # 快速异步创建集群模式存储
        >>> store = await create_offset_store(
        ...     consumer_group="quick_group",
        ...     message_model=MessageModel.CLUSTERING,
        ...     broker_manager=async_broker_mgr
        ... )
        >>>
        >>> # 异步创建广播模式存储
        >>> local_store = await create_offset_store(
        ...     consumer_group="broadcast_group",
        ...     message_model=MessageModel.BROADCASTING,
        ...     store_path="/tmp/offsets"
        ... )
    """
    return await AsyncOffsetStoreFactory.create_offset_store(
        consumer_group=consumer_group,
        message_model=message_model,
        namesrv_manager=namesrv_manager,
        broker_manager=broker_manager,
        store_path=store_path,
        persist_interval=persist_interval,
        **kwargs,
    )


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

        message_model (MessageModel): 消费消费模式
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
        ...     store = await create_offset_store(...)
        ... else:
        ...     print("配置验证失败")
        >>>
        >>> # 验证广播模式配置
        >>> if validate_offset_store_config(
        ...     consumer_group="broadcast_group",
        ...     message_model=MessageModel.BROADCASTING,
        ...     store_path="/tmp/offsets"
        ... ):
        ...     store = await create_offset_store(...)
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
