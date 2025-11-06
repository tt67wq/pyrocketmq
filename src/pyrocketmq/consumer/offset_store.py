"""
OffsetStore - 偏移量存储模块

负责管理和持久化消息消费偏移量，确保Consumer能够记录每个队列的消费进度，
并在重启后恢复消费位置。

支持两种存储模式：
1. 集群模式(Clustering) - 远程存储：偏移量存储在Broker服务器上
2. 广播模式(Broadcasting) - 本地存储：偏移量存储在本地文件中
"""

import threading
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any

from pyrocketmq.logging import get_logger
from pyrocketmq.model import MessageQueue

logger = get_logger(__name__)


class ReadOffsetType(Enum):
    """偏移量读取类型枚举

    定义了从偏移量存储中读取数据的不同策略，用于控制读取优先级和数据源。
    """

    MEMORY_FIRST_THEN_STORE = "MEMORY_FIRST_THEN_STORE"
    """优先从内存缓存读取，失败则从持久化存储读取

    读取策略：
    1. 首先尝试从内存缓存获取偏移量
    2. 如果内存中不存在，则从持久化存储加载
    3. 将加载的偏移量更新到内存缓存

    适用场景：
    - 需要保证数据完整性的读取操作
    - 首次启动时的偏移量加载
    """

    READ_FROM_MEMORY = "READ_FROM_MEMORY"
    """仅从内存缓存读取偏移量

    特点：
    - 只从内存缓存中读取数据
    - 不会触发持久化存储的读取
    - 性能最优但可能不完整

    适用场景：
    - 获取当前会话中的最新偏移量
    - 性能敏感的频繁读取操作
    """

    READ_FROM_STORE = "READ_FROM_STORE"
    """仅从持久化存储读取偏移量

    特点：
    - 直接从持久化存储读取最新数据
    - 绕过内存缓存，确保数据一致性
    - 性能相对较低但数据最新

    适用场景：
    - 需要获取最新持久化数据的场景
    - 内存缓存可能不一致时的强制刷新
    """


@dataclass
class OffsetEntry:
    """偏移量条目数据类

    包含单个消息队列的偏移量信息以及更新时间戳。
    用于序列化和持久化存储偏移量数据。

    Attributes:
        queue (MessageQueue): 消息队列对象，包含topic、broker_name和queue_id
        offset (int): 队列的消费偏移量，表示下一条要消费的消息位置
        last_update_timestamp (int): 最后更新时间戳（毫秒），用于跟踪偏移量的更新时间
    """

    queue: MessageQueue
    offset: int
    last_update_timestamp: int = 0

    def __post_init__(self) -> None:
        """初始化后处理

        如果未提供时间戳，则使用当前时间作为默认值。
        确保每个偏移量条目都有有效的时间戳信息。
        """
        if self.last_update_timestamp == 0:
            self.last_update_timestamp = int(datetime.now().timestamp() * 1000)

    def to_dict(self) -> dict[str, Any]:
        """转换为字典格式

        将偏移量条目序列化为字典，便于JSON序列化和持久化存储。

        Returns:
            dict[str, Any]: 包含所有字段的字典，键名遵循RocketMQ协议格式

            字典结构：
            - topic (str): 消息主题名称
            - broker_name (str): Broker名称
            - queue_id (int): 队列ID
            - offset (int): 消费偏移量
            - last_update_timestamp (int): 最后更新时间戳（毫秒）
        """
        return {
            "topic": self.queue.topic,
            "broker_name": self.queue.broker_name,
            "queue_id": self.queue.queue_id,
            "offset": self.offset,
            "last_update_timestamp": self.last_update_timestamp,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "OffsetEntry":
        """从字典创建OffsetEntry实例

        从字典数据反序列化创建偏移量条目，通常用于从持久化存储中加载数据。

        Args:
            data (dict[str, Any]): 包含偏移量信息的字典，必须包含以下字段：
                - topic (str): 消息主题名称
                - broker_name (str): Broker名称
                - queue_id (int): 队列ID
                - offset (int): 消费偏移量
                - last_update_timestamp (int, 可选): 最后更新时间戳，默认为0

        Returns:
            OffsetEntry: 新创建的偏移量条目实例

        Raises:
            KeyError: 当缺少必需的字段时抛出
            ValueError: 当字段值类型不正确时抛出
        """
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
    """偏移量存储抽象基类

    定义了偏移量存储的核心接口和生命周期管理方法。
    支持两种存储模式：
    1. 集群模式(Clustering) - 远程存储：偏移量存储在Broker服务器上
    2. 广播模式(Broadcasting) - 本地存储：偏移量存储在本地文件中

    生命周期管理：
    - start(): 启动偏移量存储服务，初始化资源
    - stop(): 停止偏移量存储服务，清理资源
    - load(): 加载历史偏移量数据
    - persist(): 持久化偏移量到存储
    """

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
    def start(self) -> None:
        """启动偏移量存储服务

        初始化存储服务所需的资源，如：
        - 启动后台持久化线程
        - 建立网络连接
        - 加载初始化数据
        """
        pass

    @abstractmethod
    def stop(self) -> None:
        """停止偏移量存储服务

        清理存储服务占用的资源，如：
        - 停止后台线程
        - 关闭网络连接
        - 持久化剩余数据
        """
        pass

    @abstractmethod
    def load(self) -> None:
        """加载偏移量数据

        从持久化存储中加载历史偏移量数据到内存缓存。
        通常在消费者启动时调用，用于恢复上次的消费位置。

        Raises:
            OffsetStoreError: 当加载失败时抛出，可能的原因包括：
                - 存储文件损坏或不存在
                - 网络连接失败（远程存储）
                - 数据格式错误
                - 权限不足
        """
        pass

    @abstractmethod
    def update_offset(self, queue: MessageQueue, offset: int) -> None:
        """更新偏移量到本地缓存

        将指定队列的消费偏移量更新到内存缓存中，但不立即持久化。
        这是一个高频操作，需要在锁保护下进行以确保线程安全。

        Args:
            queue (MessageQueue): 要更新偏移量的消息队列
            offset (int): 新的消费偏移量，表示下一条要消费的消息位置

        Raises:
            ValueError: 当offset值为负数时抛出
            TypeError: 当queue或offset参数类型不正确时抛出

        Note:
            此操作仅更新内存缓存，不会触发持久化。
            持久化操作需要显式调用persist()或persist_all()方法。
        """
        pass

    @abstractmethod
    def persist(self, queue: MessageQueue) -> None:
        """持久化单个队列的偏移量

        将指定队列的偏移量从内存缓存持久化到存储介质。
        对于远程存储，发送网络请求到Broker；对于本地存储，写入文件。

        Args:
            queue (MessageQueue): 要持久化的消息队列

        Raises:
            OffsetStoreError: 当持久化失败时抛出，可能的原因包括：
                - 网络连接失败（远程存储）
                - 磁盘空间不足（本地存储）
                - 权限不足
                - Broker不可用

        Note:
            此操作是同步的，会阻塞直到持久化完成或失败。
            批量持久化请使用persist_all()方法以提高性能。
        """
        pass

    @abstractmethod
    def persist_all(self) -> None:
        """持久化所有偏移量

        将内存缓存中的所有偏移量批量持久化到存储介质。
        相比多次调用persist()方法，批量操作通常有更好的性能。

        Raises:
            OffsetStoreError: 当批量持久化失败时抛出
            PartialPersistError: 当部分偏移量持久化失败时抛出（某些实现）

        Performance:
            - 远程存储：通过单次网络请求提交多个偏移量
            - 本地存储：通过单次文件写入操作保存所有数据
            - 批量操作通常比多次单独操作快数倍
        """
        pass

    @abstractmethod
    def read_offset(self, queue: MessageQueue, read_type: ReadOffsetType) -> int:
        """读取偏移量

        根据指定的读取类型从不同数据源获取队列的消费偏移量。

        Args:
            queue (MessageQueue): 要读取偏移量的消息队列
            read_type (ReadOffsetType): 读取策略，决定数据源和读取优先级

        Returns:
            int: 队列的消费偏移量，如果不存在则返回-1
                返回值表示下一条要消费的消息在队列中的位置

        Raises:
            OffsetStoreError: 当读取失败时抛出
            ValueError: 当read_type参数无效时抛出

        Behavior:
            - MEMORY_FIRST_THEN_STORE: 优先内存，失败则读取存储
            - READ_FROM_MEMORY: 仅从内存读取，可能返回-1
            - READ_FROM_STORE: 仅从存储读取，性能较低但数据最新

        Examples:
            >>> offset = store.read_offset(queue, ReadOffsetType.MEMORY_FIRST_THEN_STORE)
            >>> if offset == -1:
            ...     print("未找到偏移量，将从开始位置消费")
        """
        pass

    @abstractmethod
    def remove_offset(self, queue: MessageQueue) -> None:
        """移除队列的偏移量

        从内存缓存和持久化存储中移除指定队列的偏移量信息。
        通常在队列重新分配或消费者组变化时调用。

        Args:
            queue (MessageQueue): 要移除偏移量的消息队列

        Raises:
            OffsetStoreError: 当移除操作失败时抛出

        Note:
            此操作会同时清理内存缓存和持久化存储中的数据。
            操作不可逆，请谨慎使用。
        """
        pass

    @abstractmethod
    def get_metrics(self) -> dict[str, Any]:
        """获取偏移量存储的性能指标

        返回包含操作统计和性能指标的字典，用于监控和调优。

        Returns:
            dict[str, Any]: 包含以下指标的字典：
                - persist_success_count (int): 持久化成功次数
                - persist_failure_count (int): 持久化失败次数
                - load_success_count (int): 加载成功次数
                - load_failure_count (int): 加载失败次数
                - cache_hit_count (int): 缓存命中次数
                - cache_miss_count (int): 缓存未命中次数
                - avg_persist_latency (float): 平均持久化延迟（毫秒）

        Note:
            具体指标项可能因不同的实现而有所差异。
            返回的字典可用于性能监控和问题诊断。
        """
        pass

    def clone_offset_table(self, topic: str | None = None) -> dict[MessageQueue, int]:
        """克隆偏移量表

        创建当前偏移量表的深拷贝，用于安全的并发访问或数据处理。
        操作在锁保护下进行，确保数据一致性。

        Args:
            topic (str | None): 可选的主题过滤条件
                - 如果指定topic，只返回该主题的偏移量
                - 如果为None，返回所有主题的偏移量

        Returns:
            dict[MessageQueue, int]: 偏移量表的副本，包含队列到偏移量的映射
                返回的是新的字典实例，修改不会影响原始数据

        Examples:
            >>> # 克隆所有偏移量
            >>> all_offsets = store.clone_offset_table()
            >>>
            >>> # 只克隆特定topic的偏移量
            >>> topic_offsets = store.clone_offset_table("test_topic")
            >>>
            >>> # 安全地遍历偏移量
            >>> for queue, offset in all_offsets.items():
            ...     print(f"{queue.topic}: {offset}")
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
        """获取所有缓存的偏移量

        获取当前内存中缓存的所有队列偏移量。这是一个快照操作，
        返回的数据不受后续修改的影响。

        Returns:
            dict[MessageQueue, int]: 包含所有队列偏移量的字典副本

        Performance:
            - 时间复杂度：O(n)，其中n是缓存中队列的数量
            - 空间复杂度：O(n)，创建新的字典实例
            - 操作在锁保护下进行，可能短暂阻塞其他线程

        Thread Safety:
            - 返回的字典是线程安全的独立副本
            - 不会影响原始缓存数据的并发访问
        """
        with self._lock:
            return self.offset_table.copy()

    def clear_cached_offsets(self) -> None:
        """清空缓存的偏移量

        清空内存中的所有偏移量缓存。此操作不会影响持久化存储中的数据，
        仅清除内存缓存。通常在重置消费者状态或清理资源时使用。

        Warning:
            此操作会丢失所有未持久化的偏移量数据。
            在调用前请确保已调用persist_all()保存重要数据。

        Thread Safety:
            操作在锁保护下进行，确保原子性。
            在清理过程中，其他线程将被阻塞。
        """
        with self._lock:
            self.offset_table.clear()

    def _update_local_cache(self, queue: MessageQueue, offset: int) -> None:
        """更新本地缓存（内部方法，需要在锁保护下调用）

        将指定队列的偏移量更新到内存缓存中。这是一个内部方法，
        调用者必须已经获取了self._lock锁。

        Args:
            queue (MessageQueue): 要更新的消息队列
            offset (int): 新的偏移量值

        Note:
            此方法不是线程安全的，必须在锁保护下调用。
            主要供子类的具体实现使用。
        """
        self.offset_table[queue] = offset
        logger.info(
            "local offset cache updated", extra={"queue": str(queue), "offset": offset}
        )

    def _get_from_local_cache(self, queue: MessageQueue) -> int | None:
        """从本地缓存获取偏移量（内部方法，需要在锁保护下调用）

        从内存缓存中获取指定队列的偏移量。这是一个内部方法，
        调用者必须已经获取了self._lock锁。

        Args:
            queue (MessageQueue): 要查询的消息队列

        Returns:
            int | None: 找到的偏移量，如果不存在则返回None

        Note:
            此方法不是线程安全的，必须在锁保护下调用。
            返回None与返回-1有不同含义：None表示缓存中不存在，
            -1通常是read_offset()方法约定的"不存在"返回值。
        """
        return self.offset_table.get(queue)

    def _remove_from_local_cache(self, queue: MessageQueue) -> bool:
        """从本地缓存移除偏移量（内部方法，需要在锁保护下调用）

        从内存缓存中移除指定队列的偏移量。这是一个内部方法，
        调用者必须已经获取了self._lock锁。

        Args:
            queue (MessageQueue): 要移除的消息队列

        Returns:
            bool: 如果成功移除返回True，如果队列不存在返回False

        Note:
            此方法不是线程安全的，必须在锁保护下调用。
            仅从内存缓存中移除，不会影响持久化存储。
        """
        if queue in self.offset_table:
            del self.offset_table[queue]
            logger.info("local offset cache entry removed", extra={"queue": str(queue)})
            return True
        return False


class OffsetStoreMetrics:
    """偏移量存储指标收集器

    负责收集和统计偏移量存储操作的性能指标，包括成功率、延迟、
    缓存命中率等关键指标。用于性能监控、问题诊断和系统优化。

    主要指标类别：
    - 持久化操作统计：成功/失败次数、平均延迟
    - 加载操作统计：成功/失败次数
    - 缓存性能统计：命中/未命中次数
    - 综合性能指标：平均延迟、成功率等

    Thread Safety:
        所有方法都是线程安全的，可以在多线程环境中并发调用。
        使用原子操作更新计数器，确保数据一致性。
    """

    def __init__(self) -> None:
        """初始化指标收集器

        创建所有计数器并初始化为0。开始收集指标后，
        通过相应的record_*方法更新统计数据。
        """
        self.persist_success_count: int = 0
        self.persist_failure_count: int = 0
        self.load_success_count: int = 0
        self.load_failure_count: int = 0
        self.cache_hit_count: int = 0
        self.cache_miss_count: int = 0
        self.total_persist_latency: float = 0.0
        self.persist_operation_count: int = 0

    def record_persist_success(self, latency_ms: float) -> None:
        """记录持久化成功

        记录一次成功的偏移量持久化操作，包括操作耗时。

        Args:
            latency_ms (float): 持久化操作耗时（毫秒）
                应该是从开始操作到完成操作的总时间

        Note:
            此方法会更新以下指标：
            - persist_success_count: 持久化成功计数器 +1
            - total_persist_latency: 累计延迟增加
            - persist_operation_count: 操作计数器 +1

        Example:
            >>> start_time = time.time()
            >>> # 执行持久化操作
            >>> store.persist(queue)
            >>> latency = (time.time() - start_time) * 1000
            >>> metrics.record_persist_success(latency)
        """
        self.persist_success_count += 1
        self.total_persist_latency += latency_ms
        self.persist_operation_count += 1

    def record_persist_failure(self) -> None:
        """记录持久化失败

        记录一次失败的偏移量持久化操作。失败操作不记录延迟，
        因为失败的原因可能是网络超时、异常等，延迟数据没有意义。

        Note:
            此方法会更新persist_failure_count计数器。
            失败操作不影响平均延迟计算，以确保延迟指标的准确性。

        Example:
            >>> try:
            ...     store.persist(queue)
            ... except Exception as e:
            ...     metrics.record_persist_failure()
            ...     logger.error(f"持久化失败: {e}")
        """
        self.persist_failure_count += 1

    def record_load_success(self) -> None:
        """记录加载成功

        记录一次成功的偏移量加载操作。加载操作通常在消费者启动时
        或从持久化存储恢复数据时执行。

        Note:
            此方法会更新load_success_count计数器。
            加载操作通常批量进行，所以单个成功记录可能代表
            加载了多个偏移量。

        Example:
            >>> try:
            ...     store.load()
            ...     metrics.record_load_success()
            ... except Exception as e:
            ...     metrics.record_load_failure()
        """
        self.load_success_count += 1

    def record_load_failure(self) -> None:
        """记录加载失败

        记录一次失败的偏移量加载操作。加载失败可能由于存储损坏、
        网络问题或权限不足等原因造成。

        Note:
            此方法会更新load_failure_count计数器。
            加载失败通常是比较严重的问题，可能影响消费者的正常启动。
        """
        self.load_failure_count += 1

    def record_cache_hit(self) -> None:
        """记录缓存命中

        记录一次成功的缓存读取操作。缓存命中表示从内存缓存中
        成功获取了偏移量数据，避免了昂贵的持久化存储访问。

        Performance Impact:
            缓存命中通常比存储访问快几个数量级，是性能优化的重要指标。
            高缓存命中率通常意味着良好的性能表现。

        Example:
            >>> offset = store._get_from_local_cache(queue)
            >>> if offset is not None:
            ...     metrics.record_cache_hit()
            ... else:
            ...     metrics.record_cache_miss()
        """
        self.cache_hit_count += 1

    def record_cache_miss(self) -> None:
        """记录缓存未命中

        记录一次缓存读取失败操作。缓存未命中表示内存缓存中
        没有找到所需数据，需要从持久化存储中加载。

        Performance Impact:
            缓存未命中会触发存储访问，性能相对较差。
            过高的缓存未命中率可能需要优化缓存策略。

        Optimization:
            - 考虑增加缓存大小
            - 优化缓存过期策略
            - 预加载热点数据
        """
        self.cache_miss_count += 1

    @property
    def avg_persist_latency(self) -> float:
        """平均持久化延迟（毫秒）

        计算所有成功持久化操作的平均耗时。只考虑成功的操作，
        因为失败操作的延迟数据通常不可靠或没有意义。

        Returns:
            float: 平均持久化延迟（毫秒）
                - 如果没有成功的持久化操作，返回0.0
                - 否则返回总延迟除以成功操作次数

        Calculation:
            avg_latency = total_persist_latency / persist_success_count

        Usage:
            用于监控持久化性能，识别性能瓶颈。
            如果延迟过高，可能需要优化网络或存储性能。
        """
        if self.persist_operation_count == 0:
            return 0.0
        return self.total_persist_latency / self.persist_operation_count

    @property
    def persist_success_rate(self) -> float:
        """持久化成功率

        计算持久化操作的成功率，用于评估偏移量存储的可靠性。

        Returns:
            float: 持久化成功率（0.0-1.0）
                - 如果没有持久化操作，返回1.0（默认成功）
                - 否则返回成功次数除以总操作次数

        Calculation:
            success_rate = persist_success_count / (persist_success_count + persist_failure_count)
        """
        total_operations = self.persist_success_count + self.persist_failure_count
        if total_operations == 0:
            return 1.0
        return self.persist_success_count / total_operations

    @property
    def cache_hit_rate(self) -> float:
        """缓存命中率

        计算缓存读取的命中率，用于评估缓存策略的有效性。

        Returns:
            float: 缓存命中率（0.0-1.0）
                - 如果没有缓存访问，返回0.0
                - 否则返回命中次数除以总访问次数

        Performance Benchmark:
            - > 90%: 优秀的缓存性能
            - 70-90%: 良好的缓存性能
            - < 70%: 需要优化缓存策略
        """
        total_access = self.cache_hit_count + self.cache_miss_count
        if total_access == 0:
            return 0.0
        return self.cache_hit_count / total_access

    def to_dict(self) -> dict[str, Any]:
        """转换为字典格式

        将所有指标数据转换为字典格式，便于序列化、监控和报告。
        包含原始计数数据和计算得出的比率指标。

        Returns:
            dict[str, Any]: 包含所有指标的字典，包括：
                - persist_success_count (int): 持久化成功次数
                - persist_failure_count (int): 持久化失败次数
                - persist_success_rate (float): 持久化成功率
                - load_success_count (int): 加载成功次数
                - load_failure_count (int): 加载失败次数
                - cache_hit_count (int): 缓存命中次数
                - cache_miss_count (int): 缓存未命中次数
                - cache_hit_rate (float): 缓存命中率
                - avg_persist_latency (float): 平均持久化延迟（毫秒）
                - persist_operation_count (int): 总持久化操作次数

        Example:
            >>> metrics_dict = metrics.to_dict()
            >>> print(f"持久化成功率: {metrics_dict['persist_success_rate']:.2%}")
            >>> print(f"缓存命中率: {metrics_dict['cache_hit_rate']:.2%}")
        """
        return {
            "persist_success_count": self.persist_success_count,
            "persist_failure_count": self.persist_failure_count,
            "persist_success_rate": self.persist_success_rate,
            "load_success_count": self.load_success_count,
            "load_failure_count": self.load_failure_count,
            "cache_hit_count": self.cache_hit_count,
            "cache_miss_count": self.cache_miss_count,
            "cache_hit_rate": self.cache_hit_rate,
            "avg_persist_latency": self.avg_persist_latency,
            "persist_operation_count": self.persist_operation_count,
        }

    def reset(self) -> None:
        """重置所有指标

        将所有计数器和统计数据重置为初始状态。
        通常在开始新的统计周期或重新初始化系统时使用。

        Note:
            此操作是不可逆的，所有历史数据都会丢失。
            在重置前如有需要，请先调用to_dict()保存当前数据。
        """
        self.persist_success_count = 0
        self.persist_failure_count = 0
        self.load_success_count = 0
        self.load_failure_count = 0
        self.cache_hit_count = 0
        self.cache_miss_count = 0
        self.total_persist_latency = 0.0
        self.persist_operation_count = 0
