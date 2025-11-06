"""
队列分配策略模块 - Consumer队列分配策略

提供平均分配和哈希分配两种队列分配策略，用于在消费者重平衡时将消息队列分配给不同的消费者实例。
"""

import hashlib
from abc import ABC, abstractmethod
from dataclasses import dataclass

from pyrocketmq.logging import get_logger
from pyrocketmq.model import MessageQueue
from pyrocketmq.model.consumer import AllocateQueueStrategy

logger = get_logger(__name__)


@dataclass
class AllocateContext:
    """队列分配上下文

    包含队列分配所需的所有信息，包括消费者ID、消息队列列表等。
    """

    consumer_group: str  # 消费者组名称
    consumer_id: str  # 当前消费者ID
    current_cid_all: list[str]  # 当前消费者组所有消费者ID列表
    mq_all: list[MessageQueue]  # 所有消息队列列表
    mq_divide: dict[str, list[MessageQueue]]  # 分区后的消息队列字典

    def __post_init__(self) -> None:
        """初始化后处理"""
        if not self.current_cid_all:
            raise ValueError("消费者ID列表不能为空")

        if not self.mq_all:
            raise ValueError("消息队列列表不能为空")

        if self.consumer_id not in self.current_cid_all:
            raise ValueError(f"当前消费者ID {self.consumer_id} 不在消费者组中")


class AllocateQueueStrategyBase(ABC):
    """队列分配策略抽象基类

    定义了队列分配策略的统一接口，所有具体策略都需要实现此接口。
    """

    def __init__(self, strategy_name: str) -> None:
        """初始化队列分配策略

        Args:
            strategy_name (str): 策略名称，用于日志和监控
        """
        self.strategy_name: str = strategy_name
        self._logger = get_logger(f"{__name__}.{strategy_name}")

    @abstractmethod
    def allocate(self, context: AllocateContext) -> list[MessageQueue]:
        """分配消息队列

        根据策略将消息队列分配给当前消费者。

        Args:
            context (AllocateContext): 队列分配上下文

        Returns:
            list[MessageQueue]: 分配给当前消费者的消息队列列表

        Raises:
            ValueError: 当参数不合法时抛出
            RuntimeError: 当分配失败时抛出
        """
        pass

    def _validate_context(self, context: AllocateContext) -> None:
        """验证分配上下文

        Args:
            context (AllocateContext): 分配上文

        Raises:
            ValueError: 当上下文不合法时抛出
        """
        if not isinstance(context, AllocateContext):
            raise ValueError("上下文必须是AllocateContext类型")

        if not context.current_cid_all:
            raise ValueError("消费者ID列表不能为空")

        if not context.mq_all:
            raise ValueError("消息队列列表不能为空")

        if context.consumer_id not in context.current_cid_all:
            raise ValueError(f"当前消费者ID {context.consumer_id} 不在消费者组中")

    def _get_consumer_index(self, consumer_id: str, consumer_list: list[str]) -> int:
        """获取消费者在列表中的索引

        Args:
            consumer_id (str): 消费者ID
            consumer_list (list[str]): 消费者ID列表

        Returns:
            int: 消费者在列表中的索引
        """
        try:
            return consumer_list.index(consumer_id)
        except ValueError:
            raise ValueError(f"消费者ID {consumer_id} 不在消费者列表中")

    def _sort_consumer_list(self, consumer_list: list[str]) -> list[str]:
        """排序消费者列表

        确保所有消费者使用相同的排序规则，避免分配不一致。

        Args:
            consumer_list (list[str]): 消费者ID列表

        Returns:
            list[str]: 排序后的消费者ID列表
        """
        return sorted(consumer_list)

    def _sort_queue_list(self, queue_list: list[MessageQueue]) -> list[MessageQueue]:
        """排序消息队列列表

        确保所有消费者使用相同的排序规则，避免分配不一致。

        Args:
            queue_list (list[MessageQueue]): 消息队列列表

        Returns:
            list[MessageQueue]: 排序后的消息队列列表
        """
        return sorted(
            queue_list, key=lambda mq: (mq.topic, mq.broker_name, mq.queue_id)
        )


class AverageAllocateStrategy(AllocateQueueStrategyBase):
    """平均分配策略

    将消息队列平均分配给所有消费者，确保负载相对均衡。
    当队列数量不能整除消费者数量时，前面的消费者会多分配一个队列。

    算法特点：
    - 简单高效，计算复杂度低
    - 负载相对均衡
    - 适合大多数场景
    - MVP版本的默认策略
    """

    def __init__(self) -> None:
        """初始化平均分配策略"""
        super().__init__("AverageAllocateStrategy")

    def allocate(self, context: AllocateContext) -> list[MessageQueue]:
        """分配消息队列

        平均分配策略实现：
        1. 排序消费者和队列列表
        2. 计算每个消费者应分配的队列数量
        3. 为当前消费者分配对应的队列范围

        Args:
            context (AllocateContext): 队列分配上下文

        Returns:
            list[MessageQueue]: 分配给当前消费者的消息队列列表
        """
        self._validate_context(context)

        # 排序确保一致性
        sorted_consumers: list[str] = self._sort_consumer_list(context.current_cid_all)
        sorted_queues: list[MessageQueue] = self._sort_queue_list(context.mq_all)

        # 获取当前消费者索引
        consumer_index: int = self._get_consumer_index(
            context.consumer_id, sorted_consumers
        )

        # 计算分配范围
        consumer_count: int = len(sorted_consumers)
        queue_count: int = len(sorted_queues)

        # 计算每个消费者分配的队列数量
        base_size: int = queue_count // consumer_count
        remainder: int = queue_count % consumer_count

        # 计算当前消费者的起始和结束索引
        start_index: int = consumer_index * base_size + min(consumer_index, remainder)
        end_index: int = (
            start_index + base_size + (1 if consumer_index < remainder else 0)
        )

        # 分配队列
        allocated_queues: list[MessageQueue] = sorted_queues[start_index:end_index]

        self._logger.info(
            "平均分配完成",
            extra={
                "consumer_id": context.consumer_id,
                "consumer_index": consumer_index,
                "allocated_queue_count": len(allocated_queues),
                "queue_range_start": start_index,
                "queue_range_end": end_index,
                "strategy": "average",
            },
        )

        return allocated_queues


class HashAllocateStrategy(AllocateQueueStrategyBase):
    """哈希分配策略

    基于消费者ID的哈希值进行队列分配，确保相同的消费者ID
    总是分配到相同的队列集合。适合需要稳定分配的场景。

    算法特点：
    - 分配结果稳定，重启后不变
    - 负载相对均衡
    - 适合需要固定分配的场景
    - 哈希计算有一定开销
    """

    def __init__(self) -> None:
        """初始化哈希分配策略"""
        super().__init__("HashAllocateStrategy")

    def allocate(self, context: AllocateContext) -> list[MessageQueue]:
        """分配消息队列

        哈希分配策略实现：
        1. 对消费者ID进行哈希计算
        2. 根据哈希值确定分配的队列范围
        3. 分配对应的队列集合

        Args:
            context (AllocateContext): 队列分配上下文

        Returns:
            list[MessageQueue]: 分配给当前消费者的消息队列列表
        """
        self._validate_context(context)

        # 排序确保一致性
        sorted_consumers: list[str] = self._sort_consumer_list(context.current_cid_all)
        sorted_queues: list[MessageQueue] = self._sort_queue_list(context.mq_all)

        # 获取当前消费者索引
        consumer_index: int = self._get_consumer_index(
            context.consumer_id, sorted_consumers
        )

        # 计算消费者ID的哈希值
        consumer_hash: int = self._calculate_hash(context.consumer_id)

        # 计算分配范围
        consumer_count: int = len(sorted_consumers)
        queue_count: int = len(sorted_queues)

        base_size: int = queue_count // consumer_count
        remainder: int = queue_count % consumer_count

        start_index: int = consumer_index * base_size + min(consumer_index, remainder)
        end_index: int = (
            start_index + base_size + (1 if consumer_index < remainder else 0)
        )

        # 分配队列
        allocated_queues: list[MessageQueue] = sorted_queues[start_index:end_index]

        self._logger.info(
            "哈希分配完成",
            extra={
                "consumer_id": context.consumer_id,
                "consumer_hash": consumer_hash,
                "allocated_queue_count": len(allocated_queues),
                "queue_range_start": start_index,
                "queue_range_end": end_index,
                "strategy": "hash",
            },
        )

        return allocated_queues

    def _calculate_hash(self, consumer_id: str) -> int:
        """计算消费者ID的哈希值

        使用MD5算法计算哈希值，确保分布均匀。

        Args:
            consumer_id (str): 消费者ID

        Returns:
            int: 哈希值
        """
        hash_bytes: bytes = hashlib.md5(consumer_id.encode("utf-8")).digest()
        return int.from_bytes(hash_bytes[:4], byteorder="big")


class AllocateQueueStrategyFactory:
    """队列分配策略工厂

    提供统一的策略创建接口，支持通过枚举或字符串创建策略实例。
    """

    @staticmethod
    def create_strategy(strategy: str) -> AllocateQueueStrategyBase:
        """创建队列分配策略实例

        Args:
            strategy (str): 策略类型，使用model.consumer.AllocateQueueStrategy中的值

        Returns:
            AllocateQueueStrategyBase: 策略实例

        Raises:
            ValueError: 当策略类型不支持时抛出
        """
        if strategy == AllocateQueueStrategy.AVERAGE:
            return AverageAllocateStrategy()

        elif strategy == AllocateQueueStrategy.HASH:
            return HashAllocateStrategy()

        else:
            raise ValueError(f"不支持的分配策略: {strategy}")

    @staticmethod
    def get_available_strategies() -> list[str]:
        """获取所有可用的策略类型

        Returns:
            list[str]: 策略类型列表
        """
        return [AllocateQueueStrategy.AVERAGE, AllocateQueueStrategy.HASH]


# 便利函数
def create_average_strategy() -> AverageAllocateStrategy:
    """创建平均分配策略

    Returns:
        AverageAllocateStrategy: 平均分配策略实例
    """
    return AverageAllocateStrategy()


def create_hash_strategy() -> HashAllocateStrategy:
    """创建哈希分配策略

    Returns:
        HashAllocateStrategy: 哈希分配策略实例
    """
    return HashAllocateStrategy()


def allocate_queues(
    strategy: str,
    consumer_group: str,
    consumer_id: str,
    all_consumer_ids: list[str],
    all_queues: list[MessageQueue],
) -> list[MessageQueue]:
    """便利函数：执行队列分配

    Args:
        strategy (str): 分配策略
        consumer_group (str): 消费者组名称
        consumer_id (str): 当前消费者ID
        all_consumer_ids (list[str]): 所有消费者ID列表
        all_queues (list[MessageQueue]): 所有消息队列列表

    Returns:
        list[MessageQueue]: 分配给当前消费者的消息队列列表
    """
    # 创建策略实例
    allocator: AllocateQueueStrategyBase = AllocateQueueStrategyFactory.create_strategy(
        strategy
    )

    # 创建分配上下文
    context: AllocateContext = AllocateContext(
        consumer_group=consumer_group,
        consumer_id=consumer_id,
        current_cid_all=all_consumer_ids,
        mq_all=all_queues,
        mq_divide={},  # 可以根据需要设置分区信息
    )

    # 执行分配
    return allocator.allocate(context)
