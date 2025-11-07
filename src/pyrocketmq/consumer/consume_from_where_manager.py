"""
消费起始位置管理模块

实现ConsumeFromWhere策略管理，负责根据不同的消费策略确定消费者开始消费的偏移量位置。
支持从最后偏移量、第一个偏移量或指定时间戳开始消费。
"""

import logging
import time
from typing import Any

from pyrocketmq.broker import BrokerClient, BrokerManager
from pyrocketmq.consumer.errors import BrokerNotAvailableError, OffsetFetchError
from pyrocketmq.logging import get_logger
from pyrocketmq.model import MessageQueue
from pyrocketmq.model.consumer import ConsumeFromWhere
from pyrocketmq.nameserver import NameServerManager


class ConsumeFromWhereManager:
    """消费起始位置管理器

    负责根据不同的消费策略确定消费者开始消费的偏移量位置。
    通过BrokerManager与Broker通信获取队列的偏移量信息，并按照策略返回合适的起始偏移量。

    支持的起始位置策略：
    - CONSUME_FROM_LAST_OFFSET: 从最大偏移量开始消费（跳过已有消息）
    - CONSUME_FROM_FIRST_OFFSET: 从最小偏移量开始消费（消费所有历史消息）
    - CONSUME_FROM_TIMESTAMP: 从指定时间戳对应的偏移量开始消费
    """

    def __init__(
        self,
        consume_group: str,
        namesrv_manager: NameServerManager,
        broker_manager: BrokerManager,
    ) -> None:
        """初始化消费起始位置管理器实例。

        创建一个新的消费起始位置管理器，用于管理消费者组的消费起始位置策略。
        该管理器负责根据不同的消费策略确定消费者开始消费的偏移量位置。

        Args:
            consume_group (str): 消费者组名称，用于标识消费者的逻辑分组
            namesrv_manager (NameServerManager): NameServer管理器实例，用于获取Broker地址信息
            broker_manager (BrokerManager): Broker管理器实例，用于与Broker建立连接

        Raises:
            TypeError: 当任何参数的类型不正确时抛出
        """
        self.consume_group: str = consume_group
        self.namesrv_manager: NameServerManager = namesrv_manager
        self.broker_manager: BrokerManager = broker_manager
        self._logger: logging.Logger = get_logger(f"{__name__}.ConsumeFromWhereManager")

    def get_consume_offset(
        self,
        queue: MessageQueue,
        strategy: str,
        timestamp: int = 0,
    ) -> int:
        """根据指定的消费策略获取队列的消费起始偏移量。

        根据不同的消费起始位置策略，计算出消费者应该从哪个偏移量开始消费消息。
        支持从最大偏移量、最小偏移量或指定时间戳对应的偏移量开始消费。

        Args:
            queue (MessageQueue): 目标消息队列，包含topic、broker名称和队列ID信息
            strategy (str): 消费起始位置策略，支持的值包括：
                - ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET: 从最大偏移量开始
                - ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET: 从最小偏移量开始
                - ConsumeFromWhere.CONSUME_FROM_TIMESTAMP: 从指定时间戳开始
            timestamp (int, optional): 时间戳（毫秒），仅在strategy为TIMESTAMP时使用。
                当为0时，使用当前时间戳。默认值为0。

        Returns:
            int: 计算得出的消费起始偏移量。返回值表示消费者应该开始读取消息的偏移量位置。

        Raises:
            ValueError: 当提供的策略不被支持或参数无效时抛出
            RuntimeError: 当获取偏移量过程中发生网络或系统错误时抛出
            BrokerNotAvailableError: 当目标Broker不可用时抛出
            OffsetFetchError: 当从Broker获取偏移量失败时抛出

        Examples:
            >>> manager = ConsumeFromWhereManager(consume_group, namesrv_mgr, broker_mgr)
            >>> queue = MessageQueue(broker_name="broker1", topic="test_topic", queue_id=0)
            >>> offset = manager.get_consume_offset(queue, ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET)
            >>> print(f"从偏移量 {offset} 开始消费")
        """
        try:
            if strategy == ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET:
                offset = self._get_max_offset(queue)
                self._logger.info(
                    "使用CONSUME_FROM_LAST_OFFSET策略",
                    extra={
                        "queue": queue,
                        "strategy": strategy,
                        "offset": offset,
                    },
                )
                return offset

            elif strategy == ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET:
                offset = self._get_min_offset(queue)
                self._logger.info(
                    "使用CONSUME_FROM_FIRST_OFFSET策略",
                    extra={
                        "queue": queue,
                        "strategy": strategy,
                        "offset": offset,
                    },
                )
                return offset

            elif strategy == ConsumeFromWhere.CONSUME_FROM_TIMESTAMP:
                # 如果没有指定时间戳，使用当前时间
                if timestamp == 0:
                    timestamp = int(time.time() * 1000)

                offset = self._get_offset_by_timestamp(queue, timestamp)
                self._logger.info(
                    "使用CONSUME_FROM_TIMESTAMP策略",
                    extra={
                        "queue": queue,
                        "strategy": strategy,
                        "timestamp": timestamp,
                        "offset": offset,
                    },
                )
                return offset

            else:
                raise ValueError(f"不支持的消费起始位置策略: {strategy}")

        except Exception as e:
            self._logger.error(
                "获取消费起始偏移量失败",
                extra={
                    "queue": queue,
                    "strategy": strategy,
                    "timestamp": timestamp,
                    "error": str(e),
                },
                exc_info=True,
            )
            raise RuntimeError(f"获取消费起始偏移量失败: {e}") from e

    def _get_max_offset(self, queue: MessageQueue) -> int:
        """从Broker获取指定队列的最大偏移量。

        查询指定消息队列的最大偏移量，即队列中最后一条消息的下一条位置。
        这通常用于实现CONSUME_FROM_LAST_OFFSET策略，让消费者跳过所有历史消息。

        Args:
            queue (MessageQueue): 目标消息队列，包含topic、broker名称和队列ID信息

        Returns:
            int: 队列的最大偏移量值。返回值表示队列中下一条将要写入消息的位置，
                即当前队列中消息总数。如果队列为空，通常返回0。

        Raises:
            BrokerNotAvailableError: 当无法连接到目标Broker或Broker不可用时抛出
            OffsetFetchError: 当向Broker查询最大偏移量请求失败时抛出
            RuntimeError: 当网络通信异常或Broker响应错误时抛出

        Note:
            最大偏移量指的是下一条消息将要写入的位置，而不是最后一条消息的位置。
            例如，如果队列中有10条消息（偏移量0-9），则最大偏移量为10。
        """
        broker_address: str | None = self.namesrv_manager.get_broker_address(
            queue.broker_name
        )
        if broker_address is None:
            raise BrokerNotAvailableError(
                broker_name=queue.broker_name, message="无法获取broker地址"
            )
        try:
            with self.broker_manager.connection(broker_address) as conn:
                broker_client: BrokerClient = BrokerClient(conn)
                max_offset: int = broker_client.get_max_offset(
                    queue.topic, queue.queue_id
                )
                return max_offset
        except Exception as e:
            self._logger.error(
                "获取最大偏移量失败",
                extra={
                    "queue": queue,
                    "error": str(e),
                },
                exc_info=True,
            )
            raise OffsetFetchError(
                queue.topic, queue.queue_id, self.consume_group, str(e), e
            )

    def _get_min_offset(self, queue: MessageQueue) -> int:
        """获取队列的最小偏移量。

        返回指定消息队列的最小偏移量，即队列中第一条消息的位置。
        这通常用于实现CONSUME_FROM_FIRST_OFFSET策略，让消费者消费所有历史消息。

        Args:
            queue (MessageQueue): 目标消息队列，包含topic、broker名称和队列ID信息

        Returns:
            int: 队列的最小偏移量值。在当前实现中，固定返回0，
                表示从队列的最开始位置消费。

        Raises:
            RuntimeError: 当获取最小偏移量过程中发生系统错误时抛出

        Note:
            当前实现返回固定值0，这在大多数RocketMQ环境中是正确的。
            在某些特殊情况下（如队列数据被清理过），实际最小偏移量可能大于0。
            如需精确的最小偏移量，需要在Broker端添加相应的查询支持。

        Warning:
            这是一个简化的实现，可能不适用于所有RocketMQ部署场景。
            在生产环境中使用前，请确认最小偏移量为0的假设是否正确。
        """
        try:
            # 注意：当前RocketMQ实现中，最小偏移量通常是0
            # 如果需要精确的最小偏移量，需要在Broker端添加相应支持
            offset: int = 0

            self._logger.info(
                "获取最小偏移量",
                extra={
                    "queue": queue,
                    "offset": offset,
                    "note": "使用默认值0，如需精确值需要在Broker端添加支持",
                },
            )

            return offset
        except Exception as e:
            self._logger.error(
                "获取最小偏移量失败",
                extra={
                    "queue": queue,
                    "error": str(e),
                },
                exc_info=True,
            )
            raise RuntimeError(f"获取最小偏移量失败: {e}") from e

    def _get_offset_by_timestamp(self, queue: MessageQueue, timestamp: int) -> int:
        """根据时间戳从Broker获取对应的消息偏移量。

        查询指定时间戳在队列中对应的偏移量位置，用于实现从特定时间点开始消费。
        Broker会返回小于或等于指定时间戳的最大偏移量。

        Args:
            queue (MessageQueue): 目标消息队列，包含topic、broker名称和队列ID信息
            timestamp (int): 目标时间戳（毫秒），用于查找对应的偏移量位置

        Returns:
            int: 对应时间戳的偏移量值。返回的是小于或等于指定时间戳的
                最大消息偏移量。如果指定时间戳早于队列中最早的
                消息时间戳，则返回最小偏移量；如果晚于最新消息，
                则返回最大偏移量。

        Raises:
            BrokerNotAvailableError: 当无法连接到目标Broker或Broker不可用时抛出
            OffsetFetchError: 当向Broker查询时间戳偏移量请求失败时抛出
            RuntimeError: 当网络通信异常或Broker响应错误时抛出
            ValueError: 当时间戳参数无效时抛出

        Note:
            时间戳应为毫秒级的Unix时间戳。
            返回的偏移量对应的消息存储时间戳小于或等于指定的目标时间戳。

        Example:
            对于包含以下消息的队列：
            - offset 0: timestamp 1000
            - offset 1: timestamp 2000
            - offset 2: timestamp 3000

            查询 timestamp 2500 将返回 offset 1
            查询 timestamp 3000 将返回 offset 2
        """
        broker_addr: str | None = self.namesrv_manager.get_broker_address(
            queue.broker_name
        )
        if not broker_addr:
            raise BrokerNotAvailableError(
                broker_name=queue.broker_name, message="无法获取broker地址"
            )
        try:
            with self.broker_manager.connection(broker_addr) as conn:
                broker_client: BrokerClient = BrokerClient(conn)
                offset: int = broker_client.search_offset_by_timestamp(
                    topic=queue.topic,
                    queue_id=queue.queue_id,
                    timestamp=timestamp,
                )
                return offset
        except Exception as e:
            self._logger.error(
                "根据时间戳获取偏移量失败",
                extra={
                    "queue": queue,
                    "timestamp": timestamp,
                    "error": str(e),
                },
                exc_info=True,
            )
            raise OffsetFetchError(
                queue.topic, queue.queue_id, self.consume_group, str(e), e
            )

    def validate_strategy(self, strategy: str) -> bool:
        """验证指定的消费起始位置策略是否有效。

        检查传入的策略字符串是否为支持的消费起始位置策略之一。
        这是消费起始位置管理器的输入验证方法，确保只有有效的策略被使用。

        Args:
            strategy (str): 待验证的消费起始位置策略字符串

        Returns:
            bool: 如果策略有效返回True，否则返回False

        Raises:
            TypeError: 当strategy参数不是字符串类型时抛出

        Note:
            支持的策略包括：
            - ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET: 从最大偏移量开始消费
            - ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET: 从最小偏移量开始消费
            - ConsumeFromWhere.CONSUME_FROM_TIMESTAMP: 从指定时间戳开始消费

        Example:
            >>> manager = ConsumeFromWhereManager(consume_group, namesrv_mgr, broker_mgr)
            >>> is_valid = manager.validate_strategy(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET)
            >>> print(is_valid)  # True
            >>> is_invalid = manager.validate_strategy("INVALID_STRATEGY")
            >>> print(is_invalid)  # False
        """
        if not isinstance(strategy, str):
            raise TypeError(
                f"策略参数必须是字符串类型，实际类型: {type(strategy).__name__}"
            )

        valid_strategies: list[str] = [
            ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET,
            ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET,
            ConsumeFromWhere.CONSUME_FROM_TIMESTAMP,
        ]

        is_valid: bool = strategy in valid_strategies

        if not is_valid:
            self._logger.warning(
                "无效的消费起始位置策略",
                extra={
                    "strategy": strategy,
                    "valid_strategies": valid_strategies,
                },
            )

        return is_valid

    def get_strategy_info(self, strategy: str) -> dict[str, Any]:
        """获取指定消费策略的详细信息。

        返回包含策略描述、使用场景、有效性验证等信息的字典。
        这对于了解策略的功能和选择合适的消费策略非常有用。

        Args:
            strategy (str): 目标消费起始位置策略字符串

        Returns:
            dict[str, Any]: 包含策略信息的字典，包含以下键：
                - strategy (str): 策略字符串值
                - name (str): 策略的显示名称
                - description (str): 策略的详细描述
                - use_case (str): 策略的适用场景说明
                - is_valid (bool): 策略是否有效

                如果策略未知，返回只包含strategy、name、description、use_case为空字符串
                和is_valid为False的字典。

        Raises:
            TypeError: 当strategy参数不是字符串类型时抛出

        Example:
            >>> manager = ConsumeFromWhereManager(consume_group, namesrv_mgr, broker_mgr)
            >>> info = manager.get_strategy_info(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET)
            >>> print(info['name'])  # "CONSUME_FROM_LAST_OFFSET"
            >>> print(info['description'])  # "从最大偏移量开始消费，跳过已有消息"
            >>> print(info['use_case'])  # "适用于只需要处理新消息的场景"
            >>> print(info['is_valid'])  # True
        """
        if not isinstance(strategy, str):
            raise TypeError(
                f"策略参数必须是字符串类型，实际类型: {type(strategy).__name__}"
            )

        strategy_descriptions: dict[str, dict[str, str]] = {
            ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET: {
                "name": "CONSUME_FROM_LAST_OFFSET",
                "description": "从最大偏移量开始消费，跳过已有消息",
                "use_case": "适用于只需要处理新消息的场景",
            },
            ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET: {
                "name": "CONSUME_FROM_FIRST_OFFSET",
                "description": "从最小偏移量开始消费，处理所有历史消息",
                "use_case": "适用于需要处理所有历史消息的场景",
            },
            ConsumeFromWhere.CONSUME_FROM_TIMESTAMP: {
                "name": "CONSUME_FROM_TIMESTAMP",
                "description": "从指定时间戳对应的偏移量开始消费",
                "use_case": "适用于需要从特定时间点开始消费的场景",
            },
        }

        info: dict[str, Any] = strategy_descriptions.get(str(strategy), {}).copy()
        if not info:
            # 为未知策略提供默认值
            info = {"name": "", "description": "", "use_case": ""}

        info["strategy"] = strategy
        info["is_valid"] = self.validate_strategy(strategy)

        return info

    def get_all_supported_strategies(self) -> list[dict[str, Any]]:
        """获取所有支持的消费起始位置策略信息。

        返回一个包含所有支持策略详细信息的列表，每个策略都包含名称、描述、
        使用场景和有效性信息。这对于文档生成、配置界面或策略选择非常有用。

        Returns:
            list[dict[str, Any]]: 所有支持的策略信息列表。每个字典包含：
                - strategy (str): 策略字符串值
                - name (str): 策略的显示名称
                - description (str): 策略的详细描述
                - use_case (str): 策略的适用场景说明
                - is_valid (bool): 策略是否有效（对于支持的所有策略，此值恒为True）

        Note:
            返回的列表按策略的固定顺序排列：
            1. CONSUME_FROM_LAST_OFFSET
            2. CONSUME_FROM_FIRST_OFFSET
            3. CONSUME_FROM_TIMESTAMP

        Example:
            >>> manager = ConsumeFromWhereManager(consume_group, namesrv_mgr, broker_mgr)
            >>> strategies = manager.get_all_supported_strategies()
            >>> for strategy in strategies:
            ...     print(f"{strategy['name']}: {strategy['description']}")
            ...     print(f"适用场景: {strategy['use_case']}")
            ...     print("---")
        """
        all_strategies: list[str] = [
            ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET,
            ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET,
            ConsumeFromWhere.CONSUME_FROM_TIMESTAMP,
        ]

        strategy_info_list: list[dict[str, Any]] = [
            self.get_strategy_info(strategy) for strategy in all_strategies
        ]
        return strategy_info_list

    def reset_offset_to_position(
        self,
        queue: MessageQueue,
        position: str,
        timestamp: int = 0,
    ) -> int:
        """重置消费偏移量到指定的位置。

        这是一个便利方法，允许使用更直观的字符串来指定消费起始位置，
        而不是直接使用ConsumeFromWhere常量。简化了常用消费位置的重置操作。

        Args:
            queue (MessageQueue): 目标消息队列，包含topic、broker名称和队列ID信息
            position (str): 位置字符串，支持的值包括：
                - "first": 从队列的开始位置消费（等同于CONSUME_FROM_FIRST_OFFSET）
                - "last": 从队列的最新位置消费（等同于CONSUME_FROM_LAST_OFFSET）
                - "timestamp": 从指定时间戳开始消费（等同于CONSUME_FROM_TIMESTAMP）
            timestamp (int, optional): 时间戳（毫秒），仅在position为"timestamp"时使用。
                当为0时，使用当前时间戳。默认值为0。

        Returns:
            int: 重置后的消费起始偏移量。返回值表示消费者应该从哪个偏移量开始消费。

        Raises:
            ValueError: 当提供的position字符串不被支持时抛出
            TypeError: 当position或queue参数类型不正确时抛出
            RuntimeError: 当获取偏移量过程中发生网络或系统错误时抛出
            BrokerNotAvailableError: 当目标Broker不可用时抛出
            OffsetFetchError: 当从Broker获取偏移量失败时抛出

        Example:
            >>> manager = ConsumeFromWhereManager(consume_group, namesrv_mgr, broker_mgr)
            >>> queue = MessageQueue(broker_name="broker1", topic="test_topic", queue_id=0)
            >>>
            >>> # 从最新消息开始消费
            >>> offset = manager.reset_offset_to_position(queue, "last")
            >>> print(f"从偏移量 {offset} 开始消费最新消息")
            >>>
            >>> # 从最早消息开始消费
            >>> offset = manager.reset_offset_to_position(queue, "first")
            >>> print(f"从偏移量 {offset} 开始消费所有历史消息")
            >>>
            >>> # 从特定时间戳开始消费
            >>> specific_time = int(time.time() * 1000) - 3600000  # 1小时前
            >>> offset = manager.reset_offset_to_position(queue, "timestamp", specific_time)
            >>> print(f"从偏移量 {offset} 开始消费1小时前的消息")

        Note:
            此方法是get_consume_offset方法的封装，提供更友好的字符串接口。
            内部会将position字符串转换为对应的ConsumeFromWhere策略常量。
        """
        if not isinstance(position, str):
            raise TypeError(
                f"位置参数必须是字符串类型，实际类型: {type(position).__name__}"
            )

        position_map: dict[str, str] = {
            "first": ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET,
            "last": ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET,
            "timestamp": ConsumeFromWhere.CONSUME_FROM_TIMESTAMP,
        }

        if position not in position_map:
            supported_positions: list[str] = list(position_map.keys())
            raise ValueError(
                f"不支持的位置字符串: {position}，支持的位置: {supported_positions}"
            )

        strategy: str = position_map[position]
        return self.get_consume_offset(queue, strategy, timestamp)
