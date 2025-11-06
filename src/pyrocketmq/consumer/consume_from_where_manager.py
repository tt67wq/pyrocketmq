"""
消费起始位置管理模块

实现ConsumeFromWhere策略管理，负责根据不同的消费策略确定消费者开始消费的偏移量位置。
支持从最后偏移量、第一个偏移量或指定时间戳开始消费。
"""

import logging
import time
from asyncio import SelectorEventLoop
from typing import Any

from pyrocketmq.broker import BrokerManager
from pyrocketmq.logging import get_logger
from pyrocketmq.model import MessageQueue
from pyrocketmq.model.consumer import ConsumeFromWhere

logger = get_logger(__name__)


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
        self, consume_group: str, namesrv: str, broker_manager: BrokerManager
    ) -> None:
        """初始化消费起始位置管理器

        Args:
            namesrv (str): NameServer地址
            broker_manager (BrokerManager): Broker管理器，用于创建Broker连接
        """
        self.consume_group: str = consume_group
        self.namesrv: str = namesrv
        self.broker_manager: BrokerManager = broker_manager
        self._logger: logging.Logger = get_logger(f"{__name__}.ConsumeFromWhereManager")

    def get_consume_offset(
        self,
        queue: MessageQueue,
        strategy: ConsumeFromWhere,
        timestamp: int = 0,
    ) -> int:
        """根据策略获取消费起始偏移量

        Args:
            queue (MessageQueue): 消息队列
            strategy (ConsumeFromWhere): 消费起始位置策略
            timestamp (int, optional): 时间戳（仅在strategy为TIMESTAMP时使用）
                                     默认为0，表示当前时间

        Returns:
            int: 消费起始偏移量

        Raises:
            ValueError: 当策略不支持或参数无效时抛出
            RuntimeError: 当获取偏移量失败时抛出
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
        """获取队列的最大偏移量

        Args:
            queue (MessageQueue): 消息队列

        Returns:
            int: 最大偏移量
        """
        try:
            with self.broker_manager.connection(queue.broker_name) as broker_client:
                offset = broker_client.get_max_offset(
                    topic=queue.topic,
                    queue_id=queue.queue_id,
                )
                return offset
        except Exception as e:
            self._logger.error(
                "获取最大偏移量失败",
                extra={
                    "queue": queue,
                    "error": str(e),
                },
                exc_info=True,
            )
            raise RuntimeError(f"获取最大偏移量失败: {e}") from e

    def _get_min_offset(self, queue: MessageQueue) -> int:
        """获取队列的最小偏移量

        Args:
            queue (MessageQueue): 消息队列

        Returns:
            int: 最小偏移量
        """
        try:
            # 注意：当前RocketMQ实现中，最小偏移量通常是0
            # 如果需要精确的最小偏移量，需要在Broker端添加相应支持
            offset = 0

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
        """根据时间戳获取对应的偏移量

        Args:
            queue (MessageQueue): 消息队列
            timestamp (int): 时间戳（毫秒）

        Returns:
            int: 对应的偏移量
        """
        try:
            with self.broker_manager.connection(queue.broker_name) as broker_client:
                offset = broker_client.search_offset_by_timestamp(
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
            raise RuntimeError(f"根据时间戳获取偏移量失败: {e}") from e

    def _get_broker_addr_by_name(self, broker_name: str) -> str | None:
        """根据broker名称查询broker地址

        优先从本地缓存查询，缓存未命中或过期时查询NameServer获取最新地址信息。
        使用缓存机制可以显著减少对NameServer的查询频率，提升性能。

        缓存策略：
        - 缓存结构：broker_name -> (address, timestamp)
        - 缓存TTL：5分钟，过期自动清理
        - 线程安全：使用self._lock保护缓存操作
        - 缓存更新：查询成功后自动更新缓存

        Args:
            broker_name: 要查询的broker名称

        Returns:
            str | None: 找到的broker地址，格式为"host:port"，未找到则返回None

        Raises:
            NameServerError: 当NameServer连接不可用或查询失败时抛出

        查询流程：
        1. 检查本地缓存，命中且未过期则直接返回
        2. 缓存未命中或过期，遍历NameServer连接查询
        3. 查询成功后更新本地缓存并返回地址
        4. 所有NameServer都查询失败则返回None
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

    def validate_strategy(self, strategy: ConsumeFromWhere) -> bool:
        """验证消费起始位置策略是否有效

        Args:
            strategy (ConsumeFromWhere): 消费起始位置策略

        Returns:
            bool: 策略是否有效
        """
        valid_strategies = [
            ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET,
            ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET,
            ConsumeFromWhere.CONSUME_FROM_TIMESTAMP,
        ]

        is_valid = strategy in valid_strategies

        if not is_valid:
            self._logger.warning(
                "无效的消费起始位置策略",
                extra={
                    "strategy": strategy,
                    "valid_strategies": valid_strategies,
                },
            )

        return is_valid

    def get_strategy_info(self, strategy: ConsumeFromWhere) -> dict[str, Any]:
        """获取策略信息

        Args:
            strategy (ConsumeFromWhere): 消费起始位置策略

        Returns:
            dict[str, Any]: 策略信息
        """
        strategy_descriptions = {
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

        info = strategy_descriptions.get(strategy, {})
        info["strategy"] = strategy
        info["is_valid"] = self.validate_strategy(strategy)

        return info

    def get_all_supported_strategies(self) -> list[dict[str, Any]]:
        """获取所有支持的策略信息

        Returns:
            list[dict[str, Any]]: 所有支持的策略信息列表
        """
        all_strategies = [
            ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET,
            ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET,
            ConsumeFromWhere.CONSUME_FROM_TIMESTAMP,
        ]

        return [self.get_strategy_info(strategy) for strategy in all_strategies]

    def reset_offset_to_position(
        self,
        queue: MessageQueue,
        position: str,
        timestamp: int = 0,
    ) -> int:
        """重置偏移量到指定位置

        这是一个便利方法，支持使用字符串位置来重置偏移量。

        Args:
            queue (MessageQueue): 消息队列
            position (str): 位置字符串，支持 "first", "last", "timestamp"
            timestamp (int, optional): 时间戳（仅在position为"timestamp"时使用）

        Returns:
            int: 重置后的偏移量

        Raises:
            ValueError: 当位置字符串不支持时抛出
        """
        position_map = {
            "first": ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET,
            "last": ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET,
            "timestamp": ConsumeFromWhere.CONSUME_FROM_TIMESTAMP,
        }

        if position not in position_map:
            raise ValueError(
                f"不支持的位置字符串: {position}，支持的位置: {list(position_map.keys())}"
            )

        strategy = position_map[position]
        return self.get_consume_offset(queue, strategy, timestamp)
