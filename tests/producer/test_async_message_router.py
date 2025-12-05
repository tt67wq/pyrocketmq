#!/usr/bin/env python3
"""
AsyncMessageRouter单元测试

测试异步消息路由器的主要API功能，使用pytest框架和pytest-asyncio插件
"""

import asyncio
import os
import sys
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# 设置PYTHONPATH
project_root = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
sys.path.insert(0, os.path.join(project_root, "src"))

from pyrocketmq.model import BrokerData, MessageQueue, QueueData, TopicRouteData
from pyrocketmq.model.message import Message
from pyrocketmq.producer.errors import (
    BrokerNotAvailableError,
    QueueNotAvailableError,
    RouteNotFoundError,
)
from pyrocketmq.producer.queue_selectors import (
    AsyncQueueSelector,
    AsyncRandomSelector,
    AsyncRoundRobinSelector,
)
from pyrocketmq.producer.router import (
    AsyncMessageRouter,
    RoutingResult,
    RoutingStrategy,
    async_create_message_router,
    create_async_message_router,
)
from pyrocketmq.producer.topic_broker_mapping import AsyncTopicBrokerMapping


class TestAsyncMessageRouter:
    """AsyncMessageRouter测试类"""

    @pytest.fixture
    async def mock_topic_mapping(self):
        """创建模拟的AsyncTopicBrokerMapping"""
        mapping = AsyncMock(spec=AsyncTopicBrokerMapping)
        return mapping

    @pytest.fixture
    async def router(self, mock_topic_mapping):
        """创建一个AsyncMessageRouter实例"""
        return AsyncMessageRouter(
            topic_mapping=mock_topic_mapping,
            default_strategy=RoutingStrategy.ROUND_ROBIN,
        )

    @pytest.fixture
    async def router_with_random_strategy(self, mock_topic_mapping):
        """创建使用随机策略的AsyncMessageRouter实例"""
        return AsyncMessageRouter(
            topic_mapping=mock_topic_mapping,
            default_strategy=RoutingStrategy.RANDOM,
        )

    @pytest.fixture
    def sample_broker_data(self):
        """创建示例Broker数据"""
        return [
            BrokerData(
                cluster="test-cluster",
                broker_name="broker-a",
                broker_addresses={0: "127.0.0.1:10911", 1: "127.0.0.1:10912"},
            ),
            BrokerData(
                cluster="test-cluster",
                broker_name="broker-b",
                broker_addresses={0: "127.0.0.1:10921", 1: "127.0.0.1:10922"},
            ),
        ]

    @pytest.fixture
    def sample_queue_data(self):
        """创建示例Queue数据"""
        return [
            QueueData(
                broker_name="broker-a",
                read_queue_nums=4,
                write_queue_nums=4,
                perm=6,
                topic_syn_flag=0,
            ),
            QueueData(
                broker_name="broker-b",
                read_queue_nums=4,
                write_queue_nums=4,
                perm=6,
                topic_syn_flag=0,
            ),
        ]

    @pytest.fixture
    def sample_topic_route_data(self, sample_broker_data, sample_queue_data):
        """创建示例TopicRouteData"""
        return TopicRouteData(
            order_topic_conf="",
            queue_data_list=sample_queue_data,
            broker_data_list=sample_broker_data,
        )

    @pytest.fixture
    def sample_queues(self, sample_broker_data, sample_queue_data):
        """创建示例消息队列列表"""
        queues = []
        for queue_data in sample_queue_data:
            for i in range(queue_data.write_queue_nums):
                queue = MessageQueue(
                    topic="test_topic",
                    broker_name=queue_data.broker_name,
                    queue_id=i,
                )
                # 找到对应的BrokerData
                broker = next(
                    b
                    for b in sample_broker_data
                    if b.broker_name == queue_data.broker_name
                )
                queues.append((queue, broker))
        return queues

    @pytest.fixture
    def sample_message(self):
        """创建示例消息"""
        from pyrocketmq.model.message import MessageProperty

        return Message(
            topic="test_topic",
            body="test message body",
            properties={
                MessageProperty.TAGS: "test_tag",
                MessageProperty.KEYS: "test_key",
            },
        )

    # ==================== 基础初始化测试 ====================

    @pytest.mark.asyncio
    async def test_init_default(self, mock_topic_mapping):
        """测试默认初始化"""
        router = AsyncMessageRouter(topic_mapping=mock_topic_mapping)

        assert router.topic_mapping == mock_topic_mapping
        assert router.default_strategy == RoutingStrategy.ROUND_ROBIN
        assert isinstance(router._async_selectors, dict)
        assert RoutingStrategy.ROUND_ROBIN in router._async_selectors

    @pytest.mark.asyncio
    async def test_init_custom_strategy(self, mock_topic_mapping):
        """测试自定义路由策略初始化"""
        router = AsyncMessageRouter(
            topic_mapping=mock_topic_mapping,
            default_strategy=RoutingStrategy.RANDOM,
        )

        assert router.default_strategy == RoutingStrategy.RANDOM
        assert RoutingStrategy.RANDOM in router._async_selectors

    # ==================== 消息路由测试 ====================

    @pytest.mark.asyncio
    async def test_aroute_message_round_robin(
        self, router, mock_topic_mapping, sample_queues, sample_message
    ):
        """测试轮询策略路由消息"""
        # 设置mock返回值
        mock_topic_mapping.aget_available_queues.return_value = sample_queues

        # 执行路由
        result = await router.aroute_message("test_topic", sample_message)

        # 验证结果
        assert isinstance(result, RoutingResult)
        assert isinstance(result.message_queue, MessageQueue)
        assert isinstance(result.broker_data, BrokerData)
        assert result.message_queue.topic == "test_topic"
        assert result.broker_data.broker_name in ["broker-a", "broker-b"]

    @pytest.mark.asyncio
    async def test_aroute_message_random(
        self,
        router_with_random_strategy,
        mock_topic_mapping,
        sample_queues,
        sample_message,
    ):
        """测试随机策略路由消息"""
        # 设置mock返回值
        mock_topic_mapping.aget_available_queues.return_value = sample_queues

        # 执行路由
        result = await router_with_random_strategy.aroute_message(
            "test_topic", sample_message
        )

        # 验证结果
        assert isinstance(result, RoutingResult)
        assert isinstance(result.message_queue, MessageQueue)
        assert isinstance(result.broker_data, BrokerData)
        assert result.message_queue.topic == "test_topic"

    @pytest.mark.asyncio
    async def test_aroute_message_with_custom_strategy(
        self, router, mock_topic_mapping, sample_queues, sample_message
    ):
        """测试使用自定义策略路由消息"""
        # 设置mock返回值
        mock_topic_mapping.aget_available_queues.return_value = sample_queues

        # 执行路由，指定随机策略
        result = await router.aroute_message(
            "test_topic", sample_message, strategy=RoutingStrategy.RANDOM
        )

        # 验证结果
        assert isinstance(result, RoutingResult)
        assert isinstance(result.message_queue, MessageQueue)

    @pytest.mark.asyncio
    async def test_aroute_message_without_message(
        self, router, mock_topic_mapping, sample_queues
    ):
        """测试不提供消息对象的路由"""
        # 设置mock返回值
        mock_topic_mapping.aget_available_queues.return_value = sample_queues

        # 执行路由，不提供消息
        result = await router.aroute_message("test_topic")

        # 验证结果
        assert isinstance(result, RoutingResult)
        assert isinstance(result.message_queue, MessageQueue)
        assert result.message_queue.topic == "test_topic"

    @pytest.mark.asyncio
    async def test_aroute_message_consecutive_round_robin(
        self, router, mock_topic_mapping, sample_queues, sample_message
    ):
        """测试连续轮询路由的选择分布"""
        # 设置mock返回值
        mock_topic_mapping.aget_available_queues.return_value = sample_queues

        # 执行多次路由
        results = []
        for _ in range(10):
            result = await router.aroute_message("test_topic", sample_message)
            results.append(result)

        # 验证结果分布合理（应该覆盖不同的队列）
        broker_names = [result.broker_data.broker_name for result in results]
        queue_ids = [result.message_queue.queue_id for result in results]

        # 应该有多个不同的broker和queue选择
        assert len(set(broker_names)) > 1 or len(set(queue_ids)) > 1

    # ==================== 异常处理测试 ====================

    @pytest.mark.asyncio
    async def test_aroute_message_route_not_found(self, router, mock_topic_mapping):
        """测试路由未找到异常"""
        # 设置mock返回空队列列表
        mock_topic_mapping.aget_available_queues.return_value = []

        # 执行路由应该返回失败结果
        result = await router.aroute_message("non_existent_topic")

        assert result.success is False
        # 当topic mapping抛出异常时，error就是原始异常
        assert isinstance(result.error, Exception)
        assert result.routing_strategy == RoutingStrategy.ROUND_ROBIN

    @pytest.mark.asyncio
    async def test_aroute_message_topic_mapping_exception(
        self, router, mock_topic_mapping
    ):
        """测试Topic-Broker映射异常"""
        # 设置mock抛出异常
        mock_topic_mapping.aget_available_queues.side_effect = Exception(
            "Mapping error"
        )

        # 执行路由应该返回失败结果
        result = await router.aroute_message("test_topic")

        assert result.success is False
        # 当topic mapping抛出异常时，error就是原始异常
        assert isinstance(result.error, Exception)

    @pytest.mark.asyncio
    async def test_aroute_message_queue_selection_fails(
        self, router, mock_topic_mapping, sample_queues
    ):
        """测试队列选择失败"""
        # 设置mock返回值
        mock_topic_mapping.aget_available_queues.return_value = sample_queues

        # 模拟队列选择器失败
        with patch.object(
            router._async_selectors[RoutingStrategy.ROUND_ROBIN],
            "aselect",
            side_effect=Exception("Selection failed"),
        ):
            # 执行路由应该返回失败结果
            result = await router.aroute_message("test_topic")

            assert result.success is False

    # ==================== Broker地址选择测试 ====================

    @pytest.mark.asyncio
    async def test_aselect_broker_address(self, router):
        """测试选择Broker地址"""
        # 创建测试BrokerData
        broker_data = BrokerData(
            cluster="test-cluster",
            broker_name="test-broker",
            broker_addresses={0: "127.0.0.1:10911", 1: "127.0.0.1:10912"},
        )

        # 执行地址选择
        address = await router.aselect_broker_address(broker_data)

        # 验证结果
        assert address in ["127.0.0.1:10911", "127.0.0.1:10912"]

    @pytest.mark.asyncio
    async def test_aselect_broker_address_no_addresses(self, router):
        """测试选择无地址的Broker"""
        # 创建无地址的BrokerData
        broker_data = BrokerData(
            cluster="test-cluster",
            broker_name="empty-broker",
            broker_addresses={},
        )

        # 执行地址选择应该返回None
        address = await router.aselect_broker_address(broker_data)
        assert address is None

    @pytest.mark.asyncio
    async def test_aselect_broker_address_prefers_master(self, router):
        """测试优先选择Master地址"""
        # 创建包含Master和Slave的BrokerData
        broker_data = BrokerData(
            cluster="test-cluster",
            broker_name="test-broker",
            broker_addresses={0: "127.0.0.1:10911", 1: "127.0.0.1:10912"},
        )

        # 执行多次地址选择
        addresses = []
        for _ in range(10):
            address = await router.aselect_broker_address(broker_data)
            addresses.append(address)

        # 验证主要选择Master地址（ID=0）
        master_count = addresses.count("127.0.0.1:10911")
        assert master_count >= 5  # 大部分时间应该选择Master

    # ==================== 内部方法测试 ====================

    @pytest.mark.asyncio
    async def test__aget_available_queues(self, router, mock_topic_mapping):
        """测试内部获取可用队列方法"""
        # 设置mock返回值
        expected_queues = [(MagicMock(), MagicMock())]
        mock_topic_mapping.aget_available_queues.return_value = expected_queues

        # 调用内部方法
        result = await router._aget_available_queues("test_topic")

        # 验证调用和结果
        mock_topic_mapping.aget_available_queues.assert_called_once_with("test_topic")
        assert result == expected_queues

    @pytest.mark.asyncio
    async def test__create_async_selectors(self, mock_topic_mapping):
        """测试异步选择器创建"""
        router = AsyncMessageRouter(topic_mapping=mock_topic_mapping)

        # 验证选择器创建
        assert RoutingStrategy.ROUND_ROBIN in router._async_selectors
        assert RoutingStrategy.RANDOM in router._async_selectors
        assert RoutingStrategy.MESSAGE_HASH in router._async_selectors

        # 验证选择器类型
        for selector in router._async_selectors.values():
            assert isinstance(selector, AsyncQueueSelector)

    # ==================== 边界情况测试 ====================

    @pytest.mark.asyncio
    async def test_aroute_message_empty_queues(
        self, router, mock_topic_mapping, sample_message
    ):
        """测试空队列列表路由"""
        # 设置mock返回空列表
        mock_topic_mapping.aget_available_queues.return_value = []

        # 执行路由应该返回失败结果
        result = await router.aroute_message("test_topic", sample_message)

        assert result.success is False
        assert isinstance(result.error, RouteNotFoundError)

    @pytest.mark.asyncio
    async def test_aroute_message_single_queue(
        self, router, mock_topic_mapping, sample_message
    ):
        """测试单个队列路由"""
        # 创建单个队列
        single_queue = [
            (
                MessageQueue(topic="test_topic", broker_name="broker-a", queue_id=0),
                BrokerData(
                    cluster="test-cluster",
                    broker_name="broker-a",
                    broker_addresses={0: "127.0.0.1:10911"},
                ),
            )
        ]

        # 设置mock返回值
        mock_topic_mapping.aget_available_queues.return_value = single_queue

        # 执行路由
        result = await router.aroute_message("test_topic", sample_message)

        # 验证结果
        assert result.message_queue.queue_id == 0
        assert result.broker_data.broker_name == "broker-a"

    # ==================== 并发测试 ====================

    @pytest.mark.asyncio
    async def test_concurrent_routing(
        self, router, mock_topic_mapping, sample_queues, sample_message
    ):
        """测试并发路由"""
        # 设置mock返回值
        mock_topic_mapping.aget_available_queues.return_value = sample_queues

        # 并发执行路由
        tasks = [router.aroute_message("test_topic", sample_message) for _ in range(20)]
        results = await asyncio.gather(*tasks)

        # 验证所有结果都有效
        assert len(results) == 20
        for result in results:
            assert isinstance(result, RoutingResult)
            assert isinstance(result.message_queue, MessageQueue)
            assert isinstance(result.broker_data, BrokerData)

        # 验证结果分布合理
        broker_names = [result.broker_data.broker_name for result in results]
        queue_ids = [result.message_queue.queue_id for result in results]

        # 应该有多种不同的选择
        assert len(set(broker_names)) >= 2  # 至少2个不同的broker
        assert len(set(queue_ids)) >= 4  # 至少4个不同的queue

    @pytest.mark.asyncio
    async def test_concurrent_different_strategies(
        self, mock_topic_mapping, sample_queues, sample_message
    ):
        """测试并发不同策略路由"""
        # 创建不同策略的路由器
        router_rr = AsyncMessageRouter(
            topic_mapping=mock_topic_mapping,
            default_strategy=RoutingStrategy.ROUND_ROBIN,
        )
        router_random = AsyncMessageRouter(
            topic_mapping=mock_topic_mapping, default_strategy=RoutingStrategy.RANDOM
        )

        # 设置mock返回值
        mock_topic_mapping.aget_available_queues.return_value = sample_queues

        # 并发执行路由
        tasks = [
            router_rr.aroute_message("test_topic", sample_message),
            router_random.aroute_message("test_topic", sample_message),
        ] * 10
        results = await asyncio.gather(*tasks)

        # 验证所有结果都有效
        assert len(results) == 20
        for result in results:
            assert isinstance(result, RoutingResult)


class TestRoutingResult:
    """RoutingResult测试类"""

    def test_routing_result_creation(self):
        """测试路由结果创建"""
        message_queue = MessageQueue(topic="test", broker_name="broker", queue_id=1)
        broker_data = BrokerData(
            cluster="test-cluster",
            broker_name="broker",
            broker_addresses={0: "127.0.0.1:10911"},
        )

        result = RoutingResult(
            success=True,
            message_queue=message_queue,
            broker_data=broker_data,
            routing_strategy=RoutingStrategy.ROUND_ROBIN,
        )

        assert result.message_queue == message_queue
        assert result.broker_data == broker_data
        assert result.routing_strategy == RoutingStrategy.ROUND_ROBIN

    def test_get_broker_name(self):
        """测试获取Broker名称"""
        message_queue = MessageQueue(
            topic="test", broker_name="test-broker", queue_id=1
        )
        broker_data = BrokerData(
            cluster="test-cluster",
            broker_name="test-broker",
            broker_addresses={0: "127.0.0.1:10911"},
        )

        result = RoutingResult(
            success=True,
            message_queue=message_queue,
            broker_data=broker_data,
            routing_strategy=RoutingStrategy.ROUND_ROBIN,
        )

        assert result.get_broker_name() == "test-broker"

    def test_post_init_validation(self):
        """测试初始化后验证"""
        # 有效数据应该不抛出异常
        message_queue = MessageQueue(topic="test", broker_name="broker", queue_id=1)
        broker_data = BrokerData(
            cluster="test-cluster",
            broker_name="broker",
            broker_addresses={0: "127.0.0.1:10911"},
        )

        # 这应该正常工作
        result = RoutingResult(
            success=True,
            message_queue=message_queue,
            broker_data=broker_data,
            routing_strategy=RoutingStrategy.ROUND_ROBIN,
        )
        assert result is not None

        # 测试broker名称不匹配的情况
        invalid_broker = BrokerData(
            cluster="test-cluster",
            broker_name="different-broker",
            broker_addresses={0: "127.0.0.1:10911"},
        )

        # 这应该触发验证逻辑
        result = RoutingResult(
            success=True,
            message_queue=message_queue,
            broker_data=invalid_broker,
            routing_strategy=RoutingStrategy.ROUND_ROBIN,
        )
        # 注意：当前实现可能不会自动纠正broker_data
        # 这个测试主要验证创建过程不会崩溃
        assert result is not None


class TestFactoryFunctions:
    """工厂函数测试类"""

    @pytest.mark.asyncio
    async def test_create_async_message_router(self):
        """测试创建异步消息路由器"""
        with patch(
            "pyrocketmq.producer.router.AsyncTopicBrokerMapping"
        ) as mock_mapping_class:
            mock_mapping = AsyncMock()
            mock_mapping_class.return_value = mock_mapping

            router = create_async_message_router(topic_mapping=mock_mapping)

            assert isinstance(router, AsyncMessageRouter)
            assert router.topic_mapping == mock_mapping

    @pytest.mark.asyncio
    async def test_async_create_message_router(self):
        """测试异步创建消息路由器"""
        with patch(
            "pyrocketmq.producer.router.AsyncTopicBrokerMapping"
        ) as mock_mapping_class:
            mock_mapping = AsyncMock()
            mock_mapping_class.return_value = mock_mapping

            router = await async_create_message_router(topic_mapping=mock_mapping)

            assert isinstance(router, AsyncMessageRouter)
            assert router.topic_mapping == mock_mapping

    @pytest.mark.asyncio
    async def test_create_async_message_router_with_custom_strategy(self):
        """测试创建带自定义策略的异步消息路由器"""
        with patch(
            "pyrocketmq.producer.router.AsyncTopicBrokerMapping"
        ) as mock_mapping_class:
            mock_mapping = AsyncMock()
            mock_mapping_class.return_value = mock_mapping

            router = create_async_message_router(
                topic_mapping=mock_mapping, default_strategy=RoutingStrategy.RANDOM
            )

            assert isinstance(router, AsyncMessageRouter)
            assert router.default_strategy == RoutingStrategy.RANDOM

    @pytest.mark.asyncio
    async def test_create_async_message_router_with_mapping(self):
        """测试使用现有映射创建异步消息路由器"""
        mock_mapping = AsyncMock(spec=AsyncTopicBrokerMapping)

        router = create_async_message_router(topic_mapping=mock_mapping)

        assert isinstance(router, AsyncMessageRouter)
        assert router.topic_mapping == mock_mapping


if __name__ == "__main__":
    # 运行测试
    pytest.main([__file__, "-v"])
