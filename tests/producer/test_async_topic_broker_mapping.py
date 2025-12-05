#!/usr/bin/env python3
"""
AsyncTopicBrokerMapping单元测试

测试异步主题-Broker映射管理器的主要API功能，使用pytest框架和pytest-asyncio插件
"""

import asyncio
import os
import sys
import time
from unittest.mock import patch

import pytest

# 设置PYTHONPATH
project_root = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
sys.path.insert(0, os.path.join(project_root, "src"))

from pyrocketmq.model import BrokerData, MessageQueue, QueueData, TopicRouteData
from pyrocketmq.producer.topic_broker_mapping import (
    AsyncRouteInfo,
    AsyncTopicBrokerMapping,
    async_create_topic_broker_mapping,
    create_async_topic_broker_mapping,
)


class TestAsyncTopicBrokerMapping:
    """AsyncTopicBrokerMapping测试类"""

    @pytest.fixture
    async def mapping(self):
        """创建一个AsyncTopicBrokerMapping实例"""
        return AsyncTopicBrokerMapping()

    @pytest.fixture
    async def mapping_with_timeout(self):
        """创建一个有自定义超时时间的AsyncTopicBrokerMapping实例"""
        return AsyncTopicBrokerMapping(route_timeout=30.0)

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

    # ==================== 基础初始化测试 ====================

    @pytest.mark.asyncio
    async def test_init_default(self, mapping):
        """测试默认初始化"""
        assert mapping._default_route_timeout == 60.0
        assert mapping._route_cache == {}
        assert isinstance(mapping._lock, asyncio.Lock)

    @pytest.mark.asyncio
    async def test_init_custom_timeout(self, mapping_with_timeout):
        """测试自定义超时时间初始化"""
        assert mapping_with_timeout._default_route_timeout == 30.0

    # ==================== 路由信息管理测试 ====================

    @pytest.mark.asyncio
    async def test_aget_route_info_empty(self, mapping):
        """测试获取不存在的路由信息"""
        route_info = await mapping.aget_route_info("non_existent_topic")
        assert route_info is None

    @pytest.mark.asyncio
    async def test_aupdate_and_aget_route_info(self, mapping, sample_topic_route_data):
        """测试更新和获取路由信息"""
        topic = "test_topic"

        # 更新路由信息
        await mapping.aupdate_route_info(topic, sample_topic_route_data)

        # 获取路由信息
        route_info = await mapping.aget_route_info(topic)
        assert route_info is not None
        assert route_info.broker_data_list == sample_topic_route_data.broker_data_list
        assert route_info.queue_data_list == sample_topic_route_data.queue_data_list

    @pytest.mark.asyncio
    async def test_aupdate_route_info_replace_existing(
        self, mapping, sample_topic_route_data
    ):
        """测试更新已存在的路由信息"""
        topic = "test_topic"

        # 首次更新
        await mapping.aupdate_route_info(topic, sample_topic_route_data)
        first_route_info = await mapping.aget_route_info(topic)

        # 等待一小段时间确保更新时间不同
        await asyncio.sleep(0.01)

        # 创建新的路由数据
        new_broker_data = [
            BrokerData(
                cluster="new-cluster",
                broker_name="new-broker",
                broker_addresses={0: "127.0.0.1:10931"},
            )
        ]
        new_topic_route_data = TopicRouteData(
            order_topic_conf="",
            queue_data_list=[],
            broker_data_list=new_broker_data,
        )

        # 再次更新
        await mapping.aupdate_route_info(topic, new_topic_route_data)

        # 获取更新后的路由信息
        updated_route_info = await mapping.aget_route_info(topic)
        assert updated_route_info is not None
        assert updated_route_info.broker_data_list == new_broker_data
        assert updated_route_info != first_route_info  # 确保是新的对象

    @pytest.mark.asyncio
    async def test_aremove_route_info(self, mapping, sample_topic_route_data):
        """测试移除路由信息"""
        topic = "test_topic"

        # 添加路由信息
        await mapping.aupdate_route_info(topic, sample_topic_route_data)
        assert await mapping.aget_route_info(topic) is not None

        # 移除路由信息
        result = await mapping.aremove_route_info(topic)
        assert result is True

        # 验证已移除
        route_info = await mapping.aget_route_info(topic)
        assert route_info is None

    @pytest.mark.asyncio
    async def test_aremove_non_existent_route_info(self, mapping):
        """测试移除不存在的路由信息"""
        result = await mapping.aremove_route_info("non_existent_topic")
        assert result is False

    # ==================== 队列管理测试 ====================

    @pytest.mark.asyncio
    async def test_aget_available_queues(self, mapping, sample_topic_route_data):
        """测试获取可用队列"""
        topic = "test_topic"

        # 更新路由信息
        await mapping.aupdate_route_info(topic, sample_topic_route_data)

        # 获取可用队列
        queues = await mapping.aget_available_queues(topic)
        assert len(queues) == 8  # 2 brokers * 4 queues each

        # 验证每个元素是(MessageQueue, BrokerData)元组
        for queue_tuple in queues:
            assert len(queue_tuple) == 2
            message_queue, broker_data = queue_tuple
            assert isinstance(message_queue, MessageQueue)
            assert isinstance(broker_data, BrokerData)
            assert message_queue.topic == topic

        # 验证队列分布
        broker_a_queues = [mq for mq, bd in queues if mq.broker_name == "broker-a"]
        broker_b_queues = [mq for mq, bd in queues if mq.broker_name == "broker-b"]
        assert len(broker_a_queues) == 4
        assert len(broker_b_queues) == 4

    @pytest.mark.asyncio
    async def test_aget_available_queues_non_existent_topic(self, mapping):
        """测试获取不存在主题的可用队列"""
        queues = await mapping.aget_available_queues("non_existent_topic")
        assert queues == []

    @pytest.mark.asyncio
    async def test_aget_subscribe_queues(self, mapping, sample_topic_route_data):
        """测试获取订阅队列"""
        topic = "test_topic"

        # 更新路由信息
        await mapping.aupdate_route_info(topic, sample_topic_route_data)

        # 获取订阅队列
        queues = await mapping.aget_subscribe_queues(topic)
        assert len(queues) == 8  # 2 brokers * 4 read queues each

        # 验证每个元素是(MessageQueue, BrokerData)元组
        for queue_tuple in queues:
            assert len(queue_tuple) == 2
            message_queue, broker_data = queue_tuple
            assert isinstance(message_queue, MessageQueue)
            assert isinstance(broker_data, BrokerData)
            assert message_queue.topic == topic

    @pytest.mark.asyncio
    async def test_aget_subscribe_queues_non_existent_topic(self, mapping):
        """测试获取不存在主题的订阅队列"""
        queues = await mapping.aget_subscribe_queues("non_existent_topic")
        assert queues == []

    @pytest.mark.asyncio
    async def test_aget_queue_data(self, mapping, sample_topic_route_data):
        """测试获取队列数据"""
        topic = "test_topic"

        # 更新路由信息
        await mapping.aupdate_route_info(topic, sample_topic_route_data)

        # 获取队列数据列表
        queue_data_list = await mapping.aget_queue_data(topic)
        assert len(queue_data_list) == 2  # 2 brokers

        # 验证队列数据
        for queue_data in queue_data_list:
            assert hasattr(queue_data, "broker_name")
            assert hasattr(queue_data, "read_queue_nums")
            assert hasattr(queue_data, "write_queue_nums")

    @pytest.mark.asyncio
    async def test_aget_queue_data_non_existent(self, mapping):
        """测试获取不存在的队列数据"""
        queue_data_list = await mapping.aget_queue_data("non_existent_topic")
        assert queue_data_list == []

    # ==================== Broker管理测试 ====================

    @pytest.mark.asyncio
    async def test_aget_available_brokers(self, mapping, sample_topic_route_data):
        """测试获取可用Broker"""
        topic = "test_topic"

        # 更新路由信息
        await mapping.aupdate_route_info(topic, sample_topic_route_data)

        # 获取可用Broker
        brokers = await mapping.aget_available_brokers(topic)
        assert len(brokers) == 2
        assert all(isinstance(broker, BrokerData) for broker in brokers)

        broker_names = [broker.broker_name for broker in brokers]
        assert "broker-a" in broker_names
        assert "broker-b" in broker_names

    @pytest.mark.asyncio
    async def test_aget_available_brokers_non_existent_topic(self, mapping):
        """测试获取不存在主题的可用Broker"""
        brokers = await mapping.aget_available_brokers("non_existent_topic")
        assert brokers == []

    # ==================== 缓存管理测试 ====================

    @pytest.mark.asyncio
    async def test_aget_all_topics(self, mapping, sample_topic_route_data):
        """测试获取所有主题"""
        topics = ["topic1", "topic2", "topic3"]

        # 添加路由信息
        for topic in topics:
            await mapping.aupdate_route_info(topic, sample_topic_route_data)

        # 获取所有主题
        all_topics = await mapping.aget_all_topics()
        assert len(all_topics) == 3
        assert isinstance(all_topics, set)
        for topic in topics:
            assert topic in all_topics

    @pytest.mark.asyncio
    async def test_aget_all_topics_empty(self, mapping):
        """测试获取空的主题列表"""
        all_topics = await mapping.aget_all_topics()
        assert all_topics == set()

    @pytest.mark.asyncio
    async def test_set_route_timeout(self, mapping):
        """测试设置路由超时时间"""
        new_timeout = 120.0
        mapping.set_route_timeout(new_timeout)
        assert mapping._default_route_timeout == new_timeout

    # ==================== 过期和清理测试 ====================

    @pytest.mark.asyncio
    async def test_route_expiration(
        self, mapping_with_timeout, sample_topic_route_data
    ):
        """测试路由过期"""
        topic = "test_topic"

        # 设置短超时时间用于测试
        mapping_with_timeout.set_route_timeout(0.1)  # 0.1秒

        # 添加路由信息
        await mapping_with_timeout.aupdate_route_info(topic, sample_topic_route_data)

        # 立即获取应该成功
        route_info = await mapping_with_timeout.aget_route_info(topic)
        assert route_info is not None

        # 等待过期
        await asyncio.sleep(0.2)

        # 再次获取应该返回None（已过期）
        route_info = await mapping_with_timeout.aget_route_info(topic)
        assert route_info is None

    @pytest.mark.asyncio
    async def test_aclear_expired_routes(self, mapping, sample_topic_route_data):
        """测试清理过期路由"""
        topic1 = "topic1"
        topic2 = "topic2"

        # 设置短超时时间
        mapping.set_route_timeout(0.05)  # 50ms

        # 添加第一个路由信息
        await mapping.aupdate_route_info(topic1, sample_topic_route_data)

        # 等待超过超时时间
        await asyncio.sleep(0.06)  # 60ms

        # 添加第二个路由信息（不会过期）
        await mapping.aupdate_route_info(topic2, sample_topic_route_data)

        # 清理过期路由
        await mapping.aclear_expired_routes()

        # 验证结果
        assert await mapping.aget_route_info(topic1) is None  # 应该被清理
        assert await mapping.aget_route_info(topic2) is not None  # 应该还在

    # ==================== 缓存统计测试 ====================

    @pytest.mark.asyncio
    async def test_aget_cache_stats(self, mapping, sample_topic_route_data):
        """测试获取缓存统计信息"""
        topics = ["topic1", "topic2"]

        # 添加路由信息
        for topic in topics:
            await mapping.aupdate_route_info(topic, sample_topic_route_data)

        # 获取缓存统计
        stats = await mapping.aget_cache_stats()

        assert "total_topics" in stats
        assert "total_brokers" in stats
        assert "total_queue_data" in stats
        assert "total_available_queues" in stats
        assert "total_subscribe_queues" in stats
        assert "topics" in stats

        assert stats["total_topics"] == 2
        assert stats["total_brokers"] == 4  # 2 topics * 2 brokers each
        assert stats["total_queue_data"] == 4  # 2 topics * 2 queue_data each
        assert stats["total_available_queues"] == 16  # 2 topics * 8 queues each
        assert stats["total_subscribe_queues"] == 16  # 2 topics * 8 queues each

        # 验证主题列表
        for topic in topics:
            assert topic in stats["topics"]

    @pytest.mark.asyncio
    async def test_aget_cache_stats_empty(self, mapping):
        """测试空缓存的统计信息"""
        stats = await mapping.aget_cache_stats()

        assert stats["total_topics"] == 0
        assert stats["total_brokers"] == 0
        assert stats["total_queue_data"] == 0
        assert stats["total_available_queues"] == 0
        assert stats["total_subscribe_queues"] == 0
        assert stats["topics"] == []

    # ==================== 强制刷新测试 ====================

    @pytest.mark.asyncio
    async def test_aforce_refresh(self, mapping, sample_topic_route_data):
        """测试强制刷新（实际是移除缓存）"""
        topic = "test_topic"

        # 添加路由信息
        await mapping.aupdate_route_info(topic, sample_topic_route_data)

        # 验证路由信息存在
        route_info = await mapping.aget_route_info(topic)
        assert route_info is not None

        # 强制刷新（实际是移除缓存）
        result = await mapping.aforce_refresh(topic)
        assert result is True

        # 验证路由信息已被移除
        route_info = await mapping.aget_route_info(topic)
        assert route_info is None

    @pytest.mark.asyncio
    async def test_aforce_refresh_non_existent_topic(self, mapping):
        """测试强制刷新不存在的主题"""
        # 强制刷新不存在的主题应该返回False
        result = await mapping.aforce_refresh("non_existent_topic")
        assert result is False

        # 主题应该仍然不存在
        route_info = await mapping.aget_route_info("non_existent_topic")
        assert route_info is None

    # ==================== 并发测试 ====================

    @pytest.mark.asyncio
    async def test_concurrent_route_updates(self, mapping, sample_topic_route_data):
        """测试并发路由更新"""
        topic = "test_topic"
        num_updates = 10

        # 并发更新路由信息
        tasks = [
            mapping.aupdate_route_info(f"{topic}_{i}", sample_topic_route_data)
            for i in range(num_updates)
        ]
        await asyncio.gather(*tasks)

        # 验证所有路由信息都已更新
        all_topics = await mapping.aget_all_topics()
        assert len(all_topics) == num_updates

        for i in range(num_updates):
            topic_name = f"{topic}_{i}"
            route_info = await mapping.aget_route_info(topic_name)
            assert route_info is not None

    @pytest.mark.asyncio
    async def test_concurrent_route_access(self, mapping, sample_topic_route_data):
        """测试并发路由访问"""
        topic = "test_topic"

        # 添加路由信息
        await mapping.aupdate_route_info(topic, sample_topic_route_data)

        # 并发访问路由信息
        tasks = [
            mapping.aget_route_info(topic),
            mapping.aget_available_queues(topic),
            mapping.aget_available_brokers(topic),
            mapping.aget_cache_stats(),
        ]
        results = await asyncio.gather(*tasks)

        assert results[0] is not None  # route_info
        assert (
            len(results[1]) == 8
        )  # available_queues (MessageQueue, BrokerData tuples)
        assert len(results[2]) == 2  # available_brokers
        assert isinstance(results[3], dict)  # cache_stats

    # ==================== 字符串表示测试 ====================

    @pytest.mark.asyncio
    async def test_str_representation(self, mapping, sample_topic_route_data):
        """测试字符串表示"""
        # 添加路由信息
        await mapping.aupdate_route_info("test_topic", sample_topic_route_data)

        str_repr = str(mapping)
        assert "AsyncTopicBrokerMapping" in str_repr
        assert "topics=1" in str_repr

    @pytest.mark.asyncio
    async def test_repr_representation(self, mapping, sample_topic_route_data):
        """测试repr表示"""
        # 添加路由信息
        await mapping.aupdate_route_info("test_topic", sample_topic_route_data)

        repr_str = repr(mapping)
        assert "AsyncTopicBrokerMapping" in repr_str
        assert "route_timeout" in repr_str


class TestAsyncRouteInfo:
    """AsyncRouteInfo测试类"""

    def test_is_expired(self):
        """测试过期检查"""
        topic_route_data = TopicRouteData(
            order_topic_conf="", queue_data_list=[], broker_data_list=[]
        )
        route_info = AsyncRouteInfo(topic_route_data=topic_route_data)

        # 新创建的不应该过期
        assert not route_info.is_expired(60.0)

        # 模拟过期
        route_info.last_update_time = time.time() - 120.0
        assert route_info.is_expired(60.0)

    def test_refresh_update_time(self):
        """测试更新时间刷新"""
        topic_route_data = TopicRouteData(
            order_topic_conf="", queue_data_list=[], broker_data_list=[]
        )
        route_info = AsyncRouteInfo(topic_route_data=topic_route_data)

        old_time = route_info.last_update_time
        time.sleep(0.01)  # 确保时间不同

        route_info.refresh_update_time()
        new_time = route_info.last_update_time

        assert new_time > old_time

    def test_create_with_queues(self):
        """测试通过队列数据创建路由信息"""
        broker_data = BrokerData(
            cluster="test-cluster",
            broker_name="test-broker",
            broker_addresses={0: "127.0.0.1:10911"},
        )
        queue_data = QueueData(
            broker_name="test-broker",
            read_queue_nums=4,
            write_queue_nums=4,
            perm=6,
            topic_syn_flag=0,
        )

        topic_route_data = TopicRouteData(
            order_topic_conf="",
            queue_data_list=[queue_data],
            broker_data_list=[broker_data],
        )

        topic = "test_topic"

        route_info = AsyncRouteInfo.create_with_queues(topic_route_data, topic)

        assert route_info.topic_route_data.broker_data_list == [broker_data]
        assert route_info.topic_route_data.queue_data_list == [queue_data]
        assert route_info.topic_route_data.order_topic_conf == ""


class TestFactoryFunctions:
    """工厂函数测试类"""

    @pytest.mark.asyncio
    async def test_create_async_topic_broker_mapping(self):
        """测试创建异步Topic-Broker映射"""
        mapping = create_async_topic_broker_mapping()
        assert isinstance(mapping, AsyncTopicBrokerMapping)
        assert mapping._default_route_timeout == 60.0

        mapping_with_timeout = create_async_topic_broker_mapping(route_timeout=30.0)
        assert mapping_with_timeout._default_route_timeout == 30.0

    @pytest.mark.asyncio
    async def test_async_create_topic_broker_mapping(self):
        """测试异步创建Topic-Broker映射"""
        mapping = await async_create_topic_broker_mapping()
        assert isinstance(mapping, AsyncTopicBrokerMapping)
        assert mapping._default_route_timeout == 60.0

        mapping_with_timeout = await async_create_topic_broker_mapping(
            route_timeout=30.0
        )
        assert mapping_with_timeout._default_route_timeout == 30.0


if __name__ == "__main__":
    # 运行测试
    pytest.main([__file__, "-v"])
