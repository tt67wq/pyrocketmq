"""
异步TopicBrokerMapping测试用例

测试异步版本的TopicBrokerMapping功能，包括：
- 异步队列选择器
- 异步路由信息管理
- 异步并发安全性
- 后台清理任务

作者: pyrocketmq团队
版本: MVP 1.0
"""

import asyncio

import pytest

from pyrocketmq.model.message import Message
from pyrocketmq.model.message_queue import MessageQueue
from pyrocketmq.model.nameserver_models import (
    BrokerData,
    QueueData,
    TopicRouteData,
)
from pyrocketmq.producer.topic_broker_mapping import (
    AsyncMessageHashSelector,
    AsyncRandomSelector,
    AsyncRoundRobinSelector,
    AsyncTopicBrokerMapping,
)


class TestAsyncQueueSelectors:
    """异步队列选择器测试"""

    @pytest.mark.asyncio
    async def test_async_round_robin_selector(self):
        """测试异步轮询选择器"""
        selector = AsyncRoundRobinSelector()
        topic = "test_topic"

        # 创建测试队列
        queue1 = MessageQueue(topic=topic, broker_name="broker1", queue_id=0)
        queue2 = MessageQueue(topic=topic, broker_name="broker1", queue_id=1)
        broker_data = BrokerData(
            cluster="DefaultCluster",
            broker_name="broker1",
            broker_addresses={0: "127.0.0.1:10911", 1: "127.0.0.1:10912"},
        )

        available_queues = [(queue1, broker_data), (queue2, broker_data)]

        # 测试轮询选择
        selected1 = await selector.select(topic, available_queues)
        selected2 = await selector.select(topic, available_queues)
        selected3 = await selector.select(topic, available_queues)
        if not selected1 or not selected2 or not selected3:
            raise AssertionError("Selection failed")

        assert selected1[0].queue_id == 0  # 第一次选择队列0
        assert selected2[0].queue_id == 1  # 第二次选择队列1
        assert selected3[0].queue_id == 0  # 第三次回到队列0

        # 测试计数器重置
        await selector.reset_counter(topic)
        selected4 = await selector.select(topic, available_queues)
        if not selected4:
            raise AssertionError("Selection failed")
        assert selected4[0].queue_id == 0  # 重置后应该从0开始

    @pytest.mark.asyncio
    async def test_async_random_selector(self):
        """测试异步随机选择器"""
        selector = AsyncRandomSelector()
        topic = "test_topic"

        # 创建测试队列
        queue1 = MessageQueue(topic=topic, broker_name="broker1", queue_id=0)
        queue2 = MessageQueue(topic=topic, broker_name="broker1", queue_id=1)
        broker_data = BrokerData(
            cluster="DefaultCluster",
            broker_name="broker1",
            broker_addresses={0: "127.0.0.1:10911", 1: "127.0.0.1:10912"},
        )

        available_queues = [(queue1, broker_data), (queue2, broker_data)]

        # 测试随机选择
        selected = await selector.select(topic, available_queues)
        assert selected is not None
        assert selected[0] in [queue1, queue2]

        # 测试空队列列表
        empty_result = await selector.select(topic, [])
        assert empty_result is None

    @pytest.mark.asyncio
    async def test_async_message_hash_selector(self):
        """测试异步消息哈希选择器"""
        selector = AsyncMessageHashSelector()
        topic = "test_topic"

        # 创建测试队列
        queue1 = MessageQueue(topic=topic, broker_name="broker1", queue_id=0)
        queue2 = MessageQueue(topic=topic, broker_name="broker1", queue_id=1)
        broker_data = BrokerData(
            cluster="DefaultCluster",
            broker_name="broker1",
            broker_addresses={0: "127.0.0.1:10911", 1: "127.0.0.1:10912"},
        )

        available_queues = [(queue1, broker_data), (queue2, broker_data)]

        # 创建带分片键的消息
        message1 = Message(topic=topic, body=b"test1")
        message1.set_property("SHARDING_KEY", "user_123")

        message2 = Message(topic=topic, body=b"test2")
        message2.set_property("SHARDING_KEY", "user_123")  # 相同的分片键

        message3 = Message(topic=topic, body=b"test3")
        message3.set_property("SHARDING_KEY", "user_456")  # 不同的分片键

        # 测试相同分片键总是选择相同队列
        selected1 = await selector.select(topic, available_queues, message1)
        selected2 = await selector.select(topic, available_queues, message2)
        selected3 = await selector.select(topic, available_queues, message3)

        if not selected1 or not selected2 or not selected3:
            raise AssertionError("Selection failed")

        assert selected1[0] == selected2[0]  # 相同分片键应该选择相同队列
        # 不同分片键可能选择相同或不同队列（取决于哈希分布）
        # 这里只验证选择结果有效，不强制要求不同队列
        assert selected3[0] in [queue1, queue2]

        # 测试无分片键的消息
        message_no_key = Message(topic=topic, body=b"no_key")
        selected_no_key = await selector.select(
            topic, available_queues, message_no_key
        )
        assert selected_no_key is not None

        # 测试空消息
        selected_no_message = await selector.select(
            topic, available_queues, None
        )
        assert selected_no_message is not None


class TestAsyncTopicBrokerMapping:
    """异步TopicBrokerMapping测试"""

    @pytest.fixture
    def sample_topic_route_data(self):
        """创建示例Topic路由数据"""
        broker_data = BrokerData(
            cluster="DefaultCluster",
            broker_name="broker1",
            broker_addresses={0: "127.0.0.1:10911", 1: "127.0.0.1:10912"},
        )
        queue_data = QueueData(
            broker_name="broker1",
            read_queue_nums=4,
            write_queue_nums=4,
            perm=6,
            topic_syn_flag=0,
        )
        return TopicRouteData(
            order_topic_conf="",
            broker_data_list=[broker_data],
            queue_data_list=[queue_data],
        )

    @pytest.mark.asyncio
    async def test_async_mapping_initialization(self):
        """测试异步映射初始化"""
        # 使用默认轮询选择器
        mapping = AsyncTopicBrokerMapping()
        assert isinstance(mapping._default_selector, AsyncRoundRobinSelector)

        # 使用自定义选择器
        random_selector = AsyncRandomSelector()
        mapping_custom = AsyncTopicBrokerMapping(
            default_selector=random_selector
        )
        assert mapping_custom._default_selector == random_selector

    @pytest.mark.asyncio
    async def test_async_route_info_management(self, sample_topic_route_data):
        """测试异步路由信息管理"""
        mapping = AsyncTopicBrokerMapping()
        topic = "test_topic"

        # 测试更新路由信息
        success = await mapping.update_route_info(
            topic, sample_topic_route_data
        )
        assert success

        # 测试获取路由信息
        route_info = await mapping.get_route_info(topic)
        assert route_info == sample_topic_route_data

        # 测试获取可用队列
        available_queues = await mapping.get_available_queues(topic)
        assert len(available_queues) == 4  # 4个写队列

        # 测试获取可用Broker
        available_brokers = await mapping.get_available_brokers(topic)
        assert len(available_brokers) == 1

        # 测试获取队列数据
        queue_data = await mapping.get_queue_data(topic)
        assert len(queue_data) == 1

        # 测试获取所有Topic
        all_topics = await mapping.get_all_topics()
        assert topic in all_topics

    @pytest.mark.asyncio
    async def test_async_queue_selection(self, sample_topic_route_data):
        """测试异步队列选择"""
        mapping = AsyncTopicBrokerMapping()
        topic = "test_topic"

        # 更新路由信息
        await mapping.update_route_info(topic, sample_topic_route_data)

        # 测试默认选择器选择队列
        message = Message(topic=topic, body=b"test_message")
        selected = await mapping.select_queue(topic, message)
        assert selected is not None
        assert selected[0].topic == topic

        # 测试使用自定义选择器
        hash_selector = AsyncMessageHashSelector()
        message.set_property("SHARDING_KEY", "user_123")
        selected_hash = await mapping.select_queue(
            topic, message, hash_selector
        )
        assert selected_hash is not None

        # 测试无路由信息的情况
        result_no_route = await mapping.select_queue(
            "non_existent_topic", message
        )
        assert result_no_route is None

    @pytest.mark.asyncio
    async def test_async_route_expiration(self, sample_topic_route_data):
        """测试异步路由过期处理"""
        mapping = AsyncTopicBrokerMapping()
        mapping.set_route_timeout(0.1)  # 0.1秒过期
        topic = "test_topic"

        # 更新路由信息
        await mapping.update_route_info(topic, sample_topic_route_data)

        # 立即获取应该成功
        route_info = await mapping.get_route_info(topic)
        assert route_info is not None

        # 等待过期
        await asyncio.sleep(0.2)

        # 过期后获取应该失败
        route_info_expired = await mapping.get_route_info(topic)
        assert route_info_expired is None

        # 测试清理过期路由
        cleared_count = await mapping.clear_expired_routes()
        assert cleared_count == 1

    @pytest.mark.asyncio
    async def test_async_cache_stats(self, sample_topic_route_data):
        """测试异步缓存统计"""
        mapping = AsyncTopicBrokerMapping()
        topic1 = "test_topic1"
        topic2 = "test_topic2"

        # 更新路由信息
        await mapping.update_route_info(topic1, sample_topic_route_data)
        await mapping.update_route_info(topic2, sample_topic_route_data)

        # 获取统计信息
        stats = await mapping.get_cache_stats()
        assert stats["total_topics"] == 2
        assert stats["total_brokers"] == 2
        assert stats["total_queue_data"] == 2
        assert stats["total_available_queues"] == 8  # 每个topic 4个队列
        assert topic1 in stats["topics"]
        assert topic2 in stats["topics"]

    @pytest.mark.asyncio
    async def test_async_force_refresh(self, sample_topic_route_data):
        """测试异步强制刷新"""
        mapping = AsyncTopicBrokerMapping()
        topic = "test_topic"

        # 更新路由信息
        await mapping.update_route_info(topic, sample_topic_route_data)

        # 验证路由信息存在
        route_info = await mapping.get_route_info(topic)
        assert route_info is not None

        # 强制刷新
        refreshed = await mapping.force_refresh(topic)
        assert refreshed

        # 验证路由信息已移除
        route_info_after = await mapping.get_route_info(topic)
        assert route_info_after is None

    @pytest.mark.asyncio
    async def test_async_concurrent_access(self, sample_topic_route_data):
        """测试异步并发访问安全性"""
        mapping = AsyncTopicBrokerMapping()
        topic = "test_topic"

        # 并发更新路由信息
        update_tasks = []
        for i in range(10):
            task = asyncio.create_task(
                mapping.update_route_info(topic, sample_topic_route_data)
            )
            update_tasks.append(task)

        results = await asyncio.gather(*update_tasks)
        assert all(results)  # 所有更新都应该成功

        # 并发选择队列
        message = Message(topic=topic, body=b"test")
        select_tasks = []
        for i in range(20):
            task = asyncio.create_task(mapping.select_queue(topic, message))
            select_tasks.append(task)

        selected_results = await asyncio.gather(*select_tasks)
        assert all(
            result is not None for result in selected_results
        )  # 所有选择都应该成功

    @pytest.mark.asyncio
    async def test_async_background_cleanup(self, sample_topic_route_data):
        """测试异步后台清理任务"""
        mapping = AsyncTopicBrokerMapping()
        mapping.set_route_timeout(0.1)  # 0.1秒过期

        # 添加一些路由信息
        for i in range(3):
            await mapping.update_route_info(
                f"topic_{i}", sample_topic_route_data
            )

        # 启动后台清理任务（短时间运行）
        cleanup_task = asyncio.create_task(
            mapping.start_background_cleanup(interval=0.05)
        )

        # 等待一段时间让清理任务运行
        await asyncio.sleep(0.2)

        # 取消清理任务
        cleanup_task.cancel()
        try:
            await cleanup_task
        except asyncio.CancelledError:
            pass

        # 验证路由信息已被清理
        stats = await mapping.get_cache_stats()
        assert stats["total_topics"] == 0

    @pytest.mark.asyncio
    async def test_empty_route_data_handling(self):
        """测试空路由数据处理"""
        mapping = AsyncTopicBrokerMapping()
        topic = "test_topic"

        # 尝试更新空路由数据
        success = await mapping.update_route_info(topic, None)
        assert not success

        # 尝试更新空列表的路由数据
        empty_route_data = TopicRouteData(
            order_topic_conf="", broker_data_list=[], queue_data_list=[]
        )
        success_empty = await mapping.update_route_info(topic, empty_route_data)
        assert success_empty  # 空列表是有效的

        # 验证没有可用队列
        available_queues = await mapping.get_available_queues(topic)
        assert len(available_queues) == 0
