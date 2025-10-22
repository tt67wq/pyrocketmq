"""
TopicBrokerMapping Selector模式测试

测试队列选择器的各种策略，包括：
- RoundRobinSelector 轮询选择
- RandomSelector 随机选择
- MessageHashSelector 消息哈希选择
- 自定义选择器

作者: pyrocketmq团队
版本: MVP 1.0
"""

import unittest

from pyrocketmq.model.message import Message
from pyrocketmq.model.nameserver_models import (
    BrokerData,
    QueueData,
    TopicRouteData,
)
from pyrocketmq.producer.queue_selectors import (
    MessageHashSelector,
    RandomSelector,
    RoundRobinSelector,
)
from pyrocketmq.producer.topic_broker_mapping import TopicBrokerMapping


class TestQueueSelectors(unittest.TestCase):
    """队列选择器测试类"""

    def setUp(self):
        """测试前置条件"""
        # 创建测试用的路由数据
        self.test_topic = "test_topic"

        # 创建Broker数据
        self.broker_data_list = [
            BrokerData(
                cluster="test-cluster",
                broker_name="broker-a",
                broker_addresses={0: "127.0.0.1:10911"},
            ),
            BrokerData(
                cluster="test-cluster",
                broker_name="broker-b",
                broker_addresses={0: "127.0.0.1:10921"},
            ),
        ]

        # 创建队列数据
        self.queue_data_list = [
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

        # 创建Topic路由数据
        self.topic_route_data = TopicRouteData(
            order_topic_conf="",
            queue_data_list=self.queue_data_list,
            broker_data_list=self.broker_data_list,
        )

    def test_round_robin_selector(self):
        """测试轮询选择器"""
        selector = RoundRobinSelector()
        available_queues = []  # 这里应该传入队列列表，但为了测试选择器逻辑

        # 测试空队列列表
        result = selector.select("test_topic", available_queues)
        self.assertIsNone(result)

    def test_random_selector(self):
        """测试随机选择器"""
        selector = RandomSelector()
        available_queues = []

        # 测试空队列列表
        result = selector.select("test_topic", available_queues)
        self.assertIsNone(result)

    def test_message_hash_selector_without_key(self):
        """测试消息哈希选择器（无key时使用随机选择）"""
        selector = MessageHashSelector()
        available_queues = []
        message = None

        # 测试空队列列表
        result = selector.select("test_topic", available_queues, message)
        self.assertIsNone(result)

    def test_message_hash_selector_with_sharding_key(self):
        """测试消息哈希选择器（使用分片键）"""
        selector = MessageHashSelector()

        # 创建带有分片键的消息
        message = Message(topic="test_topic", body=b"test message")
        message.set_property("SHARDING_KEY", "shard_1")

        # 创建模拟的可用队列（实际使用时由TopicBrokerMapping提供）
        available_queues = []

        # 测试空队列列表
        result = selector.select("test_topic", available_queues, message)
        self.assertIsNone(result)

    def test_message_hash_selector_with_keys(self):
        """测试消息哈希选择器（使用消息键）"""
        selector = MessageHashSelector()

        # 创建带有消息键的消息
        message = Message(topic="test_topic", body=b"test message")
        message.set_keys("order_123")

        # 创建模拟的可用队列
        available_queues = []

        # 测试空队列列表
        result = selector.select("test_topic", available_queues, message)
        self.assertIsNone(result)


class TestTopicBrokerMappingWithSelectors(unittest.TestCase):
    """TopicBrokerMapping基础功能测试（不再包含选择器）"""

    def setUp(self):
        """测试前置条件"""
        # 创建测试数据
        self.test_topic = "test_topic"

        self.broker_data_list = [
            BrokerData(
                cluster="test-cluster",
                broker_name="broker-a",
                broker_addresses={0: "127.0.0.1:10911"},
            ),
            BrokerData(
                cluster="test-cluster",
                broker_name="broker-b",
                broker_addresses={0: "127.0.0.1:10921"},
            ),
        ]

        self.queue_data_list = [
            QueueData(
                broker_name="broker-a",
                read_queue_nums=2,
                write_queue_nums=2,
                perm=6,
                topic_syn_flag=0,
            ),
            QueueData(
                broker_name="broker-b",
                read_queue_nums=2,
                write_queue_nums=2,
                perm=6,
                topic_syn_flag=0,
            ),
        ]

        self.topic_route_data = TopicRouteData(
            order_topic_conf="",
            queue_data_list=self.queue_data_list,
            broker_data_list=self.broker_data_list,
        )

    def test_mapping_initialization(self):
        """测试映射初始化"""
        mapping = TopicBrokerMapping()
        self.assertEqual(mapping._default_route_timeout, 30.0)

        # 测试自定义超时时间
        custom_mapping = TopicBrokerMapping(route_timeout=60.0)
        self.assertEqual(custom_mapping._default_route_timeout, 60.0)

    def test_get_available_queues(self):
        """测试获取可用队列"""
        mapping = TopicBrokerMapping()
        mapping.update_route_info(self.test_topic, self.topic_route_data)

        available_queues = mapping.get_available_queues(self.test_topic)

        # 验证队列数量：2个broker，每个broker有2个写队列
        self.assertEqual(len(available_queues), 4)

        # 验证队列信息的完整性
        for message_queue, broker_data in available_queues:
            self.assertEqual(message_queue.topic, self.test_topic)
            self.assertIn(message_queue.broker_name, ["broker-a", "broker-b"])
            self.assertIn(message_queue.queue_id, [0, 1])
            self.assertEqual(broker_data.broker_name, message_queue.broker_name)

    def test_selectors_integration_with_available_queues(self):
        """测试选择器与可用队列列表的集成"""
        mapping = TopicBrokerMapping()
        mapping.update_route_info(self.test_topic, self.topic_route_data)

        available_queues = mapping.get_available_queues(self.test_topic)

        # 测试轮询选择器
        round_robin = RoundRobinSelector()
        selected_results = set()

        for i in range(8):
            result = round_robin.select(self.test_topic, available_queues)
            self.assertIsNotNone(result)
            selected_results.add(result[0].full_name)

        # 轮询应该覆盖多个队列
        self.assertGreater(len(selected_results), 1)

        # 测试随机选择器
        random_selector = RandomSelector()
        for i in range(5):
            result = random_selector.select(self.test_topic, available_queues)
            self.assertIsNotNone(result)

        # 测试消息哈希选择器
        hash_selector = MessageHashSelector()
        message1 = Message(topic=self.test_topic, body=b"message1")
        message1.set_property("SHARDING_KEY", "user_123")

        result = hash_selector.select(
            self.test_topic, available_queues, message1
        )
        self.assertIsNotNone(result)

    def test_route_info_management(self):
        """测试路由信息管理"""
        mapping = TopicBrokerMapping()

        # 初始状态没有路由信息
        route_info = mapping.get_route_info(self.test_topic)
        self.assertIsNone(route_info)

        # 更新路由信息
        success = mapping.update_route_info(
            self.test_topic, self.topic_route_data
        )
        self.assertTrue(success)

        # 获取路由信息
        route_info = mapping.get_route_info(self.test_topic)
        self.assertIsNotNone(route_info)
        self.assertEqual(len(route_info.broker_data_list), 2)
        self.assertEqual(len(route_info.queue_data_list), 2)

        # 移除路由信息
        success = mapping.remove_route_info(self.test_topic)
        self.assertTrue(success)

        # 再次获取应该返回None
        route_info = mapping.get_route_info(self.test_topic)
        self.assertIsNone(route_info)


if __name__ == "__main__":
    unittest.main()
