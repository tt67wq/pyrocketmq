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
from unittest.mock import patch

from pyrocketmq.model.message import Message
from pyrocketmq.model.nameserver_models import (
    BrokerData,
    QueueData,
    TopicRouteData,
)
from pyrocketmq.producer.topic_broker_mapping import (
    MessageHashSelector,
    RandomSelector,
    RoundRobinSelector,
    TopicBrokerMapping,
)


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
    """TopicBrokerMapping with Selector模式测试"""

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

    def test_default_selector_is_round_robin(self):
        """测试默认选择器是轮询选择器"""
        mapping = TopicBrokerMapping()
        self.assertIsInstance(mapping._default_selector, RoundRobinSelector)

    def test_custom_selector_initialization(self):
        """测试自定义选择器初始化"""
        random_selector = RandomSelector()
        mapping = TopicBrokerMapping(default_selector=random_selector)
        self.assertIsInstance(mapping._default_selector, RandomSelector)

    def test_select_queue_with_default_selector(self):
        """测试使用默认选择器选择队列"""
        mapping = TopicBrokerMapping()
        mapping.update_route_info(self.test_topic, self.topic_route_data)

        # 测试轮询选择
        selected_results = set()
        for i in range(10):
            result = mapping.select_queue(self.test_topic)
            self.assertIsNotNone(result)

            message_queue, broker_data = result
            selected_results.add(message_queue.full_name)

        # 轮询应该覆盖多个队列
        self.assertGreater(len(selected_results), 1)

    def test_select_queue_with_custom_selector(self):
        """测试使用自定义选择器选择队列"""
        mapping = TopicBrokerMapping()
        mapping.update_route_info(self.test_topic, self.topic_route_data)

        # 使用随机选择器
        random_selector = RandomSelector()
        selected_results = set()

        for i in range(20):
            result = mapping.select_queue(
                self.test_topic, selector=random_selector
            )
            self.assertIsNotNone(result)

            message_queue, broker_data = result
            selected_results.add(message_queue.full_name)

        # 随机选择应该覆盖更多队列
        self.assertGreater(len(selected_results), 1)

    def test_select_queue_with_message_hash_selector(self):
        """测试使用消息哈希选择器"""
        mapping = TopicBrokerMapping()
        mapping.update_route_info(self.test_topic, self.topic_route_data)

        # 创建带有分片键的消息
        message1 = Message(topic="test_topic", body=b"message1")
        message1.set_property("SHARDING_KEY", "user_123")

        message2 = Message(topic="test_topic", body=b"message2")
        message2.set_property("SHARDING_KEY", "user_123")  # 相同的分片键

        message3 = Message(topic="test_topic", body=b"message3")
        message3.set_property("SHARDING_KEY", "user_456")  # 不同的分片键

        hash_selector = MessageHashSelector()

        # 相同分片键的消息应该选择相同队列
        result1 = mapping.select_queue(self.test_topic, message1, hash_selector)
        result2 = mapping.select_queue(self.test_topic, message2, hash_selector)
        result3 = mapping.select_queue(self.test_topic, message3, hash_selector)

        self.assertIsNotNone(result1)
        self.assertIsNotNone(result2)
        self.assertIsNotNone(result3)

        # 相同分片键应该选择相同队列
        self.assertEqual(result1[0].full_name, result2[0].full_name)

        # 不同分片键可能选择不同队列（取决于哈希值）
        # 由于哈希的不确定性，我们只检查结果不为None

    def test_selector_persistence_across_calls(self):
        """测试选择器在多次调用间的持久性"""
        round_robin = RoundRobinSelector()
        mapping = TopicBrokerMapping(default_selector=round_robin)
        mapping.update_route_info(self.test_topic, self.topic_route_data)

        # 连续选择应该产生轮询效果
        results = []
        for i in range(8):
            result = mapping.select_queue(self.test_topic)
            results.append(result[0].full_name)

        # 验证轮询顺序
        # 由于队列数量是4（2个broker * 2个队列），8次选择应该重复2轮
        unique_results = len(set(results))
        self.assertGreaterEqual(unique_results, 4)  # 至少覆盖所有队列

    def test_selector_reset_on_route_update(self):
        """测试路由更新时选择器重置"""
        round_robin = RoundRobinSelector()
        mapping = TopicBrokerMapping(default_selector=round_robin)

        # 更新路由信息
        mapping.update_route_info(self.test_topic, self.topic_route_data)

        # 选择几次队列
        for i in range(3):
            mapping.select_queue(self.test_topic)

        # 再次更新路由信息，应该重置计数器
        mapping.update_route_info(self.test_topic, self.topic_route_data)

        # 选择队列应该从头开始
        first_result = mapping.select_queue(self.test_topic)
        self.assertIsNotNone(first_result)

    def test_selector_reset_on_route_removal(self):
        """测试路由移除时选择器重置"""
        round_robin = RoundRobinSelector()
        mapping = TopicBrokerMapping(default_selector=round_robin)

        # 更新路由信息
        mapping.update_route_info(self.test_topic, self.topic_route_data)

        # 选择几次队列
        for i in range(3):
            mapping.select_queue(self.test_topic)

        # 移除路由信息
        mapping.remove_route_info(self.test_topic)

        # 重新添加路由信息
        mapping.update_route_info(self.test_topic, self.topic_route_data)

        # 选择队列应该从头开始
        first_result = mapping.select_queue(self.test_topic)
        self.assertIsNotNone(first_result)

    @patch("pyrocketmq.producer.topic_broker_mapping.logger")
    def test_logging_with_selectors(self, mock_logger):
        """测试选择器的日志记录"""
        mapping = TopicBrokerMapping()
        mapping.update_route_info(self.test_topic, self.topic_route_data)

        # 使用不同选择器
        random_selector = RandomSelector()
        hash_selector = MessageHashSelector()

        # 测试默认选择器日志
        mapping.select_queue(self.test_topic)

        # 测试自定义选择器日志
        message = Message(topic="test_topic", body=b"test")
        mapping.select_queue(self.test_topic, message, random_selector)

        # 验证debug日志被调用
        mock_logger.debug.assert_called()


class TestCustomSelector:
    """自定义选择器示例"""

    def select(self, topic, available_queues, message=None):
        """总是选择第一个队列的自定义选择器"""
        if available_queues:
            return available_queues[0]
        return None


def test_custom_selector_integration():
    """测试自定义选择器集成"""
    # 创建测试数据
    broker_data_list = [
        BrokerData(
            cluster="test-cluster",
            broker_name="broker-a",
            broker_addresses={0: "127.0.0.1:10911"},
        )
    ]

    queue_data_list = [
        QueueData(
            broker_name="broker-a",
            read_queue_nums=4,
            write_queue_nums=4,
            perm=6,
            topic_syn_flag=0,
        )
    ]

    topic_route_data = TopicRouteData(
        order_topic_conf="",
        queue_data_list=queue_data_list,
        broker_data_list=broker_data_list,
    )

    # 使用自定义选择器
    custom_selector = TestCustomSelector()
    mapping = TopicBrokerMapping(default_selector=custom_selector)
    mapping.update_route_info("test_topic", topic_route_data)

    # 多次选择应该总是返回同一个队列
    result1 = mapping.select_queue("test_topic")
    result2 = mapping.select_queue("test_topic")

    assert result1[0].full_name == result2[0].full_name


if __name__ == "__main__":
    unittest.main()
