"""
TopicBrokerMapping基础功能测试

测试TopicBrokerMapping的基础功能，验证Selector重构后核心功能仍然正常。
包括路由信息管理、缓存操作、统计信息等。

作者: pyrocketmq团队
版本: MVP 1.0
"""

import unittest
from unittest.mock import patch

from pyrocketmq.model.nameserver_models import (
    BrokerData,
    QueueData,
    TopicRouteData,
)
from pyrocketmq.producer.topic_broker_mapping import (
    RandomSelector,
    RoundRobinSelector,
    TopicBrokerMapping,
)


class TestTopicBrokerMappingBasic(unittest.TestCase):
    """TopicBrokerMapping基础功能测试类"""

    def setUp(self):
        """测试前置条件"""
        self.mapping = TopicBrokerMapping()

        # 创建测试用的路由数据
        self.test_topic = "test_topic"

        # 创建Broker数据
        self.broker_data_list = [
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

        # 创建队列数据
        self.queue_data_list = [
            QueueData(
                broker_name="broker-a",
                read_queue_nums=8,
                write_queue_nums=8,
                perm=6,
                topic_syn_flag=0,
            ),
            QueueData(
                broker_name="broker-b",
                read_queue_nums=8,
                write_queue_nums=8,
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

    def test_init_default_selector(self):
        """测试初始化时的默认选择器"""
        self.assertIsNotNone(self.mapping)
        self.assertIsInstance(
            self.mapping._default_selector, RoundRobinSelector
        )
        stats = self.mapping.get_cache_stats()
        self.assertEqual(stats["total_topics"], 0)
        self.assertEqual(stats["total_brokers"], 0)
        self.assertEqual(stats["total_queue_data"], 0)
        self.assertEqual(stats["total_available_queues"], 0)

    def test_update_route_info(self):
        """测试更新路由信息"""
        # 更新路由信息
        result = self.mapping.update_route_info(
            self.test_topic, self.topic_route_data
        )
        self.assertTrue(result)

        # 验证缓存统计
        stats = self.mapping.get_cache_stats()
        self.assertEqual(stats["total_topics"], 1)
        self.assertEqual(stats["total_brokers"], 2)
        self.assertEqual(stats["total_queue_data"], 2)
        self.assertEqual(
            stats["total_available_queues"], 16
        )  # 2 brokers * 8 queues each

    def test_get_route_info(self):
        """测试获取路由信息"""
        # 初始状态应该返回None
        route_info = self.mapping.get_route_info(self.test_topic)
        self.assertIsNone(route_info)

        # 更新路由信息后应该返回正确数据
        self.mapping.update_route_info(self.test_topic, self.topic_route_data)
        route_info = self.mapping.get_route_info(self.test_topic)

        self.assertIsNotNone(route_info)
        self.assertEqual(len(route_info.broker_data_list), 2)
        self.assertEqual(len(route_info.queue_data_list), 2)

    def test_select_queue_default(self):
        """测试默认队列选择（轮询）"""
        # 没有路由信息时应该返回None
        result = self.mapping.select_queue(self.test_topic)
        self.assertIsNone(result)

        # 更新路由信息
        self.mapping.update_route_info(self.test_topic, self.topic_route_data)

        # 测试队列选择
        selected_results = set()
        for i in range(20):  # 选择20次，验证轮询算法
            result = self.mapping.select_queue(self.test_topic)
            self.assertIsNotNone(result)

            message_queue, broker_data = result

            # 验证返回的类型
            self.assertEqual(message_queue.topic, self.test_topic)
            self.assertIn(message_queue.broker_name, ["broker-a", "broker-b"])
            self.assertGreaterEqual(message_queue.queue_id, 0)
            self.assertLess(message_queue.queue_id, 8)  # 每个broker有8个写队列

            # 验证broker数据
            self.assertEqual(broker_data.broker_name, message_queue.broker_name)

            # 记录选择的队列
            selected_results.add(message_queue.full_name)

        # 验证所有队列都有机会被选中
        expected_queues = 16  # 2 brokers * 8 queues each
        self.assertGreaterEqual(
            len(selected_results), expected_queues * 0.6
        )  # 至少60%的队列被选中

    def test_select_queue_with_custom_selector(self):
        """测试使用自定义选择器"""
        # 更新路由信息
        self.mapping.update_route_info(self.test_topic, self.topic_route_data)

        # 使用随机选择器
        random_selector = RandomSelector()
        selected_results = set()

        for i in range(50):  # 选择50次
            result = self.mapping.select_queue(
                self.test_topic, selector=random_selector
            )
            self.assertIsNotNone(result)

            message_queue, _ = result
            selected_results.add(message_queue.full_name)

        # 随机选择应该覆盖更多队列
        self.assertGreater(len(selected_results), 5)

    def test_get_available_queues(self):
        """测试获取所有可用队列"""
        # 没有路由信息时应该返回空列表
        queues = self.mapping.get_available_queues(self.test_topic)
        self.assertEqual(len(queues), 0)

        # 更新路由信息
        self.mapping.update_route_info(self.test_topic, self.topic_route_data)

        # 获取所有队列
        queues = self.mapping.get_available_queues(self.test_topic)
        self.assertEqual(len(queues), 16)  # 2 brokers * 8 queues each

        # 验证每个队列的信息
        for message_queue, broker_data in queues:
            self.assertEqual(message_queue.topic, self.test_topic)
            self.assertIn(message_queue.broker_name, ["broker-a", "broker-b"])
            self.assertGreaterEqual(message_queue.queue_id, 0)
            self.assertLess(message_queue.queue_id, 8)
            self.assertEqual(broker_data.broker_name, message_queue.broker_name)

    def test_remove_route_info(self):
        """测试移除路由信息"""
        # 更新路由信息
        self.mapping.update_route_info(self.test_topic, self.topic_route_data)

        # 验证路由信息存在
        route_info = self.mapping.get_route_info(self.test_topic)
        self.assertIsNotNone(route_info)

        # 移除路由信息
        result = self.mapping.remove_route_info(self.test_topic)
        self.assertTrue(result)

        # 验证路由信息已移除
        route_info = self.mapping.get_route_info(self.test_topic)
        self.assertIsNone(route_info)

        # 验证统计信息
        stats = self.mapping.get_cache_stats()
        self.assertEqual(stats["total_topics"], 0)

    def test_get_all_topics(self):
        """测试获取所有Topic"""
        # 初始状态应该为空
        topics = self.mapping.get_all_topics()
        self.assertEqual(len(topics), 0)

        # 添加路由信息
        self.mapping.update_route_info("topic1", self.topic_route_data)
        self.mapping.update_route_info("topic2", self.topic_route_data)

        # 获取所有Topic
        topics = self.mapping.get_all_topics()
        self.assertEqual(len(topics), 2)
        self.assertIn("topic1", topics)
        self.assertIn("topic2", topics)

    def test_get_available_brokers(self):
        """测试获取可用Broker"""
        # 没有路由信息时应该返回空列表
        brokers = self.mapping.get_available_brokers(self.test_topic)
        self.assertEqual(len(brokers), 0)

        # 更新路由信息
        self.mapping.update_route_info(self.test_topic, self.topic_route_data)

        # 获取所有broker
        brokers = self.mapping.get_available_brokers(self.test_topic)
        self.assertEqual(len(brokers), 2)

        broker_names = [broker.broker_name for broker in brokers]
        self.assertIn("broker-a", broker_names)
        self.assertIn("broker-b", broker_names)

    def test_route_timeout_setting(self):
        """测试设置路由超时时间"""
        # 测试有效超时时间
        self.mapping.set_route_timeout(60.0)

        # 测试无效超时时间
        with self.assertRaises(ValueError):
            self.mapping.set_route_timeout(-1.0)

    def test_string_representation(self):
        """测试字符串表示"""
        # 空状态的字符串表示
        str_repr = str(self.mapping)
        self.assertIn("TopicBrokerMapping", str_repr)
        self.assertIn("topics=0", str_repr)

        # 有路由信息时的字符串表示
        self.mapping.update_route_info(self.test_topic, self.topic_route_data)
        str_repr = str(self.mapping)
        self.assertIn("topics=1", str_repr)
        self.assertIn("brokers=2", str_repr)
        self.assertIn("queue_data=2", str_repr)
        self.assertIn("available_queues=16", str_repr)

    @patch("pyrocketmq.producer.topic_broker_mapping.logger")
    def test_logging(self, mock_logger):
        """测试日志记录"""
        # 更新路由信息应该记录info日志
        self.mapping.update_route_info(self.test_topic, self.topic_route_data)
        mock_logger.info.assert_called()

        # 选择队列应该记录debug日志
        self.mapping.select_queue(self.test_topic)
        mock_logger.debug.assert_called()

    def test_custom_selector_initialization(self):
        """测试自定义选择器初始化"""
        random_selector = RandomSelector()
        custom_mapping = TopicBrokerMapping(default_selector=random_selector)
        self.assertIsInstance(custom_mapping._default_selector, RandomSelector)

        # 测试选择功能
        custom_mapping.update_route_info(self.test_topic, self.topic_route_data)
        result = custom_mapping.select_queue(self.test_topic)
        self.assertIsNotNone(result)

    def test_force_refresh(self):
        """测试强制刷新路由信息"""
        # 更新路由信息
        self.mapping.update_route_info(self.test_topic, self.topic_route_data)

        # 验证路由信息存在
        route_info = self.mapping.get_route_info(self.test_topic)
        self.assertIsNotNone(route_info)

        # 强制刷新
        result = self.mapping.force_refresh(self.test_topic)
        self.assertTrue(result)

        # 验证路由信息已清理
        route_info = self.mapping.get_route_info(self.test_topic)
        self.assertIsNone(route_info)


if __name__ == "__main__":
    unittest.main()
