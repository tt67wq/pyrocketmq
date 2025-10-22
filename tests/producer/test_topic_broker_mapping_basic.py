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
from pyrocketmq.producer.topic_broker_mapping import TopicBrokerMapping


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

    def test_init_default_configuration(self):
        """测试初始化时的默认配置"""
        self.assertIsNotNone(self.mapping)
        self.assertEqual(self.mapping._default_route_timeout, 30.0)
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

    def test_get_available_queues(self):
        """测试获取可用队列"""
        # 初始状态应该返回空列表
        available_queues = self.mapping.get_available_queues(self.test_topic)
        self.assertEqual(len(available_queues), 0)

        # 更新路由信息后应该返回正确的队列列表
        self.mapping.update_route_info(self.test_topic, self.topic_route_data)
        available_queues = self.mapping.get_available_queues(self.test_topic)

        # 验证队列数量
        self.assertEqual(len(available_queues), 16)  # 2 brokers * 8 queues

        # 验证队列信息完整性
        for message_queue, broker_data in available_queues:
            self.assertEqual(message_queue.topic, self.test_topic)
            self.assertIn(message_queue.broker_name, ["broker-a", "broker-b"])
            self.assertGreaterEqual(message_queue.queue_id, 0)
            self.assertLess(message_queue.queue_id, 8)
            self.assertEqual(broker_data.broker_name, message_queue.broker_name)

    def test_get_available_brokers(self):
        """测试获取可用Broker"""
        # 初始状态应该返回空列表
        available_brokers = self.mapping.get_available_brokers(self.test_topic)
        self.assertEqual(len(available_brokers), 0)

        # 更新路由信息后应该返回正确的Broker列表
        self.mapping.update_route_info(self.test_topic, self.topic_route_data)
        available_brokers = self.mapping.get_available_brokers(self.test_topic)

        # 验证Broker数量和信息
        self.assertEqual(len(available_brokers), 2)
        broker_names = [broker.broker_name for broker in available_brokers]
        self.assertIn("broker-a", broker_names)
        self.assertIn("broker-b", broker_names)

    def test_get_queue_data(self):
        """测试获取队列数据"""
        # 初始状态应该返回空列表
        queue_data = self.mapping.get_queue_data(self.test_topic)
        self.assertEqual(len(queue_data), 0)

        # 更新路由信息后应该返回正确的队列数据
        self.mapping.update_route_info(self.test_topic, self.topic_route_data)
        queue_data = self.mapping.get_queue_data(self.test_topic)

        # 验证队列数据
        self.assertEqual(len(queue_data), 2)
        for qd in queue_data:
            self.assertIn(qd.broker_name, ["broker-a", "broker-b"])
            self.assertEqual(qd.write_queue_nums, 8)
            self.assertEqual(qd.read_queue_nums, 8)

    def test_remove_route_info(self):
        """测试移除路由信息"""
        # 更新路由信息
        self.mapping.update_route_info(self.test_topic, self.topic_route_data)

        # 验证路由信息存在
        self.assertIsNotNone(self.mapping.get_route_info(self.test_topic))
        self.assertEqual(
            len(self.mapping.get_available_queues(self.test_topic)), 16
        )

        # 移除路由信息
        result = self.mapping.remove_route_info(self.test_topic)
        self.assertTrue(result)

        # 验证路由信息已被移除
        self.assertIsNone(self.mapping.get_route_info(self.test_topic))
        self.assertEqual(
            len(self.mapping.get_available_queues(self.test_topic)), 0
        )

        # 验证缓存统计
        stats = self.mapping.get_cache_stats()
        self.assertEqual(stats["total_topics"], 0)

    def test_force_refresh(self):
        """测试强制刷新路由信息"""
        # 更新路由信息
        self.mapping.update_route_info(self.test_topic, self.topic_route_data)

        # 验证路由信息存在
        self.assertIsNotNone(self.mapping.get_route_info(self.test_topic))

        # 强制刷新（实际上是移除缓存）
        result = self.mapping.force_refresh(self.test_topic)
        self.assertTrue(result)

        # 验证路由信息已被移除
        self.assertIsNone(self.mapping.get_route_info(self.test_topic))

    def test_get_all_topics(self):
        """测试获取所有Topic"""
        # 初始状态应该返回空集合
        all_topics = self.mapping.get_all_topics()
        self.assertEqual(len(all_topics), 0)

        # 更新多个Topic的路由信息
        topics = ["topic1", "topic2", "topic3"]
        for topic in topics:
            self.mapping.update_route_info(topic, self.topic_route_data)

        # 验证所有Topic都被返回
        all_topics = self.mapping.get_all_topics()
        self.assertEqual(len(all_topics), 3)
        for topic in topics:
            self.assertIn(topic, all_topics)

    def test_route_timeout_setting(self):
        """测试路由超时设置"""
        # 测试默认超时时间
        self.assertEqual(self.mapping._default_route_timeout, 30.0)

        # 设置新的超时时间
        self.mapping.set_route_timeout(60.0)
        self.assertEqual(self.mapping._default_route_timeout, 60.0)

        # 测试无效超时时间
        with self.assertRaises(ValueError):
            self.mapping.set_route_timeout(-10.0)

    def test_string_representation(self):
        """测试字符串表示"""
        # 更新路由信息
        self.mapping.update_route_info(self.test_topic, self.topic_route_data)

        # 测试__str__方法
        str_repr = str(self.mapping)
        self.assertIn("TopicBrokerMapping", str_repr)
        self.assertIn("topics=1", str_repr)
        self.assertIn("brokers=2", str_repr)
        self.assertIn("queue_data=2", str_repr)
        self.assertIn("available_queues=16", str_repr)

        # 测试__repr__方法
        repr_str = repr(self.mapping)
        self.assertEqual(str_repr, repr_str)

    def test_clear_expired_routes(self):
        """测试清理过期路由"""
        # 更新路由信息
        self.mapping.update_route_info(self.test_topic, self.topic_route_data)

        # 验证路由信息存在
        self.assertEqual(len(self.mapping.get_all_topics()), 1)

        # 清理过期路由（使用很长的超时时间，不应该清理）
        cleared_count = self.mapping.clear_expired_routes(timeout=3600.0)
        self.assertEqual(cleared_count, 0)
        self.assertEqual(len(self.mapping.get_all_topics()), 1)

        # 测试清理功能（实际上由于时间戳是新的，不会被清理）
        # 这个测试主要验证方法能正常调用
        cleared_count = self.mapping.clear_expired_routes()
        self.assertIsInstance(cleared_count, int)

    @patch("pyrocketmq.producer.topic_broker_mapping.logger")
    def test_logging(self, mock_logger):
        """测试日志记录"""
        # 更新路由信息应该记录info日志
        self.mapping.update_route_info(self.test_topic, self.topic_route_data)
        mock_logger.info.assert_called()

        # 移除路由信息应该记录info日志
        self.mapping.remove_route_info(self.test_topic)
        mock_logger.info.assert_called()


class TestTopicBrokerMappingEdgeCases(unittest.TestCase):
    """TopicBrokerMapping边界情况测试"""

    def setUp(self):
        self.mapping = TopicBrokerMapping()

    def test_update_empty_route_data(self):
        """测试更新空路由数据"""
        result = self.mapping.update_route_info("test_topic", None)
        self.assertFalse(result)

        result = self.mapping.update_route_info("test_topic", TopicRouteData())
        self.assertTrue(result)  # 空数据但不是None

    def test_remove_nonexistent_topic(self):
        """测试移除不存在的Topic"""
        result = self.mapping.remove_route_info("nonexistent_topic")
        self.assertFalse(result)

    def test_force_refresh_nonexistent_topic(self):
        """测试强制刷新不存在的Topic"""
        result = self.mapping.force_refresh("nonexistent_topic")
        self.assertFalse(result)


if __name__ == "__main__":
    unittest.main()
