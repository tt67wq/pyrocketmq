"""
ConcurrentConsumer单元测试

测试并发消费者的核心功能，包括：
- 消费者启动和停止
- 消息订阅和取消订阅
- 消息监听器注册
- 基本功能验证

作者: pyrocketmq开发团队
"""

import unittest
from unittest.mock import MagicMock, patch

from pyrocketmq.consumer import ConcurrentConsumer, ConsumerConfig
from pyrocketmq.consumer.errors import ConsumerStartError, SubscribeError
from pyrocketmq.consumer.listener import ConsumeResult, MessageListenerConcurrently
from pyrocketmq.model import MessageModel


class TestConcurrentConsumer(unittest.TestCase):
    """ConcurrentConsumer测试类"""

    def setUp(self) -> None:
        """测试前的设置"""
        self.config = ConsumerConfig(
            consumer_group="test_consumer_group",
            namesrv_addr="localhost:9876",
            message_model=MessageModel.CLUSTERING,
            consume_thread_max=5,
        )

        # 创建模拟的消息监听器
        self.mock_listener = MagicMock(spec=MessageListenerConcurrently)
        self.mock_listener.consume_message.return_value = ConsumeResult.SUCCESS

    def test_consumer_initialization(self) -> None:
        """测试消费者初始化"""
        consumer = ConcurrentConsumer(self.config)

        self.assertEqual(consumer.get_config(), self.config)
        self.assertFalse(consumer.is_running())
        self.assertIsNone(consumer.get_message_listener())
        self.assertEqual(consumer.get_subscribed_topics(), [])

    def test_consumer_initialization_invalid_config(self) -> None:
        """测试无效配置的初始化"""
        with self.assertRaises(ValueError):
            ConcurrentConsumer(None)

    def test_register_message_listener(self) -> None:
        """测试注册消息监听器"""
        consumer = ConcurrentConsumer(self.config)

        # 注册监听器
        consumer.register_message_listener(self.mock_listener)

        self.assertEqual(consumer.get_message_listener(), self.mock_listener)

    def test_register_message_listener_none(self) -> None:
        """测试注册None监听器"""
        consumer = ConcurrentConsumer(self.config)

        with self.assertRaises(ValueError):
            consumer.register_message_listener(None)

    def test_subscribe_topic(self) -> None:
        """测试订阅Topic"""
        consumer = ConcurrentConsumer(self.config)

        # 订阅Topic（使用模拟的选择器）
        mock_selector = MagicMock()
        consumer.subscribe("test_topic", mock_selector)

        self.assertTrue(consumer.is_subscribed("test_topic"))
        self.assertIn("test_topic", consumer.get_subscribed_topics())

    def test_subscribe_empty_topic(self) -> None:
        """测试订阅空Topic名称"""
        consumer = ConcurrentConsumer(self.config)
        mock_selector = MagicMock()

        with self.assertRaises(ValueError):
            consumer.subscribe("", mock_selector)

    def test_subscribe_none_selector(self) -> None:
        """测试订阅时使用None选择器"""
        consumer = ConcurrentConsumer(self.config)

        with self.assertRaises(ValueError):
            consumer.subscribe("test_topic", None)

    def test_unsubscribe_topic(self) -> None:
        """测试取消订阅Topic"""
        consumer = ConcurrentConsumer(self.config)
        mock_selector = MagicMock()

        # 先订阅
        consumer.subscribe("test_topic", mock_selector)
        self.assertTrue(consumer.is_subscribed("test_topic"))

        # 再取消订阅
        consumer.unsubscribe("test_topic")
        self.assertFalse(consumer.is_subscribed("test_topic"))
        self.assertNotIn("test_topic", consumer.get_subscribed_topics())

    def test_unsubscribe_empty_topic(self) -> None:
        """测试取消订阅空Topic名称"""
        consumer = ConcurrentConsumer(self.config)

        with self.assertRaises(ValueError):
            consumer.unsubscribe("")

    @patch("pyrocketmq.consumer.concurrent_consumer.NameServerManager")
    @patch("pyrocketmq.consumer.concurrent_consumer.BrokerManager")
    @patch("pyrocketmq.consumer.concurrent_consumer.OffsetStoreFactory")
    @patch("pyrocketmq.consumer.concurrent_consumer.ConsumeFromWhereManager")
    def test_start_without_listener(
        self,
        mock_consume_manager,
        mock_offset_factory,
        mock_broker_manager,
        mock_nameserver,
    ) -> None:
        """测试没有监听器时启动消费者"""
        mock_nameserver.return_value.start.return_value = None
        mock_broker_manager.return_value.start.return_value = None
        mock_offset_factory.create_offset_store.return_value.start.return_value = None
        mock_consume_manager.return_value = None

        consumer = ConcurrentConsumer(self.config)

        with self.assertRaises(ConsumerStartError):
            consumer.start()

    def test_consumer_str_representation(self) -> None:
        """测试消费者的字符串表示"""
        consumer = ConcurrentConsumer(self.config)

        str_repr = str(consumer)
        self.assertIn("ConcurrentConsumer", str_repr)
        self.assertIn(self.config.consumer_group, str_repr)
        self.assertIn("running=False", str_repr)

    def test_consumer_detailed_representation(self) -> None:
        """测试消费者的详细字符串表示"""
        consumer = ConcurrentConsumer(self.config)

        repr_str = repr(consumer)
        self.assertIn("ConcurrentConsumer", repr_str)
        self.assertIn(self.config.consumer_group, repr_str)
        self.assertIn("namesrv='localhost:9876'", repr_str)

    def test_get_status_summary(self) -> None:
        """测试获取状态摘要"""
        consumer = ConcurrentConsumer(self.config)

        summary = consumer.get_status_summary()

        self.assertIn("consumer_group", summary)
        self.assertIn("is_running", summary)
        self.assertIn("message_model", summary)
        self.assertIn("subscription_status", summary)
        self.assertEqual(summary["consumer_group"], self.config.consumer_group)
        self.assertEqual(summary["is_running"], False)
        self.assertEqual(summary["message_model"], self.config.message_model)


class TestConcurrentConsumerWithRealComponents(unittest.TestCase):
    """使用真实组件的ConcurrentConsumer测试（需要RocketMQ环境）"""

    def setUp(self) -> None:
        """测试前的设置"""
        self.config = ConsumerConfig(
            consumer_group="test_real_consumer_group",
            namesrv_addr="localhost:9876",  # 需要真实的RocketMQ环境
            message_model=MessageModel.CLUSTERING,
            consume_thread_max=3,
        )

    @unittest.skip("需要真实的RocketMQ环境")
    def test_full_consumer_lifecycle(self) -> None:
        """测试完整的消费者生命周期（需要真实环境）"""
        # 这个测试需要真实的RocketMQ环境才能运行
        # 在CI/CD环境中应该被跳过

        class TestListener(MessageListenerConcurrently):
            def __init__(self):
                self.processed_messages = []

            def consume_message(self, messages, context):
                for msg in messages:
                    self.processed_messages.append(msg.body.decode())
                return ConsumeResult.SUCCESS

        listener = TestListener()
        consumer = ConcurrentConsumer(self.config)
        consumer.register_message_listener(listener)

        # 测试启动
        consumer.start()
        self.assertTrue(consumer.is_running())

        # 测试订阅
        mock_selector = MagicMock()
        consumer.subscribe("test_topic", mock_selector)
        self.assertTrue(consumer.is_subscribed("test_topic"))

        # 等待一段时间处理消息
        import time

        time.sleep(2)

        # 测试停止
        consumer.shutdown()
        self.assertFalse(consumer.is_running())


if __name__ == "__main__":
    unittest.main()
