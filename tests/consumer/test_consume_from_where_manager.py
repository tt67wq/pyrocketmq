"""
ConsumeFromWhereManager 单元测试

测试消费起始位置管理器的各项功能，包括：
- 不同策略的偏移量获取
- 错误处理
- 策略验证
- 便利功能测试
"""

import time
import unittest
from unittest.mock import MagicMock, patch

from pyrocketmq.broker import BrokerManager
from pyrocketmq.consumer.consume_from_where_manager import ConsumeFromWhereManager
from pyrocketmq.model import MessageQueue
from pyrocketmq.model.consumer import ConsumeFromWhere


class TestConsumeFromWhereManager(unittest.TestCase):
    """ConsumeFromWhereManager 单元测试"""

    def setUp(self):
        """测试前置设置"""
        # 创建Mock BrokerManager和BrokerClient
        self.mock_broker_manager = MagicMock(spec=BrokerManager)
        self.mock_broker_client = MagicMock()

        # 设置broker_manager.connection返回mock broker client
        self.mock_broker_manager.connection.return_value.__enter__.return_value = (
            self.mock_broker_client
        )
        self.mock_broker_manager.connection.return_value.__exit__.return_value = None

        self.manager = ConsumeFromWhereManager(
            "localhost:9876", self.mock_broker_manager
        )

        # 创建测试用的消息队列
        self.test_queue = MessageQueue(
            topic="test_topic", broker_name="test_broker", queue_id=0
        )

        # 测试时间戳
        self.test_timestamp = int(time.time() * 1000)

    def test_init(self):
        """测试初始化"""
        self.assertEqual(self.manager.namesrv, "localhost:9876")
        self.assertEqual(self.manager.broker_manager, self.mock_broker_manager)
        self.assertIsNotNone(self.manager._logger)

    def test_get_consume_offset_from_last_offset(self):
        """测试从最后偏移量开始消费"""
        # Mock BrokerClient返回值
        self.mock_broker_client.get_max_offset.return_value = 100

        result = self.manager.get_consume_offset(
            self.test_queue, ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET
        )

        self.assertEqual(result, 100)
        self.mock_broker_manager.connection.assert_called_once_with("test_broker")
        self.mock_broker_client.get_max_offset.assert_called_once_with(
            topic="test_topic", queue_id=0
        )

    def test_get_consume_offset_from_first_offset(self):
        """测试从第一个偏移量开始消费"""
        result = self.manager.get_consume_offset(
            self.test_queue, ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET
        )

        # 最小偏移量应该返回0（当前实现）
        self.assertEqual(result, 0)

    def test_get_consume_offset_from_timestamp(self):
        """测试从指定时间戳开始消费"""
        # Mock BrokerClient返回值
        self.mock_broker_client.search_offset_by_timestamp.return_value = 50

        result = self.manager.get_consume_offset(
            self.test_queue,
            ConsumeFromWhere.CONSUME_FROM_TIMESTAMP,
            timestamp=self.test_timestamp,
        )

        self.assertEqual(result, 50)
        self.mock_broker_manager.connection.assert_called_once_with("test_broker")
        self.mock_broker_client.search_offset_by_timestamp.assert_called_once_with(
            topic="test_topic", queue_id=0, timestamp=self.test_timestamp
        )

    def test_get_consume_offset_from_timestamp_with_default_time(self):
        """测试从当前时间戳开始消费（不指定时间戳）"""
        # Mock BrokerClient返回值
        self.mock_broker_client.search_offset_by_timestamp.return_value = 75

        result = self.manager.get_consume_offset(
            self.test_queue, ConsumeFromWhere.CONSUME_FROM_TIMESTAMP
        )

        self.assertEqual(result, 75)

        # 验证调用时使用的时间戳是当前时间（允许1秒误差）
        call_args = self.mock_broker_client.search_offset_by_timestamp.call_args
        actual_timestamp = call_args[1]["timestamp"]
        current_timestamp = int(time.time() * 1000)
        self.assertAlmostEqual(actual_timestamp, current_timestamp, delta=1000)

    def test_get_consume_offset_invalid_strategy(self):
        """测试无效策略"""
        with self.assertRaises(RuntimeError) as context:
            self.manager.get_consume_offset(self.test_queue, "INVALID_STRATEGY")

        self.assertIn("获取消费起始偏移量失败", str(context.exception))
        self.assertIn("不支持的消费起始位置策略", str(context.exception))

    def test_get_max_offset_broker_error(self):
        """测试获取最大偏移量时Broker出错"""
        # Mock BrokerClient抛出异常
        self.mock_broker_client.get_max_offset.side_effect = Exception(
            "Broker connection failed"
        )

        with self.assertRaises(RuntimeError) as context:
            self.manager.get_consume_offset(
                self.test_queue, ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET
            )

        self.assertIn("获取消费起始偏移量失败", str(context.exception))
        self.assertIn("Broker connection failed", str(context.exception))

    def test_search_offset_by_timestamp_broker_error(self):
        """测试根据时间戳获取偏移量时Broker出错"""
        # Mock BrokerClient抛出异常
        self.mock_broker_client.search_offset_by_timestamp.side_effect = Exception(
            "Timeout"
        )

        with self.assertRaises(RuntimeError) as context:
            self.manager.get_consume_offset(
                self.test_queue,
                ConsumeFromWhere.CONSUME_FROM_TIMESTAMP,
                timestamp=self.test_timestamp,
            )

        self.assertIn("获取消费起始偏移量失败", str(context.exception))
        self.assertIn("Timeout", str(context.exception))

    def test_validate_strategy_valid(self):
        """测试有效策略验证"""
        valid_strategies = [
            ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET,
            ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET,
            ConsumeFromWhere.CONSUME_FROM_TIMESTAMP,
        ]

        for strategy in valid_strategies:
            self.assertTrue(self.manager.validate_strategy(strategy))

    def test_validate_strategy_invalid(self):
        """测试无效策略验证"""
        invalid_strategy = "INVALID_STRATEGY"
        self.assertFalse(self.manager.validate_strategy(invalid_strategy))

    def test_get_strategy_info(self):
        """测试获取策略信息"""
        # 测试CONSUME_FROM_LAST_OFFSET
        info = self.manager.get_strategy_info(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET)

        self.assertEqual(info["strategy"], ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET)
        self.assertEqual(info["name"], "CONSUME_FROM_LAST_OFFSET")
        self.assertIn("description", info)
        self.assertIn("use_case", info)
        self.assertTrue(info["is_valid"])

        # 测试无效策略
        info = self.manager.get_strategy_info("INVALID_STRATEGY")
        self.assertFalse(info["is_valid"])

    def test_get_all_supported_strategies(self):
        """测试获取所有支持的策略"""
        strategies = self.manager.get_all_supported_strategies()

        self.assertEqual(len(strategies), 3)

        strategy_names = [s["name"] for s in strategies]
        self.assertIn("CONSUME_FROM_LAST_OFFSET", strategy_names)
        self.assertIn("CONSUME_FROM_FIRST_OFFSET", strategy_names)
        self.assertIn("CONSUME_FROM_TIMESTAMP", strategy_names)

        # 验证所有策略都是有效的
        for strategy_info in strategies:
            self.assertTrue(strategy_info["is_valid"])

    def test_reset_offset_to_position_first(self):
        """测试重置偏移量到第一个位置"""
        result = self.manager.reset_offset_to_position(self.test_queue, "first")

        self.assertEqual(result, 0)

    def test_reset_offset_to_position_last(self):
        """测试重置偏移量到最后一个位置"""
        # Mock BrokerClient返回值
        self.mock_broker_client.get_max_offset.return_value = 200

        result = self.manager.reset_offset_to_position(self.test_queue, "last")

        self.assertEqual(result, 200)
        self.mock_broker_manager.connection.assert_called_once_with("test_broker")
        self.mock_broker_client.get_max_offset.assert_called_once_with(
            topic="test_topic", queue_id=0
        )

    def test_reset_offset_to_position_timestamp(self):
        """测试重置偏移量到指定时间戳"""
        # Mock BrokerClient返回值
        self.mock_broker_client.search_offset_by_timestamp.return_value = 150

        result = self.manager.reset_offset_to_position(
            self.test_queue, "timestamp", timestamp=self.test_timestamp
        )

        self.assertEqual(result, 150)
        self.mock_broker_manager.connection.assert_called_once_with("test_broker")
        self.mock_broker_client.search_offset_by_timestamp.assert_called_once_with(
            topic="test_topic", queue_id=0, timestamp=self.test_timestamp
        )

    def test_reset_offset_to_position_invalid(self):
        """测试重置偏移量到无效位置"""
        with self.assertRaises(ValueError) as context:
            self.manager.reset_offset_to_position(self.test_queue, "invalid_position")

        self.assertIn("不支持的位置字符串", str(context.exception))
        self.assertIn("invalid_position", str(context.exception))

    def test_logging_success(self):
        """测试成功操作的日志记录"""
        # Mock BrokerClient返回值
        self.mock_broker_client.get_max_offset.return_value = 300

        with patch.object(self.manager._logger, "info") as mock_logger:
            result = self.manager.get_consume_offset(
                self.test_queue, ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET
            )

            # 验证日志被调用
            mock_logger.assert_called_once()

            # 验证日志参数
            call_args = mock_logger.call_args
            self.assertEqual(call_args[0][0], "使用CONSUME_FROM_LAST_OFFSET策略")

            extra = call_args[1]["extra"]
            self.assertEqual(extra["queue"], self.test_queue)
            self.assertEqual(
                extra["strategy"], ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET
            )
            self.assertEqual(extra["offset"], 300)

    def test_logging_error(self):
        """测试错误操作的日志记录"""
        # Mock BrokerClient抛出异常
        self.mock_broker_client.get_max_offset.side_effect = Exception("Test error")

        with patch.object(self.manager._logger, "error") as mock_logger:
            with self.assertRaises(RuntimeError):
                self.manager.get_consume_offset(
                    self.test_queue, ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET
                )

            # 验证错误日志被调用（应该调用2次：一次在_get_max_offset，一次在get_consume_offset）
            self.assertEqual(mock_logger.call_count, 2)

            # 验证最后一次调用是get_consume_offset中的错误日志
            last_call_args = mock_logger.call_args
            self.assertEqual(last_call_args[0][0], "获取消费起始偏移量失败")

            extra = last_call_args[1]["extra"]
            self.assertEqual(extra["queue"], self.test_queue)
            self.assertEqual(
                extra["strategy"], ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET
            )
            self.assertIn("获取最大偏移量失败: Test error", extra["error"])

    def test_get_min_offset_logging(self):
        """测试获取最小偏移量的日志记录"""
        with patch.object(self.manager._logger, "info") as mock_logger:
            result = self.manager.get_consume_offset(
                self.test_queue, ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET
            )

            # 验证日志被调用
            self.manager._logger.info.assert_called_with(
                "使用CONSUME_FROM_FIRST_OFFSET策略",
                extra={
                    "queue": self.test_queue,
                    "strategy": ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET,
                    "offset": 0,
                },
            )

    def test_different_queue_parameters(self):
        """测试不同队列参数"""
        # 创建不同的队列
        queue1 = MessageQueue(topic="topic_a", broker_name="broker_1", queue_id=0)
        queue2 = MessageQueue(topic="topic_a", broker_name="broker_1", queue_id=1)
        queue3 = MessageQueue(topic="topic_b", broker_name="broker_2", queue_id=0)

        # Mock不同队列的不同返回值
        self.mock_broker_client.get_max_offset.side_effect = [100, 200, 300]

        # 测试队列1
        result1 = self.manager.get_consume_offset(
            queue1, ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET
        )
        self.assertEqual(result1, 100)

        # 测试队列2
        result2 = self.manager.get_consume_offset(
            queue2, ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET
        )
        self.assertEqual(result2, 200)

        # 测试队列3
        result3 = self.manager.get_consume_offset(
            queue3, ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET
        )
        self.assertEqual(result3, 300)

        # 验证调用参数
        connection_calls = self.mock_broker_manager.connection.call_args_list
        self.assertEqual(connection_calls[0][0][0], "broker_1")
        self.assertEqual(connection_calls[1][0][0], "broker_1")
        self.assertEqual(connection_calls[2][0][0], "broker_2")

        # 验证get_max_offset调用
        offset_calls = self.mock_broker_client.get_max_offset.call_args_list
        self.assertEqual(offset_calls[0][1]["topic"], "topic_a")
        self.assertEqual(offset_calls[0][1]["queue_id"], 0)
        self.assertEqual(offset_calls[1][1]["topic"], "topic_a")
        self.assertEqual(offset_calls[1][1]["queue_id"], 1)
        self.assertEqual(offset_calls[2][1]["topic"], "topic_b")
        self.assertEqual(offset_calls[2][1]["queue_id"], 0)

    def test_edge_case_zero_timestamp(self):
        """测试时间戳为0的边界情况"""
        # Mock BrokerClient返回值
        expected_timestamp = int(time.time() * 1000)
        self.mock_broker_client.search_offset_by_timestamp.return_value = 42

        result = self.manager.get_consume_offset(
            self.test_queue, ConsumeFromWhere.CONSUME_FROM_TIMESTAMP, timestamp=0
        )

        self.assertEqual(result, 42)

        # 验证使用的是当前时间戳而不是0
        call_args = self.mock_broker_client.search_offset_by_timestamp.call_args
        actual_timestamp = call_args[1]["timestamp"]
        # 允许2秒误差
        self.assertGreater(actual_timestamp, expected_timestamp - 2000)
        self.assertLess(actual_timestamp, expected_timestamp + 2000)

    def test_broker_connection_context_manager(self):
        """测试Broker连接的上下文管理器使用"""
        # Mock BrokerClient返回值
        self.mock_broker_client.get_max_offset.return_value = 999

        result = self.manager.get_consume_offset(
            self.test_queue, ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET
        )

        # 验证connection被正确调用
        self.mock_broker_manager.connection.assert_called_once_with("test_broker")

        # 验证上下文管理器的__enter__和__exit__被调用
        connection_mock = self.mock_broker_manager.connection.return_value
        connection_mock.__enter__.assert_called_once()
        connection_mock.__exit__.assert_called_once()

        self.assertEqual(result, 999)


if __name__ == "__main__":
    unittest.main()
