"""
AverageAllocateStrategy 单元测试

测试平均分配策略的各项功能，包括：
- 基本分配逻辑测试
- 边界条件测试
- 异常处理测试
- 排序一致性测试
- 日志记录测试
"""

import unittest
from unittest.mock import MagicMock, patch

from pyrocketmq.consumer.allocate_queue_strategy import (
    AllocateContext,
    AverageAllocateStrategy,
)
from pyrocketmq.model import MessageQueue


class TestAverageAllocateStrategy(unittest.TestCase):
    """AverageAllocateStrategy 单元测试"""

    def setUp(self):
        """测试前置设置"""
        self.strategy = AverageAllocateStrategy()
        self.consumer_group = "test_consumer_group"

        # 创建测试用的消息队列
        self.queue1 = MessageQueue(
            topic="test_topic", broker_name="broker_1", queue_id=0
        )
        self.queue2 = MessageQueue(
            topic="test_topic", broker_name="broker_1", queue_id=1
        )
        self.queue3 = MessageQueue(
            topic="test_topic", broker_name="broker_2", queue_id=0
        )
        self.queue4 = MessageQueue(
            topic="test_topic", broker_name="broker_2", queue_id=1
        )

    def create_context(
        self,
        consumer_id: str,
        all_consumers: list[str],
        all_queues: list[MessageQueue],
    ) -> AllocateContext:
        """创建分配上下文的便利方法"""
        return AllocateContext(
            consumer_group=self.consumer_group,
            consumer_id=consumer_id,
            current_cid_all=all_consumers,
            mq_all=all_queues,
            mq_divide={},
        )

    def test_init(self):
        """测试策略初始化"""
        self.assertEqual(self.strategy.strategy_name, "AverageAllocateStrategy")
        self.assertIsNotNone(self.strategy._logger)

    def test_basic_allocation_equal_division(self):
        """测试基本分配：队列数能被消费者数整除"""
        all_queues = [self.queue1, self.queue2, self.queue3, self.queue4]
        all_consumers = ["consumer_1", "consumer_2"]

        # 测试第一个消费者
        context1 = self.create_context("consumer_1", all_consumers, all_queues)
        result1 = self.strategy.allocate(context1)

        self.assertEqual(len(result1), 2)
        self.assertIn(self.queue1, result1)
        self.assertIn(self.queue2, result1)

        # 测试第二个消费者
        context2 = self.create_context("consumer_2", all_consumers, all_queues)
        result2 = self.strategy.allocate(context2)

        self.assertEqual(len(result2), 2)
        self.assertIn(self.queue3, result2)
        self.assertIn(self.queue4, result2)

        # 验证分配不重叠
        self.assertEqual(len(set(result1) & set(result2)), 0)

    def test_basic_allocation_unequal_division(self):
        """测试基本分配：队列数不能被消费者数整除"""
        all_queues = [self.queue1, self.queue2, self.queue3]
        all_consumers = ["consumer_1", "consumer_2"]

        # 第一个消费者应该获得更多队列（余数部分）
        context1 = self.create_context("consumer_1", all_consumers, all_queues)
        result1 = self.strategy.allocate(context1)

        self.assertEqual(len(result1), 2)

        # 第二个消费者获得较少队列
        context2 = self.create_context("consumer_2", all_consumers, all_queues)
        result2 = self.strategy.allocate(context2)

        self.assertEqual(len(result2), 1)

        # 验证所有队列都被分配
        all_allocated = result1 + result2
        self.assertEqual(len(set(all_allocated)), 3)

    def test_single_consumer_all_queues(self):
        """测试单个消费者获得所有队列"""
        all_queues = [self.queue1, self.queue2, self.queue3, self.queue4]
        all_consumers = ["consumer_1"]

        context = self.create_context("consumer_1", all_consumers, all_queues)
        result = self.strategy.allocate(context)

        self.assertEqual(len(result), 4)
        self.assertEqual(set(result), set(all_queues))

    def test_single_queue_multiple_consumers(self):
        """测试单个队列分配给多个消费者"""
        all_queues = [self.queue1]
        all_consumers = ["consumer_1", "consumer_2", "consumer_3"]

        # 只有第一个消费者应该获得队列
        context1 = self.create_context("consumer_1", all_consumers, all_queues)
        result1 = self.strategy.allocate(context1)

        self.assertEqual(len(result1), 1)
        self.assertEqual(result1[0], self.queue1)

        # 其他消费者应该获得空列表
        context2 = self.create_context("consumer_2", all_consumers, all_queues)
        result2 = self.strategy.allocate(context2)

        self.assertEqual(len(result2), 0)

        context3 = self.create_context("consumer_3", all_consumers, all_queues)
        result3 = self.strategy.allocate(context3)

        self.assertEqual(len(result3), 0)

    def test_consumer_order_independence(self):
        """测试消费者顺序不影响分配结果"""
        all_queues = [self.queue1, self.queue2, self.queue3, self.queue4]

        # 不同的消费者顺序
        consumers_order1 = ["consumer_A", "consumer_B"]
        consumers_order2 = ["consumer_B", "consumer_A"]

        # 测试第一个顺序
        context1 = self.create_context("consumer_A", consumers_order1, all_queues)
        result1 = self.strategy.allocate(context1)

        # 测试第二个顺序
        context2 = self.create_context("consumer_A", consumers_order2, all_queues)
        result2 = self.strategy.allocate(context2)

        # 同一个消费者应该获得相同的队列集合，无论消费者列表顺序如何
        self.assertEqual(set(result1), set(result2))

    def test_queue_order_independence(self):
        """测试队列顺序不影响分配结果"""
        all_queues_order1 = [self.queue1, self.queue2, self.queue3, self.queue4]
        all_queues_order2 = [self.queue3, self.queue1, self.queue4, self.queue2]
        all_consumers = ["consumer_1", "consumer_2"]

        # 测试第一种队列顺序
        context1 = self.create_context("consumer_1", all_consumers, all_queues_order1)
        result1 = self.strategy.allocate(context1)

        # 测试第二种队列顺序
        context2 = self.create_context("consumer_1", all_consumers, all_queues_order2)
        result2 = self.strategy.allocate(context2)

        # 同一个消费者应该获得相同的队列集合，无论队列列表顺序如何
        self.assertEqual(set(result1), set(result2))

    def test_large_scale_allocation(self):
        """测试大规模分配"""
        # 创建大量队列和消费者
        all_queues = [
            MessageQueue(
                topic=f"topic_{i // 10}", broker_name=f"broker_{i // 5}", queue_id=i % 5
            )
            for i in range(100)
        ]
        all_consumers = [f"consumer_{i}" for i in range(10)]

        # 测试每个消费者的分配
        total_allocated = []
        for consumer_id in all_consumers:
            context = self.create_context(consumer_id, all_consumers, all_queues)
            result = self.strategy.allocate(context)

            # 每个消费者应该获得10个队列
            self.assertEqual(len(result), 10)
            total_allocated.extend(result)

        # 验证所有队列都被分配且不重复
        self.assertEqual(len(total_allocated), 100)
        self.assertEqual(len(set(total_allocated)), 100)

    def test_validate_context_invalid_type(self):
        """测试验证无效上下文类型"""
        with self.assertRaises(ValueError) as context:
            self.strategy.allocate("invalid_context")

        self.assertIn("上下文必须是AllocateContext类型", str(context.exception))

    # 注：验证逻辑的测试已通过test_allocate_context_post_init_validation覆盖
    # 因为AllocateContext.__post_init__已经包含了这些验证逻辑

    def test_allocate_context_post_init_validation(self):
        """测试AllocateContext的初始化后验证"""
        # 测试空消费者列表
        with self.assertRaises(ValueError) as context:
            AllocateContext(
                consumer_group=self.consumer_group,
                consumer_id="consumer_1",
                current_cid_all=[],
                mq_all=[self.queue1],
                mq_divide={},
            )
        self.assertIn("消费者ID列表不能为空", str(context.exception))

        # 测试空队列列表
        with self.assertRaises(ValueError) as context:
            AllocateContext(
                consumer_group=self.consumer_group,
                consumer_id="consumer_1",
                current_cid_all=["consumer_1"],
                mq_all=[],
                mq_divide={},
            )
        self.assertIn("消息队列列表不能为空", str(context.exception))

        # 测试消费者不在列表中
        with self.assertRaises(ValueError) as context:
            AllocateContext(
                consumer_group=self.consumer_group,
                consumer_id="consumer_X",
                current_cid_all=["consumer_1", "consumer_2"],
                mq_all=[self.queue1],
                mq_divide={},
            )
        self.assertIn("当前消费者ID consumer_X 不在消费者组中", str(context.exception))

    def test_get_consumer_index_valid(self):
        """测试获取消费者索引 - 有效情况"""
        consumer_list = ["consumer_A", "consumer_B", "consumer_C"]

        index = self.strategy._get_consumer_index("consumer_B", consumer_list)
        self.assertEqual(index, 1)

    def test_get_consumer_index_invalid(self):
        """测试获取消费者索引 - 无效情况"""
        consumer_list = ["consumer_A", "consumer_B", "consumer_C"]

        with self.assertRaises(ValueError) as context:
            self.strategy._get_consumer_index("consumer_X", consumer_list)

        self.assertIn("消费者ID consumer_X 不在消费者列表中", str(context.exception))

    def test_sort_consumer_list(self):
        """测试消费者列表排序"""
        unsorted_list = ["consumer_C", "consumer_A", "consumer_B"]
        sorted_list = self.strategy._sort_consumer_list(unsorted_list)

        expected = ["consumer_A", "consumer_B", "consumer_C"]
        self.assertEqual(sorted_list, expected)

    def test_sort_queue_list(self):
        """测试队列列表排序"""
        unsorted_queues = [
            MessageQueue(topic="topic_B", broker_name="broker_2", queue_id=1),
            MessageQueue(topic="topic_A", broker_name="broker_1", queue_id=0),
            MessageQueue(topic="topic_A", broker_name="broker_1", queue_id=1),
            MessageQueue(topic="topic_B", broker_name="broker_2", queue_id=0),
        ]

        sorted_queues = self.strategy._sort_queue_list(unsorted_queues)

        # 验证排序顺序：先按topic，再按broker_name，最后按queue_id
        expected_order = [
            ("topic_A", "broker_1", 0),
            ("topic_A", "broker_1", 1),
            ("topic_B", "broker_2", 0),
            ("topic_B", "broker_2", 1),
        ]

        actual_order = [(mq.topic, mq.broker_name, mq.queue_id) for mq in sorted_queues]
        self.assertEqual(actual_order, expected_order)

    @patch("pyrocketmq.consumer.allocate_queue_strategy.get_logger")
    def test_logging(self, mock_get_logger):
        """测试日志记录功能"""
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        # 重新创建策略以使用mock logger
        strategy = AverageAllocateStrategy()

        all_queues = [self.queue1, self.queue2, self.queue3, self.queue4]
        all_consumers = ["consumer_1", "consumer_2"]

        context = self.create_context("consumer_1", all_consumers, all_queues)
        result = strategy.allocate(context)

        # 验证日志被调用
        mock_logger.info.assert_called_once()

        # 验证日志参数
        call_args = mock_logger.info.call_args
        self.assertEqual(call_args[0][0], "平均分配完成")

        # 验证extra参数包含正确的键
        extra = call_args[1]["extra"]
        self.assertIn("consumer_id", extra)
        self.assertIn("consumer_index", extra)
        self.assertIn("allocated_queue_count", extra)
        self.assertIn("queue_range_start", extra)
        self.assertIn("queue_range_end", extra)
        self.assertIn("strategy", extra)

        # 验证extra参数的值
        self.assertEqual(extra["consumer_id"], "consumer_1")
        self.assertEqual(extra["consumer_index"], 0)  # 排序后的第一个消费者
        self.assertEqual(extra["allocated_queue_count"], 2)
        self.assertEqual(extra["strategy"], "average")

    def test_multiple_consumers_different_brokers(self):
        """测试多个消费者和不同broker的队列分配"""
        all_queues = [
            MessageQueue(topic="topic_A", broker_name="broker_1", queue_id=0),
            MessageQueue(topic="topic_A", broker_name="broker_1", queue_id=1),
            MessageQueue(topic="topic_A", broker_name="broker_2", queue_id=0),
            MessageQueue(topic="topic_A", broker_name="broker_2", queue_id=1),
            MessageQueue(topic="topic_B", broker_name="broker_1", queue_id=0),
            MessageQueue(topic="topic_B", broker_name="broker_2", queue_id=0),
        ]
        all_consumers = ["consumer_1", "consumer_2", "consumer_3"]

        # 测试所有消费者的分配
        total_allocated = []
        for i, consumer_id in enumerate(all_consumers):
            context = self.create_context(consumer_id, all_consumers, all_queues)
            result = self.strategy.allocate(context)

            # 验证分配数量：6个队列分给3个消费者，每个都是2个（6//3=2, 6%3=0）
            self.assertEqual(len(result), 2)
            total_allocated.extend(result)

        # 验证所有队列都被分配且不重复
        self.assertEqual(len(set(total_allocated)), 6)

    def test_edge_case_zero_remainder(self):
        """测试边界情况：余数为0"""
        # 10个队列分给5个消费者，每个消费者2个队列
        all_queues = [
            MessageQueue(topic="test", broker_name="broker", queue_id=i)
            for i in range(10)
        ]
        all_consumers = [f"consumer_{i}" for i in range(5)]

        for consumer_id in all_consumers:
            context = self.create_context(consumer_id, all_consumers, all_queues)
            result = self.strategy.allocate(context)

            # 每个消费者应该获得2个队列
            self.assertEqual(len(result), 2)

    def test_edge_case_large_remainder(self):
        """测试边界情况：余数较大"""
        # 7个队列分给3个消费者，分配应该是 3, 2, 2
        all_queues = [
            MessageQueue(topic="test", broker_name="broker", queue_id=i)
            for i in range(7)
        ]
        all_consumers = ["consumer_1", "consumer_2", "consumer_3"]

        # 第一个消费者获得3个队列
        context1 = self.create_context("consumer_1", all_consumers, all_queues)
        result1 = self.strategy.allocate(context1)
        self.assertEqual(len(result1), 3)

        # 其他消费者获得2个队列
        context2 = self.create_context("consumer_2", all_consumers, all_queues)
        result2 = self.strategy.allocate(context2)
        self.assertEqual(len(result2), 2)

        context3 = self.create_context("consumer_3", all_consumers, all_queues)
        result3 = self.strategy.allocate(context3)
        self.assertEqual(len(result3), 2)

    def test_complex_topic_and_broker_combination(self):
        """测试复杂的topic和broker组合"""
        all_queues = [
            MessageQueue(
                topic="order_topic", broker_name="broker_shanghai", queue_id=0
            ),
            MessageQueue(
                topic="order_topic", broker_name="broker_shanghai", queue_id=1
            ),
            MessageQueue(topic="order_topic", broker_name="broker_beijing", queue_id=0),
            MessageQueue(topic="user_topic", broker_name="broker_shanghai", queue_id=0),
            MessageQueue(topic="user_topic", broker_name="broker_beijing", queue_id=0),
            MessageQueue(topic="user_topic", broker_name="broker_beijing", queue_id=1),
        ]
        all_consumers = ["order_consumer", "user_consumer"]

        # 测试分配结果
        total_allocated = []
        for consumer_id in all_consumers:
            context = self.create_context(consumer_id, all_consumers, all_queues)
            result = self.strategy.allocate(context)
            total_allocated.extend(result)

        # 验证所有队列都被分配
        self.assertEqual(len(set(total_allocated)), 6)

        # 验证负载均衡
        self.assertEqual(len(total_allocated), 6)
        # 每个消费者应该获得3个队列
        self.assertEqual(
            len(
                [
                    q
                    for q in total_allocated
                    if q
                    in self.strategy.allocate(
                        self.create_context("order_consumer", all_consumers, all_queues)
                    )
                ]
            ),
            3,
        )


if __name__ == "__main__":
    unittest.main()
