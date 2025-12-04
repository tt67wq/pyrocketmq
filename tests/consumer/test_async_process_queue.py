"""
AsyncProcessQueue单元测试

测试AsyncProcessQueue的所有核心功能，包括：
- 基本的异步添加、获取、删除操作
- 墓碑值软删除机制
- 异步线程安全性
- 缓存限制
- 异步批量操作
- 顺序消费的提交和回滚功能
- 统计信息
- 边界条件和错误处理

作者: pyrocketmq开发团队
"""

import asyncio
import unittest
from typing import Any

from pyrocketmq.consumer.async_process_queue import AsyncProcessQueue, QueueEntry
from pyrocketmq.model import MessageExt


class TestQueueEntry(unittest.TestCase):
    """QueueEntry类的单元测试"""

    def test_queue_entry_creation(self) -> None:
        """测试QueueEntry创建"""
        message = MessageExt(topic="test", body=b"test message")
        entry = QueueEntry(queue_offset=1, message=message, is_deleted=False)

        self.assertEqual(entry.queue_offset, 1)
        self.assertEqual(entry.message, message)
        self.assertFalse(entry.is_deleted)
        self.assertFalse(entry.is_tombstone)

    def test_tombstone_detection(self) -> None:
        """测试墓碑值检测"""
        message = MessageExt(topic="test", body=b"test message")

        # 测试is_deleted标记
        entry1 = QueueEntry(queue_offset=1, message=message, is_deleted=True)
        self.assertTrue(entry1.is_tombstone)

        # 测试None消息
        entry2 = QueueEntry(queue_offset=2, message=None, is_deleted=False)
        self.assertTrue(entry2.is_tombstone)

        # 测试正常消息
        entry3 = QueueEntry(queue_offset=3, message=message, is_deleted=False)
        self.assertFalse(entry3.is_tombstone)

    def test_message_offset_property(self) -> None:
        """测试message_offset属性"""
        message = MessageExt(topic="test", body=b"test")
        message.queue_offset = 5
        entry = QueueEntry(queue_offset=5, message=message, is_deleted=False)

        self.assertEqual(entry.message_offset, 5)

        # 测试墓碑值的消息偏移量
        entry_tombstone = QueueEntry(queue_offset=6, message=None, is_deleted=True)
        self.assertEqual(entry_tombstone.message_offset, -1)


class TestAsyncProcessQueue(unittest.IsolatedAsyncioTestCase):
    """AsyncProcessQueue类的异步单元测试"""

    async def asyncSetUp(self) -> None:
        """异步测试前准备"""
        self.queue = AsyncProcessQueue(max_cache_count=10, max_cache_size_mb=1)

    def create_test_message(self, offset: int, body: str = "test") -> MessageExt:
        """创建测试消息"""
        message = MessageExt(topic="test_topic", body=body.encode())
        message.queue_offset = offset
        return message

    async def test_init_default(self) -> None:
        """测试默认初始化"""
        queue = AsyncProcessQueue()
        count = await queue.get_count()
        size = await queue.get_total_size()
        empty = await queue.is_empty()

        self.assertEqual(count, 0)
        self.assertEqual(size, 0)
        self.assertTrue(empty)

    async def test_init_custom_params(self) -> None:
        """测试自定义参数初始化"""
        queue = AsyncProcessQueue(max_cache_count=100, max_cache_size_mb=2)
        stats = await queue.get_stats()

        self.assertEqual(stats["max_cache_count"], 100)
        self.assertEqual(stats["max_cache_size_mb"], 2)

    async def test_add_message_success(self) -> None:
        """测试成功添加消息"""
        message = self.create_test_message(1, "hello")

        result = await self.queue.add_message(message)

        self.assertTrue(result)
        count = await self.queue.get_count()
        size = await self.queue.get_total_size()
        self.assertEqual(count, 1)
        self.assertEqual(len(message.body), 5)
        self.assertEqual(size, 5)

    async def test_add_message_no_offset(self) -> None:
        """测试添加无偏移量消息"""
        message = MessageExt(topic="test", body=b"test")
        # 不设置queue_offset

        result = await self.queue.add_message(message)

        self.assertFalse(result)
        count = await self.queue.get_count()
        self.assertEqual(count, 0)

    async def test_add_duplicate_message(self) -> None:
        """测试添加重复消息"""
        message1 = self.create_test_message(1, "first")
        message2 = self.create_test_message(1, "second")  # 相同偏移量

        result1 = await self.queue.add_message(message1)
        result2 = await self.queue.add_message(message2)

        self.assertTrue(result1)
        self.assertFalse(result2)
        count = await self.queue.get_count()
        self.assertEqual(count, 1)

    async def test_get_message_success(self) -> None:
        """测试成功获取消息"""
        message = self.create_test_message(1, "test message")
        await self.queue.add_message(message)

        retrieved = await self.queue.get_message(1)

        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved.body, b"test message")
        self.assertEqual(retrieved.queue_offset, 1)

    async def test_get_message_not_found(self) -> None:
        """测试获取不存在的消息"""
        result = await self.queue.get_message(999)
        self.assertIsNone(result)

    async def test_contains_message(self) -> None:
        """测试消息存在性检查"""
        message = self.create_test_message(1, "test")
        await self.queue.add_message(message)

        contains = await self.queue.contains_message(1)
        not_contains = await self.queue.contains_message(999)
        self.assertTrue(contains)
        self.assertFalse(not_contains)

    async def test_remove_message_success(self) -> None:
        """测试成功删除消息"""
        message = self.create_test_message(1, "to delete")
        await self.queue.add_message(message)

        removed = await self.queue.remove_message(1)

        self.assertIsNotNone(removed)
        self.assertEqual(removed.body, b"to delete")
        count = await self.queue.get_count()
        self.assertEqual(count, 0)

        retrieved = await self.queue.get_message(1)
        self.assertIsNone(retrieved)

    async def test_remove_message_not_found(self) -> None:
        """测试删除不存在的消息"""
        result = await self.queue.remove_message(999)
        self.assertIsNone(result)

    async def test_remove_message_twice(self) -> None:
        """测试重复删除同一消息"""
        message = self.create_test_message(1, "test")
        await self.queue.add_message(message)

        first_remove = await self.queue.remove_message(1)
        second_remove = await self.queue.remove_message(1)

        self.assertIsNotNone(first_remove)
        self.assertIsNone(second_remove)

    async def test_tombstone_mechanism(self) -> None:
        """测试墓碑值软删除机制"""
        # 添加更多消息以避免墓碑值被立即清理
        for i in range(1, 8):  # 添加7条消息
            message = self.create_test_message(i, f"test_{i}")
            await self.queue.add_message(message)

        # 软删除一条消息（偏移量3）
        removed = await self.queue.remove_message(3)
        self.assertIsNotNone(removed)

        # 检查统计信息：应该有6个有效消息，1个墓碑值
        stats = await self.queue.get_stats()
        self.assertEqual(stats["count"], 6)  # 有效消息数
        self.assertEqual(stats["tombstone_count"], 1)  # 墓碑值数
        self.assertEqual(stats["total_entries"], 7)  # 总条目数

        # 检查墓碑值占比：1/7 �� 14% < 30%，所以不会被清理
        self.assertFalse(stats["cleanup_needed"])

        # 检查偏移量范围：应该只考虑有效消息
        min_offset = await self.queue.get_min_offset()
        max_offset = await self.queue.get_max_offset()
        self.assertEqual(min_offset, 1)
        self.assertEqual(max_offset, 7)

    async def test_tombstone_cleanup_threshold(self) -> None:
        """测试墓碑值清理阈值"""
        # 创建一个低阈值的队列
        queue = AsyncProcessQueue(max_cache_count=10, max_cache_size_mb=1)
        queue._cleanup_threshold = 0.2  # 20%阈值

        # 添加5条消息
        for i in range(5):
            message = self.create_test_message(i, f"message_{i}")
            await queue.add_message(message)

        # 删除2条消息，触发清理（2/5 = 40% > 20%）
        await queue.remove_message(0)
        await queue.remove_message(1)

        # 检查是否触发了清理
        stats = await queue.get_stats()
        self.assertEqual(stats["tombstone_count"], 0)  # 清理后墓碑值为0
        self.assertEqual(stats["total_entries"], 3)  # 只有3个有效条目

    async def test_offset_ranges(self) -> None:
        """测试偏移量范围跟踪"""
        # 添加多条消息
        for i in range(5):
            message = self.create_test_message(i * 10, f"message_{i}")
            await self.queue.add_message(message)

        min_offset = await self.queue.get_min_offset()
        max_offset = await self.queue.get_max_offset()
        self.assertEqual(min_offset, 0)
        self.assertEqual(max_offset, 40)

        # 删除中间的消息
        await self.queue.remove_message(20)

        # 范围应该自动更新
        min_offset = await self.queue.get_min_offset()
        max_offset = await self.queue.get_max_offset()
        self.assertEqual(min_offset, 0)
        self.assertEqual(max_offset, 40)

        # 删除最小和最大偏移量的消息
        await self.queue.remove_message(0)
        await self.queue.remove_message(40)

        # 范围应该再次更新
        min_offset = await self.queue.get_min_offset()
        max_offset = await self.queue.get_max_offset()
        self.assertEqual(min_offset, 10)
        self.assertEqual(max_offset, 30)

    async def test_clear_queue(self) -> None:
        """测试清空队列"""
        # 添加一些消息
        for i in range(5):
            message = self.create_test_message(i, f"message_{i}")
            await self.queue.add_message(message)

        # 添加一些墓碑值
        await self.queue.remove_message(1)
        await self.queue.remove_message(3)

        # 清空队列
        cleared = await self.queue.clear()

        # 验证返回的有效消息
        self.assertEqual(len(cleared), 3)  # 5条消息 - 2条已删除 = 3条有效消息
        count = await self.queue.get_count()
        self.assertEqual(count, 0)

        empty = await self.queue.is_empty()
        self.assertTrue(empty)

    async def test_cache_limits_count(self) -> None:
        """测试缓存数量限制"""
        # 创建只能容纳2条消息的队列
        queue = AsyncProcessQueue(max_cache_count=2, max_cache_size_mb=1)

        message1 = self.create_test_message(1, "first")
        message2 = self.create_test_message(2, "second")
        message3 = self.create_test_message(3, "third")

        # 前两条消息应该成功添加
        self.assertTrue(await queue.add_message(message1))
        self.assertTrue(await queue.add_message(message2))
        count = await queue.get_count()
        self.assertEqual(count, 2)

        # 第三条消息应该因为数量限制而失败
        self.assertFalse(await queue.add_message(message3))
        count = await queue.get_count()
        self.assertEqual(count, 2)

    async def test_need_flow_control(self) -> None:
        """测试流控制检查"""
        # 创建小队列
        queue = AsyncProcessQueue(max_cache_count=2, max_cache_size_mb=1)

        # 检查初始状态
        need_control = await queue.need_flow_control()
        self.assertFalse(need_control)

        # 填满队列
        await queue.add_message(self.create_test_message(1, "test1"))
        await queue.add_message(self.create_test_message(2, "test2"))

        # 检查是否需要流控制
        need_control = await queue.need_flow_control()
        self.assertTrue(need_control)

    async def test_get_stats_comprehensive(self) -> None:
        """测试全面的统计信息"""
        # 添加更多消息以避免墓碑值被立即清理
        for i in range(5):
            message = self.create_test_message(i, f"message_{i}")
            await self.queue.add_message(message)

        # 删除一条消息创建墓碑值
        await self.queue.remove_message(2)

        stats = await self.queue.get_stats()

        # 验证统计信息
        self.assertEqual(stats["count"], 4)  # 有效消息数
        self.assertEqual(stats["tombstone_count"], 1)  # 墓碑值数
        self.assertEqual(stats["total_entries"], 5)  # 总条目数
        self.assertAlmostEqual(stats["tombstone_ratio"], 0.2, places=2)
        self.assertEqual(stats["min_offset"], 0)
        self.assertEqual(stats["max_offset"], 4)
        self.assertFalse(stats["is_near_limit_count"])
        self.assertFalse(stats["is_near_limit_size"])
        self.assertFalse(stats["cleanup_needed"])  # 20% < 30% 阈值

    async def test_message_ordering(self) -> None:
        """测试消息的有序性"""
        # 乱序添加消息
        offsets = [5, 2, 8, 1, 4]
        for offset in offsets:
            message = self.create_test_message(offset, f"message_{offset}")
            await self.queue.add_message(message)

        # 验证偏移量范围正确
        min_offset = await self.queue.get_min_offset()
        max_offset = await self.queue.get_max_offset()
        self.assertEqual(min_offset, 1)
        self.assertEqual(max_offset, 8)

        # 验证所有消息都能正确获取
        for offset in offsets:
            message = await self.queue.get_message(offset)
            self.assertIsNotNone(message)
            self.assertEqual(message.body, f"message_{offset}".encode())

    async def test_empty_queue_operations(self) -> None:
        """测试空队列的各种操作"""
        # 空队列的各种操作
        empty = await self.queue.is_empty()
        count = await self.queue.get_count()
        size = await self.queue.get_total_size()
        min_offset = await self.queue.get_min_offset()
        max_offset = await self.queue.get_max_offset()
        removed = await self.queue.remove_message(1)
        retrieved = await self.queue.get_message(1)
        contains = await self.queue.contains_message(1)

        self.assertTrue(empty)
        self.assertEqual(count, 0)
        self.assertEqual(size, 0)
        self.assertIsNone(min_offset)
        self.assertIsNone(max_offset)
        self.assertIsNone(removed)
        self.assertIsNone(retrieved)
        self.assertFalse(contains)

        # 清空空队列
        cleared = await self.queue.clear()
        self.assertEqual(len(cleared), 0)

    async def test_sync_methods(self) -> None:
        """测试同步便捷方法"""
        message = self.create_test_message(1, "sync test")

        # 测试同步添加方法
        result = await self.queue.add_message_sync(message)
        self.assertTrue(result)

        # 测试同步获取方法
        retrieved = await self.queue.get_message_sync(1)
        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved.body, b"sync test")

        # 测试同步计数方法
        count = await self.queue.get_count_sync()
        self.assertEqual(count, 1)

    async def test_get_total_size_mb(self) -> None:
        """测试获取总大小（MB）"""
        # 添加消息
        await self.queue.add_message(self.create_test_message(1, "x" * 1024))  # 1KB
        await self.queue.add_message(self.create_test_message(2, "x" * 2048))  # 2KB

        size_mb = await self.queue.get_total_size_mb()
        expected_mb = (1024 + 2048) / (1024 * 1024)  # 3KB in MB
        self.assertAlmostEqual(size_mb, expected_mb, places=6)

    async def test_concurrent_operations(self) -> None:
        """测试并发操作的异步安全性"""

        async def add_messages(start_offset: int, count: int):
            """并发添加消息的任务"""
            results = []
            for i in range(count):
                offset = start_offset + i
                message = self.create_test_message(offset, f"concurrent_{offset}")
                success = await self.queue.add_message(message)
                results.append(success)
            return results

        async def remove_messages(offsets: list[int]):
            """并发删除消息的任务"""
            results = []
            for offset in offsets:
                await asyncio.sleep(0.001)  # 短暂延迟模拟实际使用
                removed = await self.queue.remove_message(offset)
                results.append(removed is not None)
            return results

        # 创建并发任务
        add_task1 = asyncio.create_task(add_messages(0, 5))
        add_task2 = asyncio.create_task(add_messages(10, 5))
        remove_task = asyncio.create_task(remove_messages([2, 7]))

        # 等待所有任务完成
        results1, results2, results3 = await asyncio.gather(
            add_task1, add_task2, remove_task
        )

        # 验证所有操作都成功完成
        self.assertEqual(len(results1), 5)
        self.assertEqual(len(results2), 5)
        self.assertEqual(len(results3), 2)

        # 验证最终状态一致性
        final_count = await self.queue.get_count()
        self.assertGreaterEqual(final_count, 0)
        self.assertLessEqual(final_count, 10)


class TestAsyncProcessQueueBatchOperations(TestAsyncProcessQueue):
    """AsyncProcessQueue批量操作测试"""

    async def test_add_batch_messages_success(self) -> None:
        """测试批量添加消息成功"""
        # 创建批量消息
        messages = [
            self.create_test_message(offset=1, body="message_1"),
            self.create_test_message(offset=2, body="message_2"),
            self.create_test_message(offset=3, body="message_3"),
            self.create_test_message(offset=4, body="message_4"),
            self.create_test_message(offset=5, body="message_5"),
        ]

        # 批量添加消息
        result = await self.queue.add_batch_messages(messages)

        # 验证成功添加所有消息
        self.assertEqual(result["success_count"], 5)
        self.assertEqual(result["failure_count"], 0)
        self.assertEqual(result["duplicate_count"], 0)
        self.assertEqual(result["total_processed"], 5)

        # 验证队列中的消息数量
        count = await self.queue.get_count()
        self.assertEqual(count, 5)

        # 验证所有消息都可以正确获取
        for i in range(1, 6):
            message = await self.queue.get_message(i)
            self.assertIsNotNone(message)
            self.assertEqual(message.body.decode(), f"message_{i}")

    async def test_add_batch_messages_with_duplicates(self) -> None:
        """测试批量添加包含重复消息"""
        # 先添加一些消息
        initial_messages = [
            self.create_test_message(offset=1, body="existing_1"),
            self.create_test_message(offset=3, body="existing_3"),
        ]
        for msg in initial_messages:
            await self.queue.add_message(msg)

        # 创建包含重复偏移量的消息
        messages = [
            self.create_test_message(offset=2, body="new_2"),
            self.create_test_message(offset=3, body="duplicate_3"),  # 重复
            self.create_test_message(offset=4, body="new_4"),
            self.create_test_message(offset=1, body="duplicate_1"),  # 重复
        ]

        # 批量添加消息
        result = await self.queue.add_batch_messages(messages)

        # 验证统计结果
        self.assertEqual(result["success_count"], 2)  # 2, 4 成功
        self.assertEqual(result["failure_count"], 2)  # 3, 1 失败
        self.assertEqual(result["duplicate_count"], 2)  # 3, 1 重复
        self.assertEqual(result["total_processed"], 4)

        # 验证队列中的消息数量
        count = await self.queue.get_count()
        self.assertEqual(count, 4)  # 原来的2个 + 新增2个

    async def test_add_batch_messages_replace_tombstones(self) -> None:
        """测试批量添加替换墓碑值"""
        # 先添加一些消息
        initial_messages = [
            self.create_test_message(offset=1, body="original_1"),
            self.create_test_message(offset=2, body="original_2"),
        ]
        for msg in initial_messages:
            await self.queue.add_message(msg)

        # 移除一些消息，创建墓碑值
        await self.queue.remove_message(2)

        # 创建包含替换墓碑值的消息
        messages = [
            self.create_test_message(offset=2, body="replacement_2"),  # 替换墓碑值
            self.create_test_message(offset=3, body="new_3"),  # 新消息
        ]

        # 批量添加消息
        result = await self.queue.add_batch_messages(messages)

        # 验证统计结果
        self.assertEqual(result["success_count"], 2)  # 都成功
        self.assertEqual(result["failure_count"], 0)
        self.assertEqual(result["duplicate_count"], 0)
        self.assertEqual(result["total_processed"], 2)

        # 验证墓碑值被正确替换
        replacement_message_2 = await self.queue.get_message(2)
        self.assertIsNotNone(replacement_message_2)
        self.assertEqual(replacement_message_2.body.decode(), "replacement_2")

        # 验证新消息被正确添加
        new_message_3 = await self.queue.get_message(3)
        self.assertIsNotNone(new_message_3)
        self.assertEqual(new_message_3.body.decode(), "new_3")

        # 验证队列总数正确
        count = await self.queue.get_count()
        self.assertEqual(count, 3)  # 1, 2(替换), 3

    async def test_remove_batch_messages_success(self) -> None:
        """测试批量移除消息成功"""
        # 添加消息
        messages = []
        for i in range(1, 6):
            msg = self.create_test_message(offset=i, body=f"message_{i}")
            messages.append(msg)
            await self.queue.add_message(msg)

        # 批量移除消息
        offsets_to_remove = [2, 4, 5]
        removed_messages = await self.queue.remove_batch_messages(offsets_to_remove)

        # 验证移除的消息数量
        self.assertEqual(len(removed_messages), 3)

        # 验证被移除的消息内容
        removed_bodies = [msg.body.decode() for msg in removed_messages]
        expected_bodies = ["message_2", "message_4", "message_5"]
        self.assertEqual(sorted(removed_bodies), sorted(expected_bodies))

        # 验证剩余消息数量
        count = await self.queue.get_count()
        self.assertEqual(count, 2)

    async def test_remove_batch_messages_partial_exist(self) -> None:
        """测试批量移除包含不存在偏移量的情况"""
        # 添加消息
        for i in range(1, 4):
            msg = self.create_test_message(offset=i, body=f"message_{i}")
            await self.queue.add_message(msg)

        # 尝试移除包含不存在偏移量的列表
        offsets_to_remove = [2, 5, 6, 10]  # 只有2存在
        removed_messages = await self.queue.remove_batch_messages(offsets_to_remove)

        # 验证只移除了存在的消息
        self.assertEqual(len(removed_messages), 1)
        self.assertEqual(removed_messages[0].body.decode(), "message_2")

        # 验证剩余消息数量
        count = await self.queue.get_count()
        self.assertEqual(count, 2)

    async def test_remove_batch_messages_already_removed(self) -> None:
        """测试批量移除包含已删除消息的情况"""
        # 添加消息
        for i in range(1, 4):
            msg = self.create_test_message(offset=i, body=f"message_{i}")
            await self.queue.add_message(msg)

        # 先移除一个消息
        await self.queue.remove_message(2)

        # 批量移除，包含已删除的消息
        offsets_to_remove = [2, 3]
        removed_messages = await self.queue.remove_batch_messages(offsets_to_remove)

        # 验证只移除了未删除的消息
        self.assertEqual(len(removed_messages), 1)
        self.assertEqual(removed_messages[0].body.decode(), "message_3")

        # 验证剩余消息数量
        count = await self.queue.get_count()
        self.assertEqual(count, 1)

    async def test_remove_batch_messages_empty_queue(self) -> None:
        """测试在空队列上批量移除消息"""
        offsets_to_remove = [1, 2, 3]
        removed_messages = await self.queue.remove_batch_messages(offsets_to_remove)

        # 验证没有移除任何消息
        self.assertEqual(len(removed_messages), 0)
        count = await self.queue.get_count()
        self.assertEqual(count, 0)

    async def test_remove_batch_messages_empty_list(self) -> None:
        """测试传入空列表进行批量移除"""
        # 添加一些消息
        for i in range(1, 4):
            msg = self.create_test_message(offset=i, body=f"message_{i}")
            await self.queue.add_message(msg)

        initial_count = await self.queue.get_count()

        # 传入空列表
        removed_messages = await self.queue.remove_batch_messages([])

        # 验证没有移除任何消息
        self.assertEqual(len(removed_messages), 0)
        final_count = await self.queue.get_count()
        self.assertEqual(final_count, initial_count)


class TestAsyncProcessQueueOrderlyConsumption(TestAsyncProcessQueue):
    """AsyncProcessQueue顺序消费功能测试"""

    async def test_take_messages_success(self) -> None:
        """测试成功取出消息"""
        # 添加消息
        messages = []
        for i in range(5):
            msg = self.create_test_message(offset=i, body=f"message_{i}")
            messages.append(msg)
            await self.queue.add_message(msg)

        # 取出3条消息
        taken_messages = await self.queue.take_messages(3)

        # 验证取出的消息数量
        self.assertEqual(len(taken_messages), 3)

        # 验证取出的消息内容（应该按offset顺序）
        for i, msg in enumerate(taken_messages):
            expected_offset = i
            self.assertEqual(msg.queue_offset, expected_offset)
            self.assertEqual(msg.body.decode(), f"message_{expected_offset}")

        # 验证队列中剩余的消息数量
        count = await self.queue.get_count()
        self.assertEqual(count, 2)

        # 验证统计信息中的待处理消息数量
        stats = await self.queue.get_stats()
        self.assertEqual(stats["consuming_count"], 3)

    async def test_take_messages_empty_queue(self) -> None:
        """测试从空队列取出消息"""
        taken_messages = await self.queue.take_messages(5)
        self.assertEqual(len(taken_messages), 0)

        # 验证统计信息
        stats = await self.queue.get_stats()
        self.assertEqual(stats["consuming_count"], 0)
        self.assertIsNone(stats["consuming_max_offset"])

    async def test_take_messages_with_tombstones(self) -> None:
        """测试从包含墓碑值的队列取出消息"""
        # 添加消息
        for i in range(5):
            msg = self.create_test_message(offset=i, body=f"message_{i}")
            await self.queue.add_message(msg)

        # 删除中间的消息创建墓碑值
        await self.queue.remove_message(2)

        # 取出所有消息（应该跳过墓碑值）
        taken_messages = await self.queue.take_messages(10)

        # 验证只取出了有效消息（4条，不包括墓碑值）
        self.assertEqual(len(taken_messages), 4)

        # 验证没有取到墓碑值对应的消息
        taken_offsets = [msg.queue_offset for msg in taken_messages]
        self.assertNotIn(2, taken_offsets)

    async def test_commit_success(self) -> None:
        """测试成功提交消息"""
        # 添加并取出消息
        messages = []
        for i in range(3):
            msg = self.create_test_message(offset=i, body=f"message_{i}")
            messages.append(msg)
            await self.queue.add_message(msg)

        taken_messages = await self.queue.take_messages(3)
        self.assertEqual(len(taken_messages), 3)

        # 提交消息
        max_offset = await self.queue.commit()

        # 验证返回的最大偏移量
        self.assertEqual(max_offset, 2)  # 最大offset是2

        # 验证待提交区被清空
        stats = await self.queue.get_stats()
        self.assertEqual(stats["consuming_count"], 0)
        self.assertIsNone(stats["consuming_max_offset"])

    async def test_commit_empty_consuming_queue(self) -> None:
        """测试提交空的待提交区"""
        max_offset = await self.queue.commit()
        self.assertEqual(max_offset, -1)

    async def test_rollback_all_messages(self) -> None:
        """测试回滚所有消息"""
        # 添加并取出消息
        messages = []
        for i in range(3):
            msg = self.create_test_message(offset=i, body=f"message_{i}")
            messages.append(msg)
            await self.queue.add_message(msg)

        taken_messages = await self.queue.take_messages(3)
        self.assertEqual(len(taken_messages), 3)

        count = await self.queue.get_count()
        self.assertEqual(count, 0)

        # 回滚所有消息
        rollback_count = await self.queue.rollback()
        self.assertEqual(rollback_count, 3)

        count = await self.queue.get_count()
        self.assertEqual(count, 3)

        # 验证消息重新回到队列
        remaining_messages = await self.queue.take_messages(10)
        self.assertEqual(len(remaining_messages), 3)

    async def test_rollback_specific_messages(self) -> None:
        """测试回滚指定的消息"""
        # 添加并取出消息
        messages = []
        for i in range(5):
            msg = self.create_test_message(offset=i, body=f"message_{i}")
            messages.append(msg)
            await self.queue.add_message(msg)

        taken_messages = await self.queue.take_messages(5)
        self.assertEqual(len(taken_messages), 5)

        count = await self.queue.get_count()
        self.assertEqual(count, 0)

        # 只回滚offset为1, 3的消息
        rollback_msgs = [messages[1], messages[3]]  # offset 1, 3
        rollback_count = await self.queue.rollback(rollback_msgs)
        self.assertEqual(rollback_count, 2)

        count = await self.queue.get_count()
        self.assertEqual(count, 2)

        # 验证只有指定的消息回到队列
        remaining_messages = await self.queue.take_messages(10)
        self.assertEqual(len(remaining_messages), 2)

        # 检查回滚的消息offset
        offsets = [msg.queue_offset for msg in remaining_messages]
        self.assertIn(1, offsets)
        self.assertIn(3, offsets)
        self.assertNotIn(0, offsets)
        self.assertNotIn(2, offsets)
        self.assertNotIn(4, offsets)

    async def test_rollback_empty_list(self) -> None:
        """测试回滚空列表"""
        # 添加并取出消息
        msg = self.create_test_message(offset=1, body="test")
        await self.queue.add_message(msg)

        taken_messages = await self.queue.take_messages(1)
        self.assertEqual(len(taken_messages), 1)

        # 回滚空列表
        rollback_count = await self.queue.rollback([])
        self.assertEqual(rollback_count, 0)

        count = await self.queue.get_count()
        self.assertEqual(count, 0)

    async def test_rollback_without_messages_in_queue(self) -> None:
        """测试在没有任何消息的情况下回滚"""
        # 不调用take_messages，直接尝试回滚
        rollback_count = await self.queue.rollback()
        self.assertEqual(rollback_count, 0)

        rollback_count = await self.queue.rollback([])
        self.assertEqual(rollback_count, 0)

        # 尝试回滚不存在的消息
        msg = self.create_test_message(offset=1, body="test")
        rollback_count = await self.queue.rollback([msg])
        self.assertEqual(rollback_count, 0)

    async def test_multiple_take_and_rollback_cycle(self) -> None:
        """测试多次取出和回滚的循环"""
        # 第一轮：添加3条消息
        for i in range(3):
            msg = self.create_test_message(offset=i, body=f"round1_{i}")
            await self.queue.add_message(msg)

        # 取出2条消息
        taken1 = await self.queue.take_messages(2)
        self.assertEqual(len(taken1), 2)

        # 添加新消息
        for i in range(3, 5):
            msg = self.create_test_message(offset=i, body=f"round2_{i}")
            await self.queue.add_message(msg)

        # 再取出2条消息
        taken2 = await self.queue.take_messages(2)
        self.assertEqual(len(taken2), 2)

        # 验证待提交区有4条消息
        stats = await self.queue.get_stats()
        self.assertEqual(stats["consuming_count"], 4)

        # 回滚所有消息
        rollback_count = await self.queue.rollback()
        self.assertEqual(rollback_count, 4)

        # 验证所有消息回到队列：回滚的4条消息 + 队列中剩余的1条(offset=4) = 5条
        count = await self.queue.get_count()
        self.assertEqual(count, 5)

        # 验证消息仍然可以正确取出
        final_taken = await self.queue.take_messages(10)
        self.assertEqual(len(final_taken), 5)

        # 验证消息顺序正确
        offsets = [msg.queue_offset for msg in final_taken]
        expected_offsets = [0, 1, 2, 3, 4]  # 所有5条消息应该都存在
        self.assertEqual(offsets, expected_offsets)


class TestAsyncProcessQueueEdgeCases(TestAsyncProcessQueue):
    """AsyncProcessQueue边界情况测试"""

    async def test_message_with_empty_body(self) -> None:
        """测试空消息体"""
        message = MessageExt(topic="test", body=b"")
        message.queue_offset = 1

        result = await self.queue.add_message(message)
        self.assertTrue(result)

        size = await self.queue.get_total_size()
        self.assertEqual(size, 0)

    async def test_message_with_none_body(self) -> None:
        """测试None消息体"""
        message = MessageExt(topic="test", body=None)
        message.queue_offset = 1

        result = await self.queue.add_message(message)
        self.assertTrue(result)

        size = await self.queue.get_total_size()
        self.assertEqual(size, 0)

    async def test_negative_offsets(self) -> None:
        """测试负偏移量"""
        message = MessageExt(topic="test", body=b"test")
        message.queue_offset = -1

        result = await self.queue.add_message(message)
        self.assertTrue(result)

        min_offset = await self.queue.get_min_offset()
        self.assertEqual(min_offset, -1)

    async def test_zero_offset(self) -> None:
        """测试零偏移量"""
        message = MessageExt(topic="test", body=b"test")
        message.queue_offset = 0

        result = await self.queue.add_message(message)
        self.assertTrue(result)

        min_offset = await self.queue.get_min_offset()
        self.assertEqual(min_offset, 0)

    async def test_large_offset_values(self) -> None:
        """测试大偏移量值"""
        large_offset = 2**31 - 1  # int32最大值
        message = MessageExt(topic="test", body=b"test")
        message.queue_offset = large_offset

        result = await self.queue.add_message(message)
        self.assertTrue(result)

        max_offset = await self.queue.get_max_offset()
        self.assertEqual(max_offset, large_offset)

    async def test_take_messages_zero_batch_size(self) -> None:
        """测试批量大小为0的取出操作"""
        # 添加一些消息
        for i in range(3):
            msg = self.create_test_message(offset=i, body=f"message_{i}")
            await self.queue.add_message(msg)

        # 尝试取出0条消息
        taken_messages = await self.queue.take_messages(0)
        self.assertEqual(len(taken_messages), 0)

    async def test_take_messages_negative_batch_size(self) -> None:
        """测试负数批量大小的取出操作"""
        # 添加一些消息
        for i in range(3):
            msg = self.create_test_message(offset=i, body=f"message_{i}")
            await self.queue.add_message(msg)

        # 尝试取出负数条消息
        taken_messages = await self.queue.take_messages(-1)
        self.assertEqual(len(taken_messages), 0)

    async def test_large_number_of_operations(self) -> None:
        """测试大量操作的性能"""
        count = 50  # 减少数量，避免超过缓存限制

        # 重新创建一个更大的队列
        large_queue = AsyncProcessQueue(max_cache_count=100, max_cache_size_mb=10)

        # 添加大量消息
        for i in range(count):
            message = self.create_test_message(i, f"message_{i}")
            result = await large_queue.add_message(message)
            self.assertTrue(result)

        final_count = await large_queue.get_count()
        self.assertEqual(final_count, count)

        # 删除一些消息
        deleted_count = 0
        for i in range(0, count, 10):  # 删除每隔10个的消息
            removed = await large_queue.remove_message(i)
            if removed:
                deleted_count += 1

        expected_count = count - deleted_count
        final_count = await large_queue.get_count()
        self.assertEqual(final_count, expected_count)


if __name__ == "__main__":
    unittest.main()
