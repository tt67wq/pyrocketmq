"""
AsyncLocalOffsetStore MVP测试

测试核心API功能，避免复杂的持久化操作导致的死锁问题。
"""

import asyncio
import os
import tempfile
import unittest
from pathlib import Path

from pyrocketmq.consumer.async_local_offset_store import AsyncLocalOffsetStore
from pyrocketmq.consumer.offset_store import ReadOffsetType
from pyrocketmq.model import MessageQueue


class TestAsyncLocalOffsetStoreMVP(unittest.IsolatedAsyncioTestCase):
    """AsyncLocalOffsetStore MVP测试类"""

    async def asyncSetUp(self) -> None:
        """异步测试前准备"""
        # 创建临时目录用于测试
        self.temp_dir = tempfile.mkdtemp()
        self.consumer_group = "test_consumer_group_async_mvp"

        # 创建AsyncLocalOffsetStore实例，关闭自动持久化
        self.offset_store = AsyncLocalOffsetStore(
            consumer_group=self.consumer_group,
            store_path=self.temp_dir,
            persist_interval=float("inf"),  # 关闭自动持久化
        )

        # 创建测试用的MessageQueue
        self.test_queue1 = MessageQueue(
            topic="test_topic_1", broker_name="broker_1", queue_id=0
        )
        self.test_queue2 = MessageQueue(
            topic="test_topic_2", broker_name="broker_2", queue_id=1
        )

    async def asyncTearDown(self) -> None:
        """异步测试后清理"""
        # 停止offset_store
        try:
            await self.offset_store.stop()
        except Exception:
            pass  # 可能已经停止

        # 清理临时目录
        import shutil

        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir, ignore_errors=True)

    async def test_init(self):
        """测试初始化"""
        offset_store = AsyncLocalOffsetStore(
            consumer_group="test_group", store_path="/tmp/test"
        )

        self.assertEqual(offset_store.consumer_group, "test_group")
        self.assertEqual(str(offset_store.store_path), "/tmp/test")
        self.assertEqual(offset_store.persist_interval, 5.0)  # 默认值
        self.assertFalse(offset_store._started)
        self.assertIsNone(offset_store._persist_task)

    async def test_start_and_stop(self):
        """测试启动和停止"""
        # 测试启动
        await self.offset_store.start()
        self.assertTrue(self.offset_store._started)
        self.assertIsNotNone(self.offset_store._persist_task)

        # 测试停止
        await self.offset_store.stop()
        self.assertFalse(self.offset_store._started)

    async def test_update_and_read_offset(self):
        """测试更新和读取偏移量（内存操作）"""
        await self.offset_store.start()

        # 更新偏移量到内存
        await self.offset_store.update_offset(self.test_queue1, 100)

        # 从内存读取偏移量
        offset = await self.offset_store.read_offset(
            self.test_queue1, ReadOffsetType.READ_FROM_MEMORY
        )
        self.assertEqual(offset, 100)

        # 从内存优先读取偏移量
        offset = await self.offset_store.read_offset(
            self.test_queue1, ReadOffsetType.MEMORY_FIRST_THEN_STORE
        )
        self.assertEqual(offset, 100)

    async def test_read_offset_not_exist(self):
        """测试读取不存在的偏移量"""
        await self.offset_store.start()

        # 读取不存在的偏移量
        offset = await self.offset_store.read_offset(
            self.test_queue1, ReadOffsetType.READ_FROM_MEMORY
        )
        self.assertEqual(offset, -1)

        offset = await self.offset_store.read_offset(
            self.test_queue1, ReadOffsetType.MEMORY_FIRST_THEN_STORE
        )
        self.assertEqual(offset, -1)

    async def test_update_multiple_offsets(self):
        """测试更新多个偏移量"""
        await self.offset_store.start()

        # 更新多个偏移量到内存
        await self.offset_store.update_offset(self.test_queue1, 100)
        await self.offset_store.update_offset(self.test_queue2, 200)

        # 验证内存中的数据
        offset1 = await self.offset_store.read_offset(
            self.test_queue1, ReadOffsetType.READ_FROM_MEMORY
        )
        offset2 = await self.offset_store.read_offset(
            self.test_queue2, ReadOffsetType.READ_FROM_MEMORY
        )

        self.assertEqual(offset1, 100)
        self.assertEqual(offset2, 200)

    async def test_remove_offset(self):
        """测试移除偏移量"""
        await self.offset_store.start()

        # 先更新一个偏移量到内存
        await self.offset_store.update_offset(self.test_queue1, 100)
        offset = await self.offset_store.read_offset(
            self.test_queue1, ReadOffsetType.READ_FROM_MEMORY
        )
        self.assertEqual(offset, 100)

        # 移除偏移量
        await self.offset_store.remove_offset(self.test_queue1)

        # 验证已从内存中移除
        offset = await self.offset_store.read_offset(
            self.test_queue1, ReadOffsetType.READ_FROM_MEMORY
        )
        self.assertEqual(offset, -1)

    async def test_get_metrics(self):
        """测试获取指标"""
        await self.offset_store.start()

        # 添加一些偏移量数据
        await self.offset_store.update_offset(self.test_queue1, 100)
        await self.offset_store.update_offset(self.test_queue2, 200)

        # 获取指标
        metrics = await self.offset_store.get_metrics()

        self.assertIsInstance(metrics, dict)
        self.assertIn("queue_count", metrics)
        self.assertEqual(metrics["queue_count"], 2)

    async def test_concurrent_memory_updates(self):
        """测试并发内存更新（不涉及文件IO）"""
        await self.offset_store.start()

        async def update_offset(queue, base_offset):
            """异步更新偏移量的任务"""
            for i in range(10):
                await self.offset_store.update_offset(queue, base_offset + i)
                await asyncio.sleep(0.001)  # 短暂延迟

        # 并发更新不同的队列
        tasks = [
            asyncio.create_task(update_offset(self.test_queue1, 1000)),
            asyncio.create_task(update_offset(self.test_queue2, 2000)),
        ]

        # 等待所有任务完成
        await asyncio.gather(*tasks)

        # 验证最终结果
        offset1 = await self.offset_store.read_offset(
            self.test_queue1, ReadOffsetType.READ_FROM_MEMORY
        )
        offset2 = await self.offset_store.read_offset(
            self.test_queue2, ReadOffsetType.READ_FROM_MEMORY
        )

        self.assertEqual(offset1, 1009)  # 1000 + 9
        self.assertEqual(offset2, 2009)  # 2000 + 9

    async def test_clear_cached_offsets(self):
        """测试清理缓存偏移量"""
        await self.offset_store.start()

        # 添加一些偏移量
        await self.offset_store.update_offset(self.test_queue1, 100)
        await self.offset_store.update_offset(self.test_queue2, 200)

        # 验证偏移量存在
        offset1 = await self.offset_store.read_offset(
            self.test_queue1, ReadOffsetType.READ_FROM_MEMORY
        )
        self.assertEqual(offset1, 100)

        # 清理缓存
        await self.offset_store.clear_cached_offsets()

        # 验证偏移量已清理
        offset1 = await self.offset_store.read_offset(
            self.test_queue1, ReadOffsetType.READ_FROM_MEMORY
        )
        self.assertEqual(offset1, -1)

    async def test_clone_offset_table(self):
        """测试克隆偏移量表"""
        await self.offset_store.start()

        # 添加一些偏移量
        await self.offset_store.update_offset(self.test_queue1, 100)
        await self.offset_store.update_offset(self.test_queue2, 200)

        # 克隆偏移量表
        cloned_table = await self.offset_store.clone_offset_table()

        # 验证克隆结果
        self.assertIsInstance(cloned_table, dict)
        self.assertEqual(len(cloned_table), 2)
        self.assertIn(self.test_queue1, cloned_table)
        self.assertIn(self.test_queue2, cloned_table)
        self.assertEqual(cloned_table[self.test_queue1], 100)
        self.assertEqual(cloned_table[self.test_queue2], 200)


if __name__ == "__main__":
    unittest.main()
