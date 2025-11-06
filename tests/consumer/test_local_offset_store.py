"""
LocalOffsetStore 单元测试

测试广播模式下的本地偏移量存储功能，包括：
- 偏移量的加载、更新、持久化
- 文件读写操作的正确性
- 线程安全性
- 指标收集功能
- 异常处理
"""

import json
import os
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from pyrocketmq.consumer.local_offset_store import LocalOffsetStore
from pyrocketmq.consumer.offset_store import OffsetEntry, ReadOffsetType
from pyrocketmq.model import MessageQueue


class TestLocalOffsetStore(unittest.TestCase):
    """LocalOffsetStore 单元测试"""

    def setUp(self):
        """测试前置设置"""
        self.temp_dir = tempfile.mkdtemp()
        self.consumer_group = "test_consumer_group"
        self.store_path = os.path.join(self.temp_dir, ".rocketmq", "offsets")

        # 创建测试用的消息队列
        self.queue1 = MessageQueue(
            topic="test_topic_1", broker_name="broker_1", queue_id=0
        )
        self.queue2 = MessageQueue(
            topic="test_topic_2", broker_name="broker_1", queue_id=1
        )
        self.queue3 = MessageQueue(
            topic="test_topic_1", broker_name="broker_2", queue_id=0
        )

    def tearDown(self):
        """测试后置清理"""
        # 清理临时文件
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_init(self):
        """测试初始化"""
        store = LocalOffsetStore(
            consumer_group=self.consumer_group, store_path=self.store_path
        )

        self.assertEqual(store.consumer_group, self.consumer_group)
        self.assertEqual(store.persist_interval, 5000)
        self.assertEqual(store.persist_batch_size, 10)
        self.assertFalse(store._running)
        self.assertIsNone(store._persist_thread)

        # 检查目录是否创建
        self.assertTrue(store.store_path.exists())

    def test_init_with_custom_params(self):
        """测试自定义参数初始化"""
        store = LocalOffsetStore(
            consumer_group=self.consumer_group,
            store_path=self.store_path,
            persist_interval=2000,
            persist_batch_size=5,
        )

        self.assertEqual(store.persist_interval, 2000)
        self.assertEqual(store.persist_batch_size, 5)

    def test_start_stop(self):
        """测试启动和停止"""
        store = LocalOffsetStore(
            consumer_group=self.consumer_group, store_path=self.store_path
        )

        # 启动
        store.start()
        self.assertTrue(store._running)
        self.assertIsNotNone(store._persist_thread)
        self.assertTrue(store._persist_thread.is_alive())

        # 重复启动应该不会创建新线程
        original_thread = store._persist_thread
        store.start()
        self.assertEqual(store._persist_thread, original_thread)

        # 停止
        store.stop()
        self.assertFalse(store._running)

        # 等待线程结束
        if store._persist_thread:
            store._persist_thread.join(timeout=2)
            self.assertFalse(store._persist_thread.is_alive())

    def test_load_empty_file(self):
        """测试加载空文件"""
        store = LocalOffsetStore(
            consumer_group=self.consumer_group, store_path=self.store_path
        )

        # 加载不存在的文件
        store.load()

        self.assertEqual(len(store.offset_table), 0)
        self.assertEqual(store.metrics.load_success_count, 1)
        self.assertEqual(store.metrics.load_failure_count, 0)

    def test_load_valid_file(self):
        """测试加载有效文件"""
        # 预先创建偏移量文件
        offset_file_path = Path(self.store_path) / f"{self.consumer_group}.offsets.json"
        offset_file_path.parent.mkdir(parents=True, exist_ok=True)

        test_data = {
            "consumer_group": self.consumer_group,
            "offsets": [
                {
                    "topic": "test_topic_1",
                    "broker_name": "broker_1",
                    "queue_id": 0,
                    "offset": 100,
                    "last_update_timestamp": 1234567890000,
                },
                {
                    "topic": "test_topic_2",
                    "broker_name": "broker_1",
                    "queue_id": 1,
                    "offset": 200,
                    "last_update_timestamp": 1234567890000,
                },
            ],
            "last_update_time": 1234567890000,
            "version": "1.0",
        }

        with open(offset_file_path, "w", encoding="utf-8") as f:
            json.dump(test_data, f)

        # 加载文件
        store = LocalOffsetStore(
            consumer_group=self.consumer_group, store_path=self.store_path
        )
        store.load()

        # 验证加载结果
        self.assertEqual(len(store.offset_table), 2)
        self.assertEqual(store.offset_table[self.queue1], 100)
        self.assertEqual(store.offset_table[self.queue2], 200)
        self.assertEqual(store.metrics.load_success_count, 1)
        self.assertEqual(store.metrics.load_failure_count, 0)

    def test_load_invalid_json(self):
        """测试加载无效JSON文件"""
        offset_file_path = Path(self.store_path) / f"{self.consumer_group}.offsets.json"
        offset_file_path.parent.mkdir(parents=True, exist_ok=True)

        # 写入无效JSON
        with open(offset_file_path, "w", encoding="utf-8") as f:
            f.write("{ invalid json")

        store = LocalOffsetStore(
            consumer_group=self.consumer_group, store_path=self.store_path
        )

        with self.assertRaises(json.JSONDecodeError):
            store.load()

        self.assertEqual(store.metrics.load_success_count, 0)
        # 加载过程中会记录两次失败：一次JSON解析失败，一次整体加载失败
        self.assertGreaterEqual(store.metrics.load_failure_count, 1)

    def test_update_offset(self):
        """测试更新偏移量"""
        store = LocalOffsetStore(
            consumer_group=self.consumer_group, store_path=self.store_path
        )

        # 更新偏移量
        store.update_offset(self.queue1, 100)
        store.update_offset(self.queue2, 200)

        self.assertEqual(store.offset_table[self.queue1], 100)
        self.assertEqual(store.offset_table[self.queue2], 200)

        # 更新已存在的偏移量
        store.update_offset(self.queue1, 150)
        self.assertEqual(store.offset_table[self.queue1], 150)

    def test_persist_single_offset(self):
        """测试持久化单个偏移量"""
        store = LocalOffsetStore(
            consumer_group=self.consumer_group, store_path=self.store_path
        )

        # 更新并持久化偏移量
        store.update_offset(self.queue1, 100)
        store.persist(self.queue1)

        # 验证文件是否存在
        offset_file_path = store.offset_file_path
        self.assertTrue(offset_file_path.exists())

        # 验证文件内容
        with open(offset_file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        self.assertEqual(data["consumer_group"], self.consumer_group)
        self.assertEqual(len(data["offsets"]), 1)
        self.assertEqual(data["offsets"][0]["topic"], "test_topic_1")
        self.assertEqual(data["offsets"][0]["offset"], 100)

        # 验证指标
        self.assertEqual(store.metrics.persist_success_count, 1)
        self.assertEqual(store.metrics.persist_failure_count, 0)

    def test_persist_nonexistent_offset(self):
        """测试持久化不存在的偏移量"""
        store = LocalOffsetStore(
            consumer_group=self.consumer_group, store_path=self.store_path
        )

        # 持久化不存在的偏移量应该不创建文件
        store.persist(self.queue1)

        offset_file_path = store.offset_file_path
        self.assertFalse(offset_file_path.exists())

    def test_persist_all_offsets(self):
        """测试持久化所有偏移量"""
        store = LocalOffsetStore(
            consumer_group=self.consumer_group, store_path=self.store_path
        )

        # 更新多个偏移量
        store.update_offset(self.queue1, 100)
        store.update_offset(self.queue2, 200)
        store.update_offset(self.queue3, 300)

        # 持久化所有偏移量
        store.persist_all()

        # 验证文件内容
        offset_file_path = store.offset_file_path
        with open(offset_file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        self.assertEqual(len(data["offsets"]), 3)

        # 验证指标
        self.assertEqual(store.metrics.persist_success_count, 1)

        # 验证空表情况
        store.offset_table.clear()
        store.persist_all()  # 应该不会报错

    def test_read_offset_memory_first(self):
        """测试优先从内存读取偏移量"""
        store = LocalOffsetStore(
            consumer_group=self.consumer_group, store_path=self.store_path
        )

        # 更新内存中的偏移量
        store.update_offset(self.queue1, 100)

        # 从内存读取
        offset = store.read_offset(self.queue1, ReadOffsetType.MEMORY_FIRST_THEN_STORE)
        self.assertEqual(offset, 100)
        self.assertEqual(store.metrics.cache_hit_count, 1)
        self.assertEqual(store.metrics.cache_miss_count, 0)

    def test_read_offset_memory_only(self):
        """测试仅从内存读取偏移量"""
        store = LocalOffsetStore(
            consumer_group=self.consumer_group, store_path=self.store_path
        )

        # 从内存读取不存在的偏移量
        offset = store.read_offset(self.queue1, ReadOffsetType.READ_FROM_MEMORY)
        self.assertEqual(offset, -1)
        self.assertEqual(store.metrics.cache_hit_count, 0)
        self.assertEqual(store.metrics.cache_miss_count, 1)

    def test_read_offset_from_file(self):
        """测试从文件读取偏移量"""
        # 先创建一个包含偏移量的文件
        store = LocalOffsetStore(
            consumer_group=self.consumer_group, store_path=self.store_path
        )
        store.update_offset(self.queue1, 100)
        store.persist(self.queue1)

        # 创建新的store实例，清空内存
        store2 = LocalOffsetStore(
            consumer_group=self.consumer_group, store_path=self.store_path
        )

        # 从文件读取
        offset = store2.read_offset(self.queue1, ReadOffsetType.READ_FROM_STORE)
        self.assertEqual(offset, 100)
        self.assertEqual(store2.metrics.cache_hit_count, 0)
        self.assertEqual(store2.metrics.cache_miss_count, 1)

        # 验证偏移量被加载到内存
        self.assertEqual(store2.offset_table[self.queue1], 100)

    def test_read_offset_file_not_found(self):
        """测试从不存在的文件读取偏移量"""
        store = LocalOffsetStore(
            consumer_group=self.consumer_group, store_path=self.store_path
        )

        offset = store.read_offset(self.queue1, ReadOffsetType.READ_FROM_STORE)
        self.assertEqual(offset, -1)

    def test_remove_offset(self):
        """测试移除偏移量"""
        store = LocalOffsetStore(
            consumer_group=self.consumer_group, store_path=self.store_path
        )

        # 添加并持久化偏移量
        store.update_offset(self.queue1, 100)
        store.update_offset(self.queue2, 200)
        store.persist_all()

        # 移除单个偏移量
        store.remove_offset(self.queue1)

        # 验证内存中的移除
        self.assertNotIn(self.queue1, store.offset_table)
        self.assertIn(self.queue2, store.offset_table)

        # 验证文件中的移除
        with open(store.offset_file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        self.assertEqual(len(data["offsets"]), 1)
        self.assertEqual(data["offsets"][0]["topic"], "test_topic_2")

    def test_remove_offset_no_file(self):
        """测试移除偏移量时文件不存在"""
        store = LocalOffsetStore(
            consumer_group=self.consumer_group, store_path=self.store_path
        )

        # 直接移除（没有文件）应该不报错
        store.remove_offset(self.queue1)

    def test_file_operations_atomicity(self):
        """测试文件操作的原子性"""
        store = LocalOffsetStore(
            consumer_group=self.consumer_group, store_path=self.store_path
        )

        store.update_offset(self.queue1, 100)

        # 模拟写入过程中的异常
        with patch.object(store, "_write_to_file") as mock_write:
            mock_write.side_effect = IOError("Write error")

            with self.assertRaises(IOError):
                store.persist(self.queue1)

            # 验证失败指标被记录
            self.assertEqual(store.metrics.persist_failure_count, 1)

    def test_build_file_data(self):
        """测试构建文件数据"""
        store = LocalOffsetStore(
            consumer_group=self.consumer_group, store_path=self.store_path
        )

        store.update_offset(self.queue1, 100)
        store.update_offset(self.queue2, 200)

        data = store._build_file_data()

        self.assertEqual(data["consumer_group"], self.consumer_group)
        self.assertEqual(len(data["offsets"]), 2)
        self.assertIn("last_update_time", data)
        self.assertEqual(data["version"], "1.0")

        # 验证偏移量条目格式
        offset_data = data["offsets"][0]
        self.assertIn("topic", offset_data)
        self.assertIn("broker_name", offset_data)
        self.assertIn("queue_id", offset_data)
        self.assertIn("offset", offset_data)
        self.assertIn("last_update_timestamp", offset_data)

    def test_temp_file_cleanup_on_error(self):
        """测试写入错误时临时文件的清理"""
        store = LocalOffsetStore(
            consumer_group=self.consumer_group, store_path=self.store_path
        )

        store.update_offset(self.queue1, 100)

        # 模拟文件写入失败
        with patch("builtins.open", side_effect=IOError("Write error")):
            with self.assertRaises(IOError):
                store.persist(self.queue1)

        # 验证临时文件被清理
        temp_file = store.offset_file_path.with_suffix(".tmp")
        self.assertFalse(temp_file.exists())

    def test_thread_safety(self):
        """测试线程安全性"""
        store = LocalOffsetStore(
            consumer_group=self.consumer_group, store_path=self.store_path
        )
        store.start()

        def worker(thread_id):
            """工作线程���数"""
            for i in range(100):
                queue = MessageQueue(
                    topic=f"topic_{thread_id}",
                    broker_name=f"broker_{thread_id}",
                    queue_id=i,  # 使用不同的queue_id确保唯一性
                )
                store.update_offset(queue, i)
                store.persist(queue)
                offset = store.read_offset(
                    queue, ReadOffsetType.MEMORY_FIRST_THEN_STORE
                )
                self.assertEqual(offset, i)

        # 创建多个线程
        threads = []
        for i in range(5):
            thread = threading.Thread(target=worker, args=(i,))
            threads.append(thread)
            thread.start()

        # 等待所有线程完成
        for thread in threads:
            thread.join(timeout=10)

        store.stop()

        # 验证最终状态
        self.assertEqual(len(store.offset_table), 500)  # 5 threads * 100 operations

    def test_periodic_persist(self):
        """测试定期持久化功能"""
        # 使用较短的持久化间隔
        store = LocalOffsetStore(
            consumer_group=self.consumer_group,
            store_path=self.store_path,
            persist_interval=100,  # 100ms
        )

        store.start()

        # 添加偏移量
        store.update_offset(self.queue1, 100)

        # 等待定期持久化触发
        time.sleep(0.2)

        # 验证文件已创建
        self.assertTrue(store.offset_file_path.exists())

        # 验证文件内容
        with open(store.offset_file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        self.assertEqual(len(data["offsets"]), 1)

        store.stop()

    def test_metrics_tracking(self):
        """测试指标跟踪"""
        store = LocalOffsetStore(
            consumer_group=self.consumer_group, store_path=self.store_path
        )

        # 模拟各种操作
        store.update_offset(self.queue1, 100)
        store.persist(self.queue1)

        store.read_offset(self.queue1, ReadOffsetType.MEMORY_FIRST_THEN_STORE)
        store.read_offset(self.queue2, ReadOffsetType.MEMORY_FIRST_THEN_STORE)

        # 验证指标
        self.assertEqual(store.metrics.persist_success_count, 1)
        self.assertEqual(store.metrics.cache_hit_count, 1)
        self.assertEqual(store.metrics.cache_miss_count, 1)
        self.assertGreater(store.metrics.avg_persist_latency, 0)

    def test_get_metrics(self):
        """测试获取指标信息"""
        store = LocalOffsetStore(
            consumer_group=self.consumer_group, store_path=self.store_path
        )

        store.update_offset(self.queue1, 100)

        metrics = store.get_metrics()

        self.assertIn("persist_success_count", metrics)
        self.assertIn("cached_offsets_count", metrics)
        self.assertIn("store_path", metrics)
        self.assertIn("persist_interval", metrics)
        self.assertIn("persist_batch_size", metrics)
        self.assertEqual(metrics["cached_offsets_count"], 1)

    def test_get_file_info(self):
        """测试获取文件信息"""
        store = LocalOffsetStore(
            consumer_group=self.consumer_group, store_path=self.store_path
        )

        # 文件不存在时
        file_info = store.get_file_info()
        self.assertFalse(file_info["file_exists"])
        self.assertEqual(file_info["file_size"], 0)
        self.assertIsNone(file_info["last_modified"])

        # 创建文件后
        store.update_offset(self.queue1, 100)
        store.persist(self.queue1)

        file_info = store.get_file_info()
        self.assertTrue(file_info["file_exists"])
        self.assertGreater(file_info["file_size"], 0)
        self.assertIsNotNone(file_info["last_modified"])

    def test_invalid_offset_entries_in_file(self):
        """测试文件中包含无效偏移量条目"""
        # 创建包含无效条目的文件
        offset_file_path = Path(self.store_path) / f"{self.consumer_group}.offsets.json"
        offset_file_path.parent.mkdir(parents=True, exist_ok=True)

        test_data = {
            "consumer_group": self.consumer_group,
            "offsets": [
                {
                    "topic": "test_topic_1",
                    "broker_name": "broker_1",
                    "queue_id": 0,
                    "offset": 100,
                    "last_update_timestamp": 1234567890000,
                },
                {
                    "topic": "test_topic_2",
                    "broker_name": "broker_1",
                    "queue_id": 1,
                    "offset": 200,
                    # 缺少 last_update_timestamp
                },
                {
                    "invalid": "entry"  # 完全无效的条目
                },
            ],
            "last_update_time": 1234567890000,
            "version": "1.0",
        }

        with open(offset_file_path, "w", encoding="utf-8") as f:
            json.dump(test_data, f)

        # 加载文件
        store = LocalOffsetStore(
            consumer_group=self.consumer_group, store_path=self.store_path
        )

        # 应该能正常加载，忽略无效条目
        store.load()

        # 只有有效条目被加载（第二个条目也是有效的，因为last_update_timestamp有默认值）
        self.assertEqual(len(store.offset_table), 2)
        self.assertEqual(store.offset_table[self.queue1], 100)
        self.assertEqual(store.metrics.load_success_count, 1)


if __name__ == "__main__":
    unittest.main()
