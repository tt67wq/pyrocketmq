"""
LocalOffsetStore测试

测试广播模式下的本地偏移量存储功能。
"""

import json
import os
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from pyrocketmq.consumer.local_offset_store import LocalOffsetStore
from pyrocketmq.consumer.offset_store import ReadOffsetType
from pyrocketmq.model import MessageQueue


class TestLocalOffsetStore(unittest.TestCase):
    """LocalOffsetStore测试类"""

    def setUp(self) -> None:
        """测试前准备"""
        # 创建临时目录用于测试
        self.temp_dir = tempfile.mkdtemp()
        self.consumer_group = "test_consumer_group"

        # 创建LocalOffsetStore实例
        self.offset_store = LocalOffsetStore(
            consumer_group=self.consumer_group,
            store_path=self.temp_dir,
            persist_interval=100,  # 100ms间隔用于快速测试
        )

        # 创建测试用的MessageQueue
        self.test_queue1 = MessageQueue(
            topic="test_topic_1", broker_name="broker_1", queue_id=0
        )
        self.test_queue2 = MessageQueue(
            topic="test_topic_2", broker_name="broker_2", queue_id=1
        )
        self.test_queue3 = MessageQueue(
            topic="test_topic_3", broker_name="broker_1", queue_id=2
        )

    def tearDown(self) -> None:
        """测试后清理"""
        # 停止offset_store
        if hasattr(self.offset_store, "_running") and self.offset_store._running:
            self.offset_store.stop()

        # 清理临时目录
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_init(self) -> None:
        """测试初始化"""
        # 验证实例属性
        self.assertEqual(self.offset_store.consumer_group, self.consumer_group)
        self.assertEqual(self.offset_store.persist_interval, 100)
        self.assertFalse(self.offset_store._running)

        # 验证文件路径创建
        expected_file_path = Path(self.temp_dir) / f"{self.consumer_group}.offsets.json"
        self.assertEqual(self.offset_store.offset_file_path, expected_file_path)
        self.assertTrue(expected_file_path.parent.exists())

    def test_start_stop(self) -> None:
        """测试启动和停止"""
        # 测试启动
        self.assertFalse(self.offset_store._running)
        self.offset_store.start()
        self.assertTrue(self.offset_store._running)
        self.assertIsNotNone(self.offset_store._persist_thread)
        self.assertTrue(self.offset_store._persist_thread.is_alive())

        # 测试重复启动
        original_thread = self.offset_store._persist_thread
        self.offset_store.start()
        self.assertEqual(self.offset_store._persist_thread, original_thread)

        # 测试停止
        self.offset_store.stop()
        self.assertFalse(self.offset_store._running)

        # 等待线程结束
        if original_thread.is_alive():
            original_thread.join(timeout=2)

        # 测试重复停止
        self.offset_store.stop()  # 不应该抛出异常

    def test_load_empty_file(self) -> None:
        """测试加载不存在的文件"""
        # 文件不存在时的加载
        self.offset_store.load()
        self.assertEqual(len(self.offset_store.offset_table), 0)

        # 验证指标记录
        metrics = self.offset_store.get_metrics()
        self.assertEqual(metrics["load_success_count"], 1)
        self.assertEqual(metrics["load_failure_count"], 0)

    def test_update_and_read_offset_memory(self) -> None:
        """测试内存偏移量更新和读取"""
        # 更新偏移量
        self.offset_store.update_offset(self.test_queue1, 100)
        self.offset_store.update_offset(self.test_queue2, 200)

        # 从内存读取
        offset1 = self.offset_store.read_offset(
            self.test_queue1, ReadOffsetType.READ_FROM_MEMORY
        )
        offset2 = self.offset_store.read_offset(
            self.test_queue2, ReadOffsetType.READ_FROM_MEMORY
        )

        self.assertEqual(offset1, 100)
        self.assertEqual(offset2, 200)

        # 测试不存在的队列
        offset3 = self.offset_store.read_offset(
            self.test_queue3, ReadOffsetType.READ_FROM_MEMORY
        )
        self.assertEqual(offset3, -1)

        # 验证内存缓存指标
        metrics = self.offset_store.get_metrics()
        self.assertGreater(metrics["cache_hit_count"], 0)
        self.assertGreater(metrics["cache_miss_count"], 0)

    def test_update_and_read_offset_memory_first_then_store(self) -> None:
        """测试内存优先的偏移量读取"""
        # 先更新偏移量到内存
        self.offset_store.update_offset(self.test_queue1, 150)

        # 读取（应该从内存获取）
        offset = self.offset_store.read_offset(
            self.test_queue1, ReadOffsetType.MEMORY_FIRST_THEN_STORE
        )
        self.assertEqual(offset, 150)

        # 验证缓存命中
        metrics = self.offset_store.get_metrics()
        self.assertGreater(metrics["cache_hit_count"], 0)

    def test_persist_single_offset(self) -> None:
        """测试单个偏移量持久化"""
        # 更新偏移量
        self.offset_store.update_offset(self.test_queue1, 300)

        # 持久化
        self.offset_store.persist(self.test_queue1)

        # 验证文件存在且内容正确
        self.assertTrue(self.offset_store.offset_file_path.exists())

        with open(self.offset_store.offset_file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        self.assertEqual(data["consumer_group"], self.consumer_group)
        self.assertEqual(len(data["offsets"]), 1)

        offset_entry = data["offsets"][0]
        self.assertEqual(offset_entry["topic"], self.test_queue1.topic)
        self.assertEqual(offset_entry["broker_name"], self.test_queue1.broker_name)
        self.assertEqual(offset_entry["queue_id"], self.test_queue1.queue_id)
        self.assertEqual(offset_entry["offset"], 300)

        # 验证持久化指标
        metrics = self.offset_store.get_metrics()
        self.assertGreater(metrics["persist_operation_count"], 0)
        self.assertGreater(metrics["persist_success_count"], 0)

    def test_persist_all_offsets(self) -> None:
        """测试批量持久化所有偏移量"""
        # 更新多个偏移量
        self.offset_store.update_offset(self.test_queue1, 100)
        self.offset_store.update_offset(self.test_queue2, 200)
        self.offset_store.update_offset(self.test_queue3, 300)

        # 批量持久化
        self.offset_store.persist_all()

        # 验证文件内容
        with open(self.offset_store.offset_file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        self.assertEqual(len(data["offsets"]), 3)

        # 验证每个偏移量
        offsets_dict = {
            f"{entry['topic']}@{entry['broker_name']}:{entry['queue_id']}": entry[
                "offset"
            ]
            for entry in data["offsets"]
        }

        self.assertEqual(
            offsets_dict[
                f"{self.test_queue1.topic}@{self.test_queue1.broker_name}:{self.test_queue1.queue_id}"
            ],
            100,
        )
        self.assertEqual(
            offsets_dict[
                f"{self.test_queue2.topic}@{self.test_queue2.broker_name}:{self.test_queue2.queue_id}"
            ],
            200,
        )
        self.assertEqual(
            offsets_dict[
                f"{self.test_queue3.topic}@{self.test_queue3.broker_name}:{self.test_queue3.queue_id}"
            ],
            300,
        )

    def test_load_from_file(self) -> None:
        """测试从文件加载偏移量"""
        # 手动创建偏移量文件
        test_data = {
            "consumer_group": self.consumer_group,
            "offsets": [
                {
                    "topic": self.test_queue1.topic,
                    "broker_name": self.test_queue1.broker_name,
                    "queue_id": self.test_queue1.queue_id,
                    "offset": 500,
                },
                {
                    "topic": self.test_queue2.topic,
                    "broker_name": self.test_queue2.broker_name,
                    "queue_id": self.test_queue2.queue_id,
                    "offset": 600,
                },
            ],
            "last_update_time": int(time.time() * 1000),
            "version": "1.0",
        }

        with open(self.offset_store.offset_file_path, "w", encoding="utf-8") as f:
            json.dump(test_data, f, indent=2)

        # 加载偏移量
        self.offset_store.load()

        # 验证加载结果
        self.assertEqual(len(self.offset_store.offset_table), 2)
        self.assertEqual(self.offset_store.offset_table[self.test_queue1], 500)
        self.assertEqual(self.offset_store.offset_table[self.test_queue2], 600)

    def test_read_offset_from_store(self) -> None:
        """测试从存储读取偏移量"""
        # 先持久化偏移量
        self.offset_store.update_offset(self.test_queue1, 700)
        self.offset_store.persist(self.test_queue1)

        # 清空内存缓存
        self.offset_store.offset_table.clear()

        # 从存储读取
        offset = self.offset_store.read_offset(
            self.test_queue1, ReadOffsetType.READ_FROM_STORE
        )

        self.assertEqual(offset, 700)
        # 验证读取后更新了内存缓存
        self.assertEqual(self.offset_store.offset_table[self.test_queue1], 700)

    def test_remove_offset(self) -> None:
        """测试移除偏移量"""
        # 添加偏移量
        self.offset_store.update_offset(self.test_queue1, 800)
        self.offset_store.persist_all()

        # 验证文件存在
        self.assertTrue(self.offset_store.offset_file_path.exists())

        # 移除偏移量
        self.offset_store.remove_offset(self.test_queue1)

        # 验证内存中已移除
        self.assertNotIn(self.test_queue1, self.offset_store.offset_table)

        # 验证文件中已移除
        with open(self.offset_store.offset_file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        self.assertEqual(len(data["offsets"]), 0)

    def test_periodic_persist(self) -> None:
        """测试定期持久化功能"""
        # 启动定期持久化
        self.offset_store.start()

        # 更新偏移量
        self.offset_store.update_offset(self.test_queue1, 900)

        # 等待定期持久化执行
        time.sleep(0.2)  # 大于persist_interval(100ms)

        # 验证文件已更新
        self.assertTrue(self.offset_store.offset_file_path.exists())

        with open(self.offset_store.offset_file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        self.assertEqual(len(data["offsets"]), 1)
        self.assertEqual(data["offsets"][0]["offset"], 900)

        # 停止服务
        self.offset_store.stop()

    def test_file_atomic_operations(self) -> None:
        """测试文件原子性操作"""
        # 更新偏移量
        self.offset_store.update_offset(self.test_queue1, 1000)

        # 模拟写入过程中的异常
        with patch.object(
            self.offset_store, "_write_to_file", side_effect=IOError("Mock error")
        ):
            with self.assertRaises(IOError):
                self.offset_store.persist(self.test_queue1)

        # 验证临时文件被清理
        temp_file = self.offset_store.offset_file_path.with_suffix(".tmp")
        self.assertFalse(temp_file.exists())

    def test_invalid_json_file(self) -> None:
        """测试无效JSON文件的处理"""
        # 先记录初始失败计数
        initial_metrics = self.offset_store.get_metrics()
        initial_failures = initial_metrics.get("load_failure_count", 0)

        # 创建无效JSON文件
        with open(self.offset_store.offset_file_path, "w", encoding="utf-8") as f:
            f.write("{ invalid json content")

        # 尝试加载应该抛出异常
        with self.assertRaises(json.JSONDecodeError):
            self.offset_store.load()

        # 验证失败指标记录有增加（可能增加1或2，因为内部可能多次记录失败）
        metrics = self.offset_store.get_metrics()
        self.assertGreater(metrics["load_failure_count"], initial_failures)

    def test_concurrent_operations(self) -> None:
        """测试并发操作"""

        def update_offsets(thread_id: int, count: int) -> None:
            """更新偏移量的线程函数"""
            for i in range(count):
                queue = MessageQueue(
                    topic=f"topic_{thread_id}",
                    broker_name=f"broker_{thread_id}",
                    queue_id=i,
                )
                self.offset_store.update_offset(queue, i * 100)

        # 启动多个线程并发更新
        threads = []
        for i in range(5):
            thread = threading.Thread(target=update_offsets, args=(i, 10))
            threads.append(thread)
            thread.start()

        # 等待所有线程完成
        for thread in threads:
            thread.join(timeout=5)

        # 验证所有偏移量都被正确更新
        expected_count = 5 * 10  # 5个线程 * 10个偏移量
        self.assertEqual(len(self.offset_store.offset_table), expected_count)

    def test_get_metrics(self) -> None:
        """测试获取指标信息"""
        # 执行一些操作
        self.offset_store.load()
        self.offset_store.update_offset(self.test_queue1, 100)
        self.offset_store.persist(self.test_queue1)

        # 获取指标
        metrics = self.offset_store.get_metrics()

        # 验证指标包含必要字段
        required_fields = [
            "load_success_count",
            "load_failure_count",
            "persist_success_count",
            "persist_failure_count",
            "cache_hit_count",
            "cache_miss_count",
            "cached_offsets_count",
            "store_path",
            "persist_interval",
        ]

        for field in required_fields:
            self.assertIn(field, metrics)

        self.assertEqual(metrics["cached_offsets_count"], 1)
        self.assertEqual(metrics["store_path"], str(self.offset_store.offset_file_path))
        self.assertEqual(metrics["persist_interval"], 100)

    def test_get_file_info(self) -> None:
        """测试获取文件信息"""
        # 初始文件信息
        file_info = self.offset_store.get_file_info()
        self.assertEqual(
            file_info["file_path"], str(self.offset_store.offset_file_path)
        )
        self.assertFalse(file_info["file_exists"])
        self.assertEqual(file_info["file_size"], 0)
        self.assertIsNone(file_info["last_modified"])

        # 创建文件后
        self.offset_store.update_offset(self.test_queue1, 100)
        self.offset_store.persist(self.test_queue1)

        file_info = self.offset_store.get_file_info()
        self.assertTrue(file_info["file_exists"])
        self.assertGreater(file_info["file_size"], 0)
        self.assertIsNotNone(file_info["last_modified"])

    def test_offset_entry_with_invalid_data(self) -> None:
        """测试处理无效偏移量条目数据"""
        # 创建包含无效数据的文件
        invalid_data = {
            "consumer_group": self.consumer_group,
            "offsets": [
                {
                    "topic": self.test_queue1.topic,
                    "broker_name": self.test_queue1.broker_name,
                    "queue_id": self.test_queue1.queue_id,
                    "offset": 100,
                },
                {
                    # 缺少必要字段的无效条目
                    "topic": "invalid_topic"
                },
            ],
            "last_update_time": int(time.time() * 1000),
            "version": "1.0",
        }

        with open(self.offset_store.offset_file_path, "w", encoding="utf-8") as f:
            json.dump(invalid_data, f, indent=2)

        # 加载应该跳过无效条目但继续处理有效条目
        self.offset_store.load()

        # 验证只加载了有效的偏移量
        self.assertEqual(len(self.offset_store.offset_table), 1)
        self.assertEqual(self.offset_store.offset_table[self.test_queue1], 100)

        # 验证加载成功（因为至少有一个有效条目）
        metrics = self.offset_store.get_metrics()
        self.assertEqual(metrics["load_success_count"], 1)

    def test_empty_offsets_list_persistence(self) -> None:
        """测试空偏移量列表的持久化"""
        # 持久化空偏移量表（空偏移量不会创建文件）
        self.offset_store.persist_all()

        # 由于偏移量表为空，文件可能不存在（这是正常行为）
        if self.offset_store.offset_file_path.exists():
            with open(self.offset_store.offset_file_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            self.assertEqual(data["consumer_group"], self.consumer_group)
            self.assertEqual(len(data["offsets"]), 0)
        else:
            # 文件不存在也是正常的，因为没有任何偏移量需要持久化
            self.assertFalse(self.offset_store.offset_file_path.exists())

    def test_multiple_offset_updates(self) -> None:
        """测试同一队列的多次偏移量更新"""
        # 多次更新同一队列的偏移量
        self.offset_store.update_offset(self.test_queue1, 100)
        self.offset_store.update_offset(self.test_queue1, 200)
        self.offset_store.update_offset(self.test_queue1, 300)

        # 验证最终值
        offset = self.offset_store.read_offset(
            self.test_queue1, ReadOffsetType.READ_FROM_MEMORY
        )
        self.assertEqual(offset, 300)

        # 持久化并验证
        self.offset_store.persist(self.test_queue1)

        with open(self.offset_store.offset_file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        self.assertEqual(len(data["offsets"]), 1)
        self.assertEqual(data["offsets"][0]["offset"], 300)

    def test_read_offset_types(self) -> None:
        """测试不同类型的偏移量读取"""
        # 先持久化一个偏移量
        self.offset_store.update_offset(self.test_queue1, 400)
        self.offset_store.persist(self.test_queue1)

        # 清空内存缓存
        self.offset_store.offset_table.clear()

        # 测试READ_FROM_MEMORY（应该返回-1）
        offset = self.offset_store.read_offset(
            self.test_queue1, ReadOffsetType.READ_FROM_MEMORY
        )
        self.assertEqual(offset, -1)

        # 测试READ_FROM_STORE（应该从文件读取）
        offset = self.offset_store.read_offset(
            self.test_queue1, ReadOffsetType.READ_FROM_STORE
        )
        self.assertEqual(offset, 400)

        # 测试MEMORY_FIRST_THEN_STORE（应该从文件读取然后缓存到内存）
        self.offset_store.offset_table.clear()  # 再次清空
        offset = self.offset_store.read_offset(
            self.test_queue1, ReadOffsetType.MEMORY_FIRST_THEN_STORE
        )
        self.assertEqual(offset, 400)
        self.assertEqual(self.offset_store.offset_table[self.test_queue1], 400)


class TestLocalOffsetStoreIntegration(unittest.TestCase):
    """LocalOffsetStore集成测试"""

    def setUp(self) -> None:
        """测试前准备"""
        self.temp_dir = tempfile.mkdtemp()
        self.consumer_group = "integration_test_group"

    def tearDown(self) -> None:
        """测试后清理"""
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_full_lifecycle(self) -> None:
        """测试完整的生命周期"""
        # 创建新的LocalOffsetStore
        offset_store = LocalOffsetStore(
            consumer_group=self.consumer_group,
            store_path=self.temp_dir,
            persist_interval=50,  # 50ms
        )

        try:
            # 1. 启动服务
            offset_store.start()

            # 2. 加载偏移量（应该是空的）
            offset_store.load()
            self.assertEqual(len(offset_store.offset_table), 0)

            # 3. 更新偏移量
            test_queue = MessageQueue(
                topic="lifecycle_topic", broker_name="test_broker", queue_id=0
            )
            offset_store.update_offset(test_queue, 100)

            # 4. 等待定期持久化
            time.sleep(0.1)  # 大于persist_interval

            # 5. 创建新实例测试恢复
            offset_store2 = LocalOffsetStore(
                consumer_group=self.consumer_group,
                store_path=self.temp_dir,
                persist_interval=50,
            )
            offset_store2.load()

            # 6. 验证偏移量恢复
            recovered_offset = offset_store2.read_offset(
                test_queue, ReadOffsetType.READ_FROM_STORE
            )
            self.assertEqual(recovered_offset, 100)

            # 7. 停止服务
            offset_store.stop()
            offset_store2.stop()

        finally:
            # 确保服务被停止
            if offset_store._running:
                offset_store.stop()


if __name__ == "__main__":
    unittest.main()
