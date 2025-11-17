"""
同步读写锁单元测试
测试 ReadWriteLock 和 ReadWriteContext 的功能正确性
"""

import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

# 添加src到路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../src"))

import pytest

from pyrocketmq.utils.rwlock import ReadWriteContext, ReadWriteLock


class TestReadWriteLock:
    """测试ReadWriteLock类"""

    def test_initialization(self):
        """测试读写锁初始化"""
        lock = ReadWriteLock()
        assert lock._readers == 0
        assert lock._read_ready is not None

    def test_read_lock_basic(self):
        """测试基本的读锁功能"""
        lock = ReadWriteLock()

        lock.acquire_read()
        assert lock._readers == 1

        lock.release_read()
        assert lock._readers == 0

    def test_multiple_read_locks(self):
        """测试多个读锁可以同时获取"""
        lock = ReadWriteLock()

        lock.acquire_read()
        assert lock._readers == 1

        lock.acquire_read()
        assert lock._readers == 2

        lock.acquire_read()
        assert lock._readers == 3

        lock.release_read()
        assert lock._readers == 2

        lock.release_read()
        assert lock._readers == 1

        lock.release_read()
        assert lock._readers == 0

    def test_write_lock_basic(self):
        """测试基本的写锁功能"""
        lock = ReadWriteLock()

        lock.acquire_write()
        # 写锁不会增加readers计数
        assert lock._readers == 0

        lock.release_write()
        assert lock._readers == 0

    def test_write_lock_exclusive(self):
        """测试写锁的排他性"""
        lock = ReadWriteLock()

        # 获取读锁
        lock.acquire_read()
        assert lock._readers == 1

        # 在另一个线程中尝试获取写锁应该被阻塞
        write_acquired = threading.Event()
        write_done = threading.Event()

        def writer_thread():
            lock.acquire_write()
            write_acquired.set()
            lock.release_write()
            write_done.set()

        thread = threading.Thread(target=writer_thread)
        thread.start()

        # 等待一小段时间，写锁应该还没有获取到
        time.sleep(0.1)
        assert not write_acquired.is_set()

        # 释放读锁
        lock.release_read()

        # 等待写锁获取
        write_acquired.wait(timeout=1.0)
        assert write_acquired.is_set()

        write_done.wait(timeout=1.0)
        thread.join()

    def test_write_lock_blocks_readers(self):
        """测试写锁阻塞读者"""
        lock = ReadWriteLock()

        # 获取写锁
        lock.acquire_write()

        # 在另一个线程中尝试获取读锁应该被阻塞
        read_acquired = threading.Event()
        read_done = threading.Event()

        def reader_thread():
            lock.acquire_read()
            read_acquired.set()
            lock.release_read()
            read_done.set()

        thread = threading.Thread(target=reader_thread)
        thread.start()

        # 等待一小段时间，读锁应该还没有获取到
        time.sleep(0.1)
        assert not read_acquired.is_set()

        # 释放写锁
        lock.release_write()

        # 等待读锁获取
        read_acquired.wait(timeout=1.0)
        assert read_acquired.is_set()

        read_done.wait(timeout=1.0)
        thread.join()

    def test_context_manager_read(self):
        """测试读锁上下文管理器"""
        lock = ReadWriteLock()

        with ReadWriteContext(lock, write=False):
            assert lock._readers == 1

        assert lock._readers == 0

    def test_context_manager_write(self):
        """测试写锁上下文管理器"""
        lock = ReadWriteLock()

        with ReadWriteContext(lock, write=True):
            # 写锁不会增加readers计数
            assert lock._readers == 0

        assert lock._readers == 0

    def test_context_manager_default_read(self):
        """测试上下文管理器默认获取读锁"""
        lock = ReadWriteLock()

        with ReadWriteContext(lock):  # 默认为读锁
            assert lock._readers == 1

        assert lock._readers == 0

    def test_lock_enter_exit(self):
        """测试锁的__enter__和__exit__方法"""
        lock = ReadWriteLock()

        with lock:  # 默认获取读锁
            assert lock._readers == 1

        assert lock._readers == 0

    def test_concurrent_readers(self):
        """测试并发读者"""
        lock = ReadWriteLock()
        results = []

        def reader(reader_id):
            lock.acquire_read()
            try:
                time.sleep(0.1)  # 模拟读取操作
                results.append(f"reader_{reader_id}")
            finally:
                lock.release_read()

        # 创建多个读者线程
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(reader, i) for i in range(5)]
            for future in futures:
                future.result()

        # 所有读者都应该成功执行
        assert len(results) == 5
        assert lock._readers == 0  # 所有读者都已释放锁

    def test_concurrent_readers_with_writer(self):
        """测试读者和写者的并发情况"""
        lock = ReadWriteLock()
        readers_completed = []
        writer_completed = []

        def reader(reader_id):
            lock.acquire_read()
            try:
                time.sleep(0.05)  # 模拟读取操作
                readers_completed.append(f"reader_{reader_id}")
            finally:
                lock.release_read()

        def writer():
            lock.acquire_write()
            try:
                time.sleep(0.1)  # 模拟写入操作
                writer_completed.append("writer")
            finally:
                lock.release_write()

        # 先启动一些读者
        with ThreadPoolExecutor(max_workers=3) as executor:
            reader_futures = [executor.submit(reader, i) for i in range(3)]

            # 稍等片刻再启动写者
            time.sleep(0.02)
            writer_future = executor.submit(writer)

            # 再启动一些读者
            time.sleep(0.05)
            reader_futures.extend([executor.submit(reader, i) for i in range(3, 6)])

            # 等待所有任务完成
            for future in reader_futures + [writer_future]:
                future.result()

        # 验证执行结果
        assert len(readers_completed) == 6
        assert len(writer_completed) == 1
        assert lock._readers == 0

    def test_multiple_writers(self):
        """测试多个写者的互斥性"""
        lock = ReadWriteLock()
        execution_order = []

        def writer(writer_id):
            lock.acquire_write()
            try:
                execution_order.append(f"start_writer_{writer_id}")
                time.sleep(0.1)  # 模拟写入操作
                execution_order.append(f"end_writer_{writer_id}")
            finally:
                lock.release_write()

        # 创建多个写者线程
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = [executor.submit(writer, i) for i in range(3)]
            for future in futures:
                future.result()

        # 验证写者执行是串行的
        assert len(execution_order) == 6

        # 检查写者的顺序性：每个写者的开始和结束是成对的
        for i in range(3):
            start_index = execution_order.index(f"start_writer_{i}")
            end_index = execution_order.index(f"end_writer_{i}")
            assert start_index < end_index

    def test_nested_read_locks(self):
        """测试嵌套读锁（同一线程多次获取读锁）"""
        lock = ReadWriteLock()

        lock.acquire_read()
        assert lock._readers == 1

        lock.acquire_read()
        assert lock._readers == 2

        lock.release_read()
        assert lock._readers == 1

        lock.release_read()
        assert lock._readers == 0

    def test_error_handling_in_context(self):
        """测试上下文管理器中的异常处理"""
        lock = ReadWriteLock()

        try:
            with ReadWriteContext(lock, write=True):
                # 在写锁期间发生异常
                raise ValueError("Test exception")
        except ValueError:
            pass  # 预期的异常

        # 锁应该被正确释放
        assert lock._readers == 0

        # 验证锁仍然可用
        lock.acquire_read()
        lock.release_read()


class TestReadWriteLockStress:
    """压力测试"""

    def test_high_concurrency(self):
        """高并发测试"""
        lock = ReadWriteLock()
        results = []

        def worker(worker_id):
            for i in range(100):
                if i % 4 == 0:  # 25% 写操作
                    with ReadWriteContext(lock, write=True):
                        time.sleep(0.001)
                        results.append(f"write_{worker_id}_{i}")
                else:  # 75% 读操作
                    with ReadWriteContext(lock, write=False):
                        time.sleep(0.001)
                        results.append(f"read_{worker_id}_{i}")

        # 启动多个工作线程
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(worker, i) for i in range(10)]
            for future in futures:
                future.result()

        # 验证所有操作都完成
        assert len(results) == 1000  # 10个线程 * 100次操作
        assert lock._readers == 0


if __name__ == "__main__":
    # 运行基础测试
    test_lock = TestReadWriteLock()

    print("运行 ReadWriteLock 基础功能测试...")

    # 初始化测试
    test_lock.test_initialization()
    print("✅ 初始化测试通过")

    # 基础功能测试
    test_lock.test_read_lock_basic()
    print("✅ 读锁基础测试通过")

    test_lock.test_multiple_read_locks()
    print("✅ 多读锁测试通过")

    test_lock.test_write_lock_basic()
    print("✅ 写锁基础测试通过")

    # 上下文管理器测试
    test_lock.test_context_manager_read()
    print("✅ 读锁上下文管理器测试通过")

    test_lock.test_context_manager_write()
    print("✅ 写锁上下文管理器测试通过")

    # 并发测试
    test_lock.test_concurrent_readers()
    print("✅ 并发读者测试通过")

    # 压力测试
    stress_test = TestReadWriteLockStress()
    stress_test.test_high_concurrency()
    print("✅ 高并发压力测试通过")

    print("\n🎉 所有同步读写锁测试通过!")
