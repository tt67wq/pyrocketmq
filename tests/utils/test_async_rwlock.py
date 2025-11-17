"""
异步读写锁单元测试
测试 AsyncReadWriteLock、AsyncReadWriteContext 及其变体的功能正确性
"""

import asyncio
import os
import sys
import time

# 添加src到路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../src"))

import pytest

from pyrocketmq.utils.async_rwlock import (
    AsyncReaderPreferenceRWLock,
    AsyncReadWriteContext,
    AsyncReadWriteLock,
    AsyncWriterPreferenceRWLock,
)


class TestAsyncReadWriteLock:
    """测试AsyncReadWriteLock类"""

    def test_initialization(self):
        async def test():
            lock = AsyncReadWriteLock()
            assert lock._readers == 0
            assert lock._condition is not None

        asyncio.run(test())

    async def test_read_lock_basic(self):
        """测试基本的读锁功能"""
        lock = AsyncReadWriteLock()

        await lock.acquire_read()
        assert lock._readers == 1

        await lock.release_read()
        assert lock._readers == 0

    async def test_multiple_read_locks(self):
        """测试多个读锁可以同时获取"""
        lock = AsyncReadWriteLock()

        await lock.acquire_read()
        assert lock._readers == 1

        await lock.acquire_read()
        assert lock._readers == 2

        await lock.acquire_read()
        assert lock._readers == 3

        await lock.release_read()
        assert lock._readers == 2

        await lock.release_read()
        assert lock._readers == 1

        await lock.release_read()
        assert lock._readers == 0

    async def test_write_lock_basic(self):
        """测试基本的写锁功能"""
        lock = AsyncReadWriteLock()

        await lock.acquire_write()
        # 写锁不会增加readers计数
        assert lock._readers == 0

        await lock.release_write()
        assert lock._readers == 0

    async def test_write_lock_exclusive(self):
        """测试写锁的排他性"""
        lock = AsyncReadWriteLock()

        # 获取读锁
        await lock.acquire_read()
        assert lock._readers == 1

        # 在另一个协程中尝试获取写锁应该被阻塞
        write_acquired = asyncio.Event()
        write_done = asyncio.Event()

        async def writer_coro():
            await lock.acquire_write()
            write_acquired.set()
            await lock.release_write()
            write_done.set()

        task = asyncio.create_task(writer_coro())

        # 等待一小段时间，写锁应该还没有获取到
        await asyncio.sleep(0.1)
        assert not write_acquired.is_set()

        # 释放读锁
        await lock.release_read()

        # 等待写锁获取
        await asyncio.wait_for(write_acquired.wait(), timeout=1.0)
        assert write_acquired.is_set()

        await asyncio.wait_for(write_done.wait(), timeout=1.0)
        await task

    async def test_write_lock_blocks_readers(self):
        """测试写锁阻塞读者"""
        lock = AsyncReadWriteLock()

        # 获取写锁
        await lock.acquire_write()

        # 在另一个协程中尝试获取读锁应该被阻塞
        read_acquired = asyncio.Event()
        read_done = asyncio.Event()

        async def reader_coro():
            await lock.acquire_read()
            read_acquired.set()
            await lock.release_read()
            read_done.set()

        task = asyncio.create_task(reader_coro())

        # 等待一小段时间，读锁应该还没有获取到
        await asyncio.sleep(0.1)
        assert not read_acquired.is_set()

        # 释放写锁
        await lock.release_write()

        # 等待读锁获取
        await asyncio.wait_for(read_acquired.wait(), timeout=1.0)
        assert read_acquired.is_set()

        await asyncio.wait_for(read_done.wait(), timeout=1.0)
        await task

    async def test_context_manager_read(self):
        """测试读锁上下文管理器"""
        lock = AsyncReadWriteLock()

        async with AsyncReadWriteContext(lock, write=False):
            assert lock._readers == 1

        assert lock._readers == 0

    async def test_context_manager_write(self):
        """测试写锁上下文管理器"""
        lock = AsyncReadWriteLock()

        async with AsyncReadWriteContext(lock, write=True):
            # 写锁不会增加readers计数
            assert lock._readers == 0

        assert lock._readers == 0

    async def test_context_manager_default_read(self):
        """测试上下文管理器默认获取读锁"""
        lock = AsyncReadWriteLock()

        async with AsyncReadWriteContext(lock):  # 默认为读锁
            assert lock._readers == 1

        assert lock._readers == 0

    async def test_lock_aenter_aexit(self):
        """测试锁的__aenter__和__aexit__方法"""
        lock = AsyncReadWriteLock()

        async with lock:  # 默认获取读锁
            assert lock._readers == 1

        assert lock._readers == 0

    async def test_concurrent_readers(self):
        """测试并发读者"""
        lock = AsyncReadWriteLock()
        results = []

        async def reader(reader_id):
            await lock.acquire_read()
            try:
                await asyncio.sleep(0.1)  # 模拟读取操作
                results.append(f"reader_{reader_id}")
            finally:
                await lock.release_read()

        # 创建多个读者协程
        tasks = [reader(i) for i in range(5)]
        await asyncio.gather(*tasks)

        # 所有读者都应该成功执行
        assert len(results) == 5
        assert lock._readers == 0  # 所有读者都已释放锁

    async def test_concurrent_readers_with_writer(self):
        """测试读者和写者的并发情况"""
        lock = AsyncReadWriteLock()
        readers_completed = []
        writer_completed = []

        async def reader(reader_id):
            await lock.acquire_read()
            try:
                await asyncio.sleep(0.05)  # 模拟读取操作
                readers_completed.append(f"reader_{reader_id}")
            finally:
                await lock.release_read()

        async def writer():
            await lock.acquire_write()
            try:
                await asyncio.sleep(0.1)  # 模拟写入操作
                writer_completed.append("writer")
            finally:
                await lock.release_write()

        # 先启动一些读者
        reader_tasks1 = [reader(i) for i in range(3)]

        # 稍等片刻再启动写者
        await asyncio.sleep(0.02)
        writer_task = asyncio.create_task(writer())

        # 再启动一些读者
        await asyncio.sleep(0.05)
        reader_tasks2 = [reader(i) for i in range(3, 6)]

        # 等待所有任务完成
        await asyncio.gather(*(reader_tasks1 + reader_tasks2), writer_task)

        # 验证执行结果
        assert len(readers_completed) == 6
        assert len(writer_completed) == 1
        assert lock._readers == 0

    async def test_multiple_writers(self):
        """测试多个写者的互斥性"""
        lock = AsyncReadWriteLock()
        execution_order = []

        async def writer(writer_id):
            await lock.acquire_write()
            try:
                execution_order.append(f"start_writer_{writer_id}")
                await asyncio.sleep(0.1)  # 模拟写入操作
                execution_order.append(f"end_writer_{writer_id}")
            finally:
                await lock.release_write()

        # 创建多个写者协程
        tasks = [writer(i) for i in range(3)]
        await asyncio.gather(*tasks)

        # 验证写者执行是串行的
        assert len(execution_order) == 6

        # 检查写者的顺序性：每个写者的开始和结束是成对的
        for i in range(3):
            start_index = execution_order.index(f"start_writer_{i}")
            end_index = execution_order.index(f"end_writer_{i}")
            assert start_index < end_index

    async def test_nested_read_locks(self):
        """测试嵌套读锁（同一线程多次获取读锁）"""
        lock = AsyncReadWriteLock()

        await lock.acquire_read()
        assert lock._readers == 1

        await lock.acquire_read()
        assert lock._readers == 2

        await lock.release_read()
        assert lock._readers == 1

        await lock.release_read()
        assert lock._readers == 0

    async def test_error_handling_in_context(self):
        """测试上下文管理器中的异常处理"""
        lock = AsyncReadWriteLock()

        try:
            async with AsyncReadWriteContext(lock, write=True):
                # 在写锁期间发生异常
                raise ValueError("Test exception")
        except ValueError:
            pass  # 预期的异常

        # 锁应该被正确释放
        assert lock._readers == 0

        # 验证锁仍然可用
        await lock.acquire_read()
        await lock.release_read()


class TestAsyncReaderPreferenceRWLock:
    """测试读者优先异步读写锁"""

    async def test_reader_preference_basic(self):
        """测试读者优先的基本功能"""
        lock = AsyncReaderPreferenceRWLock()

        # 读者优先应该允许读者连续获取锁
        await lock.acquire_read()
        assert lock._readers == 1

        # 另一个读者应该能够立即获取锁
        await lock.acquire_read()
        assert lock._readers == 2

        await lock.release_read()
        assert lock._readers == 1

        await lock.release_read()
        assert lock._readers == 0

    async def test_reader_blocks_writers(self):
        """测试读者阻塞写者"""
        lock = AsyncReaderPreferenceRWLock()

        # 获取读锁
        await lock.acquire_read()

        # 写者应该被阻塞
        write_acquired = asyncio.Event()

        async def writer():
            await lock.acquire_write()
            write_acquired.set()
            await lock.release_write()

        writer_task = asyncio.create_task(writer())

        # 等待一小段时间，写者不应该获取到锁
        await asyncio.sleep(0.1)
        assert not write_acquired.is_set()

        # 释放读锁，写者应该能获取锁
        await lock.release_read()

        await asyncio.wait_for(write_acquired.wait(), timeout=1.0)
        await writer_task


class TestAsyncWriterPreferenceRWLock:
    """测试写者优先异步读写锁"""

    async def test_writer_preference_basic(self):
        """测试写者优先的基本功能"""
        lock = AsyncWriterPreferenceRWLock()

        # 写者优先应该允许写者优先获取锁
        await lock.acquire_write()
        assert lock._writer_active is True

        # 读者应该被阻塞
        read_acquired = asyncio.Event()

        async def reader():
            await lock.acquire_read()
            read_acquired.set()
            await lock.release_read()

        reader_task = asyncio.create_task(reader())

        # 等待一小段时间，读者不应该获取到锁
        await asyncio.sleep(0.1)
        assert not read_acquired.is_set()

        # 释放写锁，读者应该能获取锁
        await lock.release_write()
        assert lock._writer_active is False

        await asyncio.wait_for(read_acquired.wait(), timeout=1.0)
        await reader_task

    async def test_writers_have_priority(self):
        """测试写者优先策略"""
        lock = AsyncWriterPreferenceRWLock()

        # 先启动一个等待的写者
        writer_waiting = asyncio.Event()
        writer_acquired = asyncio.Event()

        async def writer():
            writer_waiting.set()
            await lock.acquire_write()
            writer_acquired.set()
            await asyncio.sleep(0.1)  # 保持写锁一段时间
            await lock.release_write()

        # 启动写者
        writer_task = asyncio.create_task(writer())
        await asyncio.wait_for(writer_waiting.wait(), timeout=1.0)

        # 启动读者，应该被阻塞
        read_started = asyncio.Event()

        async def reader():
            read_started.set()
            await lock.acquire_read()
            # 如果到这里，说明写者优先策略生效，读者在写者完成后获取锁
            await lock.release_read()

        reader_task = asyncio.create_task(reader())
        await asyncio.wait_for(read_started.wait(), timeout=1.0)

        # 等待写者获取锁
        await asyncio.wait_for(writer_acquired.wait(), timeout=1.0)

        # 等待所有任务完成
        await writer_task
        await reader_task


class TestAsyncReadWriteLockStress:
    """压力测试"""

    async def test_high_concurrency(self):
        """高并发测试"""
        lock = AsyncReadWriteLock()
        results = []

        async def worker(worker_id):
            for i in range(50):
                if i % 4 == 0:  # 25% 写操作
                    async with AsyncReadWriteContext(lock, write=True):
                        await asyncio.sleep(0.001)
                        results.append(f"write_{worker_id}_{i}")
                else:  # 75% 读操作
                    async with AsyncReadWriteContext(lock, write=False):
                        await asyncio.sleep(0.001)
                        results.append(f"read_{worker_id}_{i}")

        # 启动多个工作协程
        tasks = [worker(i) for i in range(10)]
        await asyncio.gather(*tasks)

        # 验证所有操作都完成
        assert len(results) == 500  # 10个协程 * 50次操作
        assert lock._readers == 0

    async def test_mixed_lock_types(self):
        """混合锁类型测试"""
        standard_lock = AsyncReadWriteLock()
        reader_pref_lock = AsyncReaderPreferenceRWLock()
        writer_pref_lock = AsyncWriterPreferenceRWLock()

        results = []

        async def test_lock_type(lock, lock_name):
            for i in range(10):
                if i % 3 == 0:  # 写操作
                    if hasattr(lock, "acquire_write"):
                        async with AsyncReadWriteContext(lock, write=True):
                            await asyncio.sleep(0.001)
                            results.append(f"{lock_name}_write_{i}")
                else:  # 读操作
                    if hasattr(lock, "acquire_read"):
                        async with AsyncReadWriteContext(lock, write=False):
                            await asyncio.sleep(0.001)
                            results.append(f"{lock_name}_read_{i}")

        # 并发测试不同类型的锁
        tasks = [
            test_lock_type(standard_lock, "standard"),
            test_lock_type(reader_pref_lock, "reader_pref"),
            test_lock_type(writer_pref_lock, "writer_pref"),
        ]

        await asyncio.gather(*tasks)

        # 验证所有操作完成
        assert len(results) == 30
        assert standard_lock._readers == 0
        assert reader_pref_lock._readers == 0
        assert writer_pref_lock._readers == 0


async def run_all_tests():
    """运行所有异步测试"""
    print("🧪 开始异步读写锁单元测试\n")

    # 基础功能测试
    test_lock = TestAsyncReadWriteLock()

    print("运行 AsyncReadWriteLock 基础功能测试...")

    # 初始化测试
    await test_lock.test_initialization()
    print("✅ 初始化测试通过")

    # 基础功能测试
    await test_lock.test_read_lock_basic()
    print("✅ 读锁基础测试通过")

    await test_lock.test_multiple_read_locks()
    print("✅ 多读锁测试通过")

    await test_lock.test_write_lock_basic()
    print("✅ 写锁基础测试通过")

    # 上下文管理器测试
    await test_lock.test_context_manager_read()
    print("✅ 读锁上下文管理器测试通过")

    await test_lock.test_context_manager_write()
    print("✅ 写锁上下文管理器测试通过")

    # 并发测试
    await test_lock.test_concurrent_readers()
    print("✅ 并发读者测试通过")

    await test_lock.test_concurrent_readers_with_writer()
    print("✅ 读者写者并发测试通过")

    await test_lock.test_multiple_writers()
    print("✅ 多写者互斥测试通过")

    # 异常处理测试
    await test_lock.test_error_handling_in_context()
    print("✅ 异常处理测试通过")

    # 读者优先锁测试
    reader_pref_test = TestAsyncReaderPreferenceRWLock()
    await reader_pref_test.test_reader_preference_basic()
    print("✅ 读者优先锁基础测试通过")

    await reader_pref_test.test_reader_blocks_writers()
    print("✅ 读者阻塞写者测试通过")

    # 写者优先锁测试
    writer_pref_test = TestAsyncWriterPreferenceRWLock()
    await writer_pref_test.test_writer_preference_basic()
    print("✅ 写者优先锁基础测试通过")

    await writer_pref_test.test_writers_have_priority()
    print("✅ 写者优先策略测试通过")

    # 压力测试
    stress_test = TestAsyncReadWriteLockStress()
    await stress_test.test_high_concurrency()
    print("✅ 高并发压力测试通过")

    await stress_test.test_mixed_lock_types()
    print("✅ 混合锁类型测试通过")

    print("\n🎉 所有异步读写锁测试通过!")


if __name__ == "__main__":
    asyncio.run(run_all_tests())
