"""
异步读写锁基础测试
"""

import asyncio
import os
import sys

# 添加src到路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../src"))

from pyrocketmq.utils.async_rwlock import (
    AsyncReaderPreferenceRWLock,
    AsyncReadWriteContext,
    AsyncReadWriteLock,
    AsyncWriterPreferenceRWLock,
)


async def test_basic_functionality():
    """测试基础功能"""
    print("运行异步读写锁基础功能测试...")

    # 初始化测试
    lock = AsyncReadWriteLock()
    assert lock._readers == 0
    assert lock._condition is not None
    print("✅ 初始化测试通过")

    # 读锁基础测试
    await lock.acquire_read()
    assert lock._readers == 1
    await lock.release_read()
    assert lock._readers == 0
    print("✅ 读锁基础测试通过")

    # 多读锁测试
    await lock.acquire_read()
    assert lock._readers == 1

    await lock.acquire_read()
    assert lock._readers == 2

    await lock.release_read()
    assert lock._readers == 1

    await lock.release_read()
    assert lock._readers == 0
    print("✅ 多读锁测试通过")

    # 写锁基础测试
    await lock.acquire_write()
    assert lock._readers == 0
    await lock.release_write()
    assert lock._readers == 0
    print("✅ 写锁基础测试通过")


async def test_context_managers():
    """测试上下文管理器"""
    lock = AsyncReadWriteLock()

    # 读锁上下文管理器
    async with AsyncReadWriteContext(lock, write=False):
        assert lock._readers == 1
    assert lock._readers == 0
    print("✅ 读锁上下文管理器测试通过")

    # 写锁上下文管理器
    async with AsyncReadWriteContext(lock, write=True):
        assert lock._readers == 0
    assert lock._readers == 0
    print("✅ 写锁上下文管理器测试通过")

    # 默认读锁上下文管理器
    async with AsyncReadWriteContext(lock):
        assert lock._readers == 1
    assert lock._readers == 0
    print("✅ 默认上下文管理器测试通过")


async def test_concurrent_readers():
    """测试并发读者"""
    lock = AsyncReadWriteLock()
    results = []

    async def reader(reader_id):
        await lock.acquire_read()
        try:
            await asyncio.sleep(0.01)  # 模拟读取操作
            results.append(f"reader_{reader_id}")
        finally:
            await lock.release_read()

    # 创建多个读者协程
    tasks = [reader(i) for i in range(5)]
    await asyncio.gather(*tasks)

    assert len(results) == 5
    assert lock._readers == 0
    print("✅ 并发读者测试通过")


async def test_concurrent_readers_with_writer():
    """测试读者和写者的并发情况"""
    lock = AsyncReadWriteLock()
    readers_completed = []
    writer_completed = []

    async def reader(reader_id):
        await lock.acquire_read()
        try:
            await asyncio.sleep(0.02)  # 模拟读取操作
            readers_completed.append(f"reader_{reader_id}")
        finally:
            await lock.release_read()

    async def writer():
        await lock.acquire_write()
        try:
            await asyncio.sleep(0.05)  # 模拟写入操作
            writer_completed.append("writer")
        finally:
            await lock.release_write()

    # 先启动一些读者
    reader_tasks1 = [reader(i) for i in range(2)]

    # 稍等片刻再启动写者
    await asyncio.sleep(0.01)
    writer_task = asyncio.create_task(writer())

    # 再启动一些读者
    await asyncio.sleep(0.03)
    reader_tasks2 = [reader(i) for i in range(2, 4)]

    # 等待所有任务完成
    await asyncio.gather(*(reader_tasks1 + reader_tasks2), writer_task)

    assert len(readers_completed) == 4
    assert len(writer_completed) == 1
    assert lock._readers == 0
    print("✅ 读者写者并发测试通过")


async def test_multiple_writers():
    """测试多个写者的互斥性"""
    lock = AsyncReadWriteLock()
    execution_order = []

    async def writer(writer_id):
        await lock.acquire_write()
        try:
            execution_order.append(f"start_writer_{writer_id}")
            await asyncio.sleep(0.02)  # 模拟写入操作
            execution_order.append(f"end_writer_{writer_id}")
        finally:
            await lock.release_write()

    # 创建多个写者协程
    tasks = [writer(i) for i in range(3)]
    await asyncio.gather(*tasks)

    assert len(execution_order) == 6

    # 检查写者的顺序性
    for i in range(3):
        start_index = execution_order.index(f"start_writer_{i}")
        end_index = execution_order.index(f"end_writer_{i}")
        assert start_index < end_index

    print("✅ 多写者互斥测试通过")


async def test_reader_preference_lock():
    """测试读者优先锁"""
    lock = AsyncReaderPreferenceRWLock()

    # 读者优先应该允许读者连续获取锁
    await lock.acquire_read()
    assert lock._readers == 1

    await lock.acquire_read()
    assert lock._readers == 2

    await lock.release_read()
    assert lock._readers == 1

    await lock.release_read()
    assert lock._readers == 0

    print("✅ 读者优先锁测试通过")


async def test_writer_preference_lock():
    """测试写者优先锁"""
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
    await asyncio.sleep(0.01)
    assert not read_acquired.is_set()

    # 释放写锁，读者应该能获取锁
    await lock.release_write()
    assert lock._writer_active is False

    await asyncio.wait_for(read_acquired.wait(), timeout=1.0)
    await reader_task

    print("✅ 写者优先锁测试通过")


async def test_high_concurrency():
    """高并发测试"""
    lock = AsyncReadWriteLock()
    results = []

    async def worker(worker_id):
        for i in range(20):
            if i % 4 == 0:  # 25% 写操作
                async with AsyncReadWriteContext(lock, write=True):
                    await asyncio.sleep(0.001)
                    results.append(f"write_{worker_id}_{i}")
            else:  # 75% 读操作
                async with AsyncReadWriteContext(lock, write=False):
                    await asyncio.sleep(0.001)
                    results.append(f"read_{worker_id}_{i}")

    # 启动多个工作协程
    tasks = [worker(i) for i in range(5)]
    await asyncio.gather(*tasks)

    assert len(results) == 100  # 5个协程 * 20次操作
    assert lock._readers == 0

    print("✅ 高并发压力测试通过")


async def run_async_tests():
    """运行所有异步测试"""
    print("🧪 开始异步读写锁单元测试\n")

    # 基础功能测试
    await test_basic_functionality()

    # 上下文管理器测试
    await test_context_managers()

    # 并发测试
    await test_concurrent_readers()
    await test_concurrent_readers_with_writer()
    await test_multiple_writers()

    # 特殊锁类型测试
    await test_reader_preference_lock()
    await test_writer_preference_lock()

    # 压力测试
    await test_high_concurrency()

    print("\n🎉 所有异步读写锁测试通过!")


if __name__ == "__main__":
    asyncio.run(run_async_tests())
