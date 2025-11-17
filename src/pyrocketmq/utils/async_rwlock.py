"""
异步读写锁实现
支持多个协程同时读取，但写者独占访问
"""

import asyncio
from typing import Optional


class AsyncReadWriteLock:
    """异步读写锁实现

    支持多个协程同时读取，但写者独占访问。
    基于asyncio.Condition实现，适用于异步编程环境。
    """

    def __init__(self):
        """初始化异步读写锁"""
        self._condition = asyncio.Condition()
        self._readers = 0
        self._writers = 0

    async def acquire_read(self) -> None:
        """获取读锁

        多个协程可以同时持有读锁
        """
        async with self._condition:
            # 等待没有活跃的写者
            while self._writers > 0:
                await self._condition.wait()
            self._readers += 1

    async def release_read(self) -> None:
        """释放读锁

        当最后一个读锁释放时，通知等待的写者
        """
        async with self._condition:
            self._readers -= 1
            if self._readers == 0:
                self._condition.notify_all()

    async def acquire_write(self) -> None:
        """获取写锁

        等待所有读者完成，然后独占访问
        """
        async with self._condition:
            self._writers += 1
            # 等待所有读者完成
            while self._readers > 0:
                await self._condition.wait()

    async def release_write(self) -> None:
        """释放写锁"""
        async with self._condition:
            self._writers -= 1
            # 通知所有等待的协程
            self._condition.notify_all()

    async def __aenter__(self):
        """异步上下文管理器入口，默认获取读锁"""
        await self.acquire_read()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """异步上下文管理器出口，释放读锁"""
        await self.release_read()


class AsyncReadWriteContext:
    """异步读写锁上下文管理器

    提供读锁和写锁的异步上下文管理器接口
    """

    def __init__(self, lock: AsyncReadWriteLock, write: bool = False):
        """初始化上下文管理器

        Args:
            lock: 异步读写锁实例
            write: 是否获取写锁，默认为False（读锁）
        """
        self._lock = lock
        self._write = write

    async def __aenter__(self):
        """获取对应的锁"""
        if self._write:
            await self._lock.acquire_write()
        else:
            await self._lock.acquire_read()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """释放对应的锁"""
        if self._write:
            await self._lock.release_write()
        else:
            await self._lock.release_read()


class AsyncReaderPreferenceRWLock:
    """读者优先的异步读写锁

    在高并发读取场景下优先保证读者不会饿死
    """

    def __init__(self):
        """初始化读者优先读写锁"""
        self._readers = 0
        self._writers_waiting = 0
        self._writer_active = False
        self._readers_condition = asyncio.Condition()
        self._writers_condition = asyncio.Condition()

    async def acquire_read(self) -> None:
        """获取读锁

        读者优先：只有在没有活跃写者时才允许读取
        """
        async with self._readers_condition:
            # 等待没有活跃的写者
            while self._writer_active or self._writers_waiting > 0:
                await self._readers_condition.wait()

            self._readers += 1

    async def release_read(self) -> None:
        """释放读锁"""
        async with self._readers_condition:
            self._readers -= 1
            if self._readers == 0:
                # 最后一个读者离开，通知等待的写者
                async with self._writers_condition:
                    self._writers_condition.notify_all()

    async def acquire_write(self) -> None:
        """获取写锁

        需要等待所有读者完成
        """
        async with self._writers_condition:
            self._writers_waiting += 1

            # 等待没有活跃的读者和其他写者
            while self._readers > 0 or self._writer_active:
                await self._writers_condition.wait()

            self._writers_waiting -= 1
            self._writer_active = True

    async def release_write(self) -> None:
        """释放写锁"""
        async with self._writers_condition:
            self._writer_active = False
            # 首先通知其他等待的写者
            self._writers_condition.notify_all()

        # 然后通知所有等待的读者
        async with self._readers_condition:
            self._readers_condition.notify_all()


class AsyncWriterPreferenceRWLock:
    """写者优先的异步读写锁

    在写操作较多时优先保证写者不会饿死
    """

    def __init__(self):
        """初始化写者优先读写锁"""
        self._readers = 0
        self._writers_waiting = 0
        self._writer_active = False
        self._condition = asyncio.Condition()

    async def acquire_read(self) -> None:
        """获取读锁

        写者优先：如果有写者在等待，读者需要等待
        """
        async with self._condition:
            # 等待没有活跃的写者或等待的写者
            while self._writer_active or self._writers_waiting > 0:
                await self._condition.wait()

            self._readers += 1

    async def release_read(self) -> None:
        """释放读锁"""
        async with self._condition:
            self._readers -= 1
            if self._readers == 0 and not self._writer_active:
                # 没有更多读者，通知写者
                self._condition.notify_all()

    async def acquire_write(self) -> None:
        """获取写锁"""
        async with self._condition:
            self._writers_waiting += 1

            # 等待没有活跃的读者和写者
            while self._readers > 0 or self._writer_active:
                await self._condition.wait()

            self._writers_waiting -= 1
            self._writer_active = True

    async def release_write(self) -> None:
        """释放写锁"""
        async with self._condition:
            self._writer_active = False
            # 通知所有等待的协程（读者和写者）
            self._condition.notify_all()
