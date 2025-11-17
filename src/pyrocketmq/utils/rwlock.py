"""
同步读写锁实现
支持多个读者同时访问，但写者独占访问
"""

import threading
from typing import Optional


class ReadWriteLock:
    """读写锁实现

    支持多个读者同时访问，但写者独占访问。
    使用递归锁实现，避免死锁问题。
    """

    def __init__(self):
        """初始化读写锁"""
        self._read_ready = threading.Condition(threading.RLock())
        self._readers = 0

    def acquire_read(self) -> None:
        """获取读锁"""
        self._read_ready.acquire()
        try:
            self._readers += 1
        finally:
            self._read_ready.release()

    def release_read(self) -> None:
        """释放读锁"""
        self._read_ready.acquire()
        try:
            self._readers -= 1
            if self._readers == 0:
                self._read_ready.notifyAll()
        finally:
            self._read_ready.release()

    def acquire_write(self) -> None:
        """获取写锁"""
        self._read_ready.acquire()
        while self._readers > 0:
            self._read_ready.wait()

    def release_write(self) -> None:
        """释放写锁"""
        self._read_ready.release()

    def __enter__(self):
        """上下文管理器入口，默认获取读锁"""
        self.acquire_read()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口，释放读锁"""
        self.release_read()


class ReadWriteContext:
    """读写锁上下文管理器

    提供读锁和写锁的上下文管理器接口
    """

    def __init__(self, lock: ReadWriteLock, write: bool = False):
        """初始化上下文管理器

        Args:
            lock: 读写锁实例
            write: 是否获取写锁，默认为False（读锁）
        """
        self._lock = lock
        self._write = write

    def __enter__(self):
        """获取对应的锁"""
        if self._write:
            self._lock.acquire_write()
        else:
            self._lock.acquire_read()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """释放对应的锁"""
        if self._write:
            self._lock.release_write()
        else:
            self._lock.release_read()
