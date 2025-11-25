"""
ProcessQueue - 高效的消息处理队列

ProcessQueue是一个专门为RocketMQ Consumer设计的高效消息缓存数据结构，
支持快速的插入、删除、统计操作。替代简单的list[MessageExt]以提供更好的性能。

主要特性:
- 基于排序列表的高效插入和删除
- 快速的消息统计（数量、总体积）
- 支持按偏移量范围查找和删除
- 线程安全操作
- 双重缓存限制（数量和大小）

作者: pyrocketmq开发团队
"""

import threading
from bisect import bisect_left
from dataclasses import dataclass
from typing import Any

from pyrocketmq.model import MessageExt


@dataclass
class QueueEntry:
    """队列条目，支持墓碑值软删除机制"""

    queue_offset: int
    message: MessageExt | None  # None表示墓碑值（已删除）
    is_deleted: bool = False  # 软删除标记

    @property
    def is_tombstone(self) -> bool:
        """检查是否为墓碑值"""
        return self.is_deleted or self.message is None

    @property
    def message_offset(self) -> int:
        """获取消息的偏移量"""
        if not self.message:
            return -1
        return self.message.queue_offset or -1


class ProcessQueue:
    """
    消息处理队列

    高效的消息缓存数据结构，支持快速的插入、删除、统计操作。
    替代简单的list[MessageExt]以提供更好的性能。

    主要特性:
    - 基于树结构的高效插入和删除
    - 快速的消息统计（数量、总体积）
    - 支持按偏移量范围查找和删除
    - 线程安全操作
    """

    def __init__(
        self, max_cache_count: int = 10000, max_cache_size_mb: int = 512
    ) -> None:
        """
        初始化处理队列

        Args:
            max_cache_count: 最大缓存消息数量
            max_cache_size_mb: 最大缓存字节数(MB)
        """
        # 主存储：按偏移量排序的队列条目
        self._entries: list[QueueEntry] = []  # 按queue_offset排序
        self._offset_to_index: dict[int, int] = {}  # 偏移量到索引的映射
        self._lock: threading.RLock = threading.RLock()

        # 统计信息（只计算有效消息，排除墓碑值）
        self._count: int = 0
        self._total_size: int = 0  # 字节总数
        self._tombstone_count: int = 0  # 墓碑值数量

        # 配置限制
        self._max_cache_count: int = max_cache_count
        self._max_cache_size: int = max_cache_size_mb * 1024 * 1024  # 转换为字节
        self._cleanup_threshold: float = 0.3  # 当墓碑值占比超过30%时触发清理

        # 偏移量范围跟踪（只考虑有效消息）
        self._min_offset: float = float("inf")
        self._max_offset: int = -1

        # 顺序消费处理中的消息（待提交区）
        self._consuming_orderly_msgs: list[QueueEntry] = []

    def add_message(self, message: MessageExt) -> bool:
        """
        添加消息到队列

        Args:
            message: 要添加的消息

        Returns:
            bool: 是否成功添加（可能因为缓存限制而失败）
        """
        with self._lock:
            # 检查缓存限制
            if not self._check_cache_limits():
                return False

            if message.queue_offset is None:
                # impossible
                return False

            # 检查消息是否已存在（包括墓碑值）
            if message.queue_offset in self._offset_to_index:
                # 如果偏移量已存在，检查是否为墓碑值
                existing_index: int = self._offset_to_index[message.queue_offset]
                existing_entry: QueueEntry = self._entries[existing_index]

                # 如果是墓碑值，直接替换为有效消息
                if existing_entry.is_tombstone:
                    # 更新统计信息
                    self._count += 1
                    message_body_size: int = len(message.body) if message.body else 0
                    self._total_size += message_body_size
                    self._tombstone_count -= 1

                    # 替换条目
                    self._entries[existing_index] = QueueEntry(
                        queue_offset=message.queue_offset,
                        message=message,
                        is_deleted=False,
                    )

                    # 更新偏移量范围
                    self._update_offset_ranges()

                    return True
                else:
                    # 已存在有效消息，返回失败
                    return False

            # 检查是否需要清理墓碑值（在插入前清理）
            self._maybe_cleanup_tombstones()

            # 创建队列条目
            entry: QueueEntry = QueueEntry(
                queue_offset=message.queue_offset, message=message, is_deleted=False
            )

            # 使用二分查找插入位置
            index: int = bisect_left(
                [x.queue_offset for x in self._entries], entry.queue_offset
            )

            # 插入条目
            self._entries.insert(index, entry)

            # 更新索引映射：需要更新所有后续条目的索引
            for i in range(index, len(self._entries)):
                self._offset_to_index[self._entries[i].queue_offset] = i

            # 更新统计信息
            self._count += 1
            message_body_size = len(message.body) if message.body else 0
            self._total_size += message_body_size

            # 更新偏移量范围
            if message.queue_offset < self._min_offset:
                self._min_offset = message.queue_offset
            if message.queue_offset > self._max_offset:
                self._max_offset = message.queue_offset

            return True

    def add_batch_messages(self, messages: list[MessageExt]) -> dict[str, Any]:
        """
        批量添加消息到队列

        Args:
            messages: 要添加的消息列表

        Returns:
            dict: 批量操作结果，包含成功数量、失败数量、成功/失败的消息列表
        """
        with self._lock:
            # 统计结果
            successful_messages: list[MessageExt] = []
            failed_messages: list[MessageExt] = []
            duplicates: list[MessageExt] = []

            # 先对消息按偏移量排序，以提高插入效率
            sorted_messages = sorted(messages, key=lambda m: m.queue_offset or 0)

            # 检查是否需要清理墓碑值（在批量插入前清理一次）
            self._maybe_cleanup_tombstones()

            for message in sorted_messages:
                # 检查消息是否有效
                if message.queue_offset is None:
                    failed_messages.append(message)
                    continue

                # 检查缓存限制
                if not self._check_cache_limits():
                    failed_messages.append(message)
                    continue

                # 检查消息是否已存在（包括墓碑值）
                if message.queue_offset in self._offset_to_index:
                    existing_index: int = self._offset_to_index[message.queue_offset]
                    existing_entry: QueueEntry = self._entries[existing_index]

                    # 如果是墓碑值，直接替换为有效消息
                    if existing_entry.is_tombstone:
                        # 更新统计信息
                        self._count += 1
                        message_body_size: int = (
                            len(message.body) if message.body else 0
                        )
                        self._total_size += message_body_size
                        self._tombstone_count -= 1

                        # 替换条目
                        self._entries[existing_index] = QueueEntry(
                            queue_offset=message.queue_offset,
                            message=message,
                            is_deleted=False,
                        )

                        successful_messages.append(message)
                    else:
                        # 已存在有效消息，视为重复
                        duplicates.append(message)
                        failed_messages.append(message)
                else:
                    # 创建队列条目
                    entry: QueueEntry = QueueEntry(
                        queue_offset=message.queue_offset,
                        message=message,
                        is_deleted=False,
                    )

                    # 使用二分查找插入位置
                    index: int = bisect_left(
                        [x.queue_offset for x in self._entries], entry.queue_offset
                    )

                    # 插入条目
                    self._entries.insert(index, entry)

                    # 更新索引映射：需要更新所有后续条目的索引
                    for i in range(index, len(self._entries)):
                        self._offset_to_index[self._entries[i].queue_offset] = i

                    # 更新统计信息
                    self._count += 1
                    message_body_size = len(message.body) if message.body else 0
                    self._total_size += message_body_size

                    successful_messages.append(message)

            # 更新偏移量范围（批量操作后更新一次）
            self._update_offset_ranges()

            # 返回详细的结果统计
            return {
                "success_count": len(successful_messages),
                "failure_count": len(failed_messages),
                "duplicate_count": len(duplicates),
                "successful_messages": successful_messages,
                "failed_messages": failed_messages,
                "duplicate_messages": duplicates,
                "total_processed": len(messages),
            }

    def remove_message(self, queue_offset: int) -> MessageExt | None:
        """
        移除指定偏移量的消息（使用墓碑值软删除，O(1)性能）

        Args:
            queue_offset: 要移除的消息偏移量

        Returns:
            MessageExt | None: 被移除的消息，如果不存在则返回None
        """
        with self._lock:
            if queue_offset not in self._offset_to_index:
                return None

            index: int = self._offset_to_index[queue_offset]
            entry: QueueEntry = self._entries[index]

            # 检查是否已经是墓碑值
            if entry.is_tombstone:
                return None

            # 获取消息并计算大小（在设置为None之前）
            message: MessageExt = entry.message  # type: ignore
            message_body_size: int = len(message.body) if message.body else 0

            # 软删除：标记为墓碑值（O(1)操作）
            entry.is_deleted = True
            entry.message = None  # 释放消息内存
            self._tombstone_count += 1

            # 更新统计信息
            self._count -= 1
            self._total_size -= message_body_size

            # 更新偏移量范围（只考虑有效消息）
            self._update_offset_ranges()

            # 检查是否需要清理墓碑值
            self._maybe_cleanup_tombstones()

            return message

    def remove_batch_messages(self, queue_offsets: list[int]) -> list[MessageExt]:
        """
        批量移除指定偏移量的消息（使用墓碑值软删除）

        Args:
            queue_offsets: 要移除的消息偏移量列表

        Returns:
            list[MessageExt]: 被移除的消息列表（只返回成功移除的消息）
        """
        with self._lock:
            removed_messages: list[MessageExt] = []

            for queue_offset in queue_offsets:
                if queue_offset not in self._offset_to_index:
                    continue

                index: int = self._offset_to_index[queue_offset]
                entry: QueueEntry = self._entries[index]

                # 检查是否已经是墓碑值
                if entry.is_tombstone:
                    continue

                # 获取消息并计算大小（在设置为None之前）
                message: MessageExt = entry.message  # type: ignore
                message_body_size: int = len(message.body) if message.body else 0

                # 软删除：标记为墓碑值（O(1)操作）
                entry.is_deleted = True
                entry.message = None  # 释放消息内存
                self._tombstone_count += 1

                # 更新统计信息
                self._count -= 1
                self._total_size -= message_body_size

                # 添加到移除列表
                removed_messages.append(message)

            # 更新偏移量范围（只考虑有效消息）
            self._update_offset_ranges()

            # 检查是否需要清理墓碑值（批量删除后检查一次）
            self._maybe_cleanup_tombstones()

            return removed_messages

    def _maybe_cleanup_tombstones(self) -> None:
        """检查是否需要清理墓碑值，如果需要则执行清理"""
        total_entries: int = len(self._entries)
        if total_entries == 0:
            return

        # 绝对值比较
        if self._tombstone_count < self._max_cache_count // 10:
            return

        # 当墓碑值占比超过阈值时执行清理
        if self._tombstone_count / total_entries > self._cleanup_threshold:
            self._cleanup_tombstones()

    def _cleanup_tombstones(self) -> None:
        """清理墓碑值，重建索引（批量操作，摊销O(n)成本）"""
        if not self._entries:
            return

        # 过滤掉墓碑值，重建列表
        valid_entries: list[QueueEntry] = [
            entry for entry in self._entries if not entry.is_tombstone
        ]

        # 重建索引映射
        self._offset_to_index.clear()
        for idx, entry in enumerate(valid_entries):
            self._offset_to_index[entry.queue_offset] = idx

        # 替换原列表
        self._entries = valid_entries
        self._tombstone_count = 0

    def _update_offset_ranges(self) -> None:
        """更新偏移量范围（只考虑有效消息）"""
        if not self._entries or self._count == 0:
            self._min_offset = float("inf")
            self._max_offset = -1
            return

        # 重置范围
        self._min_offset = float("inf")
        self._max_offset = -1

        # 遍历所有条目，找到有效消息的偏移量范围
        for entry in self._entries:
            if not entry.is_tombstone and entry.message is not None:
                if entry.queue_offset < self._min_offset:
                    self._min_offset = entry.queue_offset
                if entry.queue_offset > self._max_offset:
                    self._max_offset = entry.queue_offset

    def get_message(self, queue_offset: int) -> MessageExt | None:
        """
        获取指定偏移量的消息

        Args:
            queue_offset: 消息偏移量

        Returns:
            MessageExt | None: 消息对象，如果不存在或已删除则返回None
        """
        with self._lock:
            if queue_offset not in self._offset_to_index:
                return None

            index: int = self._offset_to_index[queue_offset]
            entry: QueueEntry = self._entries[index]

            # 检查是否为墓碑值
            if entry.is_tombstone:
                return None

            return entry.message

    def contains_message(self, queue_offset: int) -> bool:
        """检查是否包含指定偏移量的消息"""
        with self._lock:
            return queue_offset in self._offset_to_index

    def get_count(self) -> int:
        """获取缓存消息数量"""
        with self._lock:
            return self._count

    def get_total_size(self) -> int:
        """获取缓存总字节数"""
        with self._lock:
            return self._total_size

    def get_total_size_mb(self) -> float:
        """获取缓存总大小（MB）"""
        with self._lock:
            return self._total_size / (1024 * 1024)

    def is_empty(self) -> bool:
        """检查队列是否为空"""
        with self._lock:
            return self._count == 0

    def get_min_offset(self) -> int | None:
        """获取最小偏移量"""
        with self._lock:
            return None if self._min_offset == float("inf") else int(self._min_offset)

    def get_max_offset(self) -> int | None:
        """获取最大偏移量"""
        with self._lock:
            return None if self._max_offset == -1 else self._max_offset

    def clear(self) -> list[MessageExt]:
        """清空队列并返回所有有效消息（排除墓碑值）"""
        with self._lock:
            # 收集所有有效消息（排除墓碑值）
            messages: list[MessageExt] = []
            for entry in self._entries:
                if entry.message is not None and not entry.is_tombstone:
                    messages.append(entry.message)

            self._consuming_orderly_msgs.clear()

            # 清空所有数据
            self._entries.clear()
            self._offset_to_index.clear()
            self._count = 0
            self._total_size = 0
            self._tombstone_count = 0
            self._min_offset = float("inf")
            self._max_offset = -1

            return messages

    def _check_cache_limits(self) -> bool:
        """检查缓存限制"""
        # 检查数量限制
        if self._count >= self._max_cache_count:
            return False

        # 检查大小限制
        if self.get_total_size_mb() >= self._max_cache_size:
            return False

        return True

    def need_flow_control(self) -> bool:
        """检查是否需要限流"""
        return not self._check_cache_limits()

    def get_stats(self) -> dict[str, Any]:
        """获取统计信息（包含墓碑值统计）"""
        with self._lock:
            total_size_mb: float = round(self.get_total_size_mb(), 2)
            min_offset_val: int | None = (
                None if self._min_offset == float("inf") else int(self._min_offset)
            )
            max_offset_val: int | None = (
                None if self._max_offset == -1 else self._max_offset
            )
            max_cache_size_mb: int = self._max_cache_size // (1024 * 1024)
            is_near_limit_count: bool = self._count >= (self._max_cache_count * 0.9)
            is_near_limit_size: bool = self._total_size >= (self._max_cache_size * 0.9)

            # 墓碑值统计
            total_entries: int = len(self._entries)
            tombstone_ratio: float = (
                self._tombstone_count / total_entries if total_entries > 0 else 0.0
            )
            cleanup_needed: bool = tombstone_ratio > self._cleanup_threshold

            # 待提交区统计
            consuming_count: int = len(self._consuming_orderly_msgs)
            consuming_max_offset: int | None = None
            if self._consuming_orderly_msgs:
                consuming_max_offset = max(
                    [entry.message_offset for entry in self._consuming_orderly_msgs]
                )

            return {
                # 有效消息统计
                "count": self._count,
                "total_size_bytes": self._total_size,
                "total_size_mb": total_size_mb,
                "min_offset": min_offset_val,
                "max_offset": max_offset_val,
                # 墓碑值统计
                "total_entries": total_entries,
                "tombstone_count": self._tombstone_count,
                "tombstone_ratio": round(tombstone_ratio, 3),
                "cleanup_needed": cleanup_needed,
                "cleanup_threshold": self._cleanup_threshold,
                # 待提交区统计
                "consuming_count": consuming_count,
                "consuming_max_offset": consuming_max_offset,
                # 配置信息
                "max_cache_count": self._max_cache_count,
                "max_cache_size_mb": max_cache_size_mb,
                "is_near_limit_count": is_near_limit_count,
                "is_near_limit_size": is_near_limit_size,
            }

    def take_messages(self, batch_size: int) -> list[MessageExt]:
        """
        从队列中取出一批消息用于顺序消费处理

        Args:
            batch_size: 要取出的消息数量

        Returns:
            List[MessageExt]: 取出的消息列表
        """
        if batch_size <= 0:
            return []

        taken_messages: list[MessageExt] = []
        newly_taken_entries: list[QueueEntry] = []
        taken_count = 0

        # 使用写锁保证线程安全
        with self._lock:
            # 从_entries头部开始取出有效消息
            remaining_entries: list[QueueEntry] = []

            for entry in self._entries:
                if taken_count >= batch_size:
                    remaining_entries.append(entry)
                    continue

                if not entry.is_tombstone and entry.message:
                    taken_messages.append(entry.message)
                    newly_taken_entries.append(entry)
                    taken_count += 1
                else:
                    # 保留墓碑标记
                    remaining_entries.append(entry)

            # 更新_entries列表
            if taken_count > 0:
                # 更新计数和大小统计
                for entry in newly_taken_entries:
                    if not entry.is_tombstone:
                        self._count -= 1
                        self._total_size -= (
                            len(entry.message.body) if entry.message else 0
                        )

                self._entries = remaining_entries
                # 重新计算偏移量范围
                self._update_offset_ranges()

                # 将新取出的消息添加到_consuming_orderly_msgs并保持有序性
                self._consuming_orderly_msgs.extend(newly_taken_entries)

                # 按 queue_offset 重新排序整个 _consuming_orderly_msgs
                self._consuming_orderly_msgs.sort(key=lambda entry: entry.queue_offset)

        return taken_messages

    def commit(self) -> int:
        """
        提交已处理的消息，返回最大偏移量

        Returns:
            int: 已提交消息的最大偏移量，如果没有消息则返回-1
        """
        if not self._consuming_orderly_msgs:
            return -1

        max_offset = -1

        # 使用写锁保证线程安全
        with self._lock:
            # 计算最大偏移量
            for entry in self._consuming_orderly_msgs:
                if not entry.is_tombstone and entry.message:
                    offset: int = entry.message.queue_offset or -1
                    max_offset = max(offset, max_offset)

            # 清空待提交区
            self._consuming_orderly_msgs.clear()

        return max_offset

    def rollback(self, msgs: list[MessageExt] | None = None) -> int:
        """
        回滚正在处理的消息，将它们重新放回队列头部

        这个方法用于顺序消费处理失败时，将已取出但未成功处理的消息
        重新放回待处理队列，以便后续重新处理。

        Args:
            msgs: 要回滚的特定消息列表。如果为None，则回滚所有待提交区的消息

        Returns:
            int: 回滚的消息数量，如果没有消息则返回0
        """
        if not self._consuming_orderly_msgs:
            return 0

        # 如果没有指定消息，回滚所有待提交区的消息
        if msgs is None:
            return self._rollback_all()

        # 选择性回滚指定的消息
        return self._rollback_specific_messages(msgs)

    def _rollback_all(self) -> int:
        """
        回滚所有待提交区的消息

        Returns:
            int: 回滚的消息数量
        """
        rollback_count = 0

        # 使用写锁保证线程安全
        with self._lock:
            # 将_consuming_orderly_msgs中的消息重新放回_entries头部
            # 需要保持按queue_offset的顺序，所以先按偏移量排序
            sorted_rollback_msgs: list[QueueEntry] = sorted(
                self._consuming_orderly_msgs,
                key=lambda x: x.message_offset,
            )

            # 重新构建_entries列表：回滚消息 + 原有entries
            self._entries = sorted_rollback_msgs + self._entries

            # 恢复计数和大小统计
            for entry in sorted_rollback_msgs:
                if not entry.is_tombstone and entry.message:
                    self._count += 1
                    self._total_size += len(entry.message.body) if entry.message else 0
                    rollback_count += 1

            # 重新构建偏移量到索引的映射
            self._offset_to_index.clear()
            for index, entry in enumerate(self._entries):
                if not entry.is_tombstone and entry.message:
                    offset = entry.message.queue_offset
                    if offset is not None:
                        self._offset_to_index[offset] = index

            # 重新计算偏移量范围
            self._update_offset_ranges()

            # 清空待提交区
            self._consuming_orderly_msgs.clear()

        return rollback_count

    def _rollback_specific_messages(self, msgs: list[MessageExt]) -> int:
        """
        回滚待提交区中指定的消息

        Args:
            msgs: 要回滚的消息列表

        Returns:
            int: 实际回滚的消息数量
        """
        if not msgs:
            return 0

        rollback_count = 0

        # 使用写锁保证线程安全
        with self._lock:
            # 构建要回滚的消息的queue_offset集合
            rollback_offsets = set()
            for msg in msgs:
                if msg.queue_offset is not None:
                    rollback_offsets.add(msg.queue_offset)

            # 找出要回滚的队列条目
            entries_to_rollback: list[QueueEntry] = []
            remaining_consuming_msgs: list[QueueEntry] = []

            for entry in self._consuming_orderly_msgs:
                if (
                    not entry.is_tombstone
                    and entry.message
                    and entry.message.queue_offset in rollback_offsets
                ):
                    entries_to_rollback.append(entry)
                    rollback_count += 1
                else:
                    # 保留在待提交区中的消息
                    remaining_consuming_msgs.append(entry)

            # 如果没有找到要回滚的消息，直接返回
            if not entries_to_rollback:
                return 0

            # 按queue_offset排序要回滚的消息
            entries_to_rollback.sort(key=lambda x: x.message_offset)

            # 重新构建_entries列表：回滚消息 + 原有entries
            self._entries = entries_to_rollback + self._entries

            # 恢复计数和大小统计
            for entry in entries_to_rollback:
                if not entry.is_tombstone and entry.message:
                    self._count += 1
                    self._total_size += len(entry.message.body) if entry.message else 0

            # 重新构建偏移量到索引的映射
            self._offset_to_index.clear()
            for index, entry in enumerate(self._entries):
                if not entry.is_tombstone and entry.message:
                    offset = entry.message.queue_offset
                    if offset is not None:
                        self._offset_to_index[offset] = index

            # 重新计算偏移量范围
            self._update_offset_ranges()

            # 更新待提交区，移除已回滚的消息
            self._consuming_orderly_msgs = remaining_consuming_msgs

        return rollback_count
