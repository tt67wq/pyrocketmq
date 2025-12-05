#!/usr/bin/env python3
"""
AsyncStatsManager单元测试

测试异步统计管理器的主要API功能，使用pytest框架和pytest-asyncio插件
"""

import asyncio
import os
import sys
import time
from collections import deque
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# 设置PYTHONPATH
project_root = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
sys.path.insert(0, os.path.join(project_root, "src"))

from pyrocketmq.consumer.stats_manager import (
    AsyncCallSnapshot,
    AsyncStatsItem,
    AsyncStatsItemSet,
    AsyncStatsManager,
    AsyncStatsSnapshot,
)
from pyrocketmq.model import ConsumeStatus


class TestAsyncStatsSnapshot:
    """AsyncStatsSnapshot测试类"""

    def test_init_default(self):
        """测试默认初始化"""
        snapshot = AsyncStatsSnapshot()
        assert snapshot.sum == 0
        assert snapshot.tps == 0.0
        assert snapshot.avgpt == 0.0

    def test_init_with_values(self):
        """测试带值初始化"""
        snapshot = AsyncStatsSnapshot(sum=100, tps=50.5, avgpt=25.3)
        assert snapshot.sum == 100
        assert snapshot.tps == 50.5
        assert snapshot.avgpt == 25.3

    def test_repr(self):
        """测试字符串表示"""
        snapshot = AsyncStatsSnapshot(sum=100, tps=50.5, avgpt=25.3)
        repr_str = repr(snapshot)
        assert "AsyncStatsSnapshot" in repr_str
        assert "sum=100" in repr_str
        assert "tps=50.50" in repr_str
        assert "avgpt=25.30" in repr_str


class TestAsyncCallSnapshot:
    """AsyncCallSnapshot测试类"""

    def test_init(self):
        """测试初始化"""
        timestamp = int(time.time() * 1000)
        snapshot = AsyncCallSnapshot(timestamp=timestamp, times=5, value=100)

        assert snapshot.timestamp == timestamp
        assert snapshot.times == 5
        assert snapshot.value == 100

    def test_repr(self):
        """测试字符串表示"""
        timestamp = 1640995200000  # 固定时间戳便于测试
        snapshot = AsyncCallSnapshot(timestamp=timestamp, times=5, value=100)
        repr_str = repr(snapshot)

        assert "AsyncCallSnapshot" in repr_str
        assert "timestamp=1640995200000" in repr_str
        assert "times=5" in repr_str
        assert "value=100" in repr_str


class TestAsyncStatsItem:
    """AsyncStatsItem测试类"""

    @pytest.fixture
    async def stats_item(self):
        """创建AsyncStatsItem实例"""
        return AsyncStatsItem("TEST_STATS", "test_topic@test_group")

    @pytest.mark.asyncio
    async def test_init(self, stats_item):
        """测试初始化"""
        assert stats_item.stats_name == "TEST_STATS"
        assert stats_item.stats_key == "test_topic@test_group"
        assert stats_item.value == 0
        assert stats_item.times == 0
        assert len(stats_item.minute_data) == 0
        assert len(stats_item.hour_data) == 0
        assert len(stats_item.day_data) == 0
        assert isinstance(stats_item._lock, asyncio.Lock)

    @pytest.mark.asyncio
    async def test_aadd_value(self, stats_item):
        """测试异步增加值"""
        # 初始状态
        assert stats_item.value == 0
        assert stats_item.times == 0

        # 增加值
        await stats_item.aadd_value(100, 5)
        assert stats_item.value == 100
        assert stats_item.times == 5

        # 再次增加值
        await stats_item.aadd_value(50, 3)
        assert stats_item.value == 150
        assert stats_item.times == 8

    @pytest.mark.asyncio
    async def test_asampling_in_seconds(self, stats_item):
        """测试秒级采样"""
        # 先增加值
        await stats_item.aadd_value(100, 5)

        # 执行秒级采样
        await stats_item.asampling_in_seconds()

        # 检查minute_data中有一个快照
        assert len(stats_item.minute_data) == 1
        snapshot = stats_item.minute_data[0]
        assert isinstance(snapshot, AsyncCallSnapshot)
        assert snapshot.value == 100
        assert snapshot.times == 5
        assert snapshot.timestamp > 0

    @pytest.mark.asyncio
    async def test_asampling_in_minutes(self, stats_item):
        """测试分钟级采样"""
        # 先增加值
        await stats_item.aadd_value(200, 10)

        # 执行分钟级采样
        await stats_item.asampling_in_minutes()

        # 检查hour_data中有一个快照
        assert len(stats_item.hour_data) == 1
        snapshot = stats_item.hour_data[0]
        assert isinstance(snapshot, AsyncCallSnapshot)
        assert snapshot.value == 200
        assert snapshot.times == 10

    @pytest.mark.asyncio
    async def test_asampling_in_hour(self, stats_item):
        """测试小时级采样"""
        # 先增加值
        await stats_item.aadd_value(300, 15)

        # 执行小时级采样
        await stats_item.asampling_in_hour()

        # 检查day_data中有一个快照
        assert len(stats_item.day_data) == 1
        snapshot = stats_item.day_data[0]
        assert isinstance(snapshot, AsyncCallSnapshot)
        assert snapshot.value == 300
        assert snapshot.times == 15

    @pytest.mark.asyncio
    async def test_acompute_stats_insufficient_data(self, stats_item):
        """测试数据不足时的统计计算"""
        # 空数据
        result = await stats_item.acompute_stats(deque())
        assert result.sum == 0
        assert result.tps == 0.0
        assert result.avgpt == 0.0

        # 只有1个数据点
        snapshot = AsyncCallSnapshot(
            timestamp=int(time.time() * 1000), times=5, value=100
        )
        result = await stats_item.acompute_stats(deque([snapshot]))
        assert result.sum == 0
        assert result.tps == 0.0
        assert result.avgpt == 0.0

    @pytest.mark.asyncio
    async def test_acompute_stats_sufficient_data(self, stats_item):
        """测试充足数据时的统计计算"""
        base_time = int(time.time() * 1000)

        # 创建两个快照，间隔1秒
        snapshot1 = AsyncCallSnapshot(timestamp=base_time, times=5, value=100)
        snapshot2 = AsyncCallSnapshot(timestamp=base_time + 1000, times=10, value=200)

        data = deque([snapshot1, snapshot2])
        result = await stats_item.acompute_stats(data)

        assert result.sum == 100  # 200 - 100
        assert result.tps == 100.0  # (100 * 1000) / 1000
        assert result.avgpt == 20.0  # 100 / (10-5) = 100 / 5 = 20.0

    @pytest.mark.asyncio
    async def test_aget_minute_stats(self, stats_item):
        """测试获取分钟级统计"""
        # 先添加一些采样数据
        await stats_item.aadd_value(100, 5)
        await stats_item.asampling_in_seconds()

        # 等待一小段时间后再次采样
        await asyncio.sleep(0.01)
        await stats_item.aadd_value(50, 3)
        await stats_item.asampling_in_seconds()

        result = await stats_item.aget_minute_stats()

        # 验证结果是一个AsyncStatsSnapshot
        assert isinstance(result, AsyncStatsSnapshot)

    @pytest.mark.asyncio
    async def test_aget_hour_stats(self, stats_item):
        """测试获取小时级统计"""
        # 先添加一些采样数据
        await stats_item.aadd_value(200, 10)
        await stats_item.asampling_in_minutes()

        # 等待一小段时间后再次采样
        await asyncio.sleep(0.01)
        await stats_item.aadd_value(100, 5)
        await stats_item.asampling_in_minutes()

        result = await stats_item.aget_hour_stats()

        # 验证结果是一个AsyncStatsSnapshot
        assert isinstance(result, AsyncStatsSnapshot)

    @pytest.mark.asyncio
    async def test_aget_day_stats(self, stats_item):
        """测试获取天级统计"""
        # 先添加一些采样数据
        await stats_item.aadd_value(300, 15)
        await stats_item.asampling_in_hour()

        # 等待一小段时间后再次采样
        await asyncio.sleep(0.01)
        await stats_item.aadd_value(150, 8)
        await stats_item.asampling_in_hour()

        result = await stats_item.aget_day_stats()

        # 验证结果是一个AsyncStatsSnapshot
        assert isinstance(result, AsyncStatsSnapshot)


class TestAsyncStatsItemSet:
    """AsyncStatsItemSet测试类"""

    @pytest.fixture
    async def stats_set(self):
        """创建AsyncStatsItemSet实例"""
        return AsyncStatsItemSet("TEST_SET")

    @pytest.mark.asyncio
    async def test_init(self, stats_set):
        """测试初始化"""
        assert stats_set.stats_name == "TEST_SET"
        assert len(stats_set.stats_items) == 0
        assert stats_set._closed is False
        assert stats_set._sampling_task is None
        assert stats_set._minute_task is None
        assert stats_set._hour_task is None
        assert isinstance(stats_set._lock, asyncio.Lock)

    @pytest.mark.asyncio
    async def test_start_sampling(self, stats_set):
        """测试启动采样"""
        # 启动采样
        await stats_set.start_sampling()

        # 检查任务已创建
        assert stats_set._sampling_task is not None
        assert stats_set._minute_task is not None
        assert stats_set._hour_task is not None
        assert not stats_set._sampling_task.done()
        assert not stats_set._minute_task.done()
        assert not stats_set._hour_task.done()

        # 关闭采样
        await stats_set.ashutdown()

    @pytest.mark.asyncio
    async def test_aadd_value_new_item(self, stats_set):
        """测试添加新统计项的值"""
        key = "test_topic@test_group"

        # 添加值到新键
        await stats_set.aadd_value(key, 100, 5)

        # 验证统计项被创建
        assert key in stats_set.stats_items
        item = stats_set.stats_items[key]
        assert isinstance(item, AsyncStatsItem)
        assert item.stats_name == "TEST_SET"
        assert item.stats_key == key

    @pytest.mark.asyncio
    async def test_aadd_value_existing_item(self, stats_set):
        """测试添加到已存在的统计项"""
        key = "test_topic@test_group"

        # 第一次添加
        await stats_set.aadd_value(key, 100, 5)
        item = stats_set.stats_items[key]
        assert item.value == 100
        assert item.times == 5

        # 第二次添加
        await stats_set.aadd_value(key, 50, 3)
        assert item.value == 150
        assert item.times == 8

    @pytest.mark.asyncio
    async def test_asampling_operations(self, stats_set):
        """测试采样操作"""
        key = "test_topic@test_group"

        # 添加统计项和值
        await stats_set.aadd_value(key, 100, 5)

        # 执行各种采样
        await stats_set._asampling_in_seconds()
        await stats_set._asampling_in_minutes()
        await stats_set._asampling_in_hour()

        # 验证采样数据
        item = stats_set.stats_items[key]
        assert len(item.minute_data) == 1
        assert len(item.hour_data) == 1
        assert len(item.day_data) == 1

    @pytest.mark.asyncio
    async def test_aget_minute_stats_existing_key(self, stats_set):
        """测试获取存在键的分钟级统计"""
        key = "test_topic@test_group"

        # 添加统计项和采样数据
        await stats_set.aadd_value(key, 100, 5)
        await stats_set._asampling_in_seconds()
        await asyncio.sleep(0.01)
        await stats_set.aadd_value(key, 50, 3)
        await stats_set._asampling_in_seconds()

        # 获取统计
        result = await stats_set.aget_minute_stats(key)
        assert isinstance(result, AsyncStatsSnapshot)

    @pytest.mark.asyncio
    async def test_aget_minute_stats_non_existing_key(self, stats_set):
        """测试获取不存在键的分钟级统计"""
        key = "non_existing_topic@test_group"

        result = await stats_set.aget_minute_stats(key)
        assert isinstance(result, AsyncStatsSnapshot)
        assert result.sum == 0
        assert result.tps == 0.0
        assert result.avgpt == 0.0

    @pytest.mark.asyncio
    async def test_aget_hour_stats_existing_key(self, stats_set):
        """测试获取存在键的小时级统计"""
        key = "test_topic@test_group"

        # 添加统计项和采样数据
        await stats_set.aadd_value(key, 200, 10)
        await stats_set._asampling_in_minutes()
        await asyncio.sleep(0.01)
        await stats_set.aadd_value(key, 100, 5)
        await stats_set._asampling_in_minutes()

        # 获取统计
        result = await stats_set.aget_hour_stats(key)
        assert isinstance(result, AsyncStatsSnapshot)

    @pytest.mark.asyncio
    async def test_aget_hour_stats_non_existing_key(self, stats_set):
        """测试获取不存在键的小时级统计"""
        key = "non_existing_topic@test_group"

        result = await stats_set.aget_hour_stats(key)
        assert isinstance(result, AsyncStatsSnapshot)
        assert result.sum == 0
        assert result.tps == 0.0
        assert result.avgpt == 0.0

    @pytest.mark.asyncio
    async def test_ashutdown(self, stats_set):
        """测试关闭统计集合"""
        # 先启动采样
        await stats_set.start_sampling()

        # 确保任务存在
        assert stats_set._sampling_task is not None
        assert stats_set._minute_task is not None
        assert stats_set._hour_task is not None

        # 关闭
        await stats_set.ashutdown()

        # 验证关闭标志
        assert stats_set._closed is True

        # 等待任务完成
        await asyncio.sleep(0.1)

        # 验证任务被取消或完成
        assert stats_set._sampling_task.cancelled() or stats_set._sampling_task.done()
        assert stats_set._minute_task.cancelled() or stats_set._minute_task.done()
        assert stats_set._hour_task.cancelled() or stats_set._hour_task.done()

    @pytest.mark.asyncio
    async def test_ashutdown_without_start(self, stats_set):
        """测试未启动采样的关闭"""
        # 直接关闭未启动的集合
        await stats_set.ashutdown()
        assert stats_set._closed is True


class TestAsyncStatsManager:
    """AsyncStatsManager测试类"""

    @pytest.fixture
    async def stats_manager(self):
        """创建AsyncStatsManager实例"""
        return AsyncStatsManager()

    @pytest.mark.asyncio
    async def test_init(self, stats_manager):
        """测试初始化"""
        assert stats_manager.consume_ok_tps.stats_name == "CONSUME_OK_TPS"
        assert stats_manager.consume_rt.stats_name == "CONSUME_RT"
        assert stats_manager.consume_failed_tps.stats_name == "CONSUME_FAILED_TPS"
        assert stats_manager.pull_tps.stats_name == "PULL_TPS"
        assert stats_manager.pull_rt.stats_name == "PULL_RT"
        assert stats_manager._started is False
        assert isinstance(stats_manager._lock, asyncio.Lock)

    @pytest.mark.asyncio
    async def test_astart(self, stats_manager):
        """测试启动统计管理器"""
        # 启动
        await stats_manager.astart()
        assert stats_manager._started is True

        # 再次启动应该是幂等的
        await stats_manager.astart()
        assert stats_manager._started is True

        # 关闭
        await stats_manager.ashutdown()

    @pytest.mark.asyncio
    async def test_make_key(self, stats_manager):
        """测试生成统计键"""
        group = "test_group"
        topic = "test_topic"
        key = stats_manager._make_key(group, topic)
        assert key == "test_topic@test_group"

    @pytest.mark.asyncio
    async def test_aincrease_pull_rt(self, stats_manager):
        """测试增加拉取响应时间统计"""
        group = "test_group"
        topic = "test_topic"
        rt = 50

        await stats_manager.aincrease_pull_rt(group, topic, rt)

        key = stats_manager._make_key(group, topic)
        # 验证数据被添加到pull_rt统计项中
        assert key in stats_manager.pull_rt.stats_items
        item = stats_manager.pull_rt.stats_items[key]
        assert item.value == rt
        assert item.times == 1

    @pytest.mark.asyncio
    async def test_aincrease_pull_tps(self, stats_manager):
        """测试增加拉取TPS统计"""
        group = "test_group"
        topic = "test_topic"
        msgs = 10

        await stats_manager.aincrease_pull_tps(group, topic, msgs)

        key = stats_manager._make_key(group, topic)
        # 验证数据被添加到pull_tps统计项中
        assert key in stats_manager.pull_tps.stats_items
        item = stats_manager.pull_tps.stats_items[key]
        assert item.value == msgs
        assert item.times == 1

    @pytest.mark.asyncio
    async def test_aincrease_consume_rt(self, stats_manager):
        """测试增加消费响应时间统计"""
        group = "test_group"
        topic = "test_topic"
        rt = 100

        await stats_manager.aincrease_consume_rt(group, topic, rt)

        key = stats_manager._make_key(group, topic)
        # 验证数据被添加到consume_rt统计项中
        assert key in stats_manager.consume_rt.stats_items
        item = stats_manager.consume_rt.stats_items[key]
        assert item.value == rt
        assert item.times == 1

    @pytest.mark.asyncio
    async def test_aincrease_consume_ok_tps(self, stats_manager):
        """测试增加消费成功TPS统计"""
        group = "test_group"
        topic = "test_topic"
        msgs = 8

        await stats_manager.aincrease_consume_ok_tps(group, topic, msgs)

        key = stats_manager._make_key(group, topic)
        # 验证数据被添加到consume_ok_tps统计项中
        assert key in stats_manager.consume_ok_tps.stats_items
        item = stats_manager.consume_ok_tps.stats_items[key]
        assert item.value == msgs
        assert item.times == 1

    @pytest.mark.asyncio
    async def test_aincrease_consume_failed_tps(self, stats_manager):
        """测试增加消费失败TPS统计"""
        group = "test_group"
        topic = "test_topic"
        msgs = 2

        await stats_manager.aincrease_consume_failed_tps(group, topic, msgs)

        key = stats_manager._make_key(group, topic)
        # 验证数据被添加到consume_failed_tps统计项中
        assert key in stats_manager.consume_failed_tps.stats_items
        item = stats_manager.consume_failed_tps.stats_items[key]
        assert item.value == msgs
        assert item.times == 1

    @pytest.mark.asyncio
    async def test_aget_consume_status_empty(self, stats_manager):
        """测试获取空的消费状态"""
        group = "test_group"
        topic = "test_topic"

        # 不添加任何统计数据
        status = await stats_manager.aget_consume_status(group, topic)

        assert isinstance(status, ConsumeStatus)
        assert status.pull_rt == 0.0
        assert status.pull_tps == 0.0
        assert status.consume_rt == 0.0
        assert status.consume_ok_tps == 0.0
        assert status.consume_failed_tps == 0.0
        assert status.consume_failed_msgs == 0

    @pytest.mark.asyncio
    async def test_aget_consume_status_with_data(self, stats_manager):
        """测试获取有数据的消费状态"""
        group = "test_group"
        topic = "test_topic"

        # 启动统计管理器
        await stats_manager.astart()

        # 添加各种统计数据
        await stats_manager.aincrease_pull_rt(group, topic, 50)
        await stats_manager.aincrease_pull_tps(group, topic, 10)
        await stats_manager.aincrease_consume_rt(group, topic, 100)
        await stats_manager.aincrease_consume_ok_tps(group, topic, 8)
        await stats_manager.aincrease_consume_failed_tps(group, topic, 2)

        # 等待采样
        await asyncio.sleep(0.1)

        # 获取消费状态
        status = await stats_manager.aget_consume_status(group, topic)

        assert isinstance(status, ConsumeStatus)
        # 注意：由于采样可能还未完全完成，这里只验证返回的是有效的ConsumeStatus对象
        assert isinstance(status.pull_rt, float)
        assert isinstance(status.pull_tps, float)
        assert isinstance(status.consume_rt, float)
        assert isinstance(status.consume_ok_tps, float)
        assert isinstance(status.consume_failed_tps, float)
        assert isinstance(status.consume_failed_msgs, int)

        # 关闭
        await stats_manager.ashutdown()

    @pytest.mark.asyncio
    async def test_aget_consume_status_fallback_to_hour(self, stats_manager):
        """测试消费状态回退到小时级数据"""
        group = "test_group"
        topic = "test_topic"

        # 启动统计管理器
        await stats_manager.astart()

        # 添加消费响应时间数据
        await stats_manager.aincrease_consume_rt(group, topic, 100)

        # 手动执行小时级采样而不执行分钟级采样
        key = stats_manager._make_key(group, topic)
        if key in stats_manager.consume_rt.stats_items:
            await stats_manager.consume_rt.stats_items[key].asampling_in_hour()

        # 获取消费状态（分钟级数据为空，应该回退到小时级）
        status = await stats_manager.aget_consume_status(group, topic)

        assert isinstance(status, ConsumeStatus)
        assert isinstance(status.consume_rt, float)

        # 关闭
        await stats_manager.ashutdown()

    @pytest.mark.asyncio
    async def test_ashutdown(self, stats_manager):
        """测试关闭统计管理器"""
        # 先启动并添加一些数据
        await stats_manager.astart()
        await stats_manager.aincrease_pull_tps("test_group", "test_topic", 10)

        # 关闭
        await stats_manager.ashutdown()

        # 验证所有统计集合都被关闭
        assert stats_manager.consume_ok_tps._closed is True
        assert stats_manager.consume_rt._closed is True
        assert stats_manager.consume_failed_tps._closed is True
        assert stats_manager.pull_tps._closed is True
        assert stats_manager.pull_rt._closed is True

    @pytest.mark.asyncio
    async def test_concurrent_operations(self, stats_manager):
        """测试并发操作"""
        group = "test_group"
        topic = "test_topic"

        # 启动统计管理器
        await stats_manager.astart()

        # 并发执行各种操作
        tasks = []
        for i in range(10):
            tasks.extend(
                [
                    stats_manager.aincrease_pull_rt(group, topic, 50 + i),
                    stats_manager.aincrease_pull_tps(group, topic, 10 + i),
                    stats_manager.aincrease_consume_rt(group, topic, 100 + i),
                    stats_manager.aincrease_consume_ok_tps(group, topic, 8 + i),
                    stats_manager.aincrease_consume_failed_tps(group, topic, i % 3),
                ]
            )

        # 等待所有操作完成
        await asyncio.gather(*tasks)

        # 等待采样
        await asyncio.sleep(0.1)

        # 验证数据被正确添加
        key = stats_manager._make_key(group, topic)

        pull_rt_item = stats_manager.pull_rt.stats_items.get(key)
        if pull_rt_item:
            assert pull_rt_item.times == 10  # 10次操作
            assert pull_rt_item.value == sum(50 + i for i in range(10))

        pull_tps_item = stats_manager.pull_tps.stats_items.get(key)
        if pull_tps_item:
            assert pull_tps_item.times == 10
            assert pull_tps_item.value == sum(10 + i for i in range(10))

        # 关闭
        await stats_manager.ashutdown()

    @pytest.mark.asyncio
    async def test_multiple_groups_topics(self, stats_manager):
        """测试多个消费者组和主题"""
        await stats_manager.astart()

        # 添加多个消费者组和主题的统计数据
        groups = ["group1", "group2", "group3"]
        topics = ["topic1", "topic2"]

        tasks = []
        for group in groups:
            for topic in topics:
                tasks.extend(
                    [
                        stats_manager.aincrease_pull_rt(group, topic, 50),
                        stats_manager.aincrease_pull_tps(group, topic, 10),
                        stats_manager.aincrease_consume_rt(group, topic, 100),
                        stats_manager.aincrease_consume_ok_tps(group, topic, 8),
                        stats_manager.aincrease_consume_failed_tps(group, topic, 2),
                    ]
                )

        await asyncio.gather(*tasks)
        await asyncio.sleep(0.1)

        # 验证每个组合都有统计数据
        for group in groups:
            for topic in topics:
                key = stats_manager._make_key(group, topic)
                status = await stats_manager.aget_consume_status(group, topic)
                assert isinstance(status, ConsumeStatus)

                # 验证统计项被创建
                assert key in stats_manager.pull_rt.stats_items
                assert key in stats_manager.pull_tps.stats_items
                assert key in stats_manager.consume_rt.stats_items
                assert key in stats_manager.consume_ok_tps.stats_items
                assert key in stats_manager.consume_failed_tps.stats_items

        await stats_manager.ashutdown()

    @pytest.mark.asyncio
    async def test_data_types_validation(self, stats_manager):
        """测试数据类型验证"""
        group = "test_group"
        topic = "test_topic"

        # 测试字符串消息数被正确转换为整数
        await stats_manager.aincrease_pull_tps(group, topic, "10")
        await stats_manager.aincrease_consume_ok_tps(group, topic, "8")
        await stats_manager.aincrease_consume_failed_tps(group, topic, "2")

        key = stats_manager._make_key(group, topic)

        # 验证数据被正确添加
        assert stats_manager.pull_tps.stats_items[key].value == 10
        assert stats_manager.consume_ok_tps.stats_items[key].value == 8
        assert stats_manager.consume_failed_tps.stats_items[key].value == 2


class TestIntegration:
    """集成测试"""

    @pytest.mark.asyncio
    async def test_full_workflow(self):
        """测试完整工作流程"""
        # 创建统计管理器
        stats_manager = AsyncStatsManager()

        # 启动
        await stats_manager.astart()

        # 模拟消费场景
        group = "consumer_group_1"
        topic = "order_topic"

        # 模拟一段时间内的统计数据
        for i in range(50):
            # 拉取消息
            await stats_manager.aincrease_pull_tps(group, topic, 10)
            await stats_manager.aincrease_pull_rt(group, topic, 45 + i % 20)

            # 消费消息
            await stats_manager.aincrease_consume_ok_tps(group, topic, 8)
            await stats_manager.aincrease_consume_rt(group, topic, 95 + i % 30)

            # 偶尔有失败
            if i % 10 == 0:
                await stats_manager.aincrease_consume_failed_tps(group, topic, 2)

            await asyncio.sleep(0.001)  # 短暂等待

        # 等待采样
        await asyncio.sleep(0.2)

        # 获取消费状态
        status = await stats_manager.aget_consume_status(group, topic)

        # 验证状态
        assert isinstance(status, ConsumeStatus)
        assert status.pull_rt >= 0
        assert status.pull_tps >= 0
        assert status.consume_rt >= 0
        assert status.consume_ok_tps >= 0
        assert status.consume_failed_tps >= 0
        assert status.consume_failed_msgs >= 0

        # 关闭
        await stats_manager.ashutdown()


if __name__ == "__main__":
    # 运行测试
    pytest.main([__file__, "-v"])
