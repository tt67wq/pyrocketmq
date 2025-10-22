"""
Broker管理器模块的单元测试

该测试模块包含对BrokerManager和SyncBrokerManager的全面测试，覆盖：
- Broker管理器的生命周期管理
- Broker的添加和移除
- 连接的获取和释放
- 健康检查和故障检测
- 统计信息收集
- 并发安全性

运行测试：
    export PYTHONPATH=/Users/admin/Project/Python/pyrocketmq/src
    python -m pytest tests/producer/test_broker_manager.py -v
"""

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pyrocketmq.broker.broker_manager import (
    BrokerConnectionInfo,
    BrokerManager,
    BrokerState,
    SyncBrokerManager,
)
from pyrocketmq.remote.config import RemoteConfig
from pyrocketmq.transport.config import TransportConfig


class TestBrokerConnectionInfo:
    """测试BrokerConnectionInfo类"""

    def test_broker_connection_info_creation(self):
        """测试Broker连接信息创建"""
        info = BrokerConnectionInfo(
            broker_addr="localhost:10911", broker_name="broker-a"
        )

        assert info.broker_addr == "localhost:10911"
        assert info.broker_name == "broker-a"
        assert info.state == BrokerState.UNKNOWN
        assert info.total_requests == 0
        assert info.failed_requests == 0
        assert info.consecutive_failures == 0

    def test_broker_connection_info_auto_broker_name(self):
        """测试自动提取broker名称"""
        info = BrokerConnectionInfo(broker_addr="localhost:10911")

        assert info.broker_name == "localhost"

    def test_broker_connection_info_empty_addr(self):
        """测试空地址的异常处理"""
        with pytest.raises(ValueError, match="broker_addr cannot be empty"):
            BrokerConnectionInfo(broker_addr="")

    def test_success_rate_calculation(self):
        """测试成功率计算"""
        info = BrokerConnectionInfo(
            broker_addr="localhost:10911", broker_name="broker-a"
        )

        # 初始状态应该有100%成功率
        assert info.success_rate == 1.0
        assert info.failure_rate == 0.0

        # 添加一些请求统计
        info.total_requests = 10
        info.failed_requests = 2

        assert info.success_rate == 0.8
        # 使用近似比较处理浮点数精度问题
        assert abs(info.failure_rate - 0.2) < 1e-10

    def test_update_request_stats(self):
        """测试更新请求统计"""
        info = BrokerConnectionInfo(
            broker_addr="localhost:10911", broker_name="broker-a"
        )

        # 记录一次成功的请求
        info.update_request_stats(True, 100.0)

        assert info.total_requests == 1
        assert info.failed_requests == 0
        assert info.avg_response_time == 10.0  # 0.1 * 100.0 + 0.9 * 0.0
        assert info.consecutive_failures == 0

        # 记录一次失败的请求
        info.update_request_stats(False, 50.0)

        assert info.total_requests == 2
        assert info.failed_requests == 1
        assert info.consecutive_failures == 0
        assert (
            abs(info.avg_response_time - 14.0) < 0.001
        )  # 0.1 * 50.0 + 0.9 * 10.0

        # 测试平均响应时间的指数移动平均
        info.update_request_stats(True, 200.0)
        expected_avg = (
            0.1 * 200.0 + 0.9 * 14.0
        )  # alpha=0.1，基于前一次的平均值14.0
        assert abs(info.avg_response_time - expected_avg) < 0.001

    def test_reset_stats(self):
        """测试重置统计信息"""
        info = BrokerConnectionInfo(
            broker_addr="localhost:10911", broker_name="broker-a"
        )

        # 设置一些统计数据
        info.total_requests = 100
        info.failed_requests = 10
        info.avg_response_time = 150.0
        info.consecutive_failures = 5

        # 重置统计
        info.reset_stats()

        assert info.total_requests == 0
        assert info.failed_requests == 0
        assert info.avg_response_time == 0.0
        assert info.consecutive_failures == 0


class TestBrokerManager:
    """测试BrokerManager类"""

    @pytest.fixture
    def broker_manager(self):
        """Broker管理器fixture"""
        return BrokerManager(
            remote_config=RemoteConfig(rpc_timeout=5.0),
            transport_config=TransportConfig(),
            health_check_interval=1.0,
            health_check_timeout=2.0,
            max_consecutive_failures=2,
            connection_pool_size=2,
        )

    @pytest.mark.asyncio
    async def test_broker_manager_creation(self, broker_manager):
        """测试Broker管理器创建"""
        assert broker_manager.health_check_interval == 1.0
        assert broker_manager.max_consecutive_failures == 2
        assert broker_manager.connection_pool_size == 2
        assert broker_manager.brokers_count == 0
        assert not broker_manager.is_running

    @pytest.mark.asyncio
    async def test_add_broker(self, broker_manager):
        """测试添加Broker"""
        await broker_manager.add_broker("localhost:10911", "broker-a")

        assert broker_manager.brokers_count == 1
        assert "localhost:10911" in broker_manager._brokers
        assert "localhost:10911" in broker_manager._broker_pools

        broker_info = broker_manager._brokers["localhost:10911"]
        assert broker_info.broker_addr == "localhost:10911"
        assert broker_info.broker_name == "broker-a"
        assert broker_info.state == BrokerState.UNKNOWN

    @pytest.mark.asyncio
    async def test_add_broker_auto_name(self, broker_manager):
        """测试添加Broker并自动提取名称"""
        await broker_manager.add_broker("localhost:10911")

        broker_info = broker_manager._brokers["localhost:10911"]
        assert broker_info.broker_name == "localhost"

    @pytest.mark.asyncio
    async def test_add_duplicate_broker(self, broker_manager):
        """测试添加重复的Broker"""
        await broker_manager.add_broker("localhost:10911", "broker-a")
        await broker_manager.add_broker("localhost:10911", "broker-a")

        assert broker_manager.brokers_count == 1

    @pytest.mark.asyncio
    async def test_remove_broker(self, broker_manager):
        """测试移除Broker"""
        await broker_manager.add_broker("localhost:10911", "broker-a")
        assert broker_manager.brokers_count == 1

        await broker_manager.remove_broker("localhost:10911")
        assert broker_manager.brokers_count == 0
        assert "localhost:10911" not in broker_manager._brokers
        assert "localhost:10911" not in broker_manager._broker_pools

    @pytest.mark.asyncio
    async def test_remove_nonexistent_broker(self, broker_manager):
        """测试移除不存在的Broker"""
        await broker_manager.remove_broker("localhost:10911")  # 应该不抛出异常
        assert broker_manager.brokers_count == 0

    @pytest.mark.asyncio
    async def test_start_and_shutdown(self, broker_manager):
        """测试启动和关闭"""
        assert not broker_manager.is_running

        await broker_manager.start()
        assert broker_manager.is_running

        await broker_manager.shutdown()
        assert not broker_manager.is_running

    @pytest.mark.asyncio
    async def test_get_connection(self, broker_manager):
        """测试获取连接"""
        with patch(
            "pyrocketmq.broker.broker_manager.BrokerConnectionPool"
        ) as mock_pool_class:
            mock_pool = AsyncMock()
            mock_connection = AsyncMock()
            mock_pool.get_connection.return_value = mock_connection
            mock_pool_class.return_value = mock_pool

            await broker_manager.add_broker("localhost:10911", "broker-a")

            # 设置broker为健康状态
            broker_info = broker_manager._brokers["localhost:10911"]
            broker_info.state = BrokerState.HEALTHY

            connection = await broker_manager.get_connection("localhost:10911")

            assert connection == mock_connection
            mock_pool.get_connection.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_connection_failed_broker(self, broker_manager):
        """测试从故障Broker获取连接"""
        await broker_manager.add_broker("localhost:10911", "broker-a")

        # 设置broker为故障状态
        broker_info = broker_manager._brokers["localhost:10911"]
        broker_info.state = BrokerState.FAILED

        with pytest.raises(Exception, match="Broker不可用"):
            await broker_manager.get_connection("localhost:10911")

    @pytest.mark.asyncio
    async def test_get_connection_nonexistent_broker(self, broker_manager):
        """测试从不存在的Broker获取连接"""
        with pytest.raises(Exception, match="Broker不存在"):
            await broker_manager.get_connection("localhost:10911")

    @pytest.mark.asyncio
    async def test_release_connection(self, broker_manager):
        """测试释放连接"""
        with patch(
            "pyrocketmq.broker.broker_manager.BrokerConnectionPool"
        ) as mock_pool_class:
            mock_pool = AsyncMock()
            mock_connection = AsyncMock()
            mock_pool_class.return_value = mock_pool

            await broker_manager.add_broker("localhost:10911", "broker-a")

            await broker_manager.release_connection(
                "localhost:10911", mock_connection
            )

            mock_pool.release_connection.assert_called_once_with(
                mock_connection
            )

    def test_get_healthy_brokers(self, broker_manager):
        """测试获取健康的Broker列表"""
        # 直接操作_brokers字典，因为这是同步方法
        broker_manager._brokers["localhost:10911"] = BrokerConnectionInfo(
            broker_addr="localhost:10911",
            broker_name="broker-a",
            state=BrokerState.HEALTHY,
        )
        broker_manager._brokers["localhost:10912"] = BrokerConnectionInfo(
            broker_addr="localhost:10912",
            broker_name="broker-b",
            state=BrokerState.FAILED,
        )
        broker_manager._brokers["localhost:10913"] = BrokerConnectionInfo(
            broker_addr="localhost:10913",
            broker_name="broker-c",
            state=BrokerState.UNHEALTHY,
        )

        healthy_brokers = broker_manager.get_healthy_brokers()

        assert healthy_brokers == ["localhost:10911"]

    def test_get_available_brokers(self, broker_manager):
        """测试获取可用的Broker列表"""
        # 直接操作_brokers字典
        broker_manager._brokers["localhost:10911"] = BrokerConnectionInfo(
            broker_addr="localhost:10911",
            broker_name="broker-a",
            state=BrokerState.HEALTHY,
        )
        broker_manager._brokers["localhost:10912"] = BrokerConnectionInfo(
            broker_addr="localhost:10912",
            broker_name="broker-b",
            state=BrokerState.FAILED,
        )
        broker_manager._brokers["localhost:10913"] = BrokerConnectionInfo(
            broker_addr="localhost:10913",
            broker_name="broker-c",
            state=BrokerState.RECOVERING,
        )

        available_brokers = broker_manager.get_available_brokers()

        assert set(available_brokers) == {"localhost:10911", "localhost:10913"}

    def test_get_broker_stats(self, broker_manager):
        """测试获取Broker统计信息"""
        with patch(
            "pyrocketmq.broker.broker_manager.BrokerConnectionPool"
        ) as mock_pool_class:
            mock_pool = MagicMock()
            mock_pool.get_stats.return_value = {
                "active_connections": 2,
                "available_connections": 1,
                "total_connections": 3,
            }
            mock_pool_class.return_value = mock_pool

            # 手动添加broker信息
            broker_info = BrokerConnectionInfo(
                broker_addr="localhost:10911",
                broker_name="broker-a",
                state=BrokerState.HEALTHY,
            )
            broker_info.total_requests = 100
            broker_info.failed_requests = 5
            broker_manager._brokers["localhost:10911"] = broker_info
            broker_manager._broker_pools["localhost:10911"] = mock_pool

            stats = broker_manager.get_broker_stats("localhost:10911")

            assert stats is not None
            assert stats["broker_addr"] == "localhost:10911"
            assert stats["broker_name"] == "broker-a"
            assert stats["state"] == "healthy"
            assert stats["total_requests"] == 100
            assert stats["failed_requests"] == 5
            assert stats["success_rate"] == 0.95
            assert "connection_pool" in stats

    def test_get_broker_stats_nonexistent(self, broker_manager):
        """测试获取不存在Broker的统计信息"""
        stats = broker_manager.get_broker_stats("localhost:10911")
        assert stats is None

    def test_get_all_brokers_stats(self, broker_manager):
        """测试获取所有Broker统计信息"""
        with patch(
            "pyrocketmq.broker.broker_manager.BrokerConnectionPool"
        ) as mock_pool_class:
            mock_pool = MagicMock()
            mock_pool.get_stats.return_value = {"total_connections": 1}
            mock_pool_class.return_value = mock_pool

            # 手动添加多个broker
            for i in range(2):
                broker_addr = f"localhost:1091{i + 1}"
                broker_info = BrokerConnectionInfo(
                    broker_addr=broker_addr,
                    broker_name=f"broker-{chr(97 + i)}",
                    state=BrokerState.HEALTHY,
                )
                broker_manager._brokers[broker_addr] = broker_info
                broker_manager._broker_pools[broker_addr] = mock_pool

            all_stats = broker_manager.get_all_brokers_stats()

            assert len(all_stats) == 2
            assert "localhost:10911" in all_stats
            assert "localhost:10912" in all_stats

    @pytest.mark.asyncio
    async def test_health_check_integration(self, broker_manager):
        """测试健康检查集成"""
        with patch(
            "pyrocketmq.broker.broker_manager.BrokerConnectionPool"
        ) as mock_pool_class:
            mock_pool = AsyncMock()
            mock_pool.health_check.return_value = True
            mock_pool_class.return_value = mock_pool

            await broker_manager.add_broker("localhost:10911", "broker-a")

            # 启动健康检查
            await broker_manager.start()

            # 等待一次健康检查
            await asyncio.sleep(1.5)

            # 检查状态是否更新为健康
            broker_info = broker_manager._brokers["localhost:10911"]
            assert broker_info.state == BrokerState.HEALTHY

            await broker_manager.shutdown()

    @pytest.mark.asyncio
    async def test_health_check_failure_mark_as_failed(self, broker_manager):
        """测试健康检查失败并标记为故障"""
        with patch(
            "pyrocketmq.broker.broker_manager.BrokerConnectionPool"
        ) as mock_pool_class:
            mock_pool = AsyncMock()
            mock_pool.health_check.return_value = False
            mock_pool_class.return_value = mock_pool

            await broker_manager.add_broker("localhost:10911", "broker-a")

            # 设置较小的失败次数限制以便快速测试
            broker_manager.max_consecutive_failures = 1

            # 启动健康检查
            await broker_manager.start()

            # 等待一次健康检查
            await asyncio.sleep(1.5)

            # 检查状态是否更新为故障
            broker_info = broker_manager._brokers["localhost:10911"]
            assert broker_info.state == BrokerState.FAILED

            await broker_manager.shutdown()

    @pytest.mark.asyncio
    async def test_connection_failure_detection(self, broker_manager):
        """测试连接失败检测"""
        with patch(
            "pyrocketmq.broker.broker_manager.BrokerConnectionPool"
        ) as mock_pool_class:
            mock_pool = AsyncMock()
            mock_pool.get_connection.side_effect = Exception("连接失败")
            mock_pool_class.return_value = mock_pool

            await broker_manager.add_broker("localhost:10911", "broker-a")

            # 设置broker为健康状态
            broker_info = broker_manager._brokers["localhost:10911"]
            broker_info.state = BrokerState.HEALTHY

            # 尝试获取连接，应该失败
            with pytest.raises(Exception, match="无法获取连接"):
                await broker_manager.get_connection("localhost:10911")

            # 检查失败次数是否增加
            assert broker_info.consecutive_failures == 1

    def test_properties(self, broker_manager):
        """测试属性访问器"""
        assert not broker_manager.is_running
        assert broker_manager.brokers_count == 0
        assert broker_manager.healthy_brokers_count == 0


class TestSyncBrokerManager:
    """测试SyncBrokerManager类"""

    @pytest.fixture
    def sync_broker_manager(self):
        """同步Broker管理器fixture"""
        return SyncBrokerManager(
            remote_config=RemoteConfig(rpc_timeout=5.0),
            transport_config=TransportConfig(),
            health_check_interval=1.0,
            health_check_timeout=2.0,
            max_consecutive_failures=2,
            connection_pool_size=2,
        )

    def test_sync_broker_manager_creation(self, sync_broker_manager):
        """测试同步Broker管理器创建"""
        assert sync_broker_manager.health_check_interval == 1.0
        assert sync_broker_manager.max_consecutive_failures == 2
        assert sync_broker_manager.connection_pool_size == 2
        assert sync_broker_manager.brokers_count == 0
        assert not sync_broker_manager.is_running

    def test_add_broker(self, sync_broker_manager):
        """测试添加Broker"""
        sync_broker_manager.add_broker("localhost:10911", "broker-a")

        assert sync_broker_manager.brokers_count == 1
        assert "localhost:10911" in sync_broker_manager._brokers
        assert "localhost:10911" in sync_broker_manager._broker_pools

        broker_info = sync_broker_manager._brokers["localhost:10911"]
        assert broker_info.broker_addr == "localhost:10911"
        assert broker_info.broker_name == "broker-a"
        assert broker_info.state == BrokerState.UNKNOWN

    def test_add_broker_auto_name(self, sync_broker_manager):
        """测试添加Broker并自动提取名称"""
        sync_broker_manager.add_broker("localhost:10911")

        broker_info = sync_broker_manager._brokers["localhost:10911"]
        assert broker_info.broker_name == "localhost"

    def test_add_duplicate_broker(self, sync_broker_manager):
        """测试添加重复的Broker"""
        sync_broker_manager.add_broker("localhost:10911", "broker-a")
        sync_broker_manager.add_broker("localhost:10911", "broker-a")

        assert sync_broker_manager.brokers_count == 1

    def test_remove_broker(self, sync_broker_manager):
        """测试移除Broker"""
        sync_broker_manager.add_broker("localhost:10911", "broker-a")
        assert sync_broker_manager.brokers_count == 1

        sync_broker_manager.remove_broker("localhost:10911")
        assert sync_broker_manager.brokers_count == 0
        assert "localhost:10911" not in sync_broker_manager._brokers
        assert "localhost:10911" not in sync_broker_manager._broker_pools

    def test_remove_nonexistent_broker(self, sync_broker_manager):
        """测试移除不存在的Broker"""
        sync_broker_manager.remove_broker("localhost:10911")  # 应该不抛出异常
        assert sync_broker_manager.brokers_count == 0

    def test_start_and_shutdown(self, sync_broker_manager):
        """测试启动和关闭"""
        assert not sync_broker_manager.is_running

        sync_broker_manager.start()
        assert sync_broker_manager.is_running

        sync_broker_manager.shutdown()
        assert not sync_broker_manager.is_running

    def test_get_connection(self, sync_broker_manager):
        """测试获取连接"""
        with patch(
            "pyrocketmq.broker.broker_manager.SyncBrokerConnectionPool"
        ) as mock_pool_class:
            mock_pool = MagicMock()
            mock_connection = MagicMock()
            mock_pool.get_connection.return_value = mock_connection
            mock_pool_class.return_value = mock_pool

            sync_broker_manager.add_broker("localhost:10911", "broker-a")

            # 设置broker为健康状态
            broker_info = sync_broker_manager._brokers["localhost:10911"]
            broker_info.state = BrokerState.HEALTHY

            connection = sync_broker_manager.get_connection("localhost:10911")

            assert connection == mock_connection
            mock_pool.get_connection.assert_called_once()

    def test_get_connection_failed_broker(self, sync_broker_manager):
        """测试从故障Broker获取连接"""
        sync_broker_manager.add_broker("localhost:10911", "broker-a")

        # 设置broker为故障状态
        broker_info = sync_broker_manager._brokers["localhost:10911"]
        broker_info.state = BrokerState.FAILED

        with pytest.raises(Exception, match="Broker不可用"):
            sync_broker_manager.get_connection("localhost:10911")

    def test_get_connection_nonexistent_broker(self, sync_broker_manager):
        """测试从不存在的Broker获取连接"""
        with pytest.raises(Exception, match="Broker不存在"):
            sync_broker_manager.get_connection("localhost:10911")

    def test_release_connection(self, sync_broker_manager):
        """测试释放连接"""
        with patch(
            "pyrocketmq.broker.broker_manager.SyncBrokerConnectionPool"
        ) as mock_pool_class:
            mock_pool = MagicMock()
            mock_connection = MagicMock()
            mock_pool_class.return_value = mock_pool

            sync_broker_manager.add_broker("localhost:10911", "broker-a")

            sync_broker_manager.release_connection(
                "localhost:10911", mock_connection
            )

            mock_pool.release_connection.assert_called_once_with(
                mock_connection
            )

    def test_get_healthy_brokers(self, sync_broker_manager):
        """测试获取健康的Broker列表"""
        # 直接操作_brokers字典
        sync_broker_manager._brokers["localhost:10911"] = BrokerConnectionInfo(
            broker_addr="localhost:10911",
            broker_name="broker-a",
            state=BrokerState.HEALTHY,
        )
        sync_broker_manager._brokers["localhost:10912"] = BrokerConnectionInfo(
            broker_addr="localhost:10912",
            broker_name="broker-b",
            state=BrokerState.FAILED,
        )
        sync_broker_manager._brokers["localhost:10913"] = BrokerConnectionInfo(
            broker_addr="localhost:10913",
            broker_name="broker-c",
            state=BrokerState.UNHEALTHY,
        )

        healthy_brokers = sync_broker_manager.get_healthy_brokers()

        assert healthy_brokers == ["localhost:10911"]

    def test_get_available_brokers(self, sync_broker_manager):
        """测试获取可用的Broker列表"""
        # 直接操作_brokers字典
        sync_broker_manager._brokers["localhost:10911"] = BrokerConnectionInfo(
            broker_addr="localhost:10911",
            broker_name="broker-a",
            state=BrokerState.HEALTHY,
        )
        sync_broker_manager._brokers["localhost:10912"] = BrokerConnectionInfo(
            broker_addr="localhost:10912",
            broker_name="broker-b",
            state=BrokerState.FAILED,
        )
        sync_broker_manager._brokers["localhost:10913"] = BrokerConnectionInfo(
            broker_addr="localhost:10913",
            broker_name="broker-c",
            state=BrokerState.RECOVERING,
        )

        available_brokers = sync_broker_manager.get_available_brokers()

        assert set(available_brokers) == {"localhost:10911", "localhost:10913"}

    def test_get_broker_stats(self, sync_broker_manager):
        """测试获取Broker统计信息"""
        with patch(
            "pyrocketmq.broker.broker_manager.SyncBrokerConnectionPool"
        ) as mock_pool_class:
            mock_pool = MagicMock()
            mock_pool.get_stats.return_value = {
                "active_connections": 2,
                "available_connections": 1,
                "total_connections": 3,
            }
            mock_pool_class.return_value = mock_pool

            # 手动添加broker信息
            broker_info = BrokerConnectionInfo(
                broker_addr="localhost:10911",
                broker_name="broker-a",
                state=BrokerState.HEALTHY,
            )
            broker_info.total_requests = 100
            broker_info.failed_requests = 5
            sync_broker_manager._brokers["localhost:10911"] = broker_info
            sync_broker_manager._broker_pools["localhost:10911"] = mock_pool

            stats = sync_broker_manager.get_broker_stats("localhost:10911")

            assert stats is not None
            assert stats["broker_addr"] == "localhost:10911"
            assert stats["broker_name"] == "broker-a"
            assert stats["state"] == "healthy"
            assert stats["total_requests"] == 100
            assert stats["failed_requests"] == 5
            assert stats["success_rate"] == 0.95
            assert "connection_pool" in stats

    def test_get_broker_stats_nonexistent(self, sync_broker_manager):
        """测试获取不存在Broker的统计信息"""
        stats = sync_broker_manager.get_broker_stats("localhost:10911")
        assert stats is None

    def test_get_all_brokers_stats(self, sync_broker_manager):
        """测试获取所有Broker统计信息"""
        with patch(
            "pyrocketmq.broker.broker_manager.SyncBrokerConnectionPool"
        ) as mock_pool_class:
            mock_pool = MagicMock()
            mock_pool.get_stats.return_value = {"total_connections": 1}
            mock_pool_class.return_value = mock_pool

            # 手动添加多个broker
            for i in range(2):
                broker_addr = f"localhost:1091{i + 1}"
                broker_info = BrokerConnectionInfo(
                    broker_addr=broker_addr,
                    broker_name=f"broker-{chr(97 + i)}",
                    state=BrokerState.HEALTHY,
                )
                sync_broker_manager._brokers[broker_addr] = broker_info
                sync_broker_manager._broker_pools[broker_addr] = mock_pool

            all_stats = sync_broker_manager.get_all_brokers_stats()

            assert len(all_stats) == 2
            assert "localhost:10911" in all_stats
            assert "localhost:10912" in all_stats

    def test_properties(self, sync_broker_manager):
        """测试属性访问器"""
        assert not sync_broker_manager.is_running
        assert sync_broker_manager.brokers_count == 0
        assert sync_broker_manager.healthy_brokers_count == 0


class TestIntegration:
    """集成测试"""

    @pytest.mark.asyncio
    async def test_async_broker_manager_lifecycle(self):
        """测试异步Broker管理器完整生命周期"""
        # 创建管理器
        manager = BrokerManager(
            remote_config=RemoteConfig(rpc_timeout=1.0),
            health_check_interval=0.5,
            max_consecutive_failures=1,
        )

        # 添加Broker
        await manager.add_broker("localhost:10911", "broker-a")
        assert manager.brokers_count == 1

        # 启动管理器
        await manager.start()
        assert manager.is_running

        # 等待健康检查
        await asyncio.sleep(0.8)

        # 关闭管理器
        await manager.shutdown()
        assert not manager.is_running

    def test_sync_broker_manager_lifecycle(self):
        """测试同步Broker管理器完整生命周期"""
        # 创建管理器
        manager = SyncBrokerManager(
            remote_config=RemoteConfig(rpc_timeout=1.0),
            health_check_interval=0.5,
            max_consecutive_failures=1,
        )

        # 添加Broker
        manager.add_broker("localhost:10911", "broker-a")
        assert manager.brokers_count == 1

        # 启动管理器
        manager.start()
        assert manager.is_running

        # 等待健康检查
        time.sleep(0.8)

        # 关闭管理器
        manager.shutdown()
        assert not manager.is_running

    @pytest.mark.asyncio
    async def test_concurrent_broker_operations(self):
        """测试并发Broker操作"""
        manager = BrokerManager(
            remote_config=RemoteConfig(rpc_timeout=5.0), connection_pool_size=3
        )

        # 并发添加多个Broker
        tasks = [
            manager.add_broker(f"localhost:1091{i}", f"broker-{chr(97 + i)}")
            for i in range(5)
        ]
        await asyncio.gather(*tasks)

        assert manager.brokers_count == 5

        # 并发移除多个Broker
        tasks = [manager.remove_broker(f"localhost:1091{i}") for i in range(5)]
        await asyncio.gather(*tasks)

        assert manager.brokers_count == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
