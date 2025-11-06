"""
NameServer管理器测试
测试同步和异步NameServer管理器的功能
"""

import asyncio
import time
import unittest
from unittest.mock import AsyncMock, Mock, patch

from pyrocketmq.nameserver.manager import (
    NameServerManager,
    NameServerConfig,
    create_nameserver_manager,
)
from pyrocketmq.nameserver.async_manager import (
    AsyncNameServerManager,
    create_async_nameserver_manager,
)
from pyrocketmq.nameserver.models import TopicRouteData, BrokerData
from pyrocketmq.nameserver.errors import NameServerError


class TestNameServerManager(unittest.TestCase):
    """测试同步NameServer管理器"""

    def setUp(self):
        """测试前准备"""
        self.config = NameServerConfig(
            nameserver_addrs="localhost:9876;localhost:9877",
            timeout=5.0,
            broker_cache_ttl=60,
            route_cache_ttl=60,
        )

    @patch("pyrocketmq.nameserver.manager.create_sync_remote")
    def test_create_manager(self, mock_create_remote):
        """测试创建管理器"""
        # 模拟远程连接
        mock_remote = Mock()
        mock_remote.connect.return_value = None
        mock_create_remote.return_value = mock_remote

        manager = create_nameserver_manager("localhost:9876")

        self.assertIsInstance(manager, NameServerManager)
        self.assertEqual(manager.config.nameserver_addrs, "localhost:9876")

    @patch("pyrocketmq.nameserver.manager.create_sync_remote")
    def test_start_stop(self, mock_create_remote):
        """测试启动和停止"""
        # 模拟远程连接
        mock_remote = Mock()
        mock_remote.connect.return_value = None
        mock_remote.close.return_value = None
        mock_create_remote.return_value = mock_remote

        manager = NameServerManager(self.config)

        # 测试启动
        manager.start()
        self.assertEqual(len(manager._sync_connections), 2)  # 两个NameServer地址

        # 测试停止
        manager.stop()
        self.assertEqual(len(manager._sync_connections), 0)

    def test_parse_nameserver_addrs(self):
        """测试解析NameServer地址"""
        manager = NameServerManager(self.config)

        # 测试单个地址
        addrs = manager._parse_nameserver_addrs("localhost:9876")
        self.assertEqual(addrs, ["localhost:9876"])

        # 测试多个地址
        addrs = manager._parse_nameserver_addrs(
            "localhost:9876;localhost:9877;localhost:9878"
        )
        self.assertEqual(addrs, ["localhost:9876", "localhost:9877", "localhost:9878"])

        # 测试带空格的地址
        addrs = manager._parse_nameserver_addrs(" localhost:9876 ; localhost:9877 ")
        self.assertEqual(addrs, ["localhost:9876", "localhost:9877"])

    def test_select_broker_address(self):
        """测试选择broker地址"""
        manager = NameServerManager(self.config)

        # 测试有master的情况
        broker_data = BrokerData(
            broker_name="broker-a",
            broker_addrs={"0": "192.168.1.100:10911", "1": "192.168.1.101:10911"},
        )
        address = manager._select_broker_address(broker_data)
        self.assertEqual(address, "192.168.1.100:10911")

        # 测试只有slave的情况
        broker_data = BrokerData(
            broker_name="broker-b",
            broker_addrs={"1": "192.168.1.101:10911", "2": "192.168.1.102:10911"},
        )
        address = manager._select_broker_address(broker_data)
        self.assertEqual(address, "192.168.1.101:10911")

        # 测试没有地址的情况
        broker_data = BrokerData(broker_name="broker-c", broker_addrs={})
        address = manager._select_broker_address(broker_data)
        self.assertIsNone(address)

    @patch("pyrocketmq.nameserver.manager.create_sync_remote")
    def test_get_broker_address_with_cache(self, mock_create_remote):
        """测试带缓存的broker地址查询"""
        # 模拟远程连接
        mock_remote = Mock()
        mock_remote.connect.return_value = None
        mock_remote.close.return_value = None
        mock_create_remote.return_value = mock_remote

        manager = NameServerManager(self.config)
        manager.start()

        # 手动添加缓存
        manager._broker_addr_cache["broker-a"] = Mock(
            data="192.168.1.100:10911", timestamp=time.time(), is_expired=False
        )

        # 测试缓存命中
        address = manager.get_broker_address("broker-a")
        self.assertEqual(address, "192.168.1.100:10911")

        # 验证统计信息
        metrics = manager.get_metrics()
        self.assertEqual(metrics["broker_addr_queries"], 1)
        self.assertEqual(metrics["broker_addr_cache_hits"], 1)

        manager.stop()

    @patch("pyrocketmq.nameserver.manager.create_sync_remote")
    def test_clear_cache(self, mock_create_remote):
        """测试清理缓存"""
        # 模拟远程连接
        mock_remote = Mock()
        mock_remote.connect.return_value = None
        mock_remote.close.return_value = None
        mock_create_remote.return_value = mock_remote

        manager = NameServerManager(self.config)
        manager.start()

        # 手动添加缓存
        manager._broker_addr_cache["broker-a"] = Mock()
        manager._route_cache["test-topic"] = Mock()

        # 清理缓存
        manager.clear_cache()

        # 验证缓存已清空
        self.assertEqual(len(manager._broker_addr_cache), 0)
        self.assertEqual(len(manager._route_cache), 0)

        manager.stop()


class TestAsyncNameServerManager(unittest.TestCase):
    """测试异步NameServer管理器"""

    def setUp(self):
        """测试前准备"""
        self.config = NameServerConfig(
            nameserver_addrs="localhost:9876;localhost:9877",
            timeout=5.0,
            broker_cache_ttl=60,
            route_cache_ttl=60,
        )

    @patch("pyrocketmq.nameserver.async_manager.create_async_remote")
    async def test_create_async_manager(self, mock_create_remote):
        """测试创建异步管理器"""
        # 模拟异步远程连接
        mock_remote = AsyncMock()
        mock_remote.connect.return_value = None
        mock_create_remote.return_value = mock_remote

        manager = create_async_nameserver_manager("localhost:9876")

        self.assertIsInstance(manager, AsyncNameServerManager)
        self.assertEqual(manager.config.nameserver_addrs, "localhost:9876")

    @patch("pyrocketmq.nameserver.async_manager.create_async_remote")
    async def test_async_start_stop(self, mock_create_remote):
        """测试异步启动和停止"""
        # 模拟异步远程连接
        mock_remote = AsyncMock()
        mock_remote.connect.return_value = None
        mock_remote.close.return_value = None
        mock_create_remote.return_value = mock_remote

        manager = AsyncNameServerManager(self.config)

        # 测试启动
        await manager.start()
        self.assertEqual(len(manager._async_connections), 2)  # 两个NameServer地址

        # 测试停止
        await manager.stop()
        self.assertEqual(len(manager._async_connections), 0)

    @patch("pyrocketmq.nameserver.async_manager.create_async_remote")
    async def test_async_get_broker_address_with_cache(self, mock_create_remote):
        """测试异步带缓存的broker地址查询"""
        # 模拟异步远程连接
        mock_remote = AsyncMock()
        mock_remote.connect.return_value = None
        mock_remote.close.return_value = None
        mock_create_remote.return_value = mock_remote

        manager = AsyncNameServerManager(self.config)
        await manager.start()

        # 手动添加缓存
        manager._broker_addr_cache["broker-a"] = Mock(
            data="192.168.1.100:10911", timestamp=time.time(), is_expired=False
        )

        # 测试缓存命中
        address = await manager.get_broker_address("broker-a")
        self.assertEqual(address, "192.168.1.100:10911")

        # 验证统计信息
        metrics = await manager.get_metrics()
        self.assertEqual(metrics["broker_addr_queries"], 1)
        self.assertEqual(metrics["broker_addr_cache_hits"], 1)

        await manager.stop()

    @patch("pyrocketmq.nameserver.async_manager.create_async_remote")
    async def test_async_clear_cache(self, mock_create_remote):
        """测试异步清理缓存"""
        # 模拟异步远程连接
        mock_remote = AsyncMock()
        mock_remote.connect.return_value = None
        mock_remote.close.return_value = None
        mock_create_remote.return_value = mock_remote

        manager = AsyncNameServerManager(self.config)
        await manager.start()

        # 手动添加缓存
        manager._broker_addr_cache["broker-a"] = Mock()
        manager._route_cache["test-topic"] = Mock()

        # 清理缓存
        await manager.clear_cache()

        # 验证缓存已清空
        self.assertEqual(len(manager._broker_addr_cache), 0)
        self.assertEqual(len(manager._route_cache), 0)

        await manager.stop()


class TestNameServerManagerIntegration(unittest.TestCase):
    """NameServer管理器集成测试"""

    def test_config_creation(self):
        """测试配置创建"""
        # 测试默认配置
        config = NameServerConfig(nameserver_addrs="localhost:9876")
        self.assertEqual(config.nameserver_addrs, "localhost:9876")
        self.assertEqual(config.timeout, 30.0)
        self.assertEqual(config.broker_cache_ttl, 300)
        self.assertEqual(config.route_cache_ttl, 300)

        # 测试自定义配置
        config = NameServerConfig(
            nameserver_addrs="localhost:9876;localhost:9877",
            timeout=10.0,
            broker_cache_ttl=600,
            route_cache_ttl=600,

        # 测试自定义配置
        config = NameServerConfig(
            nameserver_addrs="localhost:9876;localhost:9877",
            timeout=10.0,
            broker_cache_ttl=600,
            route_cache_ttl=600
        )
        self.assertEqual(config.timeout, 10.0)
        self.assertEqual(config.broker_cache_ttl, 600)
        self.assertEqual(config.route_cache_ttl, 600)

    def test_convenience_functions(self):
        """测试便利函数"""
        # 测试同步管理器创建
        manager = create_nameserver_manager("localhost:9876", timeout=10.0)
        self.assertIsInstance(manager, NameServerManager)
        self.assertEqual(manager.config.timeout, 10.0)

        # 测试异步管理器创建
        async_manager = create_async_nameserver_manager("localhost:9876", timeout=15.0)
        self.assertIsInstance(async_manager, AsyncNameServerManager)
        self.assertEqual(async_manager.config.timeout, 15.0)


if __name__ == "__main__":
    # 运行所有测试
    unittest.main(verbosity=2)
