"""
Broker便利函数测试

测试新添加的BrokerManager和AsyncBrokerManager便利函数。
"""

import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from pyrocketmq.broker import (
    create_async_broker_manager,
    create_broker_manager,
    create_broker_manager_auto,
    create_simple_async_broker_manager,
    create_simple_broker_manager,
)
from pyrocketmq.broker.async_broker_manager import AsyncBrokerManager
from pyrocketmq.broker.broker_manager import BrokerManager


class TestBrokerManagerConvenienceFunctions(unittest.TestCase):
    """BrokerManager便利函数测试"""

    @patch("pyrocketmq.broker.broker_manager.NameServerManager")
    def test_create_broker_manager_single_addr(self, mock_nameserver):
        """测试使用单个地址创建BrokerManager"""
        mock_nameserver.return_value = MagicMock()

        manager = create_broker_manager("localhost:9876")

        self.assertIsInstance(manager, BrokerManager)
        mock_nameserver.assert_called_once_with(["localhost:9876"])

    @patch("pyrocketmq.broker.broker_manager.NameServerManager")
    def test_create_broker_manager_multiple_addrs(self, mock_nameserver):
        """测试使用多个地址创建BrokerManager"""
        mock_nameserver.return_value = MagicMock()

        manager = create_broker_manager(["localhost:9876", "broker2:9876"])

        self.assertIsInstance(manager, BrokerManager)
        mock_nameserver.assert_called_once_with(["localhost:9876", "broker2:9876"])

    @patch("pyrocketmq.broker.broker_manager.NameServerManager")
    def test_create_broker_manager_with_kwargs(self, mock_nameserver):
        """测试使用自定义参数创建BrokerManager"""
        mock_nameserver.return_value = MagicMock()

        manager = create_broker_manager(
            "localhost:9876",
            max_connections_per_broker=20,
            connection_timeout_ms=10000,
            enable_health_check=False,
        )

        self.assertIsInstance(manager, BrokerManager)
        mock_nameserver.assert_called_once_with(["localhost:9876"])

    def test_create_broker_manager_empty_addr(self):
        """测试空地址参数"""
        with self.assertRaises(ValueError):
            create_broker_manager("")

    @patch("pyrocketmq.broker.broker_manager.create_broker_manager")
    def test_create_simple_broker_manager(self, mock_create):
        """测试创建简单BrokerManager"""
        mock_create.return_value = MagicMock()

        manager = create_simple_broker_manager("localhost:10911")

        self.assertIsInstance(manager, type(mock_create.return_value))
        mock_create.assert_called_once()

    def test_create_simple_broker_manager_invalid_format(self):
        """测试无效格式的地址"""
        with self.assertRaises(ValueError):
            create_simple_broker_manager("localhost")

    def test_create_simple_broker_manager_invalid_port(self):
        """测试无效端口号"""
        with self.assertRaises(ValueError):
            create_simple_broker_manager("localhost:abc")

    def test_create_simple_broker_manager_empty_addr(self):
        """测试空地址"""
        with self.assertRaises(ValueError):
            create_simple_broker_manager("")


class TestAsyncBrokerManagerConvenienceFunctions(unittest.TestCase):
    """AsyncBrokerManager便利函数测试"""

    @patch("pyrocketmq.broker.async_broker_manager.AsyncNameServerManager")
    def test_create_async_broker_manager_single_addr(self, mock_nameserver):
        """测试使用单个地址创建AsyncBrokerManager"""
        mock_nameserver.return_value = AsyncMock()
        mock_nameserver.return_value.__aenter__ = AsyncMock(
            return_value=mock_nameserver.return_value
        )

        async def test():
            manager = await create_async_broker_manager("localhost:9876")
            self.assertIsInstance(manager, AsyncBrokerManager)
            mock_nameserver.assert_called_once_with(["localhost:9876"])

        import asyncio

        asyncio.run(test())

    @patch("pyrocketmq.broker.async_broker_manager.AsyncNameServerManager")
    def test_create_async_broker_manager_multiple_addrs(self, mock_nameserver):
        """测试使用多个地址创建AsyncBrokerManager"""
        mock_nameserver.return_value = AsyncMock()
        mock_nameserver.return_value.__aenter__ = AsyncMock(
            return_value=mock_nameserver.return_value
        )

        async def test():
            manager = await create_async_broker_manager(
                ["localhost:9876", "broker2:9876"]
            )
            self.assertIsInstance(manager, AsyncBrokerManager)
            mock_nameserver.assert_called_once_with(["localhost:9876", "broker2:9876"])

        import asyncio

        asyncio.run(test())

    @patch("pyrocketmq.broker.async_broker_manager.AsyncNameServerManager")
    def test_create_async_broker_manager_with_kwargs(self, mock_nameserver):
        """测试使用自定义参数创建AsyncBrokerManager"""
        mock_nameserver.return_value = AsyncMock()
        mock_nameserver.return_value.__aenter__ = AsyncMock(
            return_value=mock_nameserver.return_value
        )

        async def test():
            manager = await create_async_broker_manager(
                "localhost:9876",
                max_connections_per_broker=20,
                connection_timeout_ms=10000,
                enable_health_check=False,
            )
            self.assertIsInstance(manager, AsyncBrokerManager)
            mock_nameserver.assert_called_once_with(["localhost:9876"])

        import asyncio

        asyncio.run(test())

    def test_create_async_broker_manager_empty_addr(self):
        """测试空地址参数"""

        async def test():
            with self.assertRaises(ValueError):
                await create_async_broker_manager("")

        import asyncio

        asyncio.run(test())

    @patch("pyrocketmq.broker.async_broker_manager.create_async_broker_manager")
    def test_create_simple_async_broker_manager(self, mock_create):
        """测试创建简单AsyncBrokerManager"""
        mock_create.return_value = AsyncMock()
        mock_create.return_value.__aenter__ = AsyncMock(
            return_value=mock_create.return_value
        )

        async def test():
            manager = await create_simple_async_broker_manager("localhost:10911")
            self.assertIsInstance(manager, type(mock_create.return_value))
            mock_create.assert_called_once()

        import asyncio

        asyncio.run(test())

    def test_create_simple_async_broker_manager_invalid_format(self):
        """测试无效格式的地址"""

        async def test():
            with self.assertRaises(ValueError):
                await create_simple_async_broker_manager("localhost")

        import asyncio

        asyncio.run(test())

    def test_create_simple_async_broker_manager_invalid_port(self):
        """测试无效端口号"""

        async def test():
            with self.assertRaises(ValueError):
                await create_simple_async_broker_manager("localhost:abc")

        import asyncio

        asyncio.run(test())

    def test_create_simple_async_broker_manager_empty_addr(self):
        """测试空地址"""

        async def test():
            with self.assertRaises(ValueError):
                await create_simple_async_broker_manager("")

        import asyncio

        asyncio.run(test())


class TestAutoBrokerManager(unittest.TestCase):
    """自动选择BrokerManager测试"""

    def test_auto_broker_manager_in_sync_context(self):
        """测试在同步上下文中自动选择同步版本"""
        with patch(
            "pyrocketmq.broker.broker_manager.create_broker_manager"
        ) as mock_sync:
            mock_sync.return_value = MagicMock()

            def test_sync():
                manager = create_broker_manager_auto("localhost:9876")
                self.assertEqual(manager, mock_sync.return_value)
                mock_sync.assert_called_once_with("localhost:9876")

            import asyncio

            asyncio.run(test())

    @patch("pyrocketmq.broker.async_broker_manager.create_async_broker_manager")
    def test_auto_broker_manager_in_async_context(self, mock_async):
        """测试在异步上下文中自动选择异步版本"""
        mock_async.return_value = AsyncMock()
        mock_async.return_value.__aenter__ = AsyncMock(
            return_value=mock_async.return_value
        )

        async def test_async():
            manager = await create_broker_manager_auto("localhost:9876")
            self.assertEqual(manager, mock_async.return_value)
            mock_async.assert_called_once_with("localhost:9876")

        import asyncio

        asyncio.run(test_async())


if __name__ == "__main__":
    unittest.main()
