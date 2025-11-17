import threading
import time
from unittest.mock import MagicMock, Mock, patch

import pytest

from pyrocketmq.broker.broker_manager import BrokerManager
from pyrocketmq.remote.config import RemoteConfig
from pyrocketmq.transport.config import TransportConfig


class TestBrokerManager:
    """BrokerManager 单元测试"""

    def setup_method(self):
        """每个测试方法执行前的设置"""
        self.remote_config = RemoteConfig()
        # 不设置transport_config，让Manager自动创建
        self.broker_manager = BrokerManager(
            remote_config=self.remote_config,
            transport_config=None,
            connection_pool_size=3,
        )

    def test_init_default_config(self):
        """测试默认配置初始化"""
        manager = BrokerManager(remote_config=self.remote_config)

        assert manager.remote_config == self.remote_config
        assert manager.transport_config is None
        assert manager.connection_pool_size == 5
        assert manager._broker_pools == {}
        assert isinstance(manager._lock, threading.Lock)

    def test_init_custom_config(self):
        """测试自定义配置初始化"""
        custom_transport = TransportConfig(timeout=10000)
        manager = BrokerManager(
            remote_config=self.remote_config,
            transport_config=custom_transport,
            connection_pool_size=10,
        )

        assert manager.remote_config == self.remote_config
        assert manager.transport_config == custom_transport
        assert manager.connection_pool_size == 10

    def test_start(self):
        """测试启动方法"""
        # start方法目前是空实现，只需确认不抛出异常
        self.broker_manager.start()

    @patch("pyrocketmq.broker.broker_manager.ConnectionPool")
    def test_add_broker_success(self, mock_connection_pool):
        """测试成功添加Broker"""
        # Mock连接池
        mock_pool = Mock()
        mock_connection_pool.return_value = mock_pool

        broker_addr = "localhost:9876"
        broker_name = "test_broker"

        # 添加Broker
        self.broker_manager.add_broker(broker_addr, broker_name)

        # 验证连接池创建
        mock_connection_pool.assert_called_once()
        call_args = mock_connection_pool.call_args
        assert call_args[1]["address"] == broker_addr
        assert call_args[1]["pool_size"] == 3
        assert call_args[1]["remote_config"] == self.remote_config

        # 验证连接池已添加
        assert self.broker_manager.connection_pool(broker_addr) == mock_pool

    @patch("pyrocketmq.broker.broker_manager.ConnectionPool")
    def test_add_broker_without_name(self, mock_connection_pool):
        """测试不指定broker_name时添加Broker"""
        mock_pool = Mock()
        mock_connection_pool.return_value = mock_pool

        broker_addr = "localhost:9876"

        # 添加Broker（不指定名称）
        self.broker_manager.add_broker(broker_addr)

        # 验证连接池创建时使用了正确的transport配置
        mock_connection_pool.assert_called_once()
        call_args = mock_connection_pool.call_args
        assert call_args[1]["address"] == broker_addr

        # 验证broker_name从地址提取
        transport_config = call_args[1]["transport_config"]
        assert transport_config.host == "localhost"
        assert transport_config.port == 9876

    def test_add_broker_invalid_address_empty(self):
        """测试添加空地址"""
        with pytest.raises(ValueError, match="无效的Broker地址格式"):
            self.broker_manager.add_broker("")

        with pytest.raises(ValueError, match="无效的Broker地址格式"):
            self.broker_manager.add_broker(None)

    def test_add_broker_invalid_address_no_colon(self):
        """测试添加不带冒号的地址"""
        with pytest.raises(ValueError, match="无效的Broker地址格式"):
            self.broker_manager.add_broker("localhost9876")

    def test_add_broker_invalid_port(self):
        """测试添加无效端口的地址"""
        with pytest.raises(ValueError, match="无效的Broker地址格式"):
            self.broker_manager.add_broker("localhost:0")

        with pytest.raises(ValueError, match="无效的Broker地址格式"):
            self.broker_manager.add_broker("localhost:65536")

        with pytest.raises(ValueError, match="无效的Broker地址格式"):
            self.broker_manager.add_broker("localhost:abc")

    def test_add_broker_invalid_host(self):
        """测试添加无效主机的地址"""
        with pytest.raises(ValueError, match="无效的Broker地址格式"):
            self.broker_manager.add_broker(":9876")

    @patch("pyrocketmq.broker.broker_manager.ConnectionPool")
    def test_add_broker_custom_transport_config(self, mock_connection_pool):
        """测试使用自定义transport_config添加Broker"""
        custom_transport = TransportConfig(timeout=10000)
        manager = BrokerManager(
            remote_config=self.remote_config,
            transport_config=custom_transport,
            connection_pool_size=5,
        )

        mock_pool = Mock()
        mock_connection_pool.return_value = mock_pool

        broker_addr = "broker.example.com:10911"
        manager.add_broker(broker_addr)

        # 验证transport_config被正确复制和修改
        call_args = mock_connection_pool.call_args
        transport_config = call_args[1]["transport_config"]
        assert transport_config.host == "broker.example.com"
        assert transport_config.port == 10911
        assert transport_config.timeout == 10000  # 保持原有配置

    @patch("pyrocketmq.broker.broker_manager.ConnectionPool")
    def test_add_broker_exception_cleanup(self, mock_connection_pool):
        """测试添加Broker时异常情况的清理"""
        # 模拟ConnectionPool创建时抛出异常
        mock_connection_pool.side_effect = Exception("连接创建失败")

        broker_addr = "localhost:9876"

        with pytest.raises(Exception, match="连接创建失败"):
            self.broker_manager.add_broker(broker_addr)

        # 验证异常后清理干净
        assert self.broker_manager.connection_pool(broker_addr) is None

    @patch("pyrocketmq.broker.broker_manager.ConnectionPool")
    def test_remove_broker_success(self, mock_connection_pool):
        """测试成功移除Broker"""
        mock_pool = Mock()
        mock_connection_pool.return_value = mock_pool

        broker_addr = "localhost:9876"

        # 先添加Broker
        self.broker_manager.add_broker(broker_addr)
        assert self.broker_manager.connection_pool(broker_addr) is not None

        # 移除Broker
        self.broker_manager.remove_broker(broker_addr)

        # 验证连接池被关闭
        mock_pool.close.assert_called_once()

        # 验证连接池已移除
        assert self.broker_manager.connection_pool(broker_addr) is None

    def test_remove_broker_not_exist(self):
        """测试移除不存在的Broker"""
        # 不应该抛出异常
        self.broker_manager.remove_broker("nonexistent:9876")

        # 验证没有异常
        assert True

    @patch("pyrocketmq.broker.broker_manager.ConnectionPool")
    def test_connection_pool_existing(self, mock_connection_pool):
        """测试获取已存在的连接池"""
        mock_pool = Mock()
        mock_connection_pool.return_value = mock_pool

        broker_addr = "localhost:9876"

        # 添加Broker
        self.broker_manager.add_broker(broker_addr)

        # 获取连接池
        pool = self.broker_manager.connection_pool(broker_addr)

        assert pool == mock_pool

    def test_connection_pool_not_exist(self):
        """测试获取不存在的连接池"""
        pool = self.broker_manager.connection_pool("nonexistent:9876")
        assert pool is None

    @patch("pyrocketmq.broker.broker_manager.ConnectionPool")
    def test_must_connection_pool_existing(self, mock_connection_pool):
        """测试must_connection_pool获取已存在的连接池"""
        mock_pool = Mock()
        mock_connection_pool.return_value = mock_pool

        broker_addr = "localhost:9876"

        # 添加Broker
        self.broker_manager.add_broker(broker_addr)

        # 获取连接池
        pool = self.broker_manager.must_connection_pool(broker_addr)

        assert pool == mock_pool
        # 确保只创建了一次
        mock_connection_pool.assert_called_once()

    @patch("pyrocketmq.broker.broker_manager.ConnectionPool")
    def test_must_connection_pool_create_new(self, mock_connection_pool):
        """测试must_connection_pool创建新连接池"""
        mock_pool = Mock()
        mock_connection_pool.return_value = mock_pool

        broker_addr = "localhost:9876"

        # 获取连接池（不存在时应该创建）
        pool = self.broker_manager.must_connection_pool(broker_addr)

        assert pool == mock_pool
        mock_connection_pool.assert_called_once()
        call_args = mock_connection_pool.call_args
        assert call_args[1]["address"] == broker_addr
        assert call_args[1]["pool_size"] == 3
        assert call_args[1]["remote_config"] == self.remote_config

    @patch("pyrocketmq.broker.broker_manager.ConnectionPool")
    def test_shutdown_with_brokers(self, mock_connection_pool):
        """测试关闭有Broker的管理器"""
        mock_pool1 = Mock()
        mock_pool2 = Mock()
        mock_connection_pool.side_effect = [mock_pool1, mock_pool2]

        # 添加多个Broker
        self.broker_manager.add_broker("localhost:9876")
        self.broker_manager.add_broker("localhost:9877")

        # 关闭管理器
        self.broker_manager.shutdown()

        # 验证所有连接池被关闭
        mock_pool1.close.assert_called_once()
        mock_pool2.close.assert_called_once()

        # 验证连接池映射被清空
        assert self.broker_manager.connection_pool("localhost:9876") is None
        assert self.broker_manager.connection_pool("localhost:9877") is None

    def test_shutdown_empty_manager(self):
        """测试关闭空的管理器"""
        # 不应该抛出异常
        self.broker_manager.shutdown()
        assert True

    @patch("pyrocketmq.broker.broker_manager.ConnectionPool")
    def test_concurrent_add_broker(self, mock_connection_pool):
        """测试并发添加Broker的线程安全性"""
        mock_pool = Mock()
        mock_connection_pool.return_value = mock_pool

        broker_addr = "localhost:9876"
        threads = []
        exceptions = []

        def add_broker():
            try:
                self.broker_manager.add_broker(broker_addr)
            except Exception as e:
                exceptions.append(e)

        # 启动多个线程同时添加同一个Broker
        for _ in range(5):
            thread = threading.Thread(target=add_broker)
            threads.append(thread)
            thread.start()

        # 等待所有线程完成
        for thread in threads:
            thread.join()

        # 验证没有异常
        assert len(exceptions) == 0

        # 验证连接池被创建多次（当前实现允许重复添加）
        # 但最终只有一个连接池被保存
        assert mock_connection_pool.call_count >= 1

        # 验证连接池存在
        pool = self.broker_manager.connection_pool(broker_addr)
        assert pool is not None

    @patch("pyrocketmq.broker.broker_manager.ConnectionPool")
    def test_concurrent_connection_pool_access(self, mock_connection_pool):
        """测试并发访问connection_pool的线程安全性"""
        mock_pool = Mock()
        mock_connection_pool.return_value = mock_pool

        broker_addr = "localhost:9876"
        self.broker_manager.add_broker(broker_addr)

        threads = []
        results = []

        def access_connection_pool():
            for _ in range(100):
                pool = self.broker_manager.connection_pool(broker_addr)
                results.append(pool is mock_pool)

        # 启动多个线程同时访问连接池
        for _ in range(5):
            thread = threading.Thread(target=access_connection_pool)
            threads.append(thread)
            thread.start()

        # 等待所有线程完成
        for thread in threads:
            thread.join()

        # 验证所有访问都返回正确的连接池
        assert all(results)
        assert len(results) == 500  # 5个线程 * 100次访问

    @patch("pyrocketmq.broker.broker_manager.ConnectionPool")
    def test_concurrent_must_connection_pool(self, mock_connection_pool):
        """测试并发调用must_connection_pool的线程安全性"""
        call_count = 0

        def create_pool(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            # 模拟创建耗时
            time.sleep(0.1)
            return Mock()

        mock_connection_pool.side_effect = create_pool

        broker_addr = "localhost:9876"
        threads = []
        pools = []

        def get_must_connection_pool():
            pool = self.broker_manager.must_connection_pool(broker_addr)
            pools.append(pool)

        # 启动多个线程同时调用must_connection_pool
        for _ in range(3):
            thread = threading.Thread(target=get_must_connection_pool)
            threads.append(thread)
            thread.start()

        # 等待所有线程完成
        for thread in threads:
            thread.join()

        # 验证连接池只被创建一次（避免竞态条件导致的重复创建）
        assert call_count == 1

        # 验证所有线程都获得了相同的连接池
        assert len(set(pools)) == 1
        assert len(pools) == 3

    @patch("pyrocketmq.broker.broker_manager.ConnectionPool")
    def test_broker_name_extraction(self, mock_connection_pool):
        """测试从地址提取broker_name"""
        mock_pool = Mock()
        mock_connection_pool.return_value = mock_pool

        test_cases = [
            ("localhost:9876", "localhost"),
            ("broker1.example.com:10911", "broker1.example.com"),
            ("192.168.1.100:9876", "192.168.1.100"),
        ]

        for broker_addr, expected_host in test_cases:
            manager = BrokerManager(remote_config=self.remote_config)
            manager.add_broker(broker_addr)

            # 验证连接池被创建
            mock_connection_pool.assert_called()
            call_args = mock_connection_pool.call_args
            transport_config = call_args[1]["transport_config"]

            # 验证transport_config使用了正确的主机和端口
            expected_host_part, expected_port = broker_addr.split(":")
            assert transport_config.host == expected_host_part
            assert int(transport_config.port) == int(expected_port)

            # 清理mock
            mock_connection_pool.reset_mock()

    @patch("pyrocketmq.broker.broker_manager.ConnectionPool")
    def test_edge_case_valid_addresses(self, mock_connection_pool):
        """测试边界情况的有效地址"""
        mock_pool = Mock()
        mock_connection_pool.return_value = mock_pool

        # 测试最小和最大有效端口
        test_cases = [
            ("localhost:1", 1),  # 最小端口
            ("localhost:65535", 65535),  # 最大端口
        ]

        for broker_addr, expected_port in test_cases:
            manager = BrokerManager(remote_config=self.remote_config)
            manager.add_broker(broker_addr)

            # 验证连接池被创建
            mock_connection_pool.assert_called()
            call_args = mock_connection_pool.call_args
            transport_config = call_args[1]["transport_config"]
            assert int(transport_config.port) == expected_port

            # 清理mock
            mock_connection_pool.reset_mock()

    def test_edge_case_invalid_addresses(self):
        """测试边界情况的无效地址"""
        invalid_addresses = [
            "",  # 空字符串
            ":",  # 只有冒号
            "localhost:",  # 缺少端口
            ":9876",  # 缺少主机
            "localhost:0",  # 端口为0
            "localhost:65536",  # 端口超出范围
            "localhost:-1",  # 负端口
            "localhost:abc",  # 非数字端口
            "localhost:1.5",  # 浮点端口
            "localhost:9876:10911",  # 多个冒号
            "localhost 9876",  # 空格分隔
        ]

        for invalid_addr in invalid_addresses:
            with pytest.raises(ValueError, match="无效的Broker地址格式"):
                self.broker_manager.add_broker(invalid_addr)

    @patch("pyrocketmq.broker.broker_manager.ConnectionPool")
    def test_multiple_brokers_management(self, mock_connection_pool):
        """测试管理多个Broker"""
        mock_pools = [Mock() for _ in range(3)]
        mock_connection_pool.side_effect = mock_pools

        broker_addresses = ["localhost:9876", "localhost:9877", "localhost:9878"]

        # 添加多个Broker
        for addr in broker_addresses:
            self.broker_manager.add_broker(addr)

        # 验证所有连接池都存在
        for i, addr in enumerate(broker_addresses):
            pool = self.broker_manager.connection_pool(addr)
            assert pool == mock_pools[i]

        # 移除一个Broker
        self.broker_manager.remove_broker("localhost:9877")
        assert self.broker_manager.connection_pool("localhost:9877") is None
        assert self.broker_manager.connection_pool("localhost:9876") is not None
        assert self.broker_manager.connection_pool("localhost:9878") is not None

        # 验证被移除的连接池被关闭
        mock_pools[1].close.assert_called_once()

    @patch("pyrocketmq.broker.broker_manager.ConnectionPool")
    def test_transport_config_isolation(self, mock_connection_pool):
        """测试不同Broker的transport_config隔离"""
        mock_pools = [Mock() for _ in range(2)]
        mock_connection_pool.side_effect = mock_pools

        # 使用自定义transport_config创建管理器
        custom_transport = TransportConfig(timeout=5000, connect_timeout=3000)
        manager = BrokerManager(
            remote_config=self.remote_config, transport_config=custom_transport
        )

        # 添加两个不同地址的Broker
        manager.add_broker("broker1.example.com:9876")
        manager.add_broker("broker2.example.com:9876")

        # 验证每个连接池都使用了正确的transport配置
        assert mock_connection_pool.call_count == 2

        # 第一个调用
        call_args1 = mock_connection_pool.call_args_list[0]
        transport_config1 = call_args1[1]["transport_config"]
        assert transport_config1.host == "broker1.example.com"
        assert transport_config1.port == 9876
        assert transport_config1.timeout == 5000
        assert transport_config1.connect_timeout == 3000

        # 第二个调用
        call_args2 = mock_connection_pool.call_args_list[1]
        transport_config2 = call_args2[1]["transport_config"]
        assert transport_config2.host == "broker2.example.com"
        assert transport_config2.port == 9876
        assert transport_config2.timeout == 5000
        assert transport_config2.connect_timeout == 3000

    @patch("pyrocketmq.broker.broker_manager.ConnectionPool")
    def test_duplicate_broker_add(self, mock_connection_pool):
        """测试重复添加同一个Broker"""
        mock_pool = Mock()
        mock_connection_pool.return_value = mock_pool

        broker_addr = "localhost:9876"

        # 第一次添加
        self.broker_manager.add_broker(broker_addr)
        first_call_count = mock_connection_pool.call_count

        # 第二次添加相同地址
        self.broker_manager.add_broker(broker_addr)

        # 验证连接池被创建了两次（当前实现允许重复添加）
        assert mock_connection_pool.call_count == first_call_count + 1

        # 验证最新的连接池被保存
        current_pool = self.broker_manager.connection_pool(broker_addr)
        assert current_pool == mock_pool
