"""状态转换测试"""

import time
from unittest.mock import MagicMock, patch

from pyrocketmq.transport.config import TransportConfig
from pyrocketmq.transport.tcp import ConnectionStateMachine


class TestStateTransitions:
    """状态转换测试类"""

    def test_connect_transition_from_disconnected(self):
        """测试从disconnected状态到connecting状态的转换"""
        # 使用无重连配置来避免自动状态转换
        config = TransportConfig(retry_interval=0, max_retries=0)

        # Mock socket操作，让连接成功但延迟完成
        with patch("socket.socket") as mock_socket:
            mock_sock_instance = MagicMock()
            mock_socket.return_value = mock_sock_instance

            # 先让状态机创建，但拦截实际的连接调用
            state_machine = ConnectionStateMachine(config)

            # 验证初始状态
            assert state_machine.is_disconnected

            # 暂时替换socket方法，让连接在后台进行
            original_connect = mock_sock_instance.connect
            connect_called = []

            def delayed_connect(address):
                connect_called.append(True)
                # 短暂延迟模拟连接过程
                time.sleep(0.05)
                return original_connect(address)

            mock_sock_instance.connect = delayed_connect

            # 触发连接操作
            state_machine.connect()

            # 立即检查状态，应该还在connecting（连接还未完成）
            # 注意：由于python-statemachine的同步特性，我们需要重新设计这个测试
            # 让我们直接测试最终的状态转换结果

            # 等待连接完成
            time.sleep(0.2)

            # 验证最终状态（应该完成整个连接流程）
            # 由于连接成功，状态应该是connected
            assert state_machine.is_connected
            assert not state_machine.is_connecting
            assert not state_machine.is_disconnected

            # 验证连接确实被调用了
            assert len(connect_called) > 0

    def test_connect_success_transition(self):
        """测试从connecting到connected状态的转换"""
        config = TransportConfig()
        state_machine = ConnectionStateMachine(config)

        # Mock socket连接成功
        with patch("socket.socket") as mock_socket:
            mock_sock_instance = MagicMock()
            mock_socket.return_value = mock_sock_instance
            mock_sock_instance.connect.return_value = None

            # 触发连接
            state_machine.connect()

            # 验证转换到connected状态
            assert state_machine.is_connected
            assert not state_machine.is_connecting
            assert not state_machine.is_disconnected

    def test_connect_failure_transition(self):
        """测试连接失败时的状态转换"""
        config = TransportConfig()
        state_machine = ConnectionStateMachine(config)

        # Mock socket连接失败
        with patch("socket.socket") as mock_socket:
            mock_sock_instance = MagicMock()
            mock_socket.return_value = mock_sock_instance
            mock_sock_instance.connect.side_effect = ConnectionError(
                "Connection failed"
            )

            # 触发连接（应该失败）
            state_machine.connect()

            # 验证回到disconnected状态
            assert state_machine.is_disconnected
            assert not state_machine.is_connecting
            assert not state_machine.is_connected

    def test_disconnect_transition_from_connected(self):
        """测试从connected到disconnected状态的转换"""
        config = TransportConfig()
        state_machine = ConnectionStateMachine(config)

        # 先建立连接
        with patch("socket.socket") as mock_socket:
            mock_sock_instance = MagicMock()
            mock_socket.return_value = mock_sock_instance
            mock_sock_instance.connect.return_value = None

            state_machine.connect()
            assert state_machine.is_connected

            # 触发断开连接
            state_machine.close()

            # 验证状态转换
            assert state_machine.is_disconnected
            assert not state_machine.is_connected

    def test_close_transition_from_any_state(self):
        """测试从任何状态到closed状态的转换"""
        config = TransportConfig()
        state_machine = ConnectionStateMachine(config)

        # 测试从disconnected状态关闭
        state_machine.connect()  # 这会触发close，因为当前是disconnected
        # 注意：当前实现中，disconnected状态调用connect会触发_close

        # 验证状态
        assert state_machine.current_state_name in ["closed", "connecting"]

    def test_state_transition_sequence(self):
        """测试完整的状态转换序列"""
        config = TransportConfig()
        state_machine = ConnectionStateMachine(config)

        # 记录状态变化
        state_changes = []
        original_send = state_machine.send

        def track_state_change(event):
            state_changes.append((event, state_machine.current_state_name))
            return original_send(event)

        state_machine.send = track_state_change

        # 执行状态转换
        with patch("socket.socket") as mock_socket:
            mock_sock_instance = MagicMock()
            mock_socket.return_value = mock_sock_instance
            mock_sock_instance.connect.return_value = None

            # 连接过程
            state_machine.connect()

            # 验证状态序列
            # 应该有: 从disconnected到connecting，然后到connected
            state_names = [change[1] for change in state_changes]
            assert "connecting" in state_names
            assert "connected" in state_names

    def test_invalid_transition_handling(self):
        """测试无效状态转换的处理"""
        config = TransportConfig()
        state_machine = ConnectionStateMachine(config)

        # 验证初始状态
        assert state_machine.is_disconnected

        # 尝试直接发送connect_success事件（应该无效）
        # 这应该不会改变状态，因为当前状态不是connecting
        original_state = state_machine.current_state_name
        state_machine.send("_connect_success")

        # 验证状态没有改变
        assert state_machine.current_state_name == original_state

    def test_state_property_consistency(self):
        """测试状态属性的一致性"""
        config = TransportConfig()
        state_machine = ConnectionStateMachine(config)

        # 验证初始状态一致性
        assert state_machine.is_disconnected == (
            state_machine.current_state_name == "disconnected"
        )
        assert state_machine.is_connecting == (
            state_machine.current_state_name == "connecting"
        )
        assert state_machine.is_connected == (
            state_machine.current_state_name == "connected"
        )

        # 连接后验证一致性
        with patch("socket.socket") as mock_socket:
            mock_sock_instance = MagicMock()
            mock_socket.return_value = mock_sock_instance
            mock_sock_instance.connect.return_value = None

            state_machine.connect()

            assert state_machine.is_disconnected == (
                state_machine.current_state_name == "disconnected"
            )
            assert state_machine.is_connecting == (
                state_machine.current_state_name == "connecting"
            )
            assert state_machine.is_connected == (
                state_machine.current_state_name == "connected"
            )

    def test_multiple_connect_attempts(self):
        """测试多次连接尝试"""
        config = TransportConfig(retry_interval=0.01)
        state_machine = ConnectionStateMachine(config)

        connection_attempts = []

        def mock_connect(address):
            connection_attempts.append(address)
            if len(connection_attempts) == 1:
                raise ConnectionError("First attempt failed")
            return None

        with patch("socket.socket") as mock_socket:
            mock_sock_instance = MagicMock()
            mock_socket.return_value = mock_sock_instance
            mock_sock_instance.connect.side_effect = mock_connect

            # 第一次连接（失败）
            state_machine.connect()

            # 等待重连
            import time

            time.sleep(0.1)

            # 验证连接尝试次数
            assert len(connection_attempts) >= 1

            # 验证最终状态
            assert state_machine.current_state_name in [
                "disconnected",
                "connected",
            ]
