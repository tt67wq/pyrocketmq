"""recv_pkg方法单元测试"""

import socket
import struct
from unittest.mock import MagicMock

from pyrocketmq.transport.config import TransportConfig
from pyrocketmq.transport.tcp import ConnectionStateMachine


class TestRecvPkg:
    """recv_pkg方法测试类"""

    def test_recv_pkg_normal_case(self):
        """测试正常情况下的数据包接收"""
        config = TransportConfig(max_message_size=1024)
        state_machine = ConnectionStateMachine(config)

        # 模拟已连接状态
        state_machine._socket = MagicMock()

        # 准备测试数据
        test_body = b"Hello, World!"
        body_length = len(test_body)
        header = struct.pack("!I", body_length)  # 大端序4字节header
        # test_data = header + test_body

        # 设置socket返回值
        state_machine._socket.recv.side_effect = [header, test_body]

        # 调用recv_pkg
        result = state_machine.recv_pkg()

        # 验证结果
        assert result == test_body

        # 验证socket调用
        assert state_machine._socket.recv.call_count == 2
        state_machine._socket.recv.assert_any_call(4)  # 接收header
        state_machine._socket.recv.assert_any_call(body_length)  # 接收body

    def test_recv_pkg_empty_message(self):
        """测试空消息包接收"""
        config = TransportConfig(max_message_size=1024)
        state_machine = ConnectionStateMachine(config)

        # 模拟已连接状态
        state_machine._socket = MagicMock()

        # 准备空消息数据（长度为0）
        header = struct.pack("!I", 0)  # 大端序4字节header，长度为0

        # 设置socket返回值
        state_machine._socket.recv.side_effect = [header]

        # 调用recv_pkg
        result = state_machine.recv_pkg()

        # 验证结果
        assert result == b""

    def test_recv_pkg_large_message(self):
        """测试大消息包接收"""
        config = TransportConfig(max_message_size=10240)  # 10KB
        state_machine = ConnectionStateMachine(config)

        # 模拟已连接状态
        state_machine._socket = MagicMock()

        # 准备大测试数据
        test_body = b"x" * 8192  # 8KB数据
        body_length = len(test_body)
        header = struct.pack("!I", body_length)

        # 模拟socket分包返回
        chunk1 = test_body[:4096]
        chunk2 = test_body[4096:]

        state_machine._socket.recv.side_effect = [header, chunk1, chunk2]

        # 调用recv_pkg
        result = state_machine.recv_pkg()

        # 验证结果
        assert result == test_body
        assert len(result) == 8192

    def test_recv_pkg_connection_not_established(self):
        """测试连接未建立时的错误处理"""
        config = TransportConfig()
        state_machine = ConnectionStateMachine(config)

        # 不设置socket（模拟未连接）

        # 验证抛出异常
        with pytest.raises(RuntimeError, match="连接未建立"):
            state_machine.recv_pkg()

    def test_recv_pkg_socket_not_initialized(self):
        """测试socket未初始化时的错误处理"""
        config = TransportConfig()
        state_machine = ConnectionStateMachine(config)

        # 设置socket为None
        state_machine._socket = None

        # 验证抛出异常
        with pytest.raises(RuntimeError, match="Socket未初始化"):
            state_machine.recv_pkg()

    def test_recv_pkg_connection_closed_during_header(self):
        """测试接收header时连接被关闭"""
        config = TransportConfig()
        state_machine = ConnectionStateMachine(config)

        # 模拟已连接状态
        state_machine._socket = MagicMock()

        # 设置socket返回空数据（连接关闭）
        state_machine._socket.recv.return_value = b""

        # 调用recv_pkg
        result = state_machine.recv_pkg()

        # 验证返回空数据
        assert result == b""

    def test_recv_pkg_connection_closed_during_body(self):
        """测试接收body时连接被关闭"""
        config = TransportConfig()
        state_machine = ConnectionStateMachine(config)

        # 模拟已连接状态
        state_machine._socket = MagicMock()

        # 准备测试数据
        test_body = b"Hello"
        body_length = len(test_body)
        header = struct.pack("!I", body_length)

        # 设置socket返回：header正常，body时连接关闭
        state_machine._socket.recv.side_effect = [header, b""]

        # 调用recv_pkg
        result = state_machine.recv_pkg()

        # 验证返回空数据
        assert result == b""

    def test_recv_pkg_invalid_header_length(self):
        """测试header长度不正确的错误处理"""
        config = TransportConfig()
        state_machine = ConnectionStateMachine(config)

        # 模拟已连接状态
        state_machine._socket = MagicMock()

        # 设置socket返回不完整的header
        state_machine._socket.recv.return_value = (
            b"\x00\x00"  # 只有2字节，应该有4字节
        )

        # 验证抛出异常
        with pytest.raises(ValueError, match="Header长度不正确"):
            state_machine.recv_pkg()

    def test_recv_pkg_negative_body_length(self):
        """测试负数body长度的错误处理"""
        config = TransportConfig()
        state_machine = ConnectionStateMachine(config)

        # 模拟已连接状态
        state_machine._socket = MagicMock()

        # 准备负数长度的header
        header = struct.pack("!I", 0xFFFFFFFF)  # -1 in signed int
        state_machine._socket.recv.return_value = header

        # 验证抛出异常
        with pytest.raises(ValueError, match="Body长度不能为负数"):
            state_machine.recv_pkg()

    def test_recv_pkg_body_length_exceeds_limit(self):
        """测试body长度超过限制的错误处理"""
        config = TransportConfig(max_message_size=1024)  # 1KB限制
        state_machine = ConnectionStateMachine(config)

        # 模拟已连接状态
        state_machine._socket = MagicMock()

        # 准备超长header
        header = struct.pack("!I", 2048)  # 2KB，超过1KB限制
        state_machine._socket.recv.return_value = header

        # 验证抛出异常
        with pytest.raises(ValueError, match="Body长度超过限制"):
            state_machine.recv_pkg()

    def test_recv_pkg_body_length_mismatch(self):
        """测试body长度不匹配的错误处理"""
        config = TransportConfig()
        state_machine = ConnectionStateMachine(config)

        # 模拟已连接状态
        state_machine._socket = MagicMock()

        # 准备测试数据
        header = struct.pack("!I", 10)  # 声明长度为10
        wrong_body = b"short"  # 实际长度为5

        state_machine._socket.recv.side_effect = [header, wrong_body]

        # 验证抛出异常
        with pytest.raises(ValueError, match="Body长度不匹配"):
            state_machine.recv_pkg()

    def test_recv_pkg_timeout_during_header(self):
        """测试接收header时超时"""
        config = TransportConfig()
        state_machine = ConnectionStateMachine(config)

        # 模拟已连接状态
        state_machine._socket = MagicMock()

        # 设置socket超时
        state_machine._socket.recv.side_effect = socket.timeout(
            "Receive timeout"
        )

        # 验证抛出超时异常
        with pytest.raises(TimeoutError, match="接收数据包超时"):
            state_machine.recv_pkg()

    def test_recv_pkg_timeout_during_body(self):
        """测试接收body时超时"""
        config = TransportConfig()
        state_machine = ConnectionStateMachine(config)

        # 模拟已连接状态
        state_machine._socket = MagicMock()

        # 准备测试数据
        header = struct.pack("!I", 10)

        # 设置socket：header正常，body时超时
        state_machine._socket.recv.side_effect = [
            header,
            socket.timeout("Receive timeout"),
        ]

        # 验证抛出超时异常
        with pytest.raises(TimeoutError, match="接收数据包超时"):
            state_machine.recv_pkg()

    def test_recv_pkg_partial_reception(self):
        """测试分包接收的情况"""
        config = TransportConfig()
        state_machine = ConnectionStateMachine(config)

        # 模拟已连接状态
        state_machine._socket = MagicMock()

        # 准备测试数据
        test_body = b"This is a longer message that might be received in multiple chunks"
        body_length = len(test_body)
        header = struct.pack("!I", body_length)

        # 模拟多次调用recv才能接收完整数据
        chunks = [
            header,  # 4字节header
            test_body[:10],  # 前10字节
            test_body[10:20],  # 接下来10字节
            test_body[20:],  # 剩余部分
        ]

        state_machine._socket.recv.side_effect = chunks

        # 调用recv_pkg
        result = state_machine.recv_pkg()

        # 验证结果
        assert result == test_body

        # 验证多次调用了recv
        assert (
            state_machine._socket.recv.call_count >= 3
        )  # header + 多个body chunks

    def test_recv_pkg_unicode_content(self):
        """测试包含Unicode内容的数据包"""
        config = TransportConfig(max_message_size=1024)
        state_machine = ConnectionStateMachine(config)

        # 模拟已连接状态
        state_machine._socket = MagicMock()

        # 准备包含Unicode的测试数据
        test_body = "你好，世界！Hello, World!こんにちは".encode("utf-8")
        body_length = len(test_body)
        header = struct.pack("!I", body_length)

        state_machine._socket.recv.side_effect = [header, test_body]

        # 调用recv_pkg
        result = state_machine.recv_pkg()

        # 验证结果
        assert result == test_body
        assert len(result) == body_length

    def test_recv_pkg_binary_content(self):
        """测试二进制内容的数据包"""
        config = TransportConfig(max_message_size=1024)
        state_machine = ConnectionStateMachine(config)

        # 模拟已连接状态
        state_machine._socket = MagicMock()

        # 准备二进制测试数据
        test_body = bytes(range(256))  # 0-255的所有字节
        body_length = len(test_body)
        header = struct.pack("!I", body_length)

        state_machine._socket.recv.side_effect = [header, test_body]

        # 调用recv_pkg
        result = state_machine.recv_pkg()

        # 验证结果
        assert result == test_body
        assert len(result) == 256
