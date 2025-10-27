"""
MessageID模块的测试用例
测试消息ID的生成、解析和验证功能，确保与Go实现兼容。
"""

import pytest

from pyrocketmq.model.message_id import (
    MessageID,
    create_message_id,
    create_message_id_from_bytes,
    format_message_id_info,
    get_address_by_bytes,
    is_valid_message_id,
    parse_message_id_from_string,
    unmarshal_msg_id,
)


class TestMessageID:
    """MessageID数据结构测试"""

    def test_message_id_creation(self):
        """测试MessageID对象创建"""
        msg_id = MessageID(addr="192.168.1.100", port=10911, offset=123456789)

        assert msg_id.addr == "192.168.1.100"
        assert msg_id.port == 10911
        assert msg_id.offset == 123456789

    def test_message_id_str(self):
        """测试MessageID字符串表示"""
        msg_id = MessageID(addr="192.168.1.100", port=10911, offset=123456789)
        str_repr = str(msg_id)

        assert "192.168.1.100" in str_repr
        assert "10911" in str_repr
        assert "123456789" in str_repr

    def test_message_id_repr(self):
        """测试MessageID详细字符串表示"""
        msg_id = MessageID(addr="192.168.1.100", port=10911, offset=123456789)
        repr_str = repr(msg_id)

        assert "addr='192.168.1.100'" in repr_str
        assert "port=10911" in repr_str
        assert "offset=123456789" in repr_str

    def test_message_id_to_dict(self):
        """测试MessageID转换为字典"""
        msg_id = MessageID(addr="192.168.1.100", port=10911, offset=123456789)
        data = msg_id.to_dict()

        assert data == {
            "addr": "192.168.1.100",
            "port": 10911,
            "offset": 123456789,
        }


class TestCreateMessageID:
    """消息ID创建功能测试"""

    def test_create_message_id_basic(self):
        """测试基本的消息ID创建"""
        msg_id = create_message_id("192.168.1.100", 10911, 123456789)

        # 验证长度
        assert len(msg_id) == 32

        # 验证为大写十六进制
        assert msg_id.isupper()
        assert all(c in "0123456789ABCDEF" for c in msg_id)

    def test_create_message_id_different_values(self):
        """测试不同值的消息ID创建"""
        msg_id1 = create_message_id("192.168.1.100", 10911, 123456789)
        msg_id2 = create_message_id("192.168.1.101", 10911, 123456789)
        msg_id3 = create_message_id("192.168.1.100", 10912, 123456789)
        msg_id4 = create_message_id("192.168.1.100", 10911, 123456790)

        # 确保不同的值生成不同的ID
        assert msg_id1 != msg_id2 != msg_id3 != msg_id4

    def test_create_message_id_same_values(self):
        """测试相同值生成相同ID"""
        msg_id1 = create_message_id("192.168.1.100", 10911, 123456789)
        msg_id2 = create_message_id("192.168.1.100", 10911, 123456789)

        assert msg_id1 == msg_id2

    def test_create_message_id_invalid_ip(self):
        """测试无效IP地址"""
        with pytest.raises(ValueError, match="无效的IP地址"):
            create_message_id("invalid.ip", 10911, 123456789)

        with pytest.raises(ValueError, match="无效的IP地址"):
            create_message_id("300.400.500.600", 10911, 123456789)

    def test_create_message_id_ipv6_not_supported(self):
        """测试IPv6地址不支持"""
        with pytest.raises(ValueError, match="不支持IPv6地址"):
            create_message_id("::1", 10911, 123456789)

    def test_create_message_id_zero_values(self):
        """测试零值创建"""
        msg_id = create_message_id("0.0.0.0", 0, 0)

        assert len(msg_id) == 32
        assert msg_id.upper() == msg_id  # 检查是否为大写


class TestUnmarshalMsgID:
    """消息ID解析功能测试"""

    def test_unmarshal_msg_id_basic(self):
        """测试基本的消息ID解析"""
        original_addr = "192.168.1.100"
        original_port = 10911
        original_offset = 123456789

        msg_id = create_message_id(
            original_addr, original_port, original_offset
        )
        parsed = unmarshal_msg_id(msg_id)

        assert parsed.addr == original_addr
        assert parsed.port == original_port
        assert parsed.offset == original_offset

    def test_unmarshal_msg_id_round_trip(self):
        """测试往返转换（生成->解析->再生成）"""
        test_cases = [
            ("127.0.0.1", 8080, 0),
            ("192.168.1.100", 10911, 123456789),
            ("10.0.0.1", 9876, 9876543210),
            ("255.255.255.255", 65535, 18446744073709551615),
        ]

        for addr, port, offset in test_cases:
            msg_id = create_message_id(addr, port, offset)
            parsed = unmarshal_msg_id(msg_id)

            assert parsed.addr == addr
            assert parsed.port == port
            assert parsed.offset == offset

    def test_unmarshal_msg_id_invalid_length(self):
        """测试无效长度的消息ID"""
        with pytest.raises(ValueError, match="消息ID长度不足32字符"):
            unmarshal_msg_id("123")

        with pytest.raises(ValueError, match="消息ID长度不足32字符"):
            unmarshal_msg_id("1234567890123456789012345678901")  # 31字符

    def test_unmarshal_msg_id_invalid_hex(self):
        """测试无效十六进制字符串"""
        with pytest.raises(ValueError, match="解析消息ID失败"):
            unmarshal_msg_id("G" * 32)  # 无效十六进制字符


class TestGetAddressByBytes:
    """IP地址字节转换测试"""

    def test_get_address_by_bytes_valid(self):
        """测试有效的IP地址字节转换"""
        # 192.168.1.100 = C0 A8 01 64
        ip_bytes = b"\xc0\xa8\x01\x64"
        addr = get_address_by_bytes(ip_bytes)

        assert addr == "192.168.1.100"

    def test_get_address_by_bytes_invalid_length(self):
        """测试无效长度的IP地址字节"""
        with pytest.raises(ValueError, match="IP地址字节数组长度必须为4"):
            get_address_by_bytes(b"\x01\x02\x03")  # 3字节

        with pytest.raises(ValueError, match="IP地址字节数组长度必须为4"):
            get_address_by_bytes(b"\x01\x02\x03\x04\x05")  # 5字节


class TestCreateMessageIDFromBytes:
    """从字节创建消息ID测试"""

    def test_create_message_id_from_bytes(self):
        """测试从字节数组创建消息ID"""
        # 127.0.0.1 = 7F 00 00 01
        ip_bytes = b"\x7f\x00\x00\x01"
        msg_id = create_message_id_from_bytes(ip_bytes, 8080, 123456)

        assert len(msg_id) == 32
        assert msg_id.isupper()

        # 验证解析结果
        parsed = unmarshal_msg_id(msg_id)
        assert parsed.addr == "127.0.0.1"
        assert parsed.port == 8080
        assert parsed.offset == 123456


class TestValidationFunctions:
    """验证功能测试"""

    def test_is_valid_message_id_valid(self):
        """测试有效的消息ID验证"""
        msg_id = create_message_id("192.168.1.100", 10911, 123456789)
        assert is_valid_message_id(msg_id)

    def test_is_valid_message_id_invalid(self):
        """测试无效的消息ID验证"""
        assert not is_valid_message_id("invalid")
        assert not is_valid_message_id("123")
        assert not is_valid_message_id("G" * 32)
        assert not is_valid_message_id(
            "0123456789012345678901234567890"
        )  # 31字符

    def test_parse_message_id_from_string_success(self):
        """测试成功解析消息ID字符串"""
        msg_id = create_message_id("192.168.1.100", 10911, 123456789)
        parsed = parse_message_id_from_string(msg_id)

        assert parsed is not None
        assert parsed.addr == "192.168.1.100"
        assert parsed.port == 10911
        assert parsed.offset == 123456789

    def test_parse_message_id_from_string_failure(self):
        """测试解析消息ID字符串失败"""
        parsed = parse_message_id_from_string("invalid")
        assert parsed is None

        parsed = parse_message_id_from_string("123")
        assert parsed is None

    def test_format_message_id_info_valid(self):
        """测试格式化有效消息ID信息"""
        msg_id = create_message_id("192.168.1.100", 10911, 123456789)
        info = format_message_id_info(msg_id)

        assert "Broker: 192.168.1.100:10911" in info
        assert "Offset: 123456789" in info

    def test_format_message_id_info_invalid(self):
        """测试格式化无效消息ID信息"""
        info = format_message_id_info("invalid")
        assert "Invalid MessageID" in info
        assert "invalid" in info


class TestCompatibility:
    """与Go实现兼容性测试"""

    def test_specific_values_compatibility(self):
        """测试特定值的兼容性"""
        # 测试一些特定的已知值
        test_cases = [
            ("127.0.0.1", 8080, 0),
            ("192.168.1.1", 10911, 1),
            ("10.0.0.1", 9876, 100),
        ]

        for addr, port, offset in test_cases:
            msg_id = create_message_id(addr, port, offset)
            parsed = unmarshal_msg_id(msg_id)

            # 验证往返转换的正确性
            assert parsed.addr == addr
            assert parsed.port == port
            assert parsed.offset == offset

    def test_endianness_compatibility(self):
        """测试字节序兼容性"""
        # 测试大端序（Go实现使用大端序）
        import socket
        import struct

        addr = "192.168.1.100"
        port = 10911
        offset = 123456789

        # 创建消息ID
        msg_id = create_message_id(addr, port, offset)

        # 手动验证字节序
        ip_bytes = socket.inet_aton(addr)  # 4字节IP
        port_bytes = struct.pack(">I", port)  # 大端序端口
        offset_bytes = struct.pack(">Q", offset)  # 大端序偏移量

        # 合并所有字节
        all_bytes = ip_bytes + port_bytes + offset_bytes

        # 验证与生成的消息ID一致
        assert msg_id == all_bytes.hex().upper()

    def test_boundary_values(self):
        """测试边界值"""
        # 测试最大端口值
        msg_id = create_message_id("127.0.0.1", 65535, 0)
        parsed = unmarshal_msg_id(msg_id)
        assert parsed.port == 65535

        # 测试最大偏移量值
        msg_id = create_message_id("127.0.0.1", 8080, 2**64 - 1)
        parsed = unmarshal_msg_id(msg_id)
        assert parsed.offset == 2**64 - 1

        # 测试广播地址
        msg_id = create_message_id("255.255.255.255", 8080, 0)
        parsed = unmarshal_msg_id(msg_id)
        assert parsed.addr == "255.255.255.255"


if __name__ == "__main__":
    # 运行测试
    pytest.main([__file__, "-v"])
