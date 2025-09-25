"""
RemotingCommandSerializer测试
"""

import json

import pytest

from pyrocketmq.model.command import RemotingCommand
from pyrocketmq.model.enums import FlagType, LanguageCode, RequestCode
from pyrocketmq.model.errors import (
    DeserializationError,
    HeaderTooLargeError,
    MessageTooLargeError,
    ProtocolError,
)
from pyrocketmq.model.serializer import RemotingCommandSerializer


class TestRemotingCommandSerializer:
    """RemotingCommandSerializer测试类"""

    def test_serialize_basic_command(self):
        """测试基本命令序列化"""
        command = RemotingCommand(
            code=RequestCode.SEND_MESSAGE,
            language=LanguageCode.PYTHON,
            version=1,
            opaque=123,
            flag=FlagType.RPC_TYPE,
            remark="test remark",
        )

        data = RemotingCommandSerializer.serialize(command)

        # 验证数据长度
        assert len(data) >= 8  # 至少包含两个长度字段

        # 验证可以正确反序列化
        restored = RemotingCommandSerializer.deserialize(data)
        assert restored.code == command.code
        assert restored.language == command.language
        assert restored.version == command.version
        assert restored.opaque == command.opaque
        assert restored.flag == command.flag
        assert restored.remark == command.remark

    def test_serialize_with_body(self):
        """测试带body的命令序列化"""
        body_data = b"Hello, RocketMQ!"
        command = RemotingCommand(code=RequestCode.SEND_MESSAGE, body=body_data)

        data = RemotingCommandSerializer.serialize(command)
        restored = RemotingCommandSerializer.deserialize(data)

        assert restored.body == body_data

    def test_serialize_with_ext_fields(self):
        """测试带扩展字段的命令序列化"""
        command = RemotingCommand(
            code=RequestCode.SEND_MESSAGE,
            ext_fields={
                "topic": "test_topic",
                "producer_group": "test_group",
                "queue_id": "0",
            },
        )

        data = RemotingCommandSerializer.serialize(command)
        restored = RemotingCommandSerializer.deserialize(data)

        assert restored.ext_fields == command.ext_fields

    def test_serialize_complex_command(self):
        """测试复杂命令序列化"""
        body_data = b"Complex message body"
        command = RemotingCommand(
            code=RequestCode.PULL_MESSAGE,
            language=LanguageCode.PYTHON,
            version=2,
            opaque=456,
            flag=FlagType.RPC_TYPE,
            remark="Complex test",
            ext_fields={
                "topic": "test_topic",
                "consumer_group": "test_group",
                "queue_id": "1",
                "offset": "100",
            },
            body=body_data,
        )

        data = RemotingCommandSerializer.serialize(command)
        restored = RemotingCommandSerializer.deserialize(data)

        # 验证所有字段
        assert restored == command

    def test_serialize_empty_command(self):
        """测试最小命令序列化"""
        command = RemotingCommand(code=RequestCode.HEART_BEAT)

        data = RemotingCommandSerializer.serialize(command)
        restored = RemotingCommandSerializer.deserialize(data)

        assert restored.code == command.code
        assert restored.language == LanguageCode.PYTHON  # 默认值
        assert restored.version == 1  # 默认值
        assert restored.opaque == 0  # 默认值
        assert restored.flag == 0  # 默认值
        assert restored.remark is None
        assert restored.ext_fields == {}
        assert restored.body is None

    def test_deserialize_invalid_data(self):
        """测试反序列化无效数据"""
        # 测试数据过短
        with pytest.raises(ProtocolError):
            RemotingCommandSerializer.deserialize(b"short")

        # 测试损坏的JSON
        invalid_data = (
            b"\x00\x00\x00\x20"  # 总长度32
            b"\x00\x00\x00\x10"  # header长度16
            b"invalid json data"  # 无效的JSON
        )
        with pytest.raises(DeserializationError):
            RemotingCommandSerializer.deserialize(invalid_data)

    def test_deserialize_incomplete_data(self):
        """测试反序列化不完整数据"""
        # 构造一个不完整的数据包
        incomplete_data = (
            b"\x00\x00\x00\x30"  # 总长度48
            b"\x00\x00\x00\x10"  # header长度16
            b'{"code":10}'  # 只有10字节的header，应该有16字节
        )
        with pytest.raises(ProtocolError):
            RemotingCommandSerializer.deserialize(incomplete_data)

    def test_serialize_large_body(self):
        """测试大消息体序列化"""
        # 测试接近限制的大消息
        large_body = b"x" * (1024 * 1024)  # 1MB
        command = RemotingCommand(
            code=RequestCode.SEND_MESSAGE, body=large_body
        )

        data = RemotingCommandSerializer.serialize(command)
        restored = RemotingCommandSerializer.deserialize(data)

        assert restored.body == large_body

    def test_serialize_too_large_message(self):
        """测试过大消息序列化"""
        # 创建超过限制的消息
        too_large_body = b"x" * (1024 * 1024 * 33)  # 33MB，超过32MB限制
        command = RemotingCommand(
            code=RequestCode.SEND_MESSAGE, body=too_large_body
        )

        with pytest.raises(MessageTooLargeError):
            RemotingCommandSerializer.serialize(command)

    def test_serialize_too_large_header(self):
        """测试过大header序列化"""
        # 创建过大的header
        large_ext_fields = {f"key_{i}": f"value_{i}" for i in range(10000)}
        command = RemotingCommand(
            code=RequestCode.SEND_MESSAGE, ext_fields=large_ext_fields
        )

        with pytest.raises(HeaderTooLargeError):
            RemotingCommandSerializer.serialize(command)

    def test_get_frame_info(self):
        """测试获取帧信息"""
        command = RemotingCommand(code=RequestCode.SEND_MESSAGE)
        data = RemotingCommandSerializer.serialize(command)

        total_length, header_length = RemotingCommandSerializer.get_frame_info(
            data
        )

        assert total_length == len(data)
        assert header_length > 0
        assert header_length < total_length

    def test_get_frame_info_invalid_data(self):
        """测试获取帧信息 - 无效数据"""
        with pytest.raises(ProtocolError):
            RemotingCommandSerializer.get_frame_info(b"short")

    def test_validate_frame(self):
        """测试验证帧格式"""
        command = RemotingCommand(code=RequestCode.SEND_MESSAGE)
        data = RemotingCommandSerializer.serialize(command)

        assert RemotingCommandSerializer.validate_frame(data)

        # 测试无效帧
        assert not RemotingCommandSerializer.validate_frame(b"short")
        assert not RemotingCommandSerializer.validate_frame(
            data[:4]
        )  # 只有部分数据

    def test_serialize_unicode_chars(self):
        """测试Unicode字符序列化"""
        command = RemotingCommand(
            code=RequestCode.SEND_MESSAGE,
            remark="中文测试",
            ext_fields={
                "topic": "测试主题",
                "description": "这是一个包含中文的扩展字段",
            },
        )

        data = RemotingCommandSerializer.serialize(command)
        restored = RemotingCommandSerializer.deserialize(data)

        assert restored.remark == "中文测试"
        assert restored.ext_fields["topic"] == "测试主题"
        assert (
            restored.ext_fields["description"] == "这是一个包含中文的扩展字段"
        )

    def test_serialize_flag_types(self):
        """测试不同flag类型的序列化"""
        test_cases = [
            (FlagType.RPC_TYPE, "request"),
            (FlagType.RPC_ONEWAY, "oneway"),
            (FlagType.RESPONSE_TYPE, "response"),
        ]

        for flag, flag_type in test_cases:
            command = RemotingCommand(code=RequestCode.SEND_MESSAGE, flag=flag)

            data = RemotingCommandSerializer.serialize(command)
            restored = RemotingCommandSerializer.deserialize(data)

            assert restored.flag == flag
            assert restored.is_request == (flag == FlagType.RPC_TYPE)
            assert restored.is_oneway == (flag == FlagType.RPC_ONEWAY)
            assert restored.is_response == (flag == FlagType.RESPONSE_TYPE)

    def test_serialize_header_json_format(self):
        """测试header的JSON格式"""
        command = RemotingCommand(
            code=RequestCode.SEND_MESSAGE,
            remark="test",
            ext_fields={"key": "value"},
        )

        data = RemotingCommandSerializer.serialize(command)

        # 解析length字段
        import struct

        total_length = struct.unpack_from("!i", data, 0)[0]
        header_length = struct.unpack_from("!i", data, 4)[0]

        # 提取header数据
        header_data = data[8 : 8 + header_length]
        header_json = header_data.decode("utf-8")

        # 验证JSON格式
        header_dict = json.loads(header_json)

        assert header_dict["code"] == RequestCode.SEND_MESSAGE
        assert header_dict["language"] == int(LanguageCode.PYTHON)
        assert header_dict["remark"] == "test"
        assert header_dict["extFields"]["key"] == "value"
