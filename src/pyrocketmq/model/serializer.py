"""
RocketMQ远程命令序列化器
"""

import json
import struct
from typing import Tuple

from .command import RemotingCommand
from .enums import LanguageCode
from .errors import (
    DeserializationError,
    HeaderTooLargeError,
    MessageTooLargeError,
    ProtocolError,
    SerializationError,
)


class RemotingCommandSerializer:
    """远程命令序列化器

    处理RocketMQ协议的二进制序列化和反序列化。
    协议格式：
    | length(4) | header-length(4) | header-data | body-data |
    """

    # 协议常量
    LENGTH_FORMAT = "!i"  # 4字节整数，大端序
    LENGTH_SIZE = 4

    # 配置限制
    MAX_FRAME_SIZE = 1024 * 1024 * 32  # 32MB
    MAX_HEADER_SIZE = 1024 * 64  # 64KB

    @classmethod
    def serialize(cls, command: RemotingCommand) -> bytes:
        """序列化命令为二进制数据

        Args:
            command: 要序列化的命令对象

        Returns:
            序列化后的二进制数据

        Raises:
            SerializationError: 序列化失败
            MessageTooLargeError: 消息过大
            HeaderTooLargeError: header过大
        """
        try:
            # 序列化header
            header_data = cls._serialize_header(command)
            header_length = len(header_data)

            # 检查header大小限制
            if header_length > cls.MAX_HEADER_SIZE:
                raise HeaderTooLargeError(header_length, cls.MAX_HEADER_SIZE)

            # 计算body长度
            body_length = len(command.body) if command.body else 0

            # 计算总长度
            total_length = cls.LENGTH_SIZE * 2 + header_length + body_length

            # 检查总大小限制
            if total_length > cls.MAX_FRAME_SIZE:
                raise MessageTooLargeError(total_length, cls.MAX_FRAME_SIZE)

            # 构造数据包
            buffer = bytearray()

            # 写入总长度
            buffer.extend(struct.pack(cls.LENGTH_FORMAT, total_length))

            # 写入header长度
            buffer.extend(struct.pack(cls.LENGTH_FORMAT, header_length))

            # 写入header数据
            buffer.extend(header_data)

            # 写入body数据
            if command.body:
                buffer.extend(command.body)

            return bytes(buffer)

        except (struct.error, TypeError) as e:
            raise SerializationError(f"序列化失败: {e}")

    @classmethod
    def deserialize(cls, data: bytes) -> RemotingCommand:
        """从二进制数据反序列化命令

        Args:
            data: 要反序列化的二进制数据

        Returns:
            反序列化后的命令对象

        Raises:
            DeserializationError: 反序列化失败
            ProtocolError: 协议格式错误
        """
        try:
            # 检查最小数据长度
            if len(data) < cls.LENGTH_SIZE * 2:
                raise ProtocolError(
                    f"数据长度不足，最少需要{cls.LENGTH_SIZE * 2}字节"
                )

            # 解析长度字段
            offset = 0
            total_length = struct.unpack_from(cls.LENGTH_FORMAT, data, offset)[
                0
            ]
            offset += cls.LENGTH_SIZE

            header_length = struct.unpack_from(cls.LENGTH_FORMAT, data, offset)[
                0
            ]
            offset += cls.LENGTH_SIZE

            # 验证数据完整性
            if len(data) < total_length:
                raise ProtocolError(
                    f"数据不完整，预期{total_length}字节，实际{len(data)}字节"
                )

            if (
                header_length < 0
                or header_length > total_length - cls.LENGTH_SIZE * 2
            ):
                raise ProtocolError(f"无效的header长度: {header_length}")

            # 解析header
            header_end = offset + header_length
            header_data = data[offset:header_end]
            command = cls._deserialize_header(header_data)

            # 解析body
            body_start = header_end
            body_length = total_length - body_start
            if body_length > 0:
                command.body = data[body_start:total_length]
            else:
                command.body = None

            return command

        except (struct.error, json.JSONDecodeError) as e:
            raise DeserializationError(f"反序列化失败: {e}")
        except (ValueError, KeyError) as e:
            raise DeserializationError(f"数据格式错误: {e}")

    @classmethod
    def _serialize_header(cls, command: RemotingCommand) -> bytes:
        """序列化header数据为JSON格式

        Args:
            command: 命令对象

        Returns:
            序列化后的header数据
        """
        header_dict: dict = {
            "code": command.code,
            "language": int(command.language),
            "version": command.version,
            "opaque": command.opaque,
            "flag": command.flag,
        }

        # 添加可选字段
        if command.remark is not None:
            header_dict["remark"] = command.remark

        if command.ext_fields:
            header_dict["extFields"] = command.ext_fields

        # 使用紧凑的JSON格式
        return json.dumps(
            header_dict, separators=(",", ":"), ensure_ascii=False
        ).encode("utf-8")

    @classmethod
    def _deserialize_header(cls, data: bytes) -> RemotingCommand:
        """从JSON格式反序列化header数据

        Args:
            data: header的JSON数据

        Returns:
            构造的命令对象
        """
        try:
            header_dict = json.loads(data.decode("utf-8"))

            # 提取必需字段
            code = header_dict["code"]
            language = LanguageCode(header_dict["language"])
            version = header_dict.get("version", 1)
            opaque = header_dict.get("opaque", 0)
            flag = header_dict.get("flag", 0)
            remark = header_dict.get("remark")
            ext_fields = header_dict.get("extFields", {})

            # 验证字段类型
            if not isinstance(code, int):
                raise ValueError(f"无效的code类型: {type(code)}")
            if not isinstance(ext_fields, dict):
                raise ValueError(f"无效的extFields类型: {type(ext_fields)}")

            # 创建命令对象
            command = RemotingCommand(
                code=code,
                language=language,
                version=version,
                opaque=opaque,
                flag=flag,
                remark=remark,
                ext_fields=ext_fields,
                body=None,  # body在外层解析
            )

            return command

        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            raise ValueError(f"JSON解析失败: {e}")
        except KeyError as e:
            raise ValueError(f"缺少必需字段: {e}")
        except ValueError as e:
            raise ValueError(f"字段值错误: {e}")

    @classmethod
    def get_frame_info(cls, data: bytes) -> Tuple[int, int]:
        """获取数据帧信息

        Args:
            data: 数据帧的前8个字节

        Returns:
            (总长度, header长度)

        Raises:
            ProtocolError: 数据格式错误
        """
        if len(data) < cls.LENGTH_SIZE * 2:
            raise ProtocolError(
                f"数据长度不足，需要至少{cls.LENGTH_SIZE * 2}字节"
            )

        try:
            total_length = struct.unpack_from(cls.LENGTH_FORMAT, data, 0)[0]
            header_length = struct.unpack_from(
                cls.LENGTH_FORMAT, data, cls.LENGTH_SIZE
            )[0]
            return total_length, header_length
        except struct.error as e:
            raise ProtocolError(f"长度字段解析失败: {e}")

    @classmethod
    def validate_frame(cls, data: bytes) -> bool:
        """验证数据帧格式

        Args:
            data: 要验证的数据

        Returns:
            是否为有效的数据帧
        """
        try:
            if len(data) < cls.LENGTH_SIZE * 2:
                return False

            total_length, header_length = cls.get_frame_info(data)

            # 检查长度是否合理
            if total_length <= 0 or total_length > cls.MAX_FRAME_SIZE:
                return False

            if header_length < 0 or header_length > cls.MAX_HEADER_SIZE:
                return False

            if len(data) < total_length:
                return False

            return True

        except ProtocolError:
            return False
