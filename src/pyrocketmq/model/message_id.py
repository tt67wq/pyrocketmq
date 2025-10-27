"""
MessageID数据结构
RocketMQ消息ID的生成和解析功能。
"""

import ipaddress
import struct
from dataclasses import dataclass
from typing import Optional


@dataclass
class MessageID:
    """RocketMQ消息ID数据结构

    与Go语言实现保持兼容的消息ID类，包含消息的物理存储位置信息。
    """

    addr: str  # Broker地址（IP地址）
    port: int  # Broker端口
    offset: int  # 消息在Broker中的偏移量

    def __str__(self) -> str:
        """转换为字符串表示

        Returns:
            MessageID字符串表示
        """
        return f"MessageID[addr={self.addr}, port={self.port}, offset={self.offset}]"

    def __repr__(self) -> str:
        """详细字符串表示

        Returns:
            MessageID详细字符串表示
        """
        return f"MessageID(addr='{self.addr}', port={self.port}, offset={self.offset})"

    def to_dict(self) -> dict:
        """转换为字典格式

        Returns:
            MessageID字典
        """
        return {
            "addr": self.addr,
            "port": self.port,
            "offset": self.offset,
        }


def create_message_id(addr: str, port: int, offset: int) -> str:
    """创建消息ID字符串（与Go实现保持一致）

    Args:
        addr: Broker IP地址（如 "192.168.1.100"）
        port: Broker端口
        offset: 消息在Broker中的偏移量

    Returns:
        消息ID字符串（32字符十六进制大写字符串）

    Examples:
        >>> msg_id = create_message_id("192.168.1.100", 10911, 123456789)
        >>> print(len(msg_id))  # 32
        >>> print(msg_id.islower())  # False (大写)

    Note:
        生成的消息ID格式与Go实现完全一致：
        - IP地址：4字节，大端序
        - 端口：4字节，大端序
        - 偏移量：8字节，大端序
        - 总共16字节，编码为32字符十六进制字符串
    """
    try:
        # 将IP地址转换为4字节的二进制数据
        ip_obj = ipaddress.ip_address(addr)
        if ip_obj.version == 4:
            ip_bytes = ip_obj.packed  # 4字节
        else:
            # IPv6地址需要特殊处理，但RocketMQ通常使用IPv4
            raise ValueError(f"不支持IPv6地址: {addr}")
    except ValueError as e:
        # 如果是不支持IPv6地址的错误，直接重新抛出
        if "不支持IPv6地址" in str(e):
            raise
        raise ValueError(f"无效的IP地址: {addr}") from e

    # 创建字节缓冲区
    buffer = bytearray()

    # 写入IP地址（4字节）
    buffer.extend(ip_bytes)

    # 写入端口（4字节，大端序）
    buffer.extend(struct.pack(">I", port))

    # 写入偏移量（8字节，大端序）
    buffer.extend(struct.pack(">Q", offset))

    # 转换为十六进制字符串并大写
    return buffer.hex().upper()


def unmarshal_msg_id(msg_id: str) -> MessageID:
    """解析消息ID字符串（与Go实现保持一致）

    Args:
        msg_id: 消息ID字符串（32字符十六进制字符串）

    Returns:
        解析后的MessageID对象

    Raises:
        ValueError: 当msg_id格式无效时抛出

    Examples:
        >>> msg_id = create_message_id("192.168.1.100", 10911, 123456789)
        >>> parsed = unmarshal_msg_id(msg_id)
        >>> print(parsed.addr)  # "192.168.1.100"
        >>> print(parsed.port)  # 10911
        >>> print(parsed.offset)  # 123456789

    Note:
        此函数与Go实现保持一致，解析格式：
        - 0-7字符：IP地址（4字节）
        - 8-15字符：端口（4字节）
        - 16-31字符：偏移量（8字节）
    """
    # 验证长度
    if len(msg_id) < 32:
        raise ValueError(f"消息ID长度不足32字符: {msg_id}")

    # 提取各个部分
    ip_hex = msg_id[0:8]
    port_hex = msg_id[8:16]
    offset_hex = msg_id[16:32]

    try:
        # 解析IP地址（4字节）
        ip_bytes = bytes.fromhex(ip_hex)
        ip_addr = str(ipaddress.ip_address(ip_bytes))

        # 解析端口（4字节，大端序）
        port_bytes = bytes.fromhex(port_hex)
        port = struct.unpack(">I", port_bytes)[0]

        # 解析偏移量（8字节，大端序）
        offset_bytes = bytes.fromhex(offset_hex)
        offset = struct.unpack(">Q", offset_bytes)[0]

        return MessageID(addr=ip_addr, port=port, offset=offset)
    except Exception as e:
        raise ValueError(f"解析消息ID失败: {msg_id}") from e


def get_address_by_bytes(ip_bytes: bytes) -> str:
    """将字节数组转换为IP地址字符串

    Args:
        ip_bytes: IP地址字节数组（4字节）

    Returns:
        IP地址字符串

    Examples:
        >>> addr = get_address_by_bytes(b'\xc0\xa8\x01\x64')
        >>> print(addr)  # "192.168.1.100"
    """
    if len(ip_bytes) != 4:
        raise ValueError(f"IP地址字节数组长度必须为4，实际为: {len(ip_bytes)}")

    return str(ipaddress.ip_address(ip_bytes))


def is_valid_message_id(msg_id: str) -> bool:
    """验证消息ID字符串是否有效

    Args:
        msg_id: 消息ID字符串

    Returns:
        是否有效
    """
    try:
        # 长度检查
        if len(msg_id) != 32:
            return False

        # 尝试解析
        unmarshal_msg_id(msg_id)
        return True
    except Exception:
        return False


def create_message_id_from_bytes(
    addr_bytes: bytes, port: int, offset: int
) -> str:
    """从字节数组创建消息ID字符串（与Go实现保持一致）

    Args:
        addr_bytes: IP地址字节数组（4字节）
        port: 端口
        offset: 偏移量

    Returns:
        消息ID字符串

    Examples:
        >>> addr_bytes = socket.inet_aton("192.168.1.100")
        >>> msg_id = create_message_id_from_bytes(addr_bytes, 10911, 123456789)
    """
    if len(addr_bytes) != 4:
        raise ValueError(
            f"IP地址字节数组长度必须为4，实际为: {len(addr_bytes)}"
        )

    # 转换为IP地址字符串后使用标准函数创建
    addr = get_address_by_bytes(addr_bytes)
    return create_message_id(addr, port, offset)


# 便利函数
def parse_message_id_from_string(msg_id_str: str) -> Optional[MessageID]:
    """安全解析消息ID字符串（不会抛出异常）

    Args:
        msg_id_str: 消息ID字符串

    Returns:
        解析后的MessageID对象，如果解析失败则返回None
    """
    try:
        return unmarshal_msg_id(msg_id_str)
    except Exception:
        return None


def format_message_id_info(msg_id: str) -> str:
    """格式化消息ID信息为可读字符串

    Args:
        msg_id: 消息ID字符串

    Returns:
        格式化的信息字符串

    Examples:
        >>> msg_id = create_message_id("192.168.1.100", 10911, 123456789)
        >>> info = format_message_id_info(msg_id)
        >>> print(info)  # "Broker: 192.168.1.100:10911, Offset: 123456789"
    """
    try:
        parsed = unmarshal_msg_id(msg_id)
        return f"Broker: {parsed.addr}:{parsed.port}, Offset: {parsed.offset}"
    except Exception as e:
        return f"Invalid MessageID: {msg_id} (Error: {e})"
