"""
扩展消息数据结构
定义RocketMQ扩展消息的核心数据结构，包含完整的消息信息。
"""

import struct
import time
from dataclasses import dataclass
from typing import List, Any

from pyrocketmq.model.message_queue import MessageQueue

from .message import Message, MessageProperty


class MessageSysFlag:
    """消息系统标志常量"""

    COMPRESSED = 0x1
    BORN_HOST_V6 = 0x1 << 4
    STORE_HOST_V6 = 0x1 << 5


@dataclass
class MessageExt(Message):
    """扩展消息信息

    包含消息的完整信息，包括系统属性和用户属性
    """

    # 系统属性
    msg_id: str | None = None  # 消息ID
    offset_msg_id: str | None = None  # 偏移消息ID
    store_size: int | None = None  # 存储大小
    queue_offset: int | None = None  # 队列偏移量
    sys_flag: int = 0  # 系统标志
    born_timestamp: int | None = None  # 生产时间戳
    born_host: str | None = None  # 生产主机
    store_timestamp: int | None = None  # 存储时间戳
    store_host: str | None = None  # 存储主机
    commit_log_offset: int | None = None  # 提交日志偏移量
    body_crc: int | None = None  # 消息体CRC32校验码
    reconsume_times: int = 0  # 重新消费次数
    prepared_transaction_offset: int | None = None  # 预提交事务偏移量

    def __post_init__(self):
        """后处理，确保类型正确"""
        if self.born_timestamp is None:
            self.born_timestamp = int(time.time() * 1000)

    def get_property(self, key: str, default: str | None = None) -> str | None:
        """获取用户属性"""
        return self.properties.get(key, default)

    def set_property(self, key: str, value: str) -> None:
        """设置用户属性"""
        self.properties[key] = value

    def has_property(self, key: str) -> bool:
        """检查是否包含指定属性"""
        return key in self.properties

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "MessageExt":
        """从字典创建MessageExt对象"""
        return cls(
            topic=data.get("topic", ""),
            body=data.get("body", b""),
            flag=data.get("flag", 0),
            transaction_id=data.get("transaction_id"),
            batch=data.get("batch", False),
            compress=data.get("compress", False),
            queue=MessageQueue.from_dict(data.get("queue", {})),
            properties=data.get("properties", {}),
            msg_id=data.get("msg_id"),
            offset_msg_id=data.get("offset_msg_id"),
            store_size=data.get("store_size"),
            queue_offset=data.get("queue_offset"),
            sys_flag=data.get("sys_flag", 0),
            born_timestamp=data.get("born_timestamp"),
            born_host=data.get("born_host"),
            store_timestamp=data.get("store_timestamp"),
            store_host=data.get("store_host"),
            commit_log_offset=data.get("commit_log_offset"),
            body_crc=data.get("body_crc"),
            reconsume_times=data.get("reconsume_times", 0),
            prepared_transaction_offset=data.get("prepared_transaction_offset"),
        )

    @classmethod
    def from_bytes(cls, data: bytes, offset: int = 0) -> "MessageExt":
        """从字节数组中解析出MessageExt对象

        Args:
            data: 字节数组
            offset: 起始偏移量，默认为0

        Returns:
            MessageExt对象和新的偏移量

        Raises:
            ValueError: 数据格式不正确时抛出
        """
        if len(data) < offset + 4:
            raise ValueError("Data too short to contain message size")

        pos = offset
        buf = data[pos:]

        # 1. total size (4 bytes)
        store_size = struct.unpack(">I", buf[0:4])[0]
        pos += 4
        if len(data) + 4 < pos + store_size:
            raise ValueError(
                f"Data too short to contain complete message: expected {store_size}, available {len(data) - pos}"
            )

        # 2. magic code (4 bytes) - 跳过
        pos += 4

        # 3. body CRC32 (4 bytes)
        body_crc = struct.unpack(">I", data[pos : pos + 4])[0]
        pos += 4

        # 4. queueID (4 bytes)
        queue_id = struct.unpack(">i", data[pos : pos + 4])[0]
        pos += 4

        # 5. Flag (4 bytes)
        flag = struct.unpack(">I", data[pos : pos + 4])[0]
        pos += 4

        # 6. QueueOffset (8 bytes)
        queue_offset = struct.unpack(">q", data[pos : pos + 8])[0]
        pos += 8

        # 7. physical offset (8 bytes)
        commit_log_offset = struct.unpack(">q", data[pos : pos + 8])[0]
        pos += 8

        # 8. SysFlag (4 bytes)
        sys_flag = struct.unpack(">I", data[pos : pos + 4])[0]
        pos += 4

        # 9. BornTimestamp (8 bytes)
        born_timestamp = struct.unpack(">q", data[pos : pos + 8])[0]
        pos += 8

        # 10. born host
        if sys_flag & MessageSysFlag.BORN_HOST_V6:
            # IPv6 address (16 bytes) + port (4 bytes)
            host_bytes = data[pos : pos + 16]
            port = struct.unpack(">I", data[pos + 16 : pos + 20])[0]
            pos += 20
        else:
            # IPv4 address (4 bytes) + port (4 bytes)
            host_bytes = data[pos : pos + 4]
            port = struct.unpack(">I", data[pos + 4 : pos + 8])[0]
            pos += 8

        # 转换IP地址为字符串
        if sys_flag & MessageSysFlag.BORN_HOST_V6:
            ip_parts = [str(x) for x in struct.unpack(">" + "H" * 8, host_bytes)]
            born_host = ":".join(
                [
                    ip_parts[0] + ip_parts[1],
                    ip_parts[2] + ip_parts[3],
                    ip_parts[4] + ip_parts[5],
                    ip_parts[6] + ip_parts[7],
                ]
            )
        else:
            ip_parts = struct.unpack(">BBBB", host_bytes)
            born_host = f"{ip_parts[0]}.{ip_parts[1]}.{ip_parts[2]}.{ip_parts[3]}"
        born_host += f":{port}"

        # 11. store timestamp (8 bytes)
        store_timestamp = struct.unpack(">q", data[pos : pos + 8])[0]
        pos += 8

        # 12. store host
        if sys_flag & MessageSysFlag.STORE_HOST_V6:
            # IPv6 address (16 bytes) + port (4 bytes)
            host_bytes = data[pos : pos + 16]
            port = struct.unpack(">I", data[pos + 16 : pos + 20])[0]
            pos += 20
        else:
            # IPv4 address (4 bytes) + port (4 bytes)
            host_bytes = data[pos : pos + 4]
            port = struct.unpack(">I", data[pos + 4 : pos + 8])[0]
            pos += 8

        # 转换IP地址为字符串
        if sys_flag & MessageSysFlag.STORE_HOST_V6:
            ip_parts = [str(x) for x in struct.unpack(">" + "H" * 8, host_bytes)]
            store_host = ":".join(
                [
                    ip_parts[0] + ip_parts[1],
                    ip_parts[2] + ip_parts[3],
                    ip_parts[4] + ip_parts[5],
                    ip_parts[6] + ip_parts[7],
                ]
            )
        else:
            ip_parts = struct.unpack(">BBBB", host_bytes)
            store_host = f"{ip_parts[0]}.{ip_parts[1]}.{ip_parts[2]}.{ip_parts[3]}"
        store_host += f":{port}"

        # 13. reconsume times (4 bytes)
        reconsume_times = struct.unpack(">i", data[pos : pos + 4])[0]
        pos += 4

        # 14. prepared transaction offset (8 bytes)
        prepared_transaction_offset = struct.unpack(">q", data[pos : pos + 8])[0]
        pos += 8

        # 15. body (4 bytes length + body bytes)
        body_length = struct.unpack(">i", data[pos : pos + 4])[0]
        pos += 4
        body = data[pos : pos + body_length]
        pos += body_length

        # 16. topic (1 byte length + topic bytes)
        topic_length = data[pos]
        pos += 1
        topic = data[pos : pos + topic_length].decode("utf-8")
        pos += topic_length

        # 17. properties (2 bytes length + properties bytes)
        properties_length = struct.unpack(">H", data[pos : pos + 2])[0]
        pos += 2
        properties = {}
        tmp_msg = Message(topic="", body=b"")
        if properties_length > 0:
            properties_bytes = data[pos : pos + properties_length]
            pos += properties_length
            # 使用unmarshal_properties方法解析properties
            tmp_msg.unmarshal_properties(properties_bytes.decode("utf-8"))
            properties = tmp_msg.properties

        # 生成消息ID（基于存储偏移量）
        offset_msg_id = f"{commit_log_offset:016x}"

        msg_id = tmp_msg.get_property(
            MessageProperty.UNIQUE_CLIENT_MESSAGE_ID_KEY_INDEX, ""
        )
        if not msg_id:
            msg_id = offset_msg_id

        # 创建MessageExt对象
        return cls(
            topic=topic,
            body=body,
            flag=flag,
            queue=MessageQueue(topic=topic, queue_id=queue_id, broker_name=""),
            properties=properties,
            #
            msg_id=msg_id,
            offset_msg_id=offset_msg_id,
            store_size=store_size,
            queue_offset=queue_offset,
            sys_flag=sys_flag,
            born_timestamp=born_timestamp,
            born_host=born_host,
            store_timestamp=store_timestamp,
            store_host=store_host,
            commit_log_offset=commit_log_offset,
            body_crc=body_crc,
            reconsume_times=reconsume_times,
            prepared_transaction_offset=prepared_transaction_offset,
        )

    @classmethod
    def decode_messages(cls, data: bytes) -> List["MessageExt"]:
        """从字节数组中解析出多个MessageExt对象

        Args:
            data: 包含多个消息的字节数组

        Returns:
            MessageExt对象列表
        """
        messages = []
        pos = 0
        total_length = len(data)

        while pos < total_length:
            # 检查剩余数据是否足够读取消息大小
            if pos + 4 > total_length:
                break

            # 读取消息总大小
            store_size = struct.unpack(">I", data[pos : pos + 4])[0]

            # 检查数据是否完整
            if pos + store_size > total_length:
                break

            try:
                # 解析单个消息
                message = cls.from_bytes(data, pos)
                messages.append(message)
                pos += store_size
            except ValueError:
                # 如果解析失败，停止解析
                break

        return messages
