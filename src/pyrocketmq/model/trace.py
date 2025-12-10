"""Trace related data structures for pyrocketmq."""

from dataclasses import dataclass
from enum import Enum


class MessageType(Enum):
    """Message type enumeration."""

    NORMAL = 0
    TRANSACT = 1
    DELAY = 2


class TraceType(Enum):
    """Trace type enumeration."""

    PUB = "Pub"
    SUB_BEFORE = "SubBefore"
    SUB_AFTER = "SubAfter"


# Constants for trace data
CONTENT_SPLITTER: str = "\001"
FIELD_SPLITTER: str = "\002"


@dataclass
class TraceBean:
    """Trace bean structure for message trace information."""

    topic: str
    msg_id: str
    offset_msg_id: str
    tags: str
    keys: str
    store_host: str
    client_host: str
    store_time: int
    retry_times: int
    body_length: int
    msg_type: MessageType


@dataclass
class TraceTransferBean:
    """Trace transfer bean for data transfer."""

    trans_data: str
    trans_key: list[str]  # not duplicate


@dataclass
class TraceContext:
    """Trace context containing trace information."""

    trace_type: TraceType
    timestamp: int
    region_id: str
    region_name: str
    group_name: str
    cost_time: int
    is_success: bool
    request_id: str
    context_code: int
    trace_beans: list[TraceBean]

    def marshal2bean(self) -> TraceTransferBean:
        """Marshal trace context to trace transfer bean."""
        buffer: list[str] = []

        if self.trace_type == TraceType.PUB:
            bean = self.trace_beans[0]
            buffer.append(str(self.trace_type.value))
            buffer.append(str(self.timestamp))
            buffer.append(self.region_id or "")

            # Handle group name with % separator
            if "%" in self.group_name:
                buffer.append(self.group_name.split("%")[1])
            else:
                buffer.append(self.group_name)

            # Handle topic with % separator
            if "%" in bean.topic:
                buffer.append(bean.topic.split("%")[1])
            else:
                buffer.append(bean.topic)

            buffer.append(bean.msg_id)
            buffer.append(bean.tags or "")
            buffer.append(bean.keys or "")
            buffer.append(bean.store_host or "")
            buffer.append(str(bean.body_length))
            buffer.append(str(self.cost_time))
            buffer.append(str(bean.msg_type.value))
            buffer.append(bean.offset_msg_id or "")
            buffer.append(str(self.is_success).lower())
            buffer.append(bean.client_host or "")

        elif self.trace_type == TraceType.SUB_BEFORE:
            for bean in self.trace_beans:
                buffer.append(str(self.trace_type.value))
                buffer.append(str(self.timestamp))
                buffer.append(self.region_id or "")

                # Handle group name with % separator
                if "%" in self.group_name:
                    buffer.append(self.group_name.split("%")[1])
                else:
                    buffer.append(self.group_name)

                buffer.append(self.request_id or "")
                buffer.append(bean.msg_id)
                buffer.append(str(bean.retry_times))
                buffer.append(bean.keys or "")
                buffer.append(bean.client_host or "")

        elif self.trace_type == TraceType.SUB_AFTER:
            for bean in self.trace_beans:
                buffer.append(str(self.trace_type.value))
                buffer.append(self.request_id or "")
                buffer.append(bean.msg_id)
                buffer.append(str(self.cost_time))
                buffer.append(str(self.is_success).lower())
                buffer.append(bean.keys or "")
                buffer.append(str(self.context_code))
                buffer.append(str(self.timestamp))
                buffer.append(self.group_name)

        # Create transfer bean
        trans_data = CONTENT_SPLITTER.join(buffer)

        # Collect trans keys (unique)
        trans_keys: list[str] = []
        for bean in self.trace_beans:
            trans_keys.append(bean.msg_id)
            if bean.keys:
                trans_keys.append(bean.keys)

        return TraceTransferBean(
            trans_data=trans_data + FIELD_SPLITTER, trans_key=trans_keys
        )
