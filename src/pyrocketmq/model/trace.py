"""Trace related data structures for pyrocketmq."""

from dataclasses import dataclass
from enum import Enum
from typing import Any, ClassVar


class MessageType(Enum):
    """Message type enumeration."""

    NORMAL: ClassVar[int] = 0
    TRANSACT: ClassVar[int] = 1
    DELAY: ClassVar[int] = 2


class TraceType(Enum):
    """Trace type enumeration."""

    PUB: ClassVar[str] = "Pub"
    SUB_BEFORE: ClassVar[str] = "SubBefore"
    SUB_AFTER: ClassVar[str] = "SubAfter"


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
