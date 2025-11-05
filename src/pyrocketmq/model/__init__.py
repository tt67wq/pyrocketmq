"""
pyrocketmq模型层

提供RocketMQ消息数据结构的Python实现。
"""

from .client_data import (
    SUBSCRIBE_ALL,
    ConsumerData,
    ExpressionType,
    MessageSelector,
    ProducerData,
    SubscriptionData,
    create_sql_selector,
    create_tag_selector,
    is_tag_type,
)
from .command import RemotingCommand
from .consumer import (
    AllocateQueueStrategy,
    ConsumeFromWhere,
    MessageModel,
)
from .enums import (
    TRANSACTION_COMMIT_TYPE,
    TRANSACTION_NOT_TYPE,
    TRANSACTION_PREPARED_TYPE,
    TRANSACTION_ROLLBACK_TYPE,
    FlagType,
    LanguageCode,
    LocalTransactionState,
    RequestCode,
    ResponseCode,
)
from .errors import (
    ConnectionClosedError,
    DeserializationError,
    HeaderTooLargeError,
    InvalidHeaderError,
    InvalidMessageError,
    MessageTooLargeError,
    ProtocolError,
    RemotingCommandError,
    SerializationError,
    TimeoutError,
    UnsupportedLanguageError,
    UnsupportedRequestCodeError,
    UnsupportedVersionError,
    ValidationError,
)
from .factory import (
    RemotingCommandBuilder,
    RemotingRequestFactory,
)
from .heart_beat import HeartbeatData
from .message import (
    Message,
    MessageProperty,
    create_delay_message,
    create_message,
    create_transaction_message,
)
from .message_ext import MessageExt
from .message_id import (
    MessageID,
    create_message_id,
    create_message_id_from_bytes,
    format_message_id_info,
    get_address_by_bytes,
    is_valid_message_id,
    parse_message_id_from_string,
    unmarshal_msg_id,
)
from .message_queue import MessageQueue
from .result_data import (
    ConsumeType,
    OffsetResult,
    PullMessageResult,
    SendMessageResult,
    SendStatus,
)
from .serializer import RemotingCommandSerializer
from .subscription import (
    SubscriptionConflict,
    SubscriptionEntry,
)
from .utils import (
    command_to_dict,
    commands_to_json,
    copy_command_with_new_opaque,
    create_response_for_request,
    filter_commands_by_group,
    filter_commands_by_topic,
    format_ext_fields_for_display,
    generate_opaque,
    get_command_stats,
    get_command_summary,
    get_command_type_name,
    get_group_from_command,
    get_offset_from_command,
    get_queue_id_from_command,
    get_topic_from_command,
    is_error_response,
    is_heartbeat_command,
    is_pull_message_command,
    is_send_message_command,
    is_success_response,
    parse_command_from_json,
    transaction_state,
    validate_command,
)

__all__ = [
    # 核心数据结构
    "ProducerData",
    "ConsumerData",
    "HeartbeatData",
    "RemotingCommand",
    "Message",
    "MessageQueue",
    "MessageProperty",
    "MessageExt",
    "MessageID",
    "SendMessageResult",
    "PullMessageResult",
    "OffsetResult",
    # 订阅相关数据结构
    "ExpressionType",
    "MessageSelector",
    "SubscriptionData",
    "SubscriptionEntry",
    "SubscriptionConflict",
    "SUBSCRIBE_ALL",
    # 消息创建函数
    "create_message",
    "create_transaction_message",
    "create_delay_message",
    # 订阅相关便利函数
    "is_tag_type",
    "create_tag_selector",
    "create_sql_selector",
    # MessageID函数
    "create_message_id",
    "create_message_id_from_bytes",
    "unmarshal_msg_id",
    "is_valid_message_id",
    "parse_message_id_from_string",
    "format_message_id_info",
    "get_address_by_bytes",
    # 枚举类型
    "LanguageCode",
    "RequestCode",
    "FlagType",
    "ResponseCode",
    "LocalTransactionState",
    "SendStatus",
    "ConsumeType",
    "MessageModel",
    "AllocateQueueStrategy",
    "ConsumeFromWhere",
    # 事务常量
    "TRANSACTION_NOT_TYPE",
    "TRANSACTION_PREPARED_TYPE",
    "TRANSACTION_COMMIT_TYPE",
    "TRANSACTION_ROLLBACK_TYPE",
    # 异常类型
    "RemotingCommandError",
    "SerializationError",
    "DeserializationError",
    "ProtocolError",
    "ValidationError",
    "MessageTooLargeError",
    "HeaderTooLargeError",
    "InvalidHeaderError",
    "InvalidMessageError",
    "ConnectionClosedError",
    "TimeoutError",
    "UnsupportedVersionError",
    "UnsupportedLanguageError",
    "UnsupportedRequestCodeError",
    # 序列化器
    "RemotingCommandSerializer",
    # 工厂和构建器
    "RemotingCommandBuilder",
    "RemotingRequestFactory",
    "RemotingRequestFactory",
    # 工具函数
    "validate_command",
    "generate_opaque",
    "is_success_response",
    "is_error_response",
    "get_command_summary",
    "get_topic_from_command",
    "get_group_from_command",
    "get_queue_id_from_command",
    "get_offset_from_command",
    "copy_command_with_new_opaque",
    "create_response_for_request",
    "filter_commands_by_topic",
    "filter_commands_by_group",
    "get_command_stats",
    "format_ext_fields_for_display",
    "command_to_dict",
    "commands_to_json",
    "parse_command_from_json",
    "get_command_type_name",
    "is_heartbeat_command",
    "is_send_message_command",
    "is_pull_message_command",
    "transaction_state",
]
