"""
RocketMQ协议枚举定义
"""

from enum import IntEnum


class LanguageCode(IntEnum):
    """客户端语言代码枚举"""

    JAVA = 0
    CPP = 1
    DOTNET = 2
    PYTHON = 3
    DELPHI = 4
    ERLANG = 5
    RUBY = 6
    OTHER = 7
    HTTP = 8
    GO = 9
    PHP = 10
    OM = 11


class RequestCode(IntEnum):
    """RocketMQ请求代码枚举 - 基于Go语言实现"""

    # 基础消息操作
    SEND_MESSAGE = 10
    PULL_MESSAGE = 11
    QUERY_MESSAGE = 12
    VIEW_MESSAGE_BY_ID = 13

    # 偏移量相关
    QUERY_CONSUMER_OFFSET = 14
    UPDATE_CONSUMER_OFFSET = 15

    # 主题相关
    CREATE_TOPIC = 17

    # 时间戳相关
    SEARCH_OFFSET_BY_TIMESTAMP = 29

    # 偏移量查询
    GET_MAX_OFFSET = 30
    GET_MIN_OFFSET = 31

    # 心跳和连接管理
    HEART_BEAT = 34
    UNREGISTER_CLIENT = 35
    CONSUMER_SEND_MSG_BACK = 36

    # 事务相关
    END_TRANSACTION = 37

    # 消费者管理
    GET_CONSUMER_LIST_BY_GROUP = 38
    CHECK_TRANSACTION_STATE = 39
    NOTIFY_CONSUMER_IDS_CHANGED = 40

    # 批量消息队列
    LOCK_BATCH_MQ = 41
    UNLOCK_BATCH_MQ = 42

    # 路由和集群信息
    GET_ROUTE_INFO_BY_TOPIC = 105
    GET_BROKER_CLUSTER_INFO = 106

    # NameServer相关
    GET_ALL_TOPIC_LIST_FROM_NAME_SERVER = 206
    DELETE_TOPIC_IN_BROKER = 215
    DELETE_TOPIC_IN_NAME_SRV = 216

    # 消费者重置
    RESET_CONSUMER_OFFSET = 220

    # 批量消息
    SEND_BATCH_MESSAGE = 320

    # 消费者运行信息
    GET_CONSUMER_RUNNING_INFO = 307

    # 直接消费
    CONSUME_MESSAGE_DIRECTLY = 309

    # 消息编号相关
    PUT_MSG_NO = 1001
    GET_MSG_NO = 1011


class FlagType(IntEnum):
    """通信类型枚举 - 基于Go语言实现"""

    # 0, REQUEST_COMMAND
    RPC_TYPE = 0
    # 1, RPC
    RPC_ONEWAY = 1
    # ResponseType for response
    RESPONSE_TYPE = 1


class LocalTransactionState(IntEnum):
    """本地事务状态枚举"""

    COMMIT_MESSAGE_STATE = 1  # 提交消息状态
    ROLLBACK_MESSAGE_STATE = 2  # 回滚消息状态
    UNKNOW_STATE = 3  # 未知状态


# 事务类型常量
TRANSACTION_NOT_TYPE = 0  # 非事务消息
TRANSACTION_PREPARED_TYPE = 0x1 << 2  # 事务准备状态 (4)
TRANSACTION_COMMIT_TYPE = 0x2 << 2  # 事务提交状态 (8)
TRANSACTION_ROLLBACK_TYPE = 0x3 << 2  # 事务回滚状态 (12)


class ResponseCode(IntEnum):
    """响应代码枚举"""

    SUCCESS = 0
    ERROR = 1
    FLUSH_DISK_TIMEOUT = 10
    SLAVE_NOT_AVAILABLE = 11
    FLUSH_SLAVE_TIMEOUT = 12
    SERVICE_NOT_AVAILABLE = 14
    TOPIC_NOT_EXIST = 17
    PULL_NOT_FOUND = 19
    PULL_RETRY_IMMEDIATELY = 20
    PULL_OFFSET_MOVED = 21
    QUERY_NOT_FOUND = 22
