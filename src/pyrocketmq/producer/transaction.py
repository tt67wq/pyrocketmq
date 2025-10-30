"""
事务消息Producer模块

提供RocketMQ事务消息的完整实现，包括：
- 事务状态枚举定义
- TransactionListener接口定义
- TransactionSendResult结果类
- TransactionProducer核心实现
- 事务相关的辅助工具

MVP版本特性:
- 简化的事务状态管理
- 核心事务消息流程实现
- 基础的本地事务执行和回查
"""

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Optional

from pyrocketmq.model.enums import LocalTransactionState
from pyrocketmq.model.result_data import SendMessageResult

from ..model.message import Message

logger = logging.getLogger(__name__)


class TransactionState(Enum):
    """事务状态枚举

    定义事务消息的整体状态，用于跟踪事务生命周期。
    """

    PREPARE = 1  # 准备阶段（半消息已发送）
    COMMIT = 2  # 已提交
    ROLLBACK = 3  # 已回滚
    UNKNOW_STATEN = 4  # 状态未知


@dataclass
class TransactionSendResult(SendMessageResult):
    """事务消息发送结果

    继承SendMessageResult，添加事务相关的状态信息。
    """

    local_transaction_state: Optional[LocalTransactionState] = None  # 本地事务状态
    transaction_timeout: bool = False  # 是否事务超时
    check_times: int = 0  # 回查次数

    def __post_init__(self):
        """后处理，确保数据类型正确"""
        super().__post_init__()
        if self.transaction_id is None:
            raise ValueError("transaction_id不能为None")

    @property
    def is_commit(self) -> bool:
        """检查是否为提交状态"""
        return (
            self.local_transaction_state == LocalTransactionState.COMMIT_MESSAGE_STATE
        )

    @property
    def is_rollback(self) -> bool:
        """检查是否为回滚状态"""
        return (
            self.local_transaction_state == LocalTransactionState.ROLLBACK_MESSAGE_STATE
        )

    @property
    def is_UNKNOW_STATEn(self) -> bool:
        """检查是否为未知状态"""
        return self.local_transaction_state == LocalTransactionState.UNKNOW_STATE

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        result = super().to_dict()
        result.update(
            {
                "local_transaction_state": self.local_transaction_state.value
                if self.local_transaction_state
                else None,
                "transaction_timeout": self.transaction_timeout,
                "check_times": self.check_times,
            }
        )
        return result

    def __str__(self) -> str:
        """字符串表示"""
        state_str = "UNKNOW_STATEN"
        if self.local_transaction_state:
            state_str = self.local_transaction_state.name

        return (
            f"TransactionSendResult[msgId={self.msg_id}, "
            f"status={self.status_name}, "
            f"transactionId='{self.transaction_id}', "
            f"state={state_str}, "
            f"queue={self.message_queue}]"
        )


class TransactionListener(ABC):
    """事务监听器接口

    定义本地事务执行和回查的逻辑接口，用户需要实现此接口来处理业务逻辑。
    """

    @abstractmethod
    def execute_local_transaction(
        self, message: Message, transaction_id: str, arg: Any = None
    ) -> LocalTransactionState:
        """执行本地事务

        在发送半消息成功后，此方法会被调用来执行本地事务逻辑。

        Args:
            message: 原始消息
            transaction_id: 事务ID
            arg: 执行参数（可选）

        Returns:
            本地事务状态
            - COMMIT_MESSAGE_STATE: 提交事务，消息对消费者可见
            - ROLLBACK_MESSAGE_STATE: 回滚事务，消息将被删除
            - UNKNOW_STATE: 状态未知，触发回查机制
        """
        pass

    @abstractmethod
    def check_local_transaction(
        self, message: Message, transaction_id: str
    ) -> LocalTransactionState:
        """检查本地事务状态

        当事务状态未知时，Broker会回查此方法来确认事务状态。

        Args:
            message: 原始消息
            transaction_id: 事务ID

        Returns:
            本地事务状态
            - COMMIT_MESSAGE_STATE: 提交事务
            - ROLLBACK_MESSAGE_STATE: 回滚事务
            - UNKNOW_STATE: 继续保持未知状态，可能触发后续回查
        """
        pass


class SimpleTransactionListener(TransactionListener):
    """简单事务监听器实现

    提供一个基础实现，用于快速测试和简单场景。
    """

    def __init__(
        self,
        always_commit: bool = False,
        always_rollback: bool = False,
        always_unknow: bool = False,
    ):
        """初始化简单事务监听器

        Args:
            always_commit: 总是返回COMMIT_MESSAGE_STATE
            always_rollback: 总是返回ROLLBACK_MESSAGE_STATE
            always_unknow: 总是返回UNKNOW_STATE
        """
        self.always_commit: bool = always_commit
        self.always_rollback: bool = always_rollback
        self.always_unknow: bool = always_unknow

    def execute_local_transaction(
        self, message: Message, transaction_id: str, arg: None = None
    ) -> LocalTransactionState:
        """执行本地事务"""
        logger.info(f"执行本地事务，事务ID: {transaction_id}, 主题: {message.topic}")

        if self.always_commit:
            logger.info(f"事务 {transaction_id} 配置为总是提交")
            return LocalTransactionState.COMMIT_MESSAGE_STATE
        elif self.always_rollback:
            logger.info(f"事务 {transaction_id} 配置为总是回滚")
            return LocalTransactionState.ROLLBACK_MESSAGE_STATE
        elif self.always_unknow:
            logger.info(f"事务 {transaction_id} 配置为总是未知状态")
            return LocalTransactionState.UNKNOW_STATE
        else:
            # 默认情况下基于消息体内容做简单判断
            try:
                body_str = message.body.decode("utf-8")
                logger.debug(f"事务 {transaction_id} 解析消息体: {body_str}")

                if body_str.startswith("commit"):
                    logger.info(f"事务 {transaction_id} 根据消息体内容决定提交")
                    return LocalTransactionState.COMMIT_MESSAGE_STATE
                elif body_str.startswith("rollback"):
                    logger.info(f"事务 {transaction_id} 根据消息体内容决定回滚")
                    return LocalTransactionState.ROLLBACK_MESSAGE_STATE
                else:
                    logger.info(
                        f"事务 {transaction_id} 消息体内容无法识别，返回未知状态"
                    )
                    return LocalTransactionState.UNKNOW_STATE
            except Exception as e:
                logger.error(f"事务 {transaction_id} 解析消息体失败: {e}")
                return LocalTransactionState.ROLLBACK_MESSAGE_STATE

    def check_local_transaction(
        self, message: Message, transaction_id: str
    ) -> LocalTransactionState:
        """检查本地事务状态"""
        logger.info(
            f"回查本地事务状态，事务ID: {transaction_id}, 主题: {message.topic}"
        )

        # 对于简单实现，回查时返回同样的逻辑
        state = self.execute_local_transaction(message, transaction_id)
        logger.info(f"事务 {transaction_id} 回查结果: {state.name}")
        return state


@dataclass
class TransactionMetadata:
    """事务元数据

    存储事务相关的元数据信息，用于跟踪事务状态。
    """

    transaction_id: str
    message: Message
    local_transaction_state: Optional[LocalTransactionState] = None
    create_time: int = field(
        default_factory=lambda: int(__import__("time").time() * 1000)
    )
    last_check_time: int = field(
        default_factory=lambda: int(__import__("time").time() * 1000)
    )
    check_times: int = 0
    timeout_ms: int = 60000  # 默认60秒超时

    @property
    def is_timeout(self) -> bool:
        """检查是否超时"""
        current_time = int(__import__("time").time() * 1000)
        return (current_time - self.create_time) > self.timeout_ms

    @property
    def need_check(self) -> bool:
        """检查是否需要回查"""
        return (
            self.local_transaction_state == LocalTransactionState.UNKNOW_STATE
            and not self.is_timeout
        )

    def update_check_time(self) -> None:
        """更新回查时间"""
        self.last_check_time = int(__import__("time").time() * 1000)
        self.check_times += 1


# 便利函数
def create_transaction_send_result(
    status: int,
    msg_id: str,
    message_queue,
    queue_offset: int,
    transaction_id: str,
    local_state: Optional[LocalTransactionState] = None,
    offset_msg_id: Optional[str] = None,
    region_id: str = "DefaultRegion",
    trace_on: bool = False,
    transaction_timeout: bool = False,
    check_times: int = 0,
) -> TransactionSendResult:
    """创建事务发送结果的便利函数

    Args:
        status: 发送状态 (SendStatus枚举值)
        msg_id: 消息ID
        message_queue: 消息队列信息
        queue_offset: 队列偏移量
        transaction_id: 事务ID
        local_state: 本地事务状态
        offset_msg_id: 偏移量消息ID
        region_id: 区域ID
        trace_on: 是否开启Trace
        transaction_timeout: 是否事务超时
        check_times: 回查次数

    Returns:
        TransactionSendResult实例
    """
    return TransactionSendResult(
        status=status,
        msg_id=msg_id,
        message_queue=message_queue,
        queue_offset=queue_offset,
        transaction_id=transaction_id,
        offset_msg_id=offset_msg_id,
        region_id=region_id,
        trace_on=trace_on,
        local_transaction_state=local_state,
        transaction_timeout=transaction_timeout,
        check_times=check_times,
    )


def create_transaction_send_result_from_base(
    base_result: SendMessageResult,
    local_state: Optional[LocalTransactionState] = None,
    transaction_timeout: bool = False,
    check_times: int = 0,
) -> TransactionSendResult:
    """从基础SendMessageResult创建事务发送结果的便利函数

    Args:
        base_result: 基础的消息发送结果
        local_state: 本地事务状态
        transaction_timeout: 是否事务超时
        check_times: 回查次数

    Returns:
        TransactionSendResult实例
    """
    return TransactionSendResult(
        status=base_result.status,
        msg_id=base_result.msg_id,
        message_queue=base_result.message_queue,
        queue_offset=base_result.queue_offset,
        transaction_id=base_result.transaction_id,
        offset_msg_id=base_result.offset_msg_id,
        region_id=base_result.region_id,
        trace_on=base_result.trace_on,
        local_transaction_state=local_state,
        transaction_timeout=transaction_timeout,
        check_times=check_times,
    )


def create_simple_transaction_listener(
    commit: bool = True,
) -> SimpleTransactionListener:
    """创建简单事务监听器的便利函数

    Args:
        commit: 是否总是提交

    Returns:
        SimpleTransactionListener实例
    """
    return SimpleTransactionListener(always_commit=commit)


def create_transaction_message(
    topic: str, body: Any, transaction_id: str, **kwargs
) -> Message:
    """创建事务消息的便利函数

    Args:
        topic: 主题名称
        body: 消息体
        transaction_id: 事务ID
        **kwargs: 其他消息属性

    Returns:
        事务消息实例
    """
    from ..model.message import create_transaction_message

    return create_transaction_message(topic, body, transaction_id, **kwargs)


# 事务相关的异常类
class TransactionError(Exception):
    """事务相关错误的基类"""

    pass


class TransactionTimeoutError(TransactionError):
    """事务超时错误"""

    pass


class TransactionCheckError(TransactionError):
    """事务回查错误"""

    pass


class TransactionStateError(TransactionError):
    """事务状态错误"""

    pass


class HalfMessageSendError(TransactionError):
    """半消息发送错误"""

    pass


class TransactionCommitError(TransactionError):
    """事务提交错误"""

    pass


class TransactionRollbackError(TransactionError):
    """事务回滚错误"""

    pass
