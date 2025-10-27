"""
事务消息Producer实现

基于现有Producer模块扩展，实现RocketMQ事务消息功能。
提供完整的本地事务执行和状态回查机制。
"""

from dataclasses import dataclass
from typing import Any, Optional, Tuple

from pyrocketmq.broker.client import BrokerClient
from pyrocketmq.logging import get_logger
from pyrocketmq.model import (
    LocalTransactionState,
    Message,
    RemotingCommand,
    SendMessageResult,
)
from pyrocketmq.model.enums import RequestCode
from pyrocketmq.model.headers import (
    CheckTransactionStateRequestHeader,
)
from pyrocketmq.model.message import MessageProperty
from pyrocketmq.model.message_ext import MessageExt
from pyrocketmq.model.message_id import MessageID, unmarshal_msg_id
from pyrocketmq.producer.config import ProducerConfig
from pyrocketmq.producer.errors import (
    BrokerNotAvailableError,
    MessageSendError,
    ProducerError,
    QueueNotAvailableError,
    RouteNotFoundError,
)
from pyrocketmq.producer.router import MASTER_BROKER_ID

from .producer import Producer
from .transaction import (
    HalfMessageSendError,
    TransactionCheckError,
    TransactionCommitError,
    TransactionError,
    TransactionListener,
    TransactionRollbackError,
    TransactionSendResult,
)


@dataclass
class CheckTransactionStateCallback:
    """事务状态检查回调数据结构

    包含从事务检查请求中解析出的完整信息，用于事务状态回查。
    """

    # Broker服务端地址
    addr: str

    # 消息对象，从notify消息的body中解码得到
    msg: MessageExt

    # 请求头，从notify消息的header中解码得到
    header: CheckTransactionStateRequestHeader

    def __str__(self) -> str:
        """字符串表示"""
        return f"CheckTransactionStateCallback[addr={self.addr}, msg_id={self.msg.msg_id}, transaction_id={self.header.transaction_id}]"

    def __repr__(self) -> str:
        """详细字符串表示"""
        return f"CheckTransactionStateCallback(addr='{self.addr}', msg={self.msg}, header={self.header})"


class TransactionProducer(Producer):
    """事务消息Producer

    继承自Producer，扩展事务消息功能。支持：
    - 事务消息发送
    - 本地事务执行
    - 异步事务状态回查
    - 事务超时处理

    Usage:
        >>> listener = MyTransactionListener()
        >>> producer = TransactionProducer(
        ...     config=ProducerConfig(producer_group="transaction_group"),
        ...     transaction_listener=listener
        ... )
        >>> producer.start()
        >>>
        >>> message = Message(topic="test_topic", body=b"transaction data")
        >>> result = producer.send_message_in_transaction(message)
        >>>
        >>> producer.shutdown()
    """

    def __init__(
        self,
        config=None,
        transaction_listener: Optional[TransactionListener] = None,
    ):
        """初始化TransactionProducer

        Args:
            config: Producer配置
            transaction_listener: 事务监听器
        """
        super().__init__(config)
        self._transaction_listener = transaction_listener
        self._logger = get_logger(__name__)

        # 事务检查请求码（根据RocketMQ协议定义）
        self._transaction_check_code = RequestCode.CHECK_TRANSACTION_STATE.value

        # 事务相关的配置
        self._transaction_timeout = 60000  # 默认60秒超时
        self._max_check_times = 15  # 最大回查次数

    def start(self) -> None:
        """启动TransactionProducer"""
        super().start()
        self._logger.info("TransactionProducer started")

    def send_message_in_transaction(
        self, message: Message, arg: Any = None
    ) -> TransactionSendResult:
        """发送事务消息

        完整的事务消息发送流程：
        1. 发送带有事务标记的消息
        2. 执行本地事务
        3. 提交或回滚事务

        Args:
            message: 要发送的消息
            arg: 执行本地事务时的参数

        Returns:
            TransactionSendResult: 事务发送结果

        Raises:
            TransactionError: 事务相关错误
            ProducerStateError: Producer未启动
        """
        if self._transaction_listener is None:
            raise TransactionError(
                "TransactionListener is required for transactional producer"
            )

        try:
            # 1. 发送带有事务标记的消息
            send_result = self._send_message_with_transaction_flag(message)

            if not send_result.is_success:
                raise MessageSendError(
                    message=f"发送事务消息失败: {send_result.status_name}",
                    topic=message.topic,
                    broker=send_result.message_queue.broker_name,
                )

            # 2. 从结果中获取Broker分配的transactionId
            transaction_id = send_result.transaction_id
            if not transaction_id:
                raise HalfMessageSendError(
                    "No transactionId returned from broker"
                )

            # 3. 执行本地事务
            local_state = self._execute_local_transaction(
                message, transaction_id, arg
            )

            # 4. 发送事务状态确认
            self._send_transaction_confirmation(
                send_result, local_state, send_result.message_queue
            )

            # 5. 构造事务发送结果
            return TransactionSendResult(
                status=send_result.status,
                msg_id=send_result.msg_id,
                message_queue=send_result.message_queue,
                queue_offset=send_result.queue_offset,
                transaction_id=transaction_id,
                local_transaction_state=local_state,
            )

        except Exception as e:
            self._logger.error(f"发送事务消息失败: {e}")
            raise MessageSendError(
                message=f"发送事务消息失败: {e}",
                topic=message.topic,
            )

    def _send_message_with_transaction_flag(
        self, message: Message
    ) -> SendMessageResult:
        """发送带有事务标记的消息，按需注册事务检查处理器"""
        self._check_running()

        try:
            # 1. 验证消息（使用Producer的验证逻辑）
            from .utils import validate_message

            validate_message(message, self._config.max_message_size)

            # 2. 更新路由信息（遵循Producer的设计模式）
            if message.topic not in self._topic_mapping.get_all_topics():
                self.update_route_info(message.topic)

            # 3. 通过MessageRouter获取队列和Broker
            routing_result = self._message_router.route_message(
                message.topic, message
            )
            if not routing_result.success:
                raise RouteNotFoundError(
                    f"Route not found for topic: {message.topic}"
                )

            message_queue = routing_result.message_queue
            broker_data = routing_result.broker_data

            if not message_queue:
                raise QueueNotAvailableError(
                    f"No available queue for topic: {message.topic}"
                )

            if not broker_data:
                raise BrokerNotAvailableError(
                    f"No available broker for topic: {message.topic}"
                )

            broker_addr = f"{broker_data.broker_name}:{broker_data.broker_addresses[MASTER_BROKER_ID]}"

            # 4. 发送事务消息
            with self._broker_manager.connection(broker_addr) as broker_remote:
                # 按需注册事务检查处理器到当前连接
                try:
                    broker_remote.register_request_processor_lazy(
                        self._transaction_check_code,
                        self._handle_transaction_check,
                    )
                    self._logger.debug(
                        f"为Broker {broker_addr} 注册事务检查处理器"
                    )
                except Exception as e:
                    self._logger.warning(
                        f"为Broker {broker_addr} 注册事务检查处理器失败: {e}"
                    )

                return BrokerClient(broker_remote).sync_send_message(
                    self._config.producer_group, message.body, message_queue
                )

        except Exception as e:
            self._logger.error(f"发送事务消息失败: {e}")

            if isinstance(e, ProducerError):
                raise

            raise MessageSendError(f"Oneway message send failed: {e}") from e

    def _execute_local_transaction(
        self, message: Message, transaction_id: str, arg: Any = None
    ) -> LocalTransactionState:
        """执行本地事务

        Args:
            message: 原始消息
            transaction_id: 事务ID
            arg: 执行参数

        Returns:
            LocalTransactionState: 本地事务状态
        """
        try:
            self._logger.debug(f"执行本地事务: transactionId={transaction_id}")
            if not transaction_id:
                raise ValueError("transaction_id is required")
            if not self._transaction_listener:
                raise ValueError("transaction_listener is required")

            return self._transaction_listener.execute_local_transaction(
                message, transaction_id, arg
            )
        except Exception as e:
            self._logger.error(
                f"执行本地事务失败: transactionId={transaction_id}, error={e}"
            )
            # 本地事务执行失败，回滚消息
            return LocalTransactionState.ROLLBACK_MESSAGE_STATE

    def _send_transaction_confirmation(
        self,
        result: SendMessageResult,
        local_state: LocalTransactionState,
        message_queue,
    ) -> None:
        """发送事务状态确认

        Args:
            result: 发送结果
            local_state: 本地事务状态
            message_queue: 消息队列信息
        """
        msg_id: MessageID
        if result.offset_msg_id:
            msg_id = unmarshal_msg_id(result.offset_msg_id)
        elif result.msg_id:
            msg_id = unmarshal_msg_id(result.msg_id)
        else:
            raise ValueError("Invalid message ID")

        transaction_id = result.transaction_id or ""

        try:
            # 获取Broker地址
            broker_addr = f"{message_queue.broker_name}:{message_queue.broker_addresses[0].port}"

            with self._broker_manager.connection(broker_addr) as broker_remote:
                broker_client = BrokerClient(broker_remote)

                broker_client.end_transaction(
                    self._config.producer_group,
                    result.queue_offset,
                    msg_id.offset,
                    local_state,
                    result.msg_id,
                    transaction_id,
                    True,
                )
                if local_state == LocalTransactionState.COMMIT_MESSAGE_STATE:
                    self._logger.debug(
                        f"提交事务: transactionId={transaction_id}"
                    )
                elif (
                    local_state == LocalTransactionState.ROLLBACK_MESSAGE_STATE
                ):
                    self._logger.debug(
                        f"回滚事务: transactionId={transaction_id}"
                    )
                elif local_state == LocalTransactionState.UNKNOW_STATE:
                    self._logger.debug(
                        f"未知事务状态: transactionId={transaction_id}"
                    )

        except Exception as e:
            self._logger.error(
                f"发送事务状态确认失败: transactionId={transaction_id}, error={e}"
            )

            # 根据状态类型抛出不同的异常
            if local_state == LocalTransactionState.COMMIT_MESSAGE_STATE:
                raise TransactionCommitError(
                    f"Failed to commit transaction: {transaction_id}"
                )
            elif local_state == LocalTransactionState.ROLLBACK_MESSAGE_STATE:
                raise TransactionRollbackError(
                    f"Failed to rollback transaction: {transaction_id}"
                )
            else:
                raise TransactionError(
                    f"Failed to handle unknown transaction state: {transaction_id}"
                )

    def _handle_transaction_check(
        self, request: RemotingCommand, remote_addr: Tuple[str, int]
    ) -> None:
        """处理Broker的事务状态检查请求

        Args:
            request: Broker发送的检查请求
            remote_addr: Broker地址信息

        Returns:
            Optional[RemotingCommand]: 检查结果响应
        """
        try:
            # 解析事务检查回调数据
            broker_addr = f"{remote_addr[0]}:{remote_addr[1]}"
            callback = self._parse_original_message_from_request(
                request, broker_addr
            )
            if not callback:
                self._logger.error("无法解析事务检查回调")
                return None

            # 解析transaction_id
            transaction_id = callback.msg.get_property(
                MessageProperty.TRANSACTION_ID
            )
            if not transaction_id:
                transaction_id = callback.header.transaction_id
                if not transaction_id:
                    transaction_id = callback.msg.transaction_id
                    if not transaction_id:
                        self._logger.error("无法获取事务ID")
                        return None

            # 解析UNIQ_KEY
            uniq_key = callback.msg.get_property(
                MessageProperty.UNIQUE_CLIENT_MESSAGE_ID_KEY_INDEX
            )
            if not uniq_key:
                uniq_key = callback.msg.msg_id
                if not uniq_key:
                    self._logger.error("无法获取UNIQ_KEY")
                    return None

            self._logger.debug(
                f"收到事务检查请求: transactionId={transaction_id}, "
                f"broker={broker_addr}, msg_id={callback.msg.msg_id}"
            )
            if not self._transaction_listener:
                self._logger.error("无法处理事务检查请求: 未设置事务监听器")
                return None

            # 调用用户定义的检查逻辑
            local_state = self._transaction_listener.check_local_transaction(
                callback.msg, transaction_id
            )

            self._logger.debug(
                f"发送事务检查响应: transactionId={transaction_id}, state={local_state}"
            )
            with self._broker_manager.connection(broker_addr) as broker_remote:
                BrokerClient(broker_remote).end_transaction(
                    self._config.producer_group,
                    callback.header.tran_state_table_offset,
                    callback.header.commit_log_offset,
                    local_state,
                    uniq_key,
                    transaction_id,
                    True,
                )
                if local_state == LocalTransactionState.COMMIT_MESSAGE_STATE:
                    self._logger.debug(
                        f"提交事务: transactionId={transaction_id}"
                    )
                elif (
                    local_state == LocalTransactionState.ROLLBACK_MESSAGE_STATE
                ):
                    self._logger.debug(
                        f"回滚事务: transactionId={transaction_id}"
                    )
                elif local_state == LocalTransactionState.UNKNOW_STATE:
                    self._logger.debug(
                        f"未知事务状态: transactionId={transaction_id}"
                    )

            return

        except Exception as e:
            self._logger.error(f"处理事务检查请求失败: {e}")
            raise TransactionCheckError(
                f"Failed to handle transaction check: {e}"
            )

    def _parse_original_message_from_request(
        self, request: RemotingCommand, addr: str
    ) -> Optional[CheckTransactionStateCallback]:
        """从检查请求中解析原始消息

        Args:
            request: 事务检查请求
            addr: notify的broker服务端地址

        Returns:
            Optional[CheckTransactionStateCallback]: 解析出的事务状态检查回调数据
        """
        try:
            # 解析请求头
            header = CheckTransactionStateRequestHeader.decode(
                request.ext_fields
            )

            # 从请求body中解码MessageExt
            if not request.body:
                self._logger.error("事务检查请求缺少消息体")
                return None

            # 使用MessageExt.from_bytes方法解析消息体
            message_ext = MessageExt.from_bytes(request.body)

            # 创建回调数据结构
            callback = CheckTransactionStateCallback(
                addr=addr, msg=message_ext, header=header
            )

            self._logger.debug(
                f"成功解析事务检查回调: addr={addr}, "
                f"msg_id={message_ext.msg_id}, "
                f"transaction_id={header.transaction_id}"
            )

            return callback

        except Exception as e:
            self._logger.error(f"解析事务检查回调失败: {e}")
            return None

    def set_transaction_timeout(self, timeout_ms: int) -> None:
        """设置事务超时时间

        Args:
            timeout_ms: 超时时间（毫秒）
        """
        self._transaction_timeout = timeout_ms
        self._logger.info(f"设置事务超时时间: {timeout_ms}ms")

    def set_max_check_times(self, max_times: int) -> None:
        """设置最大回查次数

        Args:
            max_times: 最大回查次数
        """
        self._max_check_times = max_times
        self._logger.info(f"设置最大回查次数: {max_times}")

    def get_stats(self) -> dict:
        """获取TransactionProducer统计信息

        Returns:
            dict: 统计信息
        """
        base_stats = super().get_stats()
        base_stats.update(
            {
                "transaction_timeout_ms": self._transaction_timeout,
                "max_check_times": self._max_check_times,
                "has_transaction_listener": self._transaction_listener
                is not None,
            }
        )
        return base_stats


# 便利函数
def create_transactional_producer(
    producer_group: str,
    namesrv_addr: str,
    transaction_listener: TransactionListener,
    **kwargs,
) -> TransactionProducer:
    """创建TransactionProducer的便利函数

    Args:
        producer_group: 生产者组名
        nameserver_addrs: NameServer地址
        transaction_listener: 事务监听器
        **kwargs: 其他配置参数

    Returns:
        TransactionProducer: 配置好的事务消息生产者
    """

    config = ProducerConfig(
        producer_group=producer_group,
        namesrv_addr=namesrv_addr,
        **kwargs,
    )

    return TransactionProducer(
        config=config, transaction_listener=transaction_listener
    )
