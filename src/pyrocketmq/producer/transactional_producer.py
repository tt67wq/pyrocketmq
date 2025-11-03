"""
事务消息Producer实现

基于现有Producer模块扩展，实现RocketMQ事务消息功能。
提供完整的本地事务执行和状态回查机制。
"""

import logging
from typing import Any
from typing_extensions import override

from pyrocketmq.broker.client import BrokerClient
from pyrocketmq.broker.errors import BrokerError, BrokerTimeoutError
from pyrocketmq.logging import get_logger
from pyrocketmq.model import (
    LocalTransactionState,
    Message,
    RemotingCommand,
    SendMessageResult,
    MessageQueue,
)
from pyrocketmq.model.enums import RequestCode
from pyrocketmq.model.headers import (
    CheckTransactionStateRequestHeader,
)
from pyrocketmq.model.message import MessageProperty
from pyrocketmq.model.message_ext import MessageExt
from pyrocketmq.model.message_id import MessageID, unmarshal_msg_id
from pyrocketmq.nameserver.models import TopicRouteData
from pyrocketmq.producer.config import ProducerConfig
from pyrocketmq.producer.errors import (
    BrokerNotAvailableError,
    MessageSendError,
    ProducerError,
    QueueNotAvailableError,
    RouteNotFoundError,
)
from pyrocketmq.nameserver.client import SyncNameServerClient
from pyrocketmq.producer.router import RoutingResult
from pyrocketmq.remote.sync_remote import Remote

from .producer import Producer
from .transaction import (
    TransactionCheckError,
    TransactionCommitError,
    TransactionError,
    TransactionListener,
    TransactionRollbackError,
    TransactionSendResult,
    CheckTransactionStateCallback,
)


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
        config: ProducerConfig | None = None,
        transaction_listener: TransactionListener | None = None,
    ) -> None:
        """初始化TransactionProducer

        Args:
            config: Producer配置
            transaction_listener: 事务监听器
        """
        super().__init__(config)
        self._transaction_listener: TransactionListener | None = transaction_listener
        self._logger: logging.Logger = get_logger(__name__)

        # 事务检查请求码（根据RocketMQ协议定义）
        self._transaction_check_code: int = RequestCode.CHECK_TRANSACTION_STATE.value

        # 事务相关的配置
        self._transaction_timeout: int = 60000  # 默认60秒超时
        self._max_check_times: int = 15  # 最大回查次数

        self._failed_registrations: set[str] = set()

    @override
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

        transaction_id: str | None = ""
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
            if send_result.transaction_id:
                message.set_property(
                    MessageProperty.TRANSACTION_ID, send_result.transaction_id
                )

            transaction_id = message.get_property(
                MessageProperty.UNIQUE_CLIENT_MESSAGE_ID_KEY_INDEX
            )
            if transaction_id:
                message.transaction_id = transaction_id

            # 3. 执行本地事务
            local_state: LocalTransactionState = self._execute_local_transaction(
                message, transaction_id or "", arg
            )

            # 4. 发送事务状态确认
            self._send_transaction_confirmation(
                send_result,
                local_state,
                send_result.message_queue,
                send_result.transaction_id or "",
            )

            # 5. 构造事务发送结果
            return TransactionSendResult(
                status=send_result.status,
                msg_id=send_result.msg_id,
                message_queue=send_result.message_queue,
                queue_offset=send_result.queue_offset,
                transaction_id=transaction_id,
                offset_msg_id=send_result.offset_msg_id,
                local_transaction_state=local_state,
            )

        except Exception as e:
            self._logger.error(
                "Failed to send transaction message",
                extra={
                    "topic": message.topic,
                    "transaction_id": transaction_id,
                    "error": str(e),
                    "error_type": type(e).__name__,
                },
                exc_info=True,
            )
            raise MessageSendError(
                message=f"发送事务消息失败: {e}",
                topic=message.topic,
            )

    def _prepare_message_routing(self, message: Message) -> RoutingResult:
        """准备消息路由：验证消息、更新路由信息、选择队列"""
        # 1. 验证消息
        from .utils import validate_message

        validate_message(message, self._config.max_message_size)

        # 2. 更新路由信息
        if message.topic not in self._topic_mapping.get_all_topics():
            _ = self.update_route_info(message.topic)

        # 3. 获取路由结果
        routing_result: RoutingResult = self._message_router.route_message(
            message.topic, message
        )
        if not routing_result.success:
            raise RouteNotFoundError(f"Route not found for topic: {message.topic}")

        # 4. 验证路由结果
        if not routing_result.message_queue:
            raise QueueNotAvailableError(
                f"No available queue for topic: {message.topic}"
            )

        if not routing_result.broker_data:
            raise BrokerNotAvailableError(
                f"No available broker for topic: {message.topic}"
            )

        return routing_result

    def _register_transaction_check_handler(
        self, broker_remote: Remote, broker_addr: str
    ) -> None:
        """注册事务检查处理器，包含详细的错误处理"""
        try:
            _ = broker_remote.register_request_processor_lazy(
                self._transaction_check_code,
                self._handle_transaction_check,
            )
            self._logger.debug(
                "为Broker注册事务检查处理器", extra={"broker_addr": broker_addr}
            )
        except ConnectionError as e:
            self._logger.error(
                "无法连接到Broker", extra={"broker_addr": broker_addr, "error": str(e)}
            )
            raise BrokerNotAvailableError(
                f"Cannot connect to broker {broker_addr}: {e}"
            )
        except TimeoutError as e:
            self._logger.error(
                "注册事务检查处理器超时",
                extra={"broker_addr": broker_addr, "error": str(e)},
            )
            raise BrokerTimeoutError(
                f"Registration timeout for broker {broker_addr}: {e}"
            )
        except Exception as e:
            self._logger.error(
                "注册事务检查处理器失败",
                extra={"broker_addr": broker_addr, "error": str(e)},
            )
            # 事务检查处理器注册失败继续发送，但记录失败信息
            self._failed_registrations.add(broker_addr)

    def _send_to_broker(
        self,
        broker_remote: Remote,
        message: Message,
        message_queue: MessageQueue,
        broker_addr: str,
    ) -> SendMessageResult:
        """发送消息到指定Broker"""
        try:
            result: SendMessageResult = BrokerClient(broker_remote).sync_send_message(
                self._config.producer_group,
                message.body,
                message_queue,
                message.properties,
            )

            if not result:
                raise MessageSendError("Broker returned empty result")

            self._logger.debug(
                "事务消息发送成功",
                extra={
                    "broker": broker_addr,
                    "msgId": result.msg_id,
                },
            )
            return result

        except ConnectionError as e:
            self._logger.error(
                "连接Broker失败", extra={"broker_addr": broker_addr, "error": str(e)}
            )
            raise BrokerNotAvailableError(
                f"Cannot connect to broker {broker_addr}: {e}"
            )
        except TimeoutError as e:
            self._logger.error(
                "发送消息超时", extra={"broker_addr": broker_addr, "error": str(e)}
            )
            raise MessageSendError(f"Send timeout to broker {broker_addr}: {e}")
        except Exception as e:
            self._logger.error(
                "发送消息失败", extra={"broker_addr": broker_addr, "error": str(e)}
            )
            raise MessageSendError(
                f"Failed to send message to {broker_addr}: {e}"
            ) from e

    def _send_message_with_transaction_flag(
        self, message: Message
    ) -> SendMessageResult:
        """发送带有事务标记的消息，按需注册事务检查处理器"""
        self._check_running()

        # 设置事务消息属性
        message.set_property(MessageProperty.TRANSACTION_PREPARED, "true")
        message.set_property(
            MessageProperty.PRODUCER_GROUP, self._config.producer_group
        )

        try:
            # 准备消息路由
            routing_result: RoutingResult = self._prepare_message_routing(message)
            broker_addr: str | None = routing_result.broker_address
            if not broker_addr:
                raise BrokerNotAvailableError()

            if not routing_result.message_queue:
                raise QueueNotAvailableError(topic=message.topic)

            # print("*" * 30)
            # print("message.body:", str(message.body))
            # print("message.queue:", routing_result.message_queue)
            # print("message.properties:", message.properties)
            # print("*" * 30)

            # 发送事务消息
            with self._broker_manager.connection(broker_addr) as broker_remote:
                # 注册事务检查处理器
                self._register_transaction_check_handler(broker_remote, broker_addr)

                # 发送消息
                return self._send_to_broker(
                    broker_remote, message, routing_result.message_queue, broker_addr
                )

        except (ProducerError, BrokerError):
            # 重新抛出已知异常
            raise
        except Exception as e:
            # 处理未知异常
            self._logger.error("发送事务消息失败", extra={"error": str(e)})
            raise MessageSendError(f"Failed to send transaction message: {e}") from e

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
            self._logger.debug("执行本地事务", extra={"transaction_id": transaction_id})

            # 参数验证
            # if not transaction_id:
            #     self._logger.error("transaction_id is required")
            #     return LocalTransactionState.ROLLBACK_MESSAGE_STATE
            if not self._transaction_listener:
                self._logger.error("transaction_listener is required")
                return LocalTransactionState.ROLLBACK_MESSAGE_STATE

            # 执行本地事务
            result: LocalTransactionState = (
                self._transaction_listener.execute_local_transaction(
                    message, transaction_id, arg
                )
            )

            self._logger.debug(
                f"本地事务执行完成: transactionId={transaction_id}, state={result}"
            )
            return result

        except TransactionError as e:
            # 业务层面的事务错误，直接回滚
            self._logger.error(
                f"事务执行失败: transactionId={transaction_id}, error={e}"
            )
            return LocalTransactionState.ROLLBACK_MESSAGE_STATE

        except (ValueError, TypeError) as e:
            # 参数错误，直接回滚
            self._logger.error(
                f"事务参数错误: transactionId={transaction_id}, error={e}"
            )
            return LocalTransactionState.ROLLBACK_MESSAGE_STATE

        except Exception as e:
            # 其他未知错误，返回UNKNOWN
            self._logger.error(
                f"未知系统错误: transactionId={transaction_id}, error={e}"
            )
            return LocalTransactionState.UNKNOW_STATE

    def _send_transaction_confirmation(
        self,
        result: SendMessageResult,
        local_state: LocalTransactionState,
        message_queue: MessageQueue,
        transaction_id: str,
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

        try:
            # 获取Broker地址
            broker_addr: str | None = self._get_broker_addr_by_name(
                message_queue.broker_name, message_queue.topic
            )
            if not broker_addr:
                raise ValueError("Broker address not found")

            with self._broker_manager.connection(broker_addr) as broker_remote:
                broker_client: BrokerClient = BrokerClient(broker_remote)

                broker_client.end_transaction(
                    self._config.producer_group,
                    result.queue_offset,
                    msg_id.offset,
                    local_state,
                    result.msg_id,
                    transaction_id,
                    False,
                )
                if local_state == LocalTransactionState.COMMIT_MESSAGE_STATE:
                    self._logger.debug(
                        "提交事务", extra={"transaction_id": transaction_id}
                    )
                elif local_state == LocalTransactionState.ROLLBACK_MESSAGE_STATE:
                    self._logger.debug(
                        "回滚事务", extra={"transaction_id": transaction_id}
                    )
                elif local_state == LocalTransactionState.UNKNOW_STATE:
                    self._logger.debug(
                        "未知事务状态", extra={"transaction_id": transaction_id}
                    )

        except Exception as e:
            self._logger.error(
                "发送事务状态确认失败",
                extra={"transaction_id": transaction_id, "error": str(e)},
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
        self, request: RemotingCommand, remote_addr: tuple[str, int]
    ) -> None:
        """处理Broker的事务状态检查请求

        Args:
            request: Broker发送的检查请求
            remote_addr: Broker地址信息

        Returns:
            RemotingCommand | None: 检查结果响应
        """
        try:
            # 解析事务检查回调数据
            broker_addr: str = f"{remote_addr[0]}:{remote_addr[1]}"
            callback: CheckTransactionStateCallback | None = (
                self._parse_original_message_from_request(request, broker_addr)
            )
            if not callback:
                self._logger.error("无法解析事务检查回调")
                return None

            # 解析transaction_id
            transaction_id: str | None = callback.msg.get_property(
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
            uniq_key: str | None = callback.msg.get_property(
                MessageProperty.UNIQUE_CLIENT_MESSAGE_ID_KEY_INDEX
            )
            if not uniq_key:
                uniq_key = callback.msg.msg_id
                if not uniq_key:
                    self._logger.error("无法获取UNIQ_KEY")
                    return None

            self._logger.debug(
                "收到事务检查请求",
                extra={
                    "transaction_id": transaction_id,
                    "broker": broker_addr,
                    "msg_id": callback.msg.msg_id,
                },
            )
            if not self._transaction_listener:
                self._logger.error("无法处理事务检查请求: 未设置事务监听器")
                return None

            # 调用用户定义的检查逻辑
            local_state: LocalTransactionState = (
                self._transaction_listener.check_local_transaction(
                    callback.msg, transaction_id
                )
            )

            self._logger.debug(
                "发送事务检查响应",
                extra={
                    "transaction_id": transaction_id,
                    "state": str(local_state),
                },
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
                        "提交事务", extra={"transaction_id": transaction_id}
                    )
                elif local_state == LocalTransactionState.ROLLBACK_MESSAGE_STATE:
                    self._logger.debug(
                        "回滚事务", extra={"transaction_id": transaction_id}
                    )
                elif local_state == LocalTransactionState.UNKNOW_STATE:
                    self._logger.debug(
                        "未知事务状态", extra={"transaction_id": transaction_id}
                    )

            return

        except Exception as e:
            self._logger.error("处理事务检查请求失败", extra={"error": str(e)})
            raise TransactionCheckError(f"Failed to handle transaction check: {e}")

    def _parse_original_message_from_request(
        self, request: RemotingCommand, addr: str
    ) -> CheckTransactionStateCallback | None:
        """从检查请求中解析原始消息

        Args:
            request: 事务检查请求
            addr: notify的broker服务端地址

        Returns:
            CheckTransactionStateCallback | None: 解析出的事务状态检查回调数据
        """
        try:
            # 解析请求头
            header: CheckTransactionStateRequestHeader = (
                CheckTransactionStateRequestHeader.decode(request.ext_fields)
            )

            # 从请求body中解码MessageExt
            if not request.body:
                self._logger.error("事务检查请求缺少消息体")
                return None

            # 使用MessageExt.from_bytes方法解析消息体
            message_ext: MessageExt = MessageExt.from_bytes(request.body)

            # 创建回调数据结构
            callback: CheckTransactionStateCallback = CheckTransactionStateCallback(
                addr=addr, msg=message_ext, header=header
            )

            self._logger.debug(
                "成功解析事务检查回调",
                extra={
                    "addr": addr,
                    "msg_id": message_ext.msg_id,
                    "transaction_id": header.transaction_id,
                },
            )

            return callback

        except Exception as e:
            self._logger.error("解析事务检查回调失败", extra={"error": str(e)})
            return None

    def set_transaction_timeout(self, timeout_ms: int) -> None:
        """设置事务超时时间

        Args:
            timeout_ms: 超时时间（毫秒）
        """
        self._transaction_timeout = timeout_ms
        self._logger.info("设置事务超时时间", extra={"timeout_ms": timeout_ms})

    def set_max_check_times(self, max_times: int) -> None:
        """设置最大回查次数

        Args:
            max_times: 最大回查次数
        """
        self._max_check_times = max_times
        self._logger.info("设置最大回查次数", extra={"max_times": max_times})

    def _get_broker_addr_by_name(
        self, broker_name: str, topic: str | None = None
    ) -> str | None:
        """根据broker名称查询broker地址

        通过查询NameServer获取指定broker名称的地址信息。
        如果提供了topic，会优先从该topic的路由信息中查找；否则遍历所有已知topic。

        Args:
            broker_name: 要查询的broker名称
            topic: 可选的topic名称，用于缩小搜索范围

        Returns:
            str | None: 找到的broker地址，格式为"host:port"，未找到则返回None

        Raises:
            ProducerError: 当NameServer连接不可用或查询失败时抛出
        """
        self._logger.debug(
            "查询broker地址", extra={"broker_name": broker_name, "topic": topic}
        )

        if not self._nameserver_connections:
            raise ProducerError("NameServer连接不可用，无法查询broker地址")

        # 准备要查询的topic列表
        topics_to_check: list[str] = []
        if topic:
            topics_to_check.append(topic)
        else:
            # 如果没有提供topic，从本地缓存中获取已知topic
            topics_to_check.extend(self._topic_mapping.get_all_topics())

            # 如果本地缓存为空，尝试一些常见的topic
            if not topics_to_check:
                topics_to_check.extend(["TBW102", "SELF_TEST_TOPIC"])

        for addr, remote in self._nameserver_connections.items():
            try:
                # 使用NameServer客户端查询路由信息
                client: SyncNameServerClient = SyncNameServerClient(
                    remote, self._config.send_msg_timeout / 1000.0
                )

                for check_topic in topics_to_check:
                    try:
                        # 查询Topic路由信息
                        topic_route_data: TopicRouteData = (
                            client.query_topic_route_info(check_topic)
                        )

                        # 在路由数据中查找目标broker
                        for broker_data in topic_route_data.broker_data_list:
                            if broker_data.broker_name == broker_name:
                                return self._message_router.select_broker_address(
                                    broker_data
                                )

                    except Exception as e:
                        self._logger.debug(
                            "查询topic失败",
                            extra={"topic": check_topic, "error": str(e)},
                        )
                        continue

            except Exception as e:
                self._logger.warning(
                    "从NameServer查询失败", extra={"addr": addr, "error": str(e)}
                )
                continue

        self._logger.warning("未找到broker地址信息", extra={"broker_name": broker_name})
        return None

    def get_stats(self) -> dict[str, str | int | bool | None]:
        """获取TransactionProducer统计信息

        Returns:
            dict: 统计信息
        """
        base_stats = super().get_stats()
        base_stats.update(
            {
                "transaction_timeout_ms": self._transaction_timeout,
                "max_check_times": self._max_check_times,
                "has_transaction_listener": self._transaction_listener is not None,
            }
        )
        return base_stats


# 便利函数
def create_transactional_producer(
    producer_group: str,
    namesrv_addr: str,
    transaction_listener: TransactionListener,
    **kwargs: Any,
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

    config: ProducerConfig = ProducerConfig(
        producer_group=producer_group,
        namesrv_addr=namesrv_addr,
        **kwargs,
    )

    return TransactionProducer(config=config, transaction_listener=transaction_listener)
