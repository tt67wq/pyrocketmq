#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import json
from dataclasses import dataclass
from enum import Enum
from typing import Any


class ConsumeResult(Enum):
    """消息消费结果

    定义消息处理后的返回状态，Consumer会根据这个状态决定
    如何处理这条消息以及后续的消费行为。
    """

    # --------- for concurrent conumser -----------
    SUCCESS = "CONSUME_SUCCESS"  # 消费成功，消息确认
    RECONSUME_LATER = "RECONSUME_LATER"  # 稍后重试消费

    # --------- for orderly conumser -----------
    COMMIT = "COMMIT"  # 提交消费
    ROLLBACK = "ROLLBACK"  # 回滚消费
    SUSPEND_CURRENT_QUEUE_A_MOMENT = (
        "SUSPEND_CURRENT_QUEUE_A_MOMENT"  # 挂起当前队列片刻
    )


# 消息模式枚举
class MessageModel:
    """消息模式"""

    BROADCASTING: str = "BROADCASTING"  # 广播模式
    CLUSTERING: str = "CLUSTERING"  # 集群模式


# 消费起始位置枚举
class ConsumeFromWhere:
    """消费起始位置"""

    CONSUME_FROM_LAST_OFFSET: str = "CONSUME_FROM_LAST_OFFSET"  # 从最后偏移量开始消费
    CONSUME_FROM_FIRST_OFFSET: str = (
        "CONSUME_FROM_FIRST_OFFSET"  # 从第一个偏移量开始消费
    )
    CONSUME_FROM_TIMESTAMP: str = "CONSUME_FROM_TIMESTAMP"  # 从指定时间戳开始消费


class AllocateQueueStrategy:
    """队列负载均衡策略

    定义在多消费者场景下如何分配Topic下的队列给不同的Consumer实例。
    不同的策略适用于不同的业务场景和部署架构。
    """

    AVERAGE: str = "AVG"  # 平均分配策略(默认，MVP实现)
    HASH: str = "HASH"  # 哈希分配策略(后续版本)


@dataclass
class ConsumeMessageDirectlyResult:
    """直接消费消息结果

    用于Broker直接向Consumer发送消息消费请求时的返回结果。
    包含消费状态、处理时间等关键信息。

    Args:
        order: 是否顺序消费
        auto_commit: 是否自动提交偏移量
        consume_result: 消费结果状态
        remark: 备注信息
        spent_time_mills: 消费耗时(毫秒)
    """

    order: bool
    auto_commit: bool
    consume_result: ConsumeResult
    remark: str
    spent_time_mills: int

    def to_dict(self) -> dict[str, Any]:
        """转换为字典格式

        Returns:
            dict: 字典格式的结果数据
        """
        return {
            "order": self.order,
            "autoCommit": self.auto_commit,
            "consumeResult": self.consume_result,
            "remark": self.remark,
            "spentTimeMills": self.spent_time_mills,
        }

    def encode(self) -> bytes:
        """编码为JSON格式的字节数据

        将结果数据序列化为JSON字符串，然后编码为UTF-8字节序列。
        这在RocketMQ协议中用于网络传输。

        Returns:
            bytes: JSON格式的字节数据

        Example:
            >>> result = ConsumeMessageDirectlyResult.success(100, "处理成功")
            >>> data = result.encode()
            >>> isinstance(data, bytes)
            True
        """
        try:
            # 转换为字典
            data_dict = self.to_dict()
            # 序列化为JSON字符串并编码为UTF-8字节
            return json.dumps(data_dict, ensure_ascii=False).encode("utf-8")
        except (TypeError, ValueError) as e:
            # 如果序列化失败，返回最小化的错误响应
            error_dict = {
                "order": False,
                "autoCommit": False,
                "remark": f"Encoding error: {str(e)}",
                "spentTimeMills": 0,
            }
            return json.dumps(error_dict, ensure_ascii=False).encode("utf-8")

    @classmethod
    def decode(cls, data: bytes) -> "ConsumeMessageDirectlyResult":
        """从字节数据解码

        将UTF-8编码的JSON字节数据反序列化为ConsumeMessageDirectlyResult实例。

        Args:
            data: JSON格式的字节数据

        Returns:
            ConsumeMessageDirectlyResult: 解码后的实例

        Raises:
            json.JSONDecodeError: JSON格式错误时抛出
            ValueError: 数据格式不正确时抛出

        Example:
            >>> result = ConsumeMessageDirectlyResult.success(100, "处理成功")
            >>> encoded = result.encode()
            >>> decoded = ConsumeMessageDirectlyResult.decode(encoded)
            >>> decoded.consume_result == ConsumeResult.SUCCESS
            True
        """
        try:
            # 解码UTF-8字节为字符串
            json_str = data.decode("utf-8")
            # 解析JSON为字典
            data_dict = json.loads(json_str)
            # 从字典创建实例
            return cls.from_dict(data_dict)
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as e:
            # 如果解码失败，返回失败结果
            return cls.failure(remark=f"Decoding error: {str(e)}", spent_time_mills=0)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ConsumeMessageDirectlyResult":
        """从字典创建实例

        Args:
            data: 字典数据

        Returns:
            ConsumeMessageDirectlyResult: 实例对象
        """
        return cls(
            order=data.get("order", False),
            auto_commit=data.get("autoCommit", True),
            consume_result=data.get("consumeResult", ConsumeResult.SUCCESS),
            remark=data.get("remark", ""),
            spent_time_mills=data.get("spentTimeMills", 0),
        )

    @classmethod
    def success(
        cls, spent_time_mills: int = 0, remark: str = ""
    ) -> "ConsumeMessageDirectlyResult":
        """创建成功结果

        Args:
            spent_time_mills: 消费耗时(毫秒)
            remark: 备注信息

        Returns:
            ConsumeMessageDirectlyResult: 成功结果实例
        """
        return cls(
            order=False,
            auto_commit=True,
            consume_result=ConsumeResult.SUCCESS,
            remark=remark,
            spent_time_mills=spent_time_mills,
        )

    @classmethod
    def failure(
        cls, remark: str = "", spent_time_mills: int = 0
    ) -> "ConsumeMessageDirectlyResult":
        """创建失败结果

        Args:
            remark: 失败原因
            spent_time_mills: 消费耗时(毫秒)

        Returns:
            ConsumeMessageDirectlyResult: 失败结果实例
        """
        return cls(
            order=False,
            auto_commit=False,
            consume_result=ConsumeResult.ROLLBACK,
            remark=remark,
            spent_time_mills=spent_time_mills,
        )

    @classmethod
    def retry_later(
        cls, remark: str = "", spent_time_mills: int = 0
    ) -> "ConsumeMessageDirectlyResult":
        """创建稍后重试结果

        Args:
            remark: 重试原因
            spent_time_mills: 消费耗时(毫秒)

        Returns:
            ConsumeMessageDirectlyResult: 重试结果实例
        """
        return cls(
            order=False,
            auto_commit=False,
            consume_result=ConsumeResult.ROLLBACK,
            remark=remark,
            spent_time_mills=spent_time_mills,
        )


@dataclass
class ConsumeStatus:
    """消费状态统计

    用于统计Consumer在一段时间内的消费性能指标，包括拉取性能、消费性能、
    成功失败统计等关键指标。这些数据可用于监控消费健康状况和性能调优。

    Args:
        pull_rt: 拉取响应时间(毫秒)
        pull_tps: 拉取吞吐量(每秒事务数)
        consume_rt: 消费响应时间(毫秒)
        consume_ok_tps: 消费成功吞吐量(每秒事务数)
        consume_failed_tps: 消费失败吞吐量(每秒事务数)
        consume_failed_msgs: 消费失败消息数
    """

    pull_rt: float = 0.0
    pull_tps: float = 0.0
    consume_rt: float = 0.0
    consume_ok_tps: float = 0.0
    consume_failed_tps: float = 0.0
    consume_failed_msgs: int = 0

    def to_dict(self) -> dict[str, Any]:
        """转换为字典格式

        将数据类转换为字典，使用指定的JSON字段名称。

        Returns:
            dict: 字典格式的消费状态数据
        """
        return {
            "pullRT": self.pull_rt,
            "pullTPS": self.pull_tps,
            "consumeRT": self.consume_rt,
            "consumeOKTPS": self.consume_ok_tps,
            "consumeFailedTPS": self.consume_failed_tps,
            "consumeFailedMsgs": self.consume_failed_msgs,
        }

    def encode(self) -> bytes:
        """编码为JSON格式的字节数据

        将消费状态数据序列化为JSON字符串，然后编码为UTF-8字节序列。

        Returns:
            bytes: JSON格式的字节数据

        Example:
            >>> status = ConsumeStatus()
            >>> data = status.encode()
            >>> isinstance(data, bytes)
            True
        """
        try:
            data_dict = self.to_dict()
            return json.dumps(data_dict, ensure_ascii=False).encode("utf-8")
        except (TypeError, ValueError) as _:
            # 如果序列化失败，返回最小化的错误响应
            error_dict = {
                "pullRT": 0.0,
                "pullTPS": 0.0,
                "consumeRT": 0.0,
                "consumeOKTPS": 0.0,
                "consumeFailedTPS": 0.0,
                "consumeFailedMsgs": 0,
            }
            return json.dumps(error_dict, ensure_ascii=False).encode("utf-8")

    @classmethod
    def decode(cls, data: bytes) -> "ConsumeStatus":
        """从字节数据解码

        将UTF-8编码的JSON字节数据反序列化为ConsumeStatus实例。

        Args:
            data: JSON格式的字节数据

        Returns:
            ConsumeStatus: 解码后的实例

        Raises:
            json.JSONDecodeError: JSON格式错误时抛出
            ValueError: 数据格式不正确时抛出

        Example:
            >>> status = ConsumeStatus()
            >>> encoded = status.encode()
            >>> decoded = ConsumeStatus.decode(encoded)
            >>> decoded.pull_rt == status.pull_rt
            True
        """
        try:
            json_str = data.decode("utf-8")
            data_dict = json.loads(json_str)
            return cls.from_dict(data_dict)
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as _:
            # 如果解码失败，返回默认状态
            return cls()

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ConsumeStatus":
        """从字典创建实例

        Args:
            data: 字典数据

        Returns:
            ConsumeStatus: 实例对象
        """
        return cls(
            pull_rt=float(data.get("pullRT", 0.0)),
            pull_tps=float(data.get("pullTPS", 0.0)),
            consume_rt=float(data.get("consumeRT", 0.0)),
            consume_ok_tps=float(data.get("consumeOKTPS", 0.0)),
            consume_failed_tps=float(data.get("consumeFailedTPS", 0.0)),
            consume_failed_msgs=int(data.get("consumeFailedMsgs", 0)),
        )

    @classmethod
    def create_empty(cls) -> "ConsumeStatus":
        """创建空的消费状态

        创建一个所有字段都为默认值的空状态对象。

        Returns:
            ConsumeStatus: 空状态实例
        """
        return cls()

    def update_pull_metrics(self, response_time_ms: float, message_count: int):
        """更新拉取性能指标

        Args:
            response_time_ms: 拉取响应时间(毫秒)
            message_count: 拉取的消息数量
        """
        self.pull_rt = response_time_ms
        # 简化计算，实际应用中需要根据时间窗口计算TPS
        if message_count > 0 and response_time_ms > 0:
            self.pull_tps = (message_count * 1000.0) / response_time_ms

    def update_consume_metrics(
        self, response_time_ms: float, success_count: int, failed_count: int
    ):
        """更新消费性能指标

        Args:
            response_time_ms: 消费响应时间(毫秒)
            success_count: 成功消费的消息数量
            failed_count: 失败消费的消息数量
        """
        self.consume_rt = response_time_ms
        self.consume_failed_msgs += failed_count

        total_messages = success_count + failed_count
        if total_messages > 0 and response_time_ms > 0:
            tps = (total_messages * 1000.0) / response_time_ms
            self.consume_ok_tps = tps * (success_count / total_messages)
            self.consume_failed_tps = tps * (failed_count / total_messages)
