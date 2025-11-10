#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import json
from dataclasses import dataclass
from typing import Any


# 消费结果枚举
class ConsumeResult:
    """消费结果状态"""

    SUCCESS: str = "SUCCESS"  # 消费成功
    FAIL: str = "FAIL"  # 消费失败
    RETURN: str = "RETURN"  # 消费返回，后续重试
    SUSPEND_CURRENT_QUEUE_A_MOMENT: str = (
        "SUSPEND_CURRENT_QUEUE_A_MOMENT"  # 暂停消费队列一段时间
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
    consume_result: str
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
                "consumeResult": ConsumeResult.FAIL,
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
            consume_result=ConsumeResult.FAIL,
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
            consume_result=ConsumeResult.RETURN,
            remark=remark,
            spent_time_mills=spent_time_mills,
        )
