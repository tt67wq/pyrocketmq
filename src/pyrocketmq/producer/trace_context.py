"""Trace context management for Producer.

This module provides utilities for managing message trace contexts
in a clean and structured way.
"""

import time
from contextlib import contextmanager
from typing import Generator

from pyrocketmq.model import Message
from pyrocketmq.producer.producer import Producer


class TraceContextManager:
    """跟踪上下文管理器

    用于自动管理消息跟踪的生命周期，包括：
    - 创建跟踪上下文
    - 记录执行时间
    - 处理成功/失败情况
    - 分发跟踪信息

    使用示例:
        with TraceContextManager(producer, message) as trace:
            # 发送消息
            result = producer._send_message_to_broker(...)
            if result.is_success:
                trace.success(result.msg_id, broker_addr)
            else:
                trace.failure()
    """

    def __init__(self, producer: "Producer", message: "Message"):
        """初始化跟踪上下文管理器

        Args:
            producer: Producer实例
            message: 要跟踪的消息
        """
        self._producer = producer
        self._message = message
        self._trace_context = None
        self._start_time = None

    def __enter__(self) -> "TraceContextManager":
        """进入上下文，创建跟踪上下文"""
        self._start_time = time.time()
        self._trace_context = self._producer._create_trace_context(self._start_time)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """退出上下文，处理异常情况"""
        if self._trace_context and (exc_type or exc_val):
            self.failure()
        return False  # 不抑制异常

    def success(self, msg_id: str = "", store_host: str = "", cost_time_ms: int = 0):
        """标记成功

        Args:
            msg_id: 消息ID
            store_host: 存储主机地址
            cost_time_ms: 耗时（毫秒）
        """
        if self._trace_context:
            self._producer.dispatch_trace(
                self._trace_context,
                self._message,
                msg_id=msg_id,
                store_host=store_host,
                is_success=True,
                cost_time_ms=cost_time_ms,
            )

    def failure(self, msg_id: str = "", store_host: str = "", cost_time_ms: int = 0):
        """标记失败

        Args:
            msg_id: 消息ID
            store_host: 存储主机地址
            cost_time_ms: 耗时（毫秒）
        """
        if self._trace_context:
            self._producer.dispatch_trace(
                self._trace_context,
                self._message,
                msg_id=msg_id,
                store_host=store_host,
                is_success=False,
                cost_time_ms=cost_time_ms,
            )


@contextmanager
def trace_message(
    producer: "Producer", message: "Message"
) -> Generator[TraceContextManager, None, None]:
    """跟踪消息的上下文管理器函数

    Args:
        producer: Producer实例
        message: 要跟踪的消息

    Yields:
        TraceContextManager: 跟踪上下文管理器
    """
    with TraceContextManager(producer, message) as tracer:
        yield tracer
