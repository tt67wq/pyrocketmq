"""Trace context management utilities.

This module provides utilities for managing message trace contexts
in a clean and structured way, independent of any specific component.
"""

import time
from contextlib import contextmanager
from typing import Any, Generator, Type

from pyrocketmq.model.message import Message
from pyrocketmq.model.trace import MessageType, TraceBean, TraceContext, TraceType
from pyrocketmq.trace import TraceDispatcher


class TraceContextManager:
    """跟踪上下文管理器

    用于自动管理消息跟踪的生命周期，包括：
    - 创建跟踪上下文
    - 记录执行时间
    - 处理成功/失败情况
    - 分发跟踪信息

    使用示例:
        # 使用 TraceDispatcher
        trace_dispatcher = TraceDispatcher(config)
        with TraceContextManager(trace_dispatcher, "group_name", message) as trace:
            # 发送消息
            result = send_message(...)
            if result.is_success:
                trace.success(result.msg_id, broker_addr)
            else:
                trace.failure()
    """

    def __init__(
        self,
        trace_dispatcher: TraceDispatcher | None,
        group_name: str,
        message: Message,
    ) -> None:
        """初始化跟踪上下文管理器

        Args:
            trace_dispatcher: TraceDispatcher实例，可以为None
            group_name: 生产者组名
            message: 要跟踪的消息
        """
        self._trace_dispatcher: TraceDispatcher | None = trace_dispatcher
        self._group_name: str = group_name
        self._message: Message = message
        self._trace_context: TraceContext | None = None
        self._start_time: float = 0

    def __enter__(self) -> "TraceContextManager":
        """进入上下文，创建跟踪上下文"""
        self._start_time = time.time()
        self._trace_context = TraceContext(
            trace_type=TraceType.PUB,
            timestamp=int(self._start_time * 1000),
            region_id="",
            region_name="",
            group_name=self._group_name,
            cost_time=0,
            is_success=True,
            request_id="",
            context_code=0,
            trace_beans=[],
        )
        return self

    def __exit__(
        self,
        exc_type: Type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> bool:
        """退出上下文，处理异常情况"""
        if self._trace_context and (exc_type or exc_val):
            self.failure()
        return False  # 不抑制异常

    def _create_trace_bean(
        self,
        msg_id: str,
        store_host: str,
        store_time: float,
        is_success: bool,
        offset_msg_id: str = "",
    ) -> TraceBean:
        """创建跟踪 Bean

        Args:
            msg_id: 消息ID
            store_host: 存储主机地址
            store_time: 存储时间戳
            is_success: 是否成功
            offset_msg_id: 偏移消息ID，默认为空

        Returns:
            TraceBean: 跟踪 Bean 对象
        """
        return TraceBean(
            topic=self._message.topic,
            msg_id=msg_id,
            offset_msg_id=offset_msg_id
            if offset_msg_id
            else (msg_id if is_success else ""),
            tags=self._message.get_tags() or "",
            keys=self._message.get_keys() or "",
            store_host=store_host,
            client_host="",
            store_time=int(store_time * 1000),
            retry_times=0,
            body_length=len(self._message.body),
            msg_type=MessageType.NORMAL,
        )

    def _dispatch_trace(
        self,
        msg_id: str = "",
        store_host: str = "",
        is_success: bool = True,
        cost_time_ms: int = 0,
        offset_msg_id: str = "",
    ) -> None:
        """分发跟踪信息

        Args:
            msg_id: 消息ID
            store_host: 存储主机地址
            is_success: 是否成功
            cost_time_ms: 耗时（毫秒）
            offset_msg_id: 偏移消息ID
        """
        if not self._trace_context or not self._trace_dispatcher:
            return

        end_time = time.time()
        self._trace_context.cost_time = cost_time_ms or int(
            (end_time - self._start_time) * 1000
        )
        self._trace_context.is_success = is_success

        trace_bean = self._create_trace_bean(
            msg_id, store_host, end_time, is_success, offset_msg_id
        )
        self._trace_context.trace_beans = [trace_bean]

        self._trace_dispatcher.dispatch(self._trace_context)

    def success(
        self,
        msg_id: str = "",
        store_host: str = "",
        cost_time_ms: int = 0,
        offset_msg_id: str = "",
    ) -> None:
        """标记成功

        Args:
            msg_id: 消息ID
            store_host: 存储主机地址
            cost_time_ms: 耗时（毫秒）
            offset_msg_id: 偏移消息ID
        """
        self._dispatch_trace(
            msg_id=msg_id,
            store_host=store_host,
            is_success=True,
            cost_time_ms=cost_time_ms,
            offset_msg_id=offset_msg_id,
        )

    def failure(
        self,
        msg_id: str = "",
        store_host: str = "",
        cost_time_ms: int = 0,
        offset_msg_id: str = "",
    ) -> None:
        """标记失败

        Args:
            msg_id: 消息ID
            store_host: 存储主机地址
            cost_time_ms: 耗时（毫秒）
            offset_msg_id: 偏移消息ID
        """
        self._dispatch_trace(
            msg_id=msg_id,
            store_host=store_host,
            is_success=False,
            cost_time_ms=cost_time_ms,
            offset_msg_id=offset_msg_id,
        )


@contextmanager
def trace_message(
    trace_dispatcher: TraceDispatcher | None, group_name: str, message: Message
) -> Generator[TraceContextManager, None, None]:
    """跟踪消息的上下文管理器函数

    Args:
        trace_dispatcher: TraceDispatcher实例，可以为None
        group_name: 生产者组名
        message: 要跟踪的消息

    Yields:
        TraceContextManager: 跟踪上下文管理器
    """
    with TraceContextManager(trace_dispatcher, group_name, message) as tracer:
        yield tracer
