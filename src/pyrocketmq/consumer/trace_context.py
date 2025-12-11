"""Trace context management utilities for consumer.

This module provides utilities for managing message trace contexts
in consumer operations, including SubBefore and SubAfter trace types.
"""

import time
from contextlib import asynccontextmanager, contextmanager
from typing import Any, AsyncGenerator, Generator, Type

from pyrocketmq.model import MessageExt
from pyrocketmq.model.message import Message
from pyrocketmq.model.trace import MessageType, TraceBean, TraceContext, TraceType
from pyrocketmq.trace import TraceDispatcher
from pyrocketmq.trace.async_trace_dispatcher import AsyncTraceDispatcher


class ConsumerTraceContextManager:
    """消费者跟踪上下文管理器

    用于自动管理消费者消息跟踪的生命周期，包括：
    - 创建跟踪上下文（SubBefore 和 SubAfter）
    - 记录执行时间
    - 处理成功/失败情况
    - 分发跟踪信息

    使用示例:
        # 使用 TraceDispatcher
        trace_dispatcher = TraceDispatcher(config)
        with ConsumerTraceContextManager(trace_dispatcher, "group_name", message) as trace:
            # 消费消息
            result = consume_message(...)
            if result.is_success:
                trace.success(msg_ext, cost_time_ms)
            else:
                trace.failure(msg_ext, cost_time_ms)
    """

    def __init__(
        self,
        trace_dispatcher: TraceDispatcher | None,
        group_name: str,
        message: Message | None = None,
    ) -> None:
        """初始化消费者跟踪上下文管理器

        Args:
            trace_dispatcher: TraceDispatcher实例，可以为None
            group_name: 消费者组名
            message: 要跟踪的消息，可以为None（仅用于创建上下文）
        """
        self._trace_dispatcher: TraceDispatcher | None = trace_dispatcher
        self._group_name: str = group_name
        self._message: Message | None = message
        self._sub_before_context: TraceContext | None = None
        self._sub_after_context: TraceContext | None = None
        self._start_time: float = 0
        self._request_id: str = ""

    def __enter__(self) -> "ConsumerTraceContextManager":
        """进入上下文，创建SubBefore跟踪上下文"""
        self._start_time = time.time()
        self._request_id = f"req_{int(self._start_time * 1000)}"

        # 创建 SubBefore 跟踪上下文
        self._sub_before_context = TraceContext(
            trace_type=TraceType.SUB_BEFORE,
            timestamp=int(self._start_time * 1000),
            region_id="",
            region_name="",
            group_name=self._group_name,
            cost_time=0,
            is_success=True,
            request_id=self._request_id,
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
        if exc_type or exc_val:
            # 如果有异常，发送SubAfter失败跟踪
            if self._trace_dispatcher and self._message:
                end_time = time.time()
                cost_time = int((end_time - self._start_time) * 1000)
                self._dispatch_sub_after_trace(
                    msg_ext=None,
                    is_success=False,
                    cost_time_ms=cost_time,
                    context_code=1,
                )
        return False  # 不抑制异常

    def _create_sub_before_trace_bean(self, msg_ext: MessageExt) -> TraceBean:
        """创建SubBefore跟踪Bean

        Args:
            msg_ext: 消息扩展对象

        Returns:
            TraceBean: 跟踪 Bean 对象
        """
        return TraceBean(
            topic=msg_ext.topic,
            msg_id=msg_ext.msg_id or "",
            offset_msg_id=msg_ext.offset_msg_id or "",
            tags=msg_ext.get_tags() or "",
            keys=msg_ext.get_keys() or "",
            store_host="",
            client_host=msg_ext.born_host or "",
            store_time=0,
            retry_times=msg_ext.reconsume_times or 0,
            body_length=len(msg_ext.body) if msg_ext.body else 0,
            msg_type=MessageType.NORMAL,
        )

    def _create_sub_after_trace_bean(self, msg_ext: MessageExt) -> TraceBean:
        """创建SubAfter跟踪Bean

        Args:
            msg_ext: 消息扩展对象

        Returns:
            TraceBean: 跟踪 Bean 对象
        """
        return TraceBean(
            topic=msg_ext.topic,
            msg_id=msg_ext.msg_id or "",
            offset_msg_id=msg_ext.offset_msg_id or "",
            tags=msg_ext.get_tags() or "",
            keys=msg_ext.get_keys() or "",
            store_host="",
            client_host="",
            store_time=0,
            retry_times=msg_ext.reconsume_times or 0,
            body_length=len(msg_ext.body) if msg_ext.body else 0,
            msg_type=MessageType.NORMAL,
        )

    def _dispatch_sub_before_trace(self, msg_ext: MessageExt) -> None:
        """分发SubBefore跟踪信息

        Args:
            msg_ext: 消息扩展对象
        """
        if not self._sub_before_context or not self._trace_dispatcher:
            return

        trace_bean = self._create_sub_before_trace_bean(msg_ext)
        self._sub_before_context.trace_beans = [trace_bean]

        self._trace_dispatcher.dispatch(self._sub_before_context)

    def _dispatch_sub_after_trace(
        self,
        msg_ext: MessageExt | None,
        is_success: bool,
        cost_time_ms: int = 0,
        context_code: int = 0,
    ) -> None:
        """分发SubAfter跟踪信息

        Args:
            msg_ext: 消息扩展对象，可以为None
            is_success: 是否成功
            cost_time_ms: 耗时（毫秒）
            context_code: 上下文代码
        """
        if not self._trace_dispatcher:
            return

        end_time = time.time()

        # 创建 SubAfter 跟踪上下文
        self._sub_after_context = TraceContext(
            trace_type=TraceType.SUB_AFTER,
            timestamp=int(end_time * 1000),
            region_id="",
            region_name="",
            group_name=self._group_name,
            cost_time=cost_time_ms or int((end_time - self._start_time) * 1000),
            is_success=is_success,
            request_id=self._request_id,
            context_code=context_code,
            trace_beans=[],
        )

        if msg_ext:
            trace_bean = self._create_sub_after_trace_bean(msg_ext)
            self._sub_after_context.trace_beans = [trace_bean]

        self._trace_dispatcher.dispatch(self._sub_after_context)

    def sub_before(self, msg_ext: MessageExt) -> None:
        """发送SubBefore跟踪信息

        Args:
            msg_ext: 消息扩展对象
        """
        self._dispatch_sub_before_trace(msg_ext)

    def success(
        self,
        msg_ext: MessageExt,
        cost_time_ms: int = 0,
    ) -> None:
        """标记成功并发送SubAfter跟踪信息

        Args:
            msg_ext: 消息扩展对象
            cost_time_ms: 耗时（毫秒）
        """
        self._dispatch_sub_after_trace(
            msg_ext=msg_ext,
            is_success=True,
            cost_time_ms=cost_time_ms,
            context_code=0,
        )

    def failure(
        self,
        msg_ext: MessageExt | None = None,
        cost_time_ms: int = 0,
        context_code: int = 1,
    ) -> None:
        """标记失败并发送SubAfter跟踪信息

        Args:
            msg_ext: 消��扩展对象，可以为None
            cost_time_ms: 耗时（毫秒）
            context_code: 上下文代码，默认为1（失败）
        """
        self._dispatch_sub_after_trace(
            msg_ext=msg_ext,
            is_success=False,
            cost_time_ms=cost_time_ms,
            context_code=context_code,
        )


@contextmanager
def trace_consumer_message(
    trace_dispatcher: TraceDispatcher | None, group_name: str
) -> Generator[ConsumerTraceContextManager, None, None]:
    """跟踪消费者消息的上下文管理器函数

    Args:
        trace_dispatcher: TraceDispatcher实例，可以为None
        group_name: 消费者组名

    Yields:
        ConsumerTraceContextManager: 消费者跟踪上下文管理器
    """
    with ConsumerTraceContextManager(trace_dispatcher, group_name) as tracer:
        yield tracer


class AsyncConsumerTraceContextManager:
    """异步消费者跟踪上下文管理器

    用于自动管理异步消费者消息跟踪的生命周期，包括：
    - 创建跟踪上下文（SubBefore 和 SubAfter）
    - 记录执行时间
    - 处理成功/失败情况
    - 异步分发跟踪信息

    使用示例:
        # 使用 AsyncTraceDispatcher
        trace_dispatcher = AsyncTraceDispatcher(config)
        async with AsyncConsumerTraceContextManager(trace_dispatcher, "group_name") as trace:
            # 消费消息
            result = await consume_message_async(...)
            if result.is_success:
                await trace.success(msg_ext, cost_time_ms)
            else:
                await trace.failure(msg_ext, cost_time_ms)
    """

    def __init__(
        self,
        trace_dispatcher: AsyncTraceDispatcher | None,
        group_name: str,
        message: Message | None = None,
    ) -> None:
        """初始化异步消费者跟踪上下文管理器

        Args:
            trace_dispatcher: AsyncTraceDispatcher实例，可以为None
            group_name: 消费者组名
            message: 要跟踪的消息，可以为None（仅用于创建上下文）
        """
        self._trace_dispatcher: AsyncTraceDispatcher | None = trace_dispatcher
        self._group_name: str = group_name
        self._message: Message | None = message
        self._sub_before_context: TraceContext | None = None
        self._sub_after_context: TraceContext | None = None
        self._start_time: float = 0
        self._request_id: str = ""

    async def __aenter__(self) -> "AsyncConsumerTraceContextManager":
        """进入异步上下文，创建SubBefore跟踪上下文"""
        self._start_time = time.time()
        self._request_id = f"req_{int(self._start_time * 1000)}"

        # 创建 SubBefore 跟踪上下文
        self._sub_before_context = TraceContext(
            trace_type=TraceType.SUB_BEFORE,
            timestamp=int(self._start_time * 1000),
            region_id="",
            region_name="",
            group_name=self._group_name,
            cost_time=0,
            is_success=True,
            request_id=self._request_id,
            context_code=0,
            trace_beans=[],
        )

        return self

    async def __aexit__(
        self,
        exc_type: Type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> bool:
        """退出异步上下文，处理异常情况"""
        if exc_type or exc_val:
            # 如果有异常，发送SubAfter失败跟踪
            if self._trace_dispatcher and self._message:
                end_time = time.time()
                cost_time = int((end_time - self._start_time) * 1000)
                await self._dispatch_sub_after_trace(
                    msg_ext=None,
                    is_success=False,
                    cost_time_ms=cost_time,
                    context_code=1,
                )
        return False  # 不抑制异常

    def _create_sub_before_trace_bean(self, msg_ext: MessageExt) -> TraceBean:
        """创建SubBefore跟踪Bean

        Args:
            msg_ext: 消息扩展对象

        Returns:
            TraceBean: 跟踪 Bean 对象
        """
        return TraceBean(
            topic=msg_ext.topic,
            msg_id=msg_ext.msg_id or "",
            offset_msg_id=msg_ext.offset_msg_id or "",
            tags=msg_ext.get_tags() or "",
            keys=msg_ext.get_keys() or "",
            store_host="",
            client_host=msg_ext.born_host or "",
            store_time=0,
            retry_times=msg_ext.reconsume_times or 0,
            body_length=len(msg_ext.body) if msg_ext.body else 0,
            msg_type=MessageType.NORMAL,
        )

    def _create_sub_after_trace_bean(self, msg_ext: MessageExt) -> TraceBean:
        """创建SubAfter跟踪Bean

        Args:
            msg_ext: 消息扩展对象

        Returns:
            TraceBean: 跟踪 Bean 对象
        """
        return TraceBean(
            topic=msg_ext.topic,
            msg_id=msg_ext.msg_id or "",
            offset_msg_id=msg_ext.offset_msg_id or "",
            tags=msg_ext.get_tags() or "",
            keys=msg_ext.get_keys() or "",
            store_host="",
            client_host="",
            store_time=0,
            retry_times=msg_ext.reconsume_times or 0,
            body_length=len(msg_ext.body) if msg_ext.body else 0,
            msg_type=MessageType.NORMAL,
        )

    async def _dispatch_sub_before_trace(self, msg_ext: MessageExt) -> None:
        """异步分发SubBefore跟踪信息

        Args:
            msg_ext: 消息扩展对象
        """
        if not self._sub_before_context or not self._trace_dispatcher:
            return

        trace_bean = self._create_sub_before_trace_bean(msg_ext)
        self._sub_before_context.trace_beans = [trace_bean]

        await self._trace_dispatcher.dispatch(self._sub_before_context)

    async def _dispatch_sub_after_trace(
        self,
        msg_ext: MessageExt | None,
        is_success: bool,
        cost_time_ms: int = 0,
        context_code: int = 0,
    ) -> None:
        """异步分发SubAfter跟踪信息

        Args:
            msg_ext: 消息扩展对象，可以为None
            is_success: 是否成功
            cost_time_ms: 耗时（毫秒）
            context_code: 上下文代码
        """
        if not self._trace_dispatcher:
            return

        end_time = time.time()

        # 创建 SubAfter 跟踪上下文
        self._sub_after_context = TraceContext(
            trace_type=TraceType.SUB_AFTER,
            timestamp=int(end_time * 1000),
            region_id="",
            region_name="",
            group_name=self._group_name,
            cost_time=cost_time_ms or int((end_time - self._start_time) * 1000),
            is_success=is_success,
            request_id=self._request_id,
            context_code=context_code,
            trace_beans=[],
        )

        if msg_ext:
            trace_bean = self._create_sub_after_trace_bean(msg_ext)
            self._sub_after_context.trace_beans = [trace_bean]

        await self._trace_dispatcher.dispatch(self._sub_after_context)

    async def sub_before(self, msg_ext: MessageExt) -> None:
        """发送SubBefore跟踪信息

        Args:
            msg_ext: 消息扩展对象
        """
        await self._dispatch_sub_before_trace(msg_ext)

    async def success(
        self,
        msg_ext: MessageExt,
        cost_time_ms: int = 0,
    ) -> None:
        """标记成功并发送SubAfter跟踪信息

        Args:
            msg_ext: 消息扩展对象
            cost_time_ms: 耗时（毫秒）
        """
        await self._dispatch_sub_after_trace(
            msg_ext=msg_ext,
            is_success=True,
            cost_time_ms=cost_time_ms,
            context_code=0,
        )

    async def failure(
        self,
        msg_ext: MessageExt | None = None,
        cost_time_ms: int = 0,
        context_code: int = 1,
    ) -> None:
        """标记失败并发送SubAfter跟踪信息

        Args:
            msg_ext: 消息扩展对象，可以为None
            cost_time_ms: 耗时（毫秒）
            context_code: 上下文代码，默认为1（失败）
        """
        await self._dispatch_sub_after_trace(
            msg_ext=msg_ext,
            is_success=False,
            cost_time_ms=cost_time_ms,
            context_code=context_code,
        )


@asynccontextmanager
async def async_trace_consumer_message(
    trace_dispatcher: AsyncTraceDispatcher | None, group_name: str
) -> AsyncGenerator[AsyncConsumerTraceContextManager, None]:
    """异步跟踪消费者消息的上下文管理器函数

    Args:
        trace_dispatcher: AsyncTraceDispatcher实例，可以为None
        group_name: 消费者组名

    Yields:
        AsyncConsumerTraceContextManager: 异步消费者跟踪上下文管理器
    """
    async with AsyncConsumerTraceContextManager(trace_dispatcher, group_name) as tracer:
        yield tracer
