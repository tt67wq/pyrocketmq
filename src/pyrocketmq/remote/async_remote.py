"""
异步远程通信实现
"""

import asyncio
import time
import logging
from typing import Any, Callable
from collections.abc import Awaitable

from pyrocketmq.logging import get_logger
from pyrocketmq.model import RemotingCommand, RemotingCommandSerializer
from pyrocketmq.transport.config import TransportConfig
from pyrocketmq.transport.tcp import AsyncConnectionStateMachine

from .config import RemoteConfig
from .errors import (
    ConnectionError,
    MaxWaitersExceededError,
    ProtocolError,
    RemoteError,
    RpcTimeoutError,
    SerializationError,
    TransportError,
)

# 客户端请求处理函数类型（异步版本）
AsyncClientRequestFunc = Callable[
    [RemotingCommand, tuple[str, int]], Awaitable[RemotingCommand | None]
]


class AsyncRemote:
    """异步远程通信类"""

    def __init__(self, transport_cfg: TransportConfig, config: RemoteConfig) -> None:
        self.transport: AsyncConnectionStateMachine = AsyncConnectionStateMachine(
            transport_cfg
        )
        self.config: RemoteConfig = config
        self._logger: logging.Logger = get_logger("remote.async")

        # 请求等待者管理: opaque -> (event, response, timestamp)
        self._waiters: dict[
            int, tuple[asyncio.Event, RemotingCommand | None, float]
        ] = {}
        self._waiters_lock: asyncio.Lock = asyncio.Lock()

        # opaque生成器
        self._opaque_lock: asyncio.Lock = asyncio.Lock()
        self._next_opaque: int = config.opaque_start

        # 请求处理器映射: code -> handler function
        self._processors: dict[int, AsyncClientRequestFunc] = {}
        self._processors_lock: asyncio.Lock = asyncio.Lock()

        # 清理任务
        self._cleanup_task: asyncio.Task[None] | None = None

        # 消息接收任务
        self._recv_task: asyncio.Task[None] | None = None

        # 序列化器
        self._serializer: RemotingCommandSerializer = RemotingCommandSerializer()

    async def connect(self) -> None:
        """建立连接"""
        try:
            await self.transport.start()
            # 启动清理任务
            self._start_cleanup_task()

            # 启动消息接收任务
            self._start_recv_task()

        except Exception as e:
            self._logger.error(f"异步连接建立失败: {e}")
            raise ConnectionError(f"连接建立失败: {e}") from e

    async def close(self) -> None:
        """关闭连接"""
        try:
            # 停止消息接收任务
            await self._stop_recv_task()

            # 停止清理任务
            await self._stop_cleanup_task()

            # 关闭传输层
            await self.transport.stop()

            # 清理所有等待者
            async with self._waiters_lock:
                for opaque, (event, _, _) in self._waiters.items():
                    event.set()
                self._waiters.clear()

            self._logger.info("异步连接已关闭")

        except Exception as e:
            self._logger.error(f"关闭异步连接失败: {e}")
            raise RemoteError(f"关闭连接失败: {e}") from e

    async def register_request_processor(
        self, code: int, processor: AsyncClientRequestFunc
    ) -> None:
        """注册请求处理器

        Args:
            code: 请求代码
            processor: 处理函数
        """
        async with self._processors_lock:
            self._processors[code] = processor
            self._logger.debug(f"注册异步请求处理器: code={code}")

    async def unregister_request_processor(self, code: int) -> bool:
        """取消注册请求处理器

        Args:
            code: 请求代码

        Returns:
            是否成功取消
        """
        async with self._processors_lock:
            if code in self._processors:
                del self._processors[code]
                self._logger.debug(f"取消注册异步请求处理器: code={code}")
                return True
            return False

    async def rpc(
        self, command: RemotingCommand, timeout: float | None = None
    ) -> RemotingCommand:
        """发送RPC请求并等待响应

        Args:
            command: 要发送的命令
            timeout: 超时时间，None则使用配置值

        Returns:
            响应命令

        Raises:
            ConnectionError: 连接错误
            TimeoutError: 超时错误
            SerializationError: 序列化错误
            ProtocolError: 协议错误
            ResourceExhaustedError: 资源耗尽错误
        """
        if not self.transport.is_connected:
            raise ConnectionError("连接未建立")

        # 使用传入的超时或配置的超时
        rpc_timeout = timeout if timeout is not None else self.config.rpc_timeout

        # 生成opaque并设置为请求
        opaque = await self._generate_opaque()
        command.opaque = opaque
        command.set_request()

        # 创建等待事件
        event = asyncio.Event()

        # 注册等待者
        if not await self._register_waiter(opaque, event):
            raise MaxWaitersExceededError(
                f"等待者数量超过限制: {self.config.max_waiters}"
            )

        try:
            # 序列化并发送命令
            data = self._serializer.serialize(command)
            await self.transport.output(data)

            self._logger.debug(f"发送异步RPC请求: opaque={opaque}, code={command.code}")

            # 等待响应
            try:
                await asyncio.wait_for(event.wait(), timeout=rpc_timeout)
            except asyncio.TimeoutError:
                # 超时，移除等待者
                await self._unregister_waiter(opaque)
                raise RpcTimeoutError(f"RPC请求超时: opaque={opaque}")

            # 获取响应
            response = await self._get_waiter_response(opaque)
            if response is None:
                raise ProtocolError(f"未收到有效响应: opaque={opaque}")

            self._logger.debug(
                f"收到异步RPC响应: opaque={opaque}, code={response.code}"
            )
            return response

        except SerializationError:
            raise
        except (ConnectionError, TransportError):
            # 连接或传输错误，移除等待者
            await self._unregister_waiter(opaque)
            raise
        except Exception as e:
            # 其他错误，移除等待者
            await self._unregister_waiter(opaque)
            self._logger.error(f"异步RPC调用失败: opaque={opaque}, error={e}")
            raise RemoteError(f"RPC调用失败: {e}") from e

    async def oneway(self, command: RemotingCommand) -> None:
        """发送单向消息

        Args:
            command: 要发送的命令

        Raises:
            ConnectionError: 连接错误
            SerializationError: 序列化错误
        """
        if not self.transport.is_connected:
            raise ConnectionError("连接未建立")

        # 生成opaque并设置为单向请求
        opaque = await self._generate_opaque()
        command.opaque = opaque
        command.set_oneway()

        try:
            # 序列化并发送命令
            data = self._serializer.serialize(command)
            await self.transport.output(data)

            self._logger.debug(
                f"发送异步单向消息: opaque={opaque}, code={command.code}"
            )

        except SerializationError:
            raise
        except (ConnectionError, TransportError):
            raise
        except Exception as e:
            self._logger.error(f"异步单向消息发送失败: opaque={opaque}, error={e}")
            raise RemoteError(f"单向消息发送失败: {e}") from e

    async def _generate_opaque(self) -> int:
        """生成唯一的opaque值"""
        async with self._opaque_lock:
            opaque = self._next_opaque
            self._next_opaque += 1
            return opaque

    async def _register_waiter(self, opaque: int, event: asyncio.Event) -> bool:
        """注册等待者

        Returns:
            是否注册成功
        """
        async with self._waiters_lock:
            if len(self._waiters) >= self.config.max_waiters:
                return False

            self._waiters[opaque] = (event, None, time.time())
            return True

    async def _unregister_waiter(self, opaque: int) -> bool:
        """取消注册等待者

        Returns:
            是否取消成功
        """
        async with self._waiters_lock:
            return self._waiters.pop(opaque, None) is not None

    async def _get_waiter_response(self, opaque: int) -> RemotingCommand | None:
        """获取等待者的响应

        Returns:
            响应命令，如果不存在则返回None
        """
        async with self._waiters_lock:
            waiter = self._waiters.get(opaque)
            if waiter:
                _, response, _ = waiter
                return response
            return None

    async def _set_waiter_response(
        self, opaque: int, response: RemotingCommand
    ) -> bool:
        """设置等待者的响应

        Returns:
            是否设置成功
        """
        async with self._waiters_lock:
            waiter = self._waiters.get(opaque)
            if waiter:
                event, _, timestamp = waiter
                # 更新响应和时间戳
                self._waiters[opaque] = (event, response, time.time())
                event.set()
                return True
            return False

    def _start_cleanup_task(self) -> None:
        """启动清理任务"""
        if self._cleanup_task is None or self._cleanup_task.done():
            self._cleanup_task = asyncio.create_task(
                self._cleanup_worker(), name="remote-cleanup"
            )
            self._logger.debug("异步清理任务已启动")

    async def _stop_cleanup_task(self) -> None:
        """停止清理任务"""
        if self._cleanup_task and not self._cleanup_task.done():
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
            self._logger.debug("异步清理任务已停止")

    async def _cleanup_worker(self) -> None:
        """清理工作协程"""
        while True:
            try:
                await asyncio.sleep(self.config.cleanup_interval)
                await self._cleanup_expired_waiters()
            except asyncio.CancelledError:
                break
            except Exception as e:
                self._logger.error(f"异步清理等待者时发生错误: {e}")

    async def _cleanup_expired_waiters(self) -> None:
        """清理过期的等待者"""
        current_time = time.time()
        expired_opaques = []

        async with self._waiters_lock:
            for opaque, (_, _, timestamp) in self._waiters.items():
                if current_time - timestamp > self.config.waiter_timeout:
                    expired_opaques.append(opaque)

            # 移除过期的等待者
            for opaque in expired_opaques:
                event, _, _ = self._waiters.pop(opaque, (None, None, None))
                if event:
                    event.set()

        if expired_opaques:
            self._logger.warning(f"清理了 {len(expired_opaques)} 个过期的等待者")

    def _start_recv_task(self) -> None:
        """启动消息接收任务"""
        if self._recv_task is None or self._recv_task.done():
            self._recv_task = asyncio.create_task(
                self._recv_worker(), name="remote-recv"
            )
            self._logger.debug("异步消息接收任务已启动")

    async def _stop_recv_task(self) -> None:
        """停止消息接收任务"""
        if self._recv_task and not self._recv_task.done():
            self._recv_task.cancel()
            try:
                await self._recv_task
            except asyncio.CancelledError:
                pass
            self._logger.debug("异步消息接收任务已停止")

    async def _recv_worker(self) -> None:
        """消息接收工作协程"""
        self._logger.info("异步消息接收协程开始工作")

        while True:
            try:
                if not self.transport.is_connected:
                    # 连接断开，等待重连
                    self._logger.debug("连接断开，等待重连...")
                    await asyncio.sleep(1.0)
                    continue

                # 接收数据包
                data = await self.transport.recv_pkg()
                if not data:
                    # 连接关闭
                    self._logger.info("连接已关闭，停止接收")
                    break

                # 反序列化响应
                try:
                    response = self._serializer.deserialize(data)
                    self._logger.debug(
                        f"接收到响应: opaque={response.opaque}, code={response.code}"
                    )
                except Exception as e:
                    self._logger.error(f"反序列化响应失败: {e}")
                    continue

                # 处理响应
                await self._handle_response(response)

            except asyncio.CancelledError:
                break
            except TimeoutError:
                # 接收超时，继续循环
                continue
            except ConnectionError:
                # 连接错误，等待重连
                self._logger.warning("连接错误，等待重连...")
                await asyncio.sleep(1.0)
                continue
            except Exception as e:
                self._logger.error(f"接收消息时发生错误: {e}")
                await asyncio.sleep(1.0)

        self._logger.info("异步消息接收协程结束")

    async def _handle_response(self, command: RemotingCommand) -> None:
        """处理接收到的命令（响应或请求）"""
        if command.is_response:
            # 处理响应消息
            await self._handle_response_message(command)
        else:
            # 处理请求消息（服务器主动通知）
            await self._handle_request_message(command)

    async def _handle_response_message(self, response: RemotingCommand) -> None:
        """处理响应消息"""
        opaque = response.opaque
        if opaque is None:
            self._logger.warning("响应消息缺少opaque字段")
            return

        # 查找对应的等待者并设置响应
        if await self._set_waiter_response(opaque, response):
            self._logger.debug(f"响应已匹配到等待者: opaque={opaque}")
        else:
            self._logger.warning(f"未找到对应的等待者: opaque={opaque}")

    async def _handle_request_message(self, request: RemotingCommand) -> None:
        """处理请求消息（服务器主动通知）"""
        self._logger.debug(
            f"收到服务器请求: code={request.code}, opaque={request.opaque}"
        )

        # 查找对应的处理器
        processor_func = None
        async with self._processors_lock:
            processor_func = self._processors.get(request.code)

        if processor_func is None:
            self._logger.warning(
                f"收到服务器请求，但没有对应的处理器: code={request.code}"
            )
            return

        try:
            # 获取远程地址信息
            remote_addr = (
                self.transport.config.host,
                self.transport.config.port,
            )

            # 调用处理器
            response = await processor_func(request, remote_addr)

            # 如果处理器返回了响应，则发送回服务器
            if response is not None:
                await self._send_processor_response(request, response)

        except Exception as e:
            self._logger.error(
                f"处理服务器请求时发生错误: code={request.code}, error={e}"
            )

    async def _send_processor_response(
        self, request: RemotingCommand, response: RemotingCommand
    ) -> None:
        """发送处理器生成的响应"""
        try:
            # 设置响应的opaque和flag
            if request.opaque is not None:
                response.opaque = request.opaque
            response.set_response()

            # 序列化并发送响应
            data = self._serializer.serialize(response)
            await self.transport.output(data)

            self._logger.debug(
                f"发送处理器响应: opaque={response.opaque}, code={response.code}"
            )

        except Exception as e:
            self._logger.error(
                f"发送处理器响应失败: opaque={response.opaque}, error={e}"
            )

    async def __aenter__(self) -> "AsyncRemote":
        """异步上下文管理器入口"""
        await self.connect()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """异步上下文管理器出口"""
        await self.close()

    @property
    def is_connected(self) -> bool:
        """是否已连接"""
        return self.transport.is_connected

    async def get_active_waiters_count(self) -> int:
        """获取活跃等待者数量"""
        async with self._waiters_lock:
            return len(self._waiters)
