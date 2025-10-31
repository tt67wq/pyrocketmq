"""连接状态管理模块 - 使用python-statemachine重构"""

import asyncio
import dis
import socket
import time
import logging
from typing import Any

from statemachine import State, StateMachine

from pyrocketmq.logging import get_logger

from .config import TransportConfig


class ConnectionStateMachine(StateMachine):
    """连接状态机 - 基于python-statemachine实现"""

    # 状态定义
    disconnected: State = State(initial=True)
    connecting: State = State()
    connected: State = State()
    closed: State = State(final=True)

    # 事件定义
    connect = disconnected.to(connecting)
    connect_success = connecting.to(connected)
    disconnect = connected.to(disconnected) | connecting.to(disconnected)
    close = disconnected.to(closed) | connected.to(closed) | connecting.to(closed)

    def __init__(self, config: TransportConfig) -> None:
        self.config: TransportConfig = config
        self._socket: socket.socket | None = None  # 这里可以存储实际的socket对象
        self._logger: logging.Logger = get_logger("transport.tcp")
        super().__init__()

    def start(self) -> None:
        """启动连接过程"""
        self.connect(self.disconnected)

    def stop(self) -> None:
        """关闭连接"""
        if not self.is_disconnected:
            self.close(self)
        else:
            self._logger.warning("当前状态不允许断开操作")

    # 状态转换回调
    def on_connect(self) -> None:
        """进入连接中状态"""
        self._logger.info(
            "开始连接到服务器",
            extra={
                "address": self.config.address,
                "host": self.config.host,
                "port": self.config.port,
            },
        )
        try:
            self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._socket.settimeout(self.config.connect_timeout)
            self._socket.connect(self.config.address)
        except Exception as e:
            self._logger.error(
                "连接失败",
                extra={
                    "address": self.config.address,
                    "error": str(e),
                    "error_type": type(e).__name__,
                },
            )
            self.disconnect(self.connecting)
        else:
            self.connect_success(self.connecting)

    def on_connect_success(self) -> None:
        """进入已连接状态"""
        # 设置socket选项
        if self._socket:
            # 基础选项
            self._socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            self._socket.settimeout(self.config.timeout)

            # TCP KeepAlive配置
            if self.config.keep_alive:
                self._set_keepalive()

            self._logger.info(
                "Socket选项已设置",
                extra={
                    "tcp_nodelay": True,
                    "timeout": self.config.timeout,
                    "address": self.config.address,
                },
            )

    def _set_keepalive(self) -> None:
        if not self._socket:
            return
        try:
            # 启用KeepAlive
            self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

            # 设置KeepAlive参数（仅在支持的系统上）
            if hasattr(socket, "TCP_KEEPIDLE"):
                # 空闲多久后开始探测（秒）
                self._socket.setsockopt(
                    socket.IPPROTO_TCP,
                    socket.TCP_KEEPIDLE,
                    int(self.config.keep_alive_interval),
                )

            if hasattr(socket, "TCP_KEEPINTVL"):
                # 探测间隔（秒）
                self._socket.setsockopt(
                    socket.IPPROTO_TCP,
                    socket.TCP_KEEPINTVL,
                    int(self.config.keep_alive_interval // 2),
                )

            if hasattr(socket, "TCP_KEEPCNT"):
                # 探测次数
                self._socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 3)

            self._logger.info(
                "TCP KeepAlive已启用",
                extra={
                    "keepalive_interval": self.config.keep_alive_interval,
                    "keepalive_timeout": self.config.keep_alive_timeout,
                    "address": self.config.address,
                },
            )

        except Exception as e:
            self._logger.warning(
                "设置TCP KeepAlive失败",
                extra={
                    "error": str(e),
                    "error_type": type(e).__name__,
                    "address": self.config.address,
                },
            )

    def on_disconnect(self) -> None:
        """进入断开连接状态"""
        self._logger.info(
            "连接已断开",
            extra={
                "address": self.config.address,
                "host": self.config.host,
                "port": self.config.port,
            },
        )
        # 清理socket资源
        self._close_socket()

        # 重连逻辑：只有在配置允许重连时才自动重连
        if self.config.max_retries != 0 and self.config.retry_interval > 0:
            self._logger.info(
                "准备重连",
                extra={
                    "address": self.config.address,
                    "retry_interval": self.config.retry_interval,
                    "max_retries": self.config.max_retries,
                },
            )
            time.sleep(self.config.retry_interval)
            self.connect(self.disconnected)
        else:
            self._logger.info(
                "重连已禁用，保持断开状态",
                extra={
                    "address": self.config.address,
                    "retry_interval": self.config.retry_interval,
                    "max_retries": self.config.max_retries,
                },
            )

    def on_close(self) -> None:
        """进入关闭状态"""
        self._logger.info(
            "连接已关闭",
            extra={
                "address": self.config.address,
                "host": self.config.host,
                "port": self.config.port,
            },
        )
        # 清理socket资源
        self._close_socket()

    def _close_socket(self) -> None:
        """关闭socket连接"""
        if self._socket:
            try:
                self._socket.close()
                self._logger.info("Socket已关闭")
            except Exception as e:
                self._logger.error(
                    "关闭Socket时发生错误",
                    extra={
                        "error": str(e),
                        "error_type": type(e).__name__,
                    },
                )
            finally:
                self._socket = None

    def output(self, msg: bytes) -> None:
        """发送二进制消息"""
        if not self.is_connected:
            raise RuntimeError("连接未建立，无法发送消息")

        if not self._socket:
            raise RuntimeError("Socket未初始化")

        try:
            # 检查消息大小
            if len(msg) > self.config.max_message_size:
                raise ValueError(
                    f"消息大小超过限制: {len(msg)} > {self.config.max_message_size}"
                )

            # 发送消息
            total_sent = 0
            while total_sent < len(msg):
                sent = self._socket.send(msg[total_sent:])
                if sent == 0:
                    raise ConnectionError("连接已断开")
                total_sent += sent

            self._logger.debug(
                "发送消息成功",
                extra={
                    "bytes_sent": total_sent,
                },
            )

        except Exception as e:
            self._logger.error(
                "发送消息失败",
                extra={
                    "error": str(e),
                    "error_type": type(e).__name__,
                },
            )
            # 发送失败，断开连接
            self.disconnect(self.connected)
            raise

    def recv(self, size: int) -> bytes:
        """接收二进制消息"""
        if not self.is_connected:
            raise RuntimeError("连接未建立，无法接收消息")

        if not self._socket:
            raise RuntimeError("Socket未初始化")

        # 验证参数
        if size <= 0:
            raise ValueError("接收大小必须大于0")
        if size > self.config.max_message_size:
            raise ValueError(
                f"接收大小超过限制: {size} > {self.config.max_message_size}"
            )

        try:
            # 接收消息
            data = self._socket.recv(size)

            if not data:
                # 连接被对方关闭
                self._logger.info("连接被对方关闭")
                self.disconnect(self.connected)
                return b""

            self._logger.debug(
                "接收消息成功",
                extra={
                    "bytes_received": len(data),
                    "bytes_requested": size,
                },
            )
            return data

        except socket.timeout:
            self._logger.debug("接收消息超时")
            raise TimeoutError("接收消息超时")
        except Exception as e:
            self._logger.error(
                "接收消息失败",
                extra={
                    "error": str(e),
                    "error_type": type(e).__name__,
                },
            )
            # 接收失败，断开连接
            self.disconnect(self.connected)
            raise

    def recv_pkg(self) -> bytes:
        """接收完整的数据包（header + body）

        数据包格式:
        - Header: 4字节大端序整数，表示body长度
        - Body: 实际数据内容

        Returns:
            完整的数据包（body部分），连接关闭时返回空字节
        """
        if not self.is_connected:
            raise RuntimeError("连接未建立，无法接收消息")

        if not self._socket:
            raise RuntimeError("Socket未初始化")

        try:
            # 1. 接收4字节header
            header_data = self._recv_exactly(4)
            if not header_data:
                # 连接被对方关闭
                self._logger.info("连接被对方关闭（接收header时）")
                self.disconnect(self.connected)
                return b""

            if len(header_data) != 4:
                raise ValueError(
                    f"Header长度不正确，期望4字节，实际{len(header_data)}字节"
                )

            # 2. 解析body长度（大端序）
            body_length = int.from_bytes(header_data, byteorder="big", signed=False)

            # 3. 验证body长度合理性
            if body_length < 0:
                raise ValueError(f"Body长度不能为负数: {body_length}")
            if body_length > self.config.max_message_size:
                raise ValueError(
                    f"Body长度超过限制: {body_length} > {self.config.max_message_size}"
                )

            # 4. 接收body数据
            if body_length == 0:
                # 空消息包
                self._logger.debug("接收到空消息包")
                return b""

            body_data = self._recv_exactly(body_length)
            if not body_data:
                # 连接被对方关闭
                self._logger.info("连接被对方关闭（接收body时）")
                self.disconnect(self.connected)
                return b""

            if len(body_data) != body_length:
                raise ValueError(
                    f"Body长度不匹配，期望{body_length}字节，实际{len(body_data)}字节"
                )

            self._logger.debug(
                "接收数据包成功",
                extra={
                    "header_bytes": 4,
                    "body_bytes": len(body_data),
                },
            )
            return body_data

        except socket.timeout:
            raise TimeoutError("接收数据包超时")
        except Exception as e:
            self._logger.error(
                "接收数据包失败",
                extra={
                    "error": str(e),
                    "error_type": type(e).__name__,
                },
            )
            # 接收失败，断开连接
            self.disconnect(self.connected)
            raise

    def _recv_exactly(self, size: int) -> bytes:
        """精确接收指定长度的数据"""
        if size <= 0:
            return b""

        if not self.is_connected:
            raise RuntimeError("连接未建立，无法接收消息")

        if not self._socket:
            raise RuntimeError("Socket未初始化")

        data = b""
        remaining = size

        while remaining > 0:
            try:
                chunk = self._socket.recv(remaining)
                if not chunk:
                    # 连接被对方关闭
                    return b""

                data += chunk
                remaining -= len(chunk)

            except socket.timeout:
                self._logger.debug(
                    "接收数据超时",
                    extra={
                        "bytes_received": len(data),
                        "bytes_requested": size,
                    },
                )
                raise
            except Exception as e:
                self._logger.error(
                    "接收数据失败",
                    extra={
                        "error": str(e),
                        "error_type": type(e).__name__,
                    },
                )
                raise

        return data

    @property
    def current_state_name(self) -> str:
        """获取当前状态名称"""
        return self.current_state.id

    @property
    def is_connected(self) -> bool:
        """是否已连接"""
        return self.current_state == self.connected

    @property
    def is_connecting(self) -> bool:
        """是否正在连接"""
        return self.current_state == self.connecting

    @property
    def is_disconnected(self) -> bool:
        """是否已断开连接"""
        return self.current_state == self.disconnected


class AsyncConnectionStateMachine(StateMachine):
    """异步连接状态机 - 基于python-statemachine实现"""

    # 状态定义
    disconnected: State = State(initial=True)
    connecting: State = State()
    connected: State = State()
    closed: State = State(final=True)

    # 事件定义
    connect = disconnected.to(connecting)
    connect_success = connecting.to(connected)
    disconnect = connected.to(disconnected) | connecting.to(disconnected)
    close = disconnected.to(closed) | connected.to(closed) | connecting.to(closed)

    def __init__(self, config: TransportConfig) -> None:
        self.config: TransportConfig = config
        self._socket: socket.socket | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._reader: asyncio.StreamReader | None = None
        self._logger: logging.Logger = get_logger("transport.tcp.async")
        super().__init__()

    async def start(self) -> None:
        """启动连接过程"""
        await self.connect(None)

    async def stop(self) -> None:
        """关闭连接"""
        if not self.is_disconnected:
            await self.close(None)
        else:
            self._logger.warning("当前状态不允许断开操作")

    # 状态转换回调
    async def on_connect(self) -> None:
        """进入连接中状态"""
        self._logger.info(
            "开始异步连接到服务器",
            extra={
                "address": self.config.address,
                "host": self.config.host,
                "port": self.config.port,
            },
        )
        try:
            # 使用asyncio.open_connection建立异步连接
            self._reader, self._writer = await asyncio.wait_for(
                asyncio.open_connection(*self.config.address),
                timeout=self.config.timeout,
            )

            # 获取底层socket用于配置
            if self._writer:
                self._socket = self._writer.get_extra_info("socket")
                if self._socket:
                    # 设置socket选项
                    self._socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

                    # TCP KeepAlive配置
                    if self.config.keep_alive:
                        await self._set_keepalive_async()

        except asyncio.TimeoutError:
            self._logger.error(
                "连接超时",
                extra={
                    "address": self.config.address,
                    "host": self.config.host,
                    "port": self.config.port,
                },
            )
            await self.disconnect(None)
        except Exception as e:
            self._logger.error(
                "连接失败",
                extra={
                    "address": self.config.address,
                    "error": str(e),
                    "error_type": type(e).__name__,
                },
            )
            await self.disconnect(None)
        else:
            self._logger.info("异步连接成功")
            await self.connect_success(None)

    async def on_connect_success(self) -> None:
        """进入已连接状态"""
        self._logger.info(
            "异步连接已建立",
            extra={
                "address": self.config.address,
                "host": self.config.host,
                "port": self.config.port,
            },
        )
        if self._writer and self._socket:
            self._logger.info(
                "异步Socket选项已设置",
                extra={
                    "tcp_nodelay": True,
                    "timeout": self.config.timeout,
                    "address": self.config.address,
                },
            )

    async def _set_keepalive_async(self) -> None:
        """设置TCP KeepAlive（异步版本）"""
        if not self._socket:
            return

        try:
            # 启用KeepAlive
            self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

            # 设置KeepAlive参数（仅在支持的系统上）
            if hasattr(socket, "TCP_KEEPIDLE"):
                # 空闲多久后开始探测（秒）
                self._socket.setsockopt(
                    socket.IPPROTO_TCP,
                    socket.TCP_KEEPIDLE,
                    int(self.config.keep_alive_interval),
                )

            if hasattr(socket, "TCP_KEEPINTVL"):
                # 探测间隔（秒）
                self._socket.setsockopt(
                    socket.IPPROTO_TCP,
                    socket.TCP_KEEPINTVL,
                    int(self.config.keep_alive_interval // 2),
                )

            if hasattr(socket, "TCP_KEEPCNT"):
                # 探测次数
                self._socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 3)

            self._logger.info(
                "异步TCP KeepAlive已启用",
                extra={
                    "keepalive_interval": self.config.keep_alive_interval,
                    "keepalive_timeout": self.config.keep_alive_timeout,
                    "address": self.config.address,
                },
            )

        except Exception as e:
            self._logger.warning(
                "设置异步TCP KeepAlive失败",
                extra={
                    "error": str(e),
                    "error_type": type(e).__name__,
                    "address": self.config.address,
                },
            )

    async def on_disconnect(self) -> None:
        """进入断开连接状态"""
        self._logger.info(
            "异步连接已断开",
            extra={
                "address": self.config.address,
                "host": self.config.host,
                "port": self.config.port,
            },
        )
        # 清理socket资源
        await self._close_connection()

        # 重连逻辑：只有在配置允许重连时才自动重连
        if self.config.max_retries != 0 and self.config.retry_interval > 0:
            self._logger.info(
                "准备异步重连",
                extra={
                    "address": self.config.address,
                    "retry_interval": self.config.retry_interval,
                    "max_retries": self.config.max_retries,
                },
            )
            await asyncio.sleep(self.config.retry_interval)
            await self.connect(None)
        else:
            self._logger.info(
                "异步重连已禁用，保持断开状态",
                extra={
                    "address": self.config.address,
                    "retry_interval": self.config.retry_interval,
                    "max_retries": self.config.max_retries,
                },
            )

    async def on_close(self) -> None:
        """进入关闭状态"""
        self._logger.info("异步连接已关闭")
        # 清理socket资源
        await self._close_connection()

    async def _close_connection(self) -> None:
        """关闭异步连接"""
        if self._writer:
            try:
                self._writer.close()
                await self._writer.wait_closed()
                self._logger.info("异步Writer已关闭")
            except Exception as e:
                self._logger.error(
                    "关闭异步Writer时发生错误",
                    extra={
                        "error": str(e),
                        "error_type": type(e).__name__,
                    },
                )
            finally:
                self._writer = None

        self._reader = None
        self._socket = None

    async def output(self, msg: bytes) -> None:
        """异步发送二进制消息"""
        if not self.is_connected:
            raise RuntimeError("连接未建立，无法发送消息")

        if not self._writer:
            raise RuntimeError("Writer未初始化")

        try:
            # 检查消息大小
            if len(msg) > self.config.max_message_size:
                raise ValueError(
                    f"消息大小超过限制: {len(msg)} > {self.config.max_message_size}"
                )

            # 异步发送消息
            self._writer.write(msg)
            await self._writer.drain()

            self._logger.debug(
                "异步发送消息成功",
                extra={
                    "bytes_sent": len(msg),
                },
            )

        except Exception as e:
            self._logger.error(
                "异步发送消息失败",
                extra={
                    "error": str(e),
                    "error_type": type(e).__name__,
                },
            )
            # 发送失败，断开连接
            await self.disconnect(None)
            raise

    async def recv(self, size: int) -> bytes:
        """异步接收二进制消息"""
        if not self.is_connected:
            raise RuntimeError("连接未建立，无法接收消息")

        if not self._reader:
            raise RuntimeError("Reader未初始化")

        # 验证参数
        if size <= 0:
            raise ValueError("接收大小必须大于0")
        if size > self.config.max_message_size:
            raise ValueError(
                f"接收大小超过限制: {size} > {self.config.max_message_size}"
            )

        try:
            # 异步接收消息
            data = await asyncio.wait_for(
                self._reader.readexactly(size), timeout=self.config.timeout
            )

            self._logger.debug(
                "异步接收消息成功",
                extra={
                    "bytes_received": len(data),
                    "bytes_requested": size,
                },
            )
            return data

        except asyncio.IncompleteReadError:
            # 连接被对方关闭
            self._logger.info("异步连接被对方关闭")
            await self.disconnect(None)
            return b""
        except asyncio.TimeoutError:
            self._logger.warning("异步接收消息超时")
            raise TimeoutError("异步接收消息超时")
        except Exception as e:
            self._logger.error(
                "异步接收消息失败",
                extra={
                    "error": str(e),
                    "error_type": type(e).__name__,
                },
            )
            # 接收失败，断开连接
            await self.disconnect(None)
            raise

    async def recv_pkg(self) -> bytes:
        """异步接收完整的数据包（header + body）

        数据包格式:
        - Header: 4字节大端序整数，表示body长度
        - Body: 实际数据内容

        Returns:
            完整的数据包（body部分），连接关闭时返回空字节
        """
        if not self.is_connected:
            raise RuntimeError("连接未建立，无法接收消息")

        if not self._reader:
            raise RuntimeError("Reader未初始化")

        try:
            # 1. 异步接收4字节header
            header_data = await self._recv_exactly_async(4)
            if not header_data:
                # 连接被对方关闭
                self._logger.info("异步连接被对方关闭（接收header时）")
                await self.disconnect(None)
                return b""

            if len(header_data) != 4:
                raise ValueError(
                    f"Header长度不正确，期望4字节，实际{len(header_data)}字节"
                )

            # 2. 解析body长度（大端序）
            body_length = int.from_bytes(header_data, byteorder="big", signed=False)

            # 3. 验证body长度合理性
            if body_length < 0:
                raise ValueError(f"Body长度不能为负数: {body_length}")
            if body_length > self.config.max_message_size:
                raise ValueError(
                    f"Body长度超过限制: {body_length} > {self.config.max_message_size}"
                )

            # 4. 异步接收body数据
            if body_length == 0:
                # 空消息包
                self._logger.debug("异步接收到空消息包")
                return b""

            body_data = await self._recv_exactly_async(body_length)
            if not body_data:
                # 连接被对方关闭
                self._logger.info("异步连接被对方关闭（接收body时）")
                await self.disconnect(None)
                return b""

            if len(body_data) != body_length:
                raise ValueError(
                    f"Body长度不匹配，期望{body_length}字节，实际{len(body_data)}字节"
                )

            self._logger.debug(
                "异步接收数据包成功",
                extra={
                    "header_bytes": 4,
                    "body_bytes": len(body_data),
                },
            )
            return body_data

        except asyncio.TimeoutError:
            self._logger.warning("异步接收数据包超时")
            raise TimeoutError("异步接收数据包超时")
        except Exception as e:
            self._logger.error(
                "异步接收数据包失败",
                extra={
                    "error": str(e),
                    "error_type": type(e).__name__,
                },
            )
            # 接收失败，断开连接
            await self.disconnect(None)
            raise

    async def _recv_exactly_async(self, size: int) -> bytes:
        """异步精确接收指定长度的数据"""
        if size <= 0:
            return b""

        if not self.is_connected:
            raise RuntimeError("连接未建立，无法接收消息")

        if not self._reader:
            raise RuntimeError("Reader未初始化")

        try:
            return await asyncio.wait_for(
                self._reader.readexactly(size), timeout=self.config.timeout
            )
        except asyncio.IncompleteReadError:
            # 连接被对方关闭
            return b""
        except asyncio.TimeoutError:
            self._logger.warning(
                "异步接收数据超时",
                extra={
                    "bytes_requested": size,
                },
            )
            raise

    @property
    def current_state_name(self) -> str:
        """获取当前状态名称"""
        return self.current_state.id

    @property
    def is_connected(self) -> bool:
        """是否已连接"""
        return self.current_state == self.connected

    @property
    def is_connecting(self) -> bool:
        """是否正在连接"""
        return self.current_state == self.connecting

    @property
    def is_disconnected(self) -> bool:
        """是否已断开连接"""
        return self.current_state == self.disconnected

    @property
    def is_closed(self) -> bool:
        """是否已关闭"""
        return self.current_state == self.closed
