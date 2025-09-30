"""连接状态管理模块 - 使用python-statemachine重构"""

import asyncio
import socket
import time
from typing import Optional

from statemachine import State, StateMachine

from pyrocketmq.logging import get_logger

from .config import TransportConfig


class ConnectionStateMachine(StateMachine):
    """连接状态机 - 基于python-statemachine实现"""

    # 状态定义
    disconnected = State(initial=True)
    connecting = State()
    connected = State()
    closed = State(final=True)

    # 事件定义
    connect = disconnected.to(connecting)
    connect_success = connecting.to(connected)
    disconnect = connected.to(disconnected) | connecting.to(disconnected)
    close = (
        disconnected.to(closed) | connected.to(closed) | connecting.to(closed)
    )

    def __init__(self, config: TransportConfig):
        self.config = config
        self._socket = None  # 这里可以存储实际的socket对象
        self._logger = get_logger("transport.tcp")
        super().__init__()

    def start(self):
        """启动连接过程"""
        self.connect()

    def stop(self):
        """关闭连接"""
        if not self.is_disconnected:
            self.disconnect()
        else:
            self._logger.warning("当前状态不允许断开操作")

    # 状态转换回调
    def on_connect(self):
        """进入连接中状态"""
        self._logger.info(f"开始连接到 {self.config.address}")
        try:
            self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._socket.settimeout(self.config.connect_timeout)
            self._socket.connect(self.config.address)
        except Exception as e:
            self._logger.error(f"连接失败: {e}")
            self.disconnect()
        else:
            self._logger.info("连接成功")
            self.connect_success()

    def on_connect_success(self):
        """进入已连接状态"""
        self._logger.info(f"连接已建立到 {self.config.address}")
        # 设置socket选项
        if self._socket:
            # 基础选项
            self._socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            self._socket.settimeout(self.config.timeout)

            # TCP KeepAlive配置
            if self.config.keep_alive:
                self._set_keepalive()

            self._logger.info(
                f"Socket选项已设置: TCP_NODELAY=True, timeout={self.config.timeout}"
            )

    def _set_keepalive(self):
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
                self._socket.setsockopt(
                    socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 3
                )

            self._logger.info(
                f"TCP KeepAlive已启用: interval={self.config.keep_alive_interval}s, timeout={self.config.keep_alive_timeout}s"
            )

        except Exception as e:
            self._logger.warning(f"设置TCP KeepAlive失败: {e}")

    def on_disconnect(self):
        """进入断开连接状态"""
        self._logger.info(f"连接已断开: {self.config.address}")
        # 清理socket资源
        self._close_socket()

        # 重连逻辑：只有在配置允许重连时才自动重连
        if self.config.max_retries != 0 and self.config.retry_interval > 0:
            self._logger.info("准备重连...")
            time.sleep(self.config.retry_interval)
            self.connect()
        else:
            self._logger.info("重连已禁用，保持断开状态")

    def on_close(self):
        """进入关闭状态"""
        self._logger.info("连接已关闭")
        # 清理socket资源
        self._close_socket()

    def _close_socket(self):
        """关闭socket连接"""
        if self._socket:
            try:
                self._socket.close()
                self._logger.info("Socket已关闭")
            except Exception as e:
                self._logger.error(f"关闭Socket时发生错误: {e}")
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

            self._logger.debug(f"发送消息成功: {total_sent} bytes")

        except Exception as e:
            self._logger.error(f"发送消息失败: {e}")
            # 发送失败，断开连接
            self.disconnect()
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
                self.disconnect()
                return b""

            self._logger.debug(
                f"接收消息成功: {len(data)} bytes (请求: {size})"
            )
            return data

        except socket.timeout:
            self._logger.debug("接收消息超时")
            raise TimeoutError("接收消息超时")
        except Exception as e:
            self._logger.error(f"接收消息失败: {e}")
            # 接收失败，断开连接
            self.disconnect()
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
                self.disconnect()
                return b""

            if len(header_data) != 4:
                raise ValueError(
                    f"Header长度不正确，期望4字节，实际{len(header_data)}字节"
                )

            # 2. 解析body长度（大端序）
            body_length = int.from_bytes(
                header_data, byteorder="big", signed=False
            )

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
                self.disconnect()
                return b""

            if len(body_data) != body_length:
                raise ValueError(
                    f"Body长度不匹配，期望{body_length}字节，实际{len(body_data)}字节"
                )

            self._logger.debug(
                f"接收数据包成功: header=4bytes, body={len(body_data)}bytes"
            )
            return body_data

        except socket.timeout:
            raise TimeoutError("接收数据包超时")
        except Exception as e:
            self._logger.error(f"接收数据包失败: {e}")
            # 接收失败，断开连接
            self.disconnect()
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
                    f"接收数据超时，已接收{len(data)}/{size}字节"
                )
                raise
            except Exception as e:
                self._logger.error(f"接收数据失败: {e}")
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
    disconnected = State(initial=True)
    connecting = State()
    connected = State()
    closed = State(final=True)

    # 事件定义
    connect = disconnected.to(connecting)
    connect_success = connecting.to(connected)
    disconnect = connected.to(disconnected) | connecting.to(disconnected)
    close = (
        disconnected.to(closed) | connected.to(closed) | connecting.to(closed)
    )

    def __init__(self, config: TransportConfig):
        self.config = config
        self._socket: Optional[socket.socket] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._reader: Optional[asyncio.StreamReader] = None
        self._logger = get_logger("transport.tcp.async")
        super().__init__()

    async def start(self) -> None:
        """启动连接过程"""
        await self.connect()

    async def stop(self) -> None:
        """关闭连接"""
        if not self.is_disconnected:
            await self.disconnect()
        else:
            self._logger.warning("当前状态不允许断开操作")

    # 状态转换回调
    async def on_connect(self) -> None:
        """进入连接中状态"""
        self._logger.info(f"开始异步连接到 {self.config.address}")
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
                    self._socket.setsockopt(
                        socket.IPPROTO_TCP, socket.TCP_NODELAY, 1
                    )

                    # TCP KeepAlive配置
                    if self.config.keep_alive:
                        await self._set_keepalive_async()

        except asyncio.TimeoutError:
            self._logger.error(f"连接超时: {self.config.address}")
            await self.disconnect()
        except Exception as e:
            self._logger.error(f"连接失败: {e}")
            await self.disconnect()
        else:
            self._logger.info("异步连接成功")
            await self.connect_success()

    async def on_connect_success(self) -> None:
        """进入已连接状态"""
        self._logger.info(f"异步连接已建立到 {self.config.address}")
        if self._writer and self._socket:
            self._logger.info(
                f"异步Socket选项已设置: TCP_NODELAY=True, timeout={self.config.timeout}"
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
                self._socket.setsockopt(
                    socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 3
                )

            self._logger.info(
                f"异步TCP KeepAlive已启用: interval={self.config.keep_alive_interval}s, timeout={self.config.keep_alive_timeout}s"
            )

        except Exception as e:
            self._logger.warning(f"设置异步TCP KeepAlive失败: {e}")

    async def on_disconnect(self) -> None:
        """进入断开连接状态"""
        self._logger.info(f"异步连接已断开: {self.config.address}")
        # 清理socket资源
        await self._close_connection()

        # 重连逻辑：只有在配置允许重连时才自动重连
        if self.config.max_retries != 0 and self.config.retry_interval > 0:
            self._logger.info("准备异步重连...")
            await asyncio.sleep(self.config.retry_interval)
            await self.connect()
        else:
            self._logger.info("异步重连已禁用，保持断开状态")

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
                self._logger.error(f"关闭异步Writer时发生错误: {e}")
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

            self._logger.debug(f"异步发送消息成功: {len(msg)} bytes")

        except Exception as e:
            self._logger.error(f"异步发送消息失败: {e}")
            # 发送失败，断开连接
            await self.disconnect()
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
                f"异步接收消息成功: {len(data)} bytes (请求: {size})"
            )
            return data

        except asyncio.IncompleteReadError:
            # 连接被对方关闭
            self._logger.info("异步连接被对方关闭")
            await self.disconnect()
            return b""
        except asyncio.TimeoutError:
            self._logger.warning("异步接收消息超时")
            raise TimeoutError("异步接收消息超时")
        except Exception as e:
            self._logger.error(f"异步接收消息失败: {e}")
            # 接收失败，断开连接
            await self.disconnect()
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
                await self.disconnect()
                return b""

            if len(header_data) != 4:
                raise ValueError(
                    f"Header长度不正确，期望4字节，实际{len(header_data)}字节"
                )

            # 2. 解析body长度（大端序）
            body_length = int.from_bytes(
                header_data, byteorder="big", signed=False
            )

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
                await self.disconnect()
                return b""

            if len(body_data) != body_length:
                raise ValueError(
                    f"Body长度不匹配，期望{body_length}字节，实际{len(body_data)}字节"
                )

            self._logger.debug(
                f"异步接收数据包成功: header=4bytes, body={len(body_data)}bytes"
            )
            return body_data

        except asyncio.TimeoutError:
            self._logger.warning("异步接收数据包超时")
            raise TimeoutError("异步接收数据包超时")
        except Exception as e:
            self._logger.error(f"异步接收数据包失败: {e}")
            # 接收失败，断开连接
            await self.disconnect()
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
            self._logger.warning(f"异步接收数据超时，请求{size}字节")
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
