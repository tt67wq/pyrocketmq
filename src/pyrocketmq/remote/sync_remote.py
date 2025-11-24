"""
同步远程通信实现
"""

import logging
import threading
import time
from typing import Any, Callable

from pyrocketmq.logging import get_logger
from pyrocketmq.model import RemotingCommand, RemotingCommandSerializer
from pyrocketmq.transport.config import TransportConfig
from pyrocketmq.transport.tcp import ConnectionStateMachine

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

# 客户端请求处理函数类型
ClientRequestFunc = Callable[[RemotingCommand, tuple[str, int]], RemotingCommand | None]


class Remote:
    """同步远程通信类"""

    def __init__(self, transport_cfg: TransportConfig, config: RemoteConfig) -> None:
        self.transport: ConnectionStateMachine = ConnectionStateMachine(transport_cfg)
        self.config: RemoteConfig = config
        self._logger: logging.Logger = get_logger("remote.sync")

        # 请求等待者管理: opaque -> (event, response, timestamp)
        self._waiters: dict[
            int, tuple[threading.Event, RemotingCommand | None, float]
        ] = {}
        self._waiters_lock: threading.Lock = threading.Lock()

        # opaque生成器
        self._opaque_lock: threading.Lock = threading.Lock()
        self._next_opaque: int = config.opaque_start

        # 请求处理器映射: code -> handler function
        self._processors: dict[int, ClientRequestFunc] = {}
        self._processors_lock: threading.Lock = threading.Lock()

        # 清理线程
        self._cleanup_thread: threading.Thread | None = None
        self._cleanup_stop_event: threading.Event = threading.Event()

        # 消息接收线程
        self._recv_thread: threading.Thread | None = None
        self._recv_stop_event: threading.Event = threading.Event()

        # 序列化器
        self._serializer: RemotingCommandSerializer = RemotingCommandSerializer()

    def connect(self) -> None:
        """建立连接"""
        if self.is_connected:
            return

        try:
            self.transport.start()
            # 启动清理线程
            self._start_cleanup_thread()

            # 启动消息接收线程
            self._start_recv_thread()

        except Exception as e:
            self._logger.error(
                "连接建立失败",
                extra={"error": str(e)},
            )
            raise ConnectionError(f"连接建立失败: {e}") from e

    def close(self) -> None:
        """关闭连接"""
        try:
            # 停止消息接收线程
            self._stop_recv_thread()

            # 停止清理线程
            self._stop_cleanup_thread()

            # 关闭传输层
            self.transport.stop()

            # 清理所有等待者
            with self._waiters_lock:
                for _opaque, (event, _, _) in self._waiters.items():
                    event.set()
                self._waiters.clear()

        except Exception as e:
            self._logger.error(
                "关闭连接失败",
                extra={"error": str(e)},
            )
            raise RemoteError(f"关闭连接失败: {e}") from e

    def register_request_processor(
        self, code: int, processor: ClientRequestFunc
    ) -> None:
        """注册请求处理器

        Args:
            code: 请求代码
            processor: 处理函数
        """
        with self._processors_lock:
            self._processors[code] = processor
            self._logger.debug(
                "注册请求处理器",
                extra={"code": code},
            )

    def register_request_processor_lazy(
        self, code: int, processor: ClientRequestFunc
    ) -> bool:
        """惰性注册请求处理器，如果已存在相同code的processor则不更新

        Args:
            code: 请求代码
            processor: 处理函数

        Returns:
            bool: 是否成功注册（True表示新注册，False表示已存在）
        """
        with self._processors_lock:
            if code in self._processors:
                self._logger.debug("请求处理器已存在，跳过注册", extra={"code": code})
                return False

            self._processors[code] = processor
            self._logger.debug("惰性注册请求处理器", extra={"code": code})
            return True

    def unregister_request_processor(self, code: int) -> bool:
        """取消注册请求处理器

        Args:
            code: 请求代码

        Returns:
            是否成功取消
        """
        with self._processors_lock:
            if code in self._processors:
                del self._processors[code]
                self._logger.debug("取消注册请求处理器", extra={"code": code})
                return True
            return False

    def rpc(
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
            RpcTimeoutError: 超时错误
            SerializationError: 序列化错误
            ProtocolError: 协议错误
            ResourceExhaustedError: 资源耗尽错误
        """
        if not self.transport.is_connected:
            raise ConnectionError("连接未建立")

        # 使用传入的超时或配置的超时
        rpc_timeout = timeout if timeout is not None else self.config.rpc_timeout

        # 生成opaque并设置为请求
        opaque = self._generate_opaque()
        command.opaque = opaque
        command.set_request()

        # 创建等待事件
        event = threading.Event()

        # 注册等待者
        if not self._register_waiter(opaque, event):
            raise MaxWaitersExceededError(
                f"等待者数量超过限制: {self.config.max_waiters}"
            )

        try:
            # 序列化并发送命令
            data = self._serializer.serialize(command)
            self.transport.output(data)

            self._logger.debug(
                "发送RPC请求",
                extra={"opaque": opaque, "code": command.code},
            )

            # 等待响应
            if not event.wait(rpc_timeout):
                # 超时，移除等待者
                _ = self._unregister_waiter(opaque)
                raise RpcTimeoutError(f"RPC请求超时: opaque={opaque}")

            # 获取响应
            response = self._get_waiter_response(opaque)
            if response is None:
                raise ProtocolError(f"未收到有效响应: opaque={opaque}")

            self._logger.debug(
                "收到RPC响应", extra={"opaque": opaque, "code": response.code}
            )
            return response

        except SerializationError:
            raise
        except (ConnectionError, TransportError):
            # 连接或传输错误，移除等待者
            _ = self._unregister_waiter(opaque)
            raise
        except RpcTimeoutError:
            _ = self._unregister_waiter(opaque)
            self._logger.error(
                "RPC调用失败",
                extra={"opaque": opaque, "reason": "timeout"},
            )
            raise
        except Exception as e:
            _ = self._unregister_waiter(opaque)
            raise RemoteError(f"RPC调用失败: {e}") from e

    def oneway(self, command: RemotingCommand) -> None:
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
        opaque = self._generate_opaque()
        command.opaque = opaque

        try:
            # 序列化并发送命令
            data = self._serializer.serialize(command)
            self.transport.output(data)

            self._logger.debug(
                "发送单向消息", extra={"opaque": opaque, "code": command.code}
            )
        except SerializationError:
            raise
        except (ConnectionError, TransportError):
            raise
        except Exception as e:
            self._logger.error(
                "单向消息发送失败", extra={"opaque": opaque, "error": str(e)}
            )
            raise RemoteError(f"单向消息发送失败: {e}") from e

    def _generate_opaque(self) -> int:
        """生成唯一的opaque值"""
        with self._opaque_lock:
            opaque = self._next_opaque
            self._next_opaque += 1
            return opaque

    def _register_waiter(self, opaque: int, event: threading.Event) -> bool:
        """注册等待者

        Returns:
            是否注册成功
        """
        with self._waiters_lock:
            if len(self._waiters) >= self.config.max_waiters:
                return False

            self._waiters[opaque] = (event, None, time.time())
            return True

    def _unregister_waiter(self, opaque: int) -> bool:
        """取消注册等待者

        Returns:
            是否取消成功
        """
        with self._waiters_lock:
            return self._waiters.pop(opaque, None) is not None

    def _get_waiter_response(self, opaque: int) -> RemotingCommand | None:
        """获取等待者的响应

        Returns:
            响应命令，如果不存在则返回None
        """
        with self._waiters_lock:
            waiter = self._waiters.get(opaque)
            if waiter:
                _, response, _ = waiter
                return response
            return None

    def _set_waiter_response(self, opaque: int, response: RemotingCommand) -> bool:
        """设置等待者的响应

        Returns:
            是否设置成功
        """
        with self._waiters_lock:
            waiter = self._waiters.get(opaque)
            if waiter:
                event, _, _timestamp = waiter
                # 更新响应和时间戳
                self._waiters[opaque] = (event, response, time.time())
                event.set()
                return True
            return False

    def _start_cleanup_thread(self) -> None:
        """启动清理线程"""
        if self._cleanup_thread is None or not self._cleanup_thread.is_alive():
            self._cleanup_stop_event.clear()
            self._cleanup_thread = threading.Thread(
                target=self._cleanup_worker, daemon=True, name="remote-cleanup"
            )
            self._cleanup_thread.start()
            self._logger.debug("清理线程已启动")

    def _stop_cleanup_thread(self) -> None:
        """停止清理线程"""
        if self._cleanup_thread and self._cleanup_thread.is_alive():
            self._cleanup_stop_event.set()
            self._cleanup_thread.join(timeout=5.0)
            self._logger.debug("清理线程已停止")

    def _cleanup_worker(self) -> None:
        """清理工作线程"""
        while not self._cleanup_stop_event.wait(self.config.cleanup_interval):
            try:
                self._cleanup_expired_waiters()
            except Exception as e:
                self._logger.error("清理等待者时发生错误", extra={"error": str(e)})

    def _cleanup_expired_waiters(self) -> None:
        """清理过期的等待者"""
        current_time = time.time()
        expired_opaques: list[int] = []

        with self._waiters_lock:
            for opaque, (_, _, timestamp) in self._waiters.items():
                if current_time - timestamp > self.config.waiter_timeout:
                    expired_opaques.append(opaque)

            # 移除过期的等待者
            for opaque in expired_opaques:
                event, _, _ = self._waiters.pop(opaque, (None, None, None))
                if event:
                    event.set()

        if expired_opaques:
            self._logger.warning(
                "清理过期的等待者", extra={"count": len(expired_opaques)}
            )

    def __enter__(self) -> "Remote":
        """上下文管理器入口"""
        self.connect()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """上下文管理器出口"""
        self.close()

    @property
    def is_connected(self) -> bool:
        """是否已连接"""
        return self.transport.is_connected

    @property
    def active_waiters_count(self) -> int:
        """活跃等待者数量"""
        with self._waiters_lock:
            return len(self._waiters)

    def _start_recv_thread(self) -> None:
        """启动消息接收线程"""
        if self._recv_thread is None or not self._recv_thread.is_alive():
            self._recv_stop_event.clear()
            self._recv_thread = threading.Thread(
                target=self._recv_worker, daemon=True, name="remote-recv"
            )
            self._recv_thread.start()
            self._logger.debug("消息接收线程已启动")

    def _stop_recv_thread(self) -> None:
        """停止消息接收线程"""
        if self._recv_thread and self._recv_thread.is_alive():
            self._recv_stop_event.set()
            self._recv_thread.join(timeout=5.0)
            self._logger.debug("消息接收线程已停止")

    def _recv_worker(self) -> None:
        """消息接收工作线程"""
        while not self._recv_stop_event.is_set():
            try:
                if not self.transport.is_connected:
                    # 连接断开，等待重连
                    self._logger.debug("连接断开，等待重连")
                    time.sleep(1.0)
                    continue

                # 接收数据包
                data = self.transport.recv_pkg()
                if not data:
                    # 连接关闭
                    self._logger.debug("连接已关闭，停止接收")
                    break

                # 反序列化响应
                try:
                    response = self._serializer.deserialize(data)
                    self._logger.debug(
                        "接收到响应",
                        extra={"opaque": response.opaque, "code": response.code},
                    )
                except Exception as e:
                    self._logger.error("反序列化响应失败", extra={"error": str(e)})
                    continue

                # 处理响应
                self._handle_response(response)

            except TimeoutError:
                # 接收超时，继续循环
                continue
            except ConnectionError:
                # 连接错误，等待重连
                self._logger.warning("连接错误，等待重连")
                time.sleep(1.0)
                continue
            except Exception as e:
                self._logger.error("接收消息时发生错误", extra={"error": str(e)})
                time.sleep(1.0)

    def _handle_response(self, command: RemotingCommand) -> None:
        """处理接收到的命令（响应或请求）"""
        if command.is_response:
            # 处理响应消息
            self._handle_response_message(command)
        else:
            # 处理请求消息（服务器主动通知）
            self._handle_request_message(command)

    def _handle_response_message(self, response: RemotingCommand) -> None:
        """处理响应消息"""
        opaque = response.opaque
        # if opaque is None:
        #     self._logger.warning("响应消息缺少opaque字段")
        #     return

        # 查找对应的等待者并设置响应
        if self._set_waiter_response(opaque, response):
            self._logger.debug("响应已匹配到等待者", extra={"opaque": opaque})
        else:
            self._logger.warning("未找到对应的等待者", extra={"opaque": opaque})

    def _handle_request_message(self, request: RemotingCommand) -> None:
        """处理请求消息（服务器主动通知）"""
        self._logger.debug(
            "收到服务器请求", extra={"code": request.code, "opaque": request.opaque}
        )

        # 查找对应的处理器
        processor_func = None
        with self._processors_lock:
            processor_func = self._processors.get(request.code)

        if processor_func is None:
            self._logger.warning(
                "收到服务器请求，但没有对应的处理器，可能是oneway请求",
                extra={"code": request.code},
            )
            return

        try:
            # 获取远程地址信息
            remote_addr = (
                self.transport.config.host,
                self.transport.config.port,
            )

            # 调用处理器
            response = processor_func(request, remote_addr)

            # 如果处理器返回了响应，则发送回服务器
            if response is not None:
                self._send_processor_response(request, response)

        except Exception as e:
            self._logger.error(
                "处理服务器请求时发生错误",
                extra={"code": request.code, "error": str(e)},
            )

    def _send_processor_response(
        self, _request: RemotingCommand, response: RemotingCommand
    ) -> None:
        """发送处理器生成的响应"""
        try:
            # 设置响应的opaque和flag
            # if request.opaque is not None:
            #     response.opaque = request.opaque
            response.set_response()

            # 序列化并发送响应
            data = self._serializer.serialize(response)
            self.transport.output(data)

            self._logger.debug(
                "发送处理器响应",
                extra={"opaque": response.opaque, "code": response.code},
            )

        except Exception as e:
            self._logger.error(
                "发送处理器响应失败", extra={"opaque": response.opaque, "error": str(e)}
            )
