"""
同步远程通信实现
"""

import threading
import time
from typing import Dict, Optional

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
    SerializationError,
    TimeoutError,
    TransportError,
)


class Remote:
    """同步远程通信类"""

    def __init__(self, transport_cfg: TransportConfig, config: RemoteConfig):
        self.transport: ConnectionStateMachine = ConnectionStateMachine(
            transport_cfg
        )
        self.config = config
        self._logger = get_logger("remote.sync")

        # 请求等待者管理: opaque -> (event, response, timestamp)
        self._waiters: Dict[
            int, tuple[threading.Event, Optional[RemotingCommand], float]
        ] = {}
        self._waiters_lock = threading.Lock()

        # opaque生成器
        self._opaque_lock = threading.Lock()
        self._next_opaque = config.opaque_start

        # 清理线程
        self._cleanup_thread: Optional[threading.Thread] = None
        self._cleanup_stop_event = threading.Event()

        # 消息接收线程
        self._recv_thread: Optional[threading.Thread] = None
        self._recv_stop_event = threading.Event()

        # 序列化器
        self._serializer = RemotingCommandSerializer()

    def connect(self) -> None:
        """建立连接"""
        try:
            self.transport.start()
            self._logger.info("连接建立成功")

            # 启动清理线程
            self._start_cleanup_thread()

            # 启动消息接收线程
            self._start_recv_thread()

        except Exception as e:
            self._logger.error(f"连接建立失败: {e}")
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
                for opaque, (event, _, _) in self._waiters.items():
                    event.set()
                self._waiters.clear()

            self._logger.info("连接已关闭")

        except Exception as e:
            self._logger.error(f"关闭连接失败: {e}")
            raise RemoteError(f"关闭连接失败: {e}") from e

    def rpc(
        self, command: RemotingCommand, timeout: Optional[float] = None
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
        rpc_timeout = (
            timeout if timeout is not None else self.config.rpc_timeout
        )

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
                f"发送RPC请求: opaque={opaque}, code={command.code}"
            )

            # 等待响应
            if not event.wait(rpc_timeout):
                # 超时，移除等待者
                self._unregister_waiter(opaque)
                raise TimeoutError(f"RPC请求超时: opaque={opaque}")

            # 获取响应
            response = self._get_waiter_response(opaque)
            if response is None:
                raise ProtocolError(f"未收到有效响应: opaque={opaque}")

            self._logger.debug(
                f"收到RPC响应: opaque={opaque}, code={response.code}"
            )
            return response

        except SerializationError:
            raise
        except (ConnectionError, TransportError):
            # 连接或传输错误，移除等待者
            self._unregister_waiter(opaque)
            raise
        except Exception as e:
            # 其他错误，移除等待者
            self._unregister_waiter(opaque)
            self._logger.error(f"RPC调用失败: opaque={opaque}, error={e}")
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
        command.set_oneway()

        try:
            # 序列化并发送命令
            data = self._serializer.serialize(command)
            self.transport.output(data)

            self._logger.debug(
                f"发送单向消息: opaque={opaque}, code={command.code}"
            )

        except SerializationError:
            raise
        except (ConnectionError, TransportError):
            raise
        except Exception as e:
            self._logger.error(f"单向消息发送失败: opaque={opaque}, error={e}")
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

    def _get_waiter_response(self, opaque: int) -> Optional[RemotingCommand]:
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

    def _set_waiter_response(
        self, opaque: int, response: RemotingCommand
    ) -> bool:
        """设置等待者的响应

        Returns:
            是否设置成功
        """
        with self._waiters_lock:
            waiter = self._waiters.get(opaque)
            if waiter:
                event, _, timestamp = waiter
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
                self._logger.error(f"清理等待者时发生错误: {e}")

    def _cleanup_expired_waiters(self) -> None:
        """清理过期的等待者"""
        current_time = time.time()
        expired_opaques = []

        with self._waiters_lock:
            for opaque, (_, _, timestamp) in self._waiters.items():
                if current_time - timestamp > self.config.waiter_timeout:
                    expired_opaques.append(opaque)

            # 移除过期的等待者
            for opaque in expired_opaques:
                event, _, _ = self._waiters.pop(opaque, None)
                if event:
                    event.set()

        if expired_opaques:
            self._logger.warning(
                f"清理了 {len(expired_opaques)} 个过期的等待者"
            )

    def __enter__(self):
        """上下文管理器入口"""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
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
        self._logger.info("消息接收线程开始工作")

        while not self._recv_stop_event.is_set():
            try:
                if not self.transport.is_connected:
                    # 连接断开，等待重连
                    self._logger.debug("连接断开，等待重连...")
                    time.sleep(1.0)
                    continue

                # 接收数据包
                data = self.transport.recv_pkg()
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
                self._handle_response(response)

            except TimeoutError:
                # 接收超时，继续循环
                continue
            except ConnectionError:
                # 连接错误，等待重连
                self._logger.warning("连接错误，等待重连...")
                time.sleep(1.0)
                continue
            except Exception as e:
                self._logger.error(f"接收消息时发生错误: {e}")
                time.sleep(1.0)

        self._logger.info("消息接收线程结束")

    def _handle_response(self, response: RemotingCommand) -> None:
        """处理接收到的响应"""
        if not response.is_response:
            self._logger.warning(
                f"收到的不是响应消息: opaque={response.opaque}"
            )
            return

        opaque = response.opaque
        if opaque is None:
            self._logger.warning("响应消息缺少opaque字段")
            return

        # 查找对应的等待者并设置响应
        if self._set_waiter_response(opaque, response):
            self._logger.debug(f"响应已匹配到等待者: opaque={opaque}")
        else:
            self._logger.warning(f"未找到对应的等待者: opaque={opaque}")
