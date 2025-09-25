"""连接状态管理模块 - 使用python-statemachine重构"""

from typing import Callable, List

from statemachine import State, StateMachine


class ConnectionStateMachine(StateMachine):
    """连接状态机 - 基于python-statemachine实现"""

    # 状态定义
    disconnected = State(initial=True)
    connecting = State()
    connected = State()

    # 事件定义
    connect = disconnected.to(connecting)
    connect_success = connecting.to(connected)
    disconnect = connected.to(disconnected) | connecting.to(disconnected)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._callbacks: List[Callable[[str, str], None]] = []

    # 状态转换回调
    def on_enter_connecting(self):
        """进入连接中状态"""
        pass

    def on_enter_connected(self):
        """进入已连接状态"""
        pass

    def on_enter_disconnected(self):
        """进入断开连接状态"""
        pass

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
