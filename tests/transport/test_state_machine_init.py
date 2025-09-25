"""状态机初始化测试"""

from pyrocketmq.transport.config import TransportConfig
from pyrocketmq.transport.tcp import ConnectionStateMachine


class TestStateMachineInitialization:
    """状态机初始化测试类"""

    def test_state_machine_initialization_default_config(self):
        """测试状态机初始化 - 默认配置"""
        # 使用默认配置创建状态机
        config = TransportConfig()
        state_machine = ConnectionStateMachine(config)

        # 验证初始状态
        assert state_machine.current_state_name == "disconnected"
        assert state_machine.is_disconnected
        assert state_machine.is_connecting
        assert state_machine.is_connected

        # 验证配置正确设置
        assert state_machine.config == config
        assert state_machine.config.host == "localhost"
        assert state_machine.config.port == 8080

        # 验证socket初始状态
        assert state_machine._socket is None

        # 验证logger已创建
        assert state_machine._logger is not None

    def test_state_machine_initialization_custom_config(self):
        """测试状态机初始化 - 自定义配置"""
        # 创建自定义配置
        custom_config = TransportConfig(
            host="testhost",
            port=9999,
            timeout=60.0,
            keep_alive=True,
            keep_alive_interval=45.0,
        )

        state_machine = ConnectionStateMachine(custom_config)

        # 验证配置正确应用
        assert state_machine.config == custom_config
        assert state_machine.config.host == "testhost"
        assert state_machine.config.port == 9999
        assert state_machine.config.timeout == 60.0
        assert state_machine.config.keep_alive
        assert state_machine.config.keep_alive_interval == 45.0

        # 验证初始状态不变
        assert state_machine.current_state_name == "disconnected"
        assert state_machine.is_disconnected

    def test_state_machine_initialization_minimal_config(self):
        """测试状态机初始化 - 最小配置"""
        # 最小配置
        minimal_config = TransportConfig(host="127.0.0.1", port=3000)
        state_machine = ConnectionStateMachine(minimal_config)

        # 验证基本设置
        assert state_machine.config.host == "127.0.0.1"
        assert state_machine.config.port == 3000
        assert state_machine.is_disconnected
        assert state_machine._socket is None

    def test_state_machine_initialization_state_properties(self):
        """测试状态机初始化 - 状态属性"""
        config = TransportConfig()
        state_machine = ConnectionStateMachine(config)

        # 测试所有状态属性
        assert hasattr(state_machine, "current_state_name")
        assert hasattr(state_machine, "is_connected")
        assert hasattr(state_machine, "is_connecting")
        assert hasattr(state_machine, "is_disconnected")

        # 验证初始值
        assert state_machine.current_state_name == "disconnected"
        assert not state_machine.is_connected
        assert not state_machine.is_connecting
        assert not state_machine.is_disconnected

    def test_state_machine_initialization_logger_name(self):
        """测试状态机初始化 - Logger名称"""
        config = TransportConfig()
        state_machine = ConnectionStateMachine(config)

        # 验证logger名称
        logger_name = state_machine._logger.name
        assert logger_name == "transport.tcp"

        # 验证logger可用性
        assert hasattr(state_machine._logger, "info")
        assert hasattr(state_machine._logger, "error")
        assert hasattr(state_machine._logger, "warning")
        assert hasattr(state_machine._logger, "debug")
