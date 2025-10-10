"""
远程通信实例工厂
"""

from typing import Optional, Tuple, Union

from pyrocketmq.logging import get_logger
from pyrocketmq.transport.config import TransportConfig

from .async_remote import AsyncRemote
from .config import RemoteConfig, get_config
from .sync_remote import Remote


class RemoteFactory:
    """远程通信实例工厂"""

    @staticmethod
    def create_sync_remote(
        address: Union[str, Tuple[str, int]],
        remote_config: Optional[RemoteConfig] = None,
        transport_config: Optional[TransportConfig] = None,
    ) -> Remote:
        """创建同步远程通信实例

        Args:
            address: 服务器地址，可以是 "host:port" 或 (host, port) 格式
            remote_config: 远程通信配置，None则使用默认配置
            transport_config: 传输层配置，None则使用默认配置

        Returns:
            同步远程通信实例

        Raises:
            ValueError: 地址格式错误
            ConfigurationError: 配置错误
        """
        logger = get_logger("remote.factory")

        # 处理地址
        parsed_address = RemoteFactory._parse_address(address)
        logger.debug(f"创建同步远程连接: {parsed_address}")

        # 使用传入的配置或默认配置
        config = remote_config or get_config()

        # 设置传输层配置
        if transport_config is None:
            transport_config = config.transport_config or TransportConfig()
        else:
            # 更新配置中的传输层配置
            config = config.with_transport_config(transport_config)

        # 设置传输层地址
        host, port = parsed_address
        transport_config.host = host
        transport_config.port = port

        # 创建远程通信实例
        remote = Remote(transport_config, config)

        logger.info(f"同步远程通信实例创建成功: {parsed_address}")
        return remote

    @staticmethod
    def create_async_remote(
        address: Union[str, Tuple[str, int]],
        remote_config: Optional[RemoteConfig] = None,
        transport_config: Optional[TransportConfig] = None,
    ) -> AsyncRemote:
        """创建异步远程通信实例

        Args:
            address: 服务器地址，可以是 "host:port" 或 (host, port) 格式
            remote_config: 远程通信配置，None则使用默认配置
            transport_config: 传输层配置，None则使用默认配置

        Returns:
            异步远程通信实例

        Raises:
            ValueError: 地址格式错误
            ConfigurationError: 配置错误
        """
        logger = get_logger("remote.factory")

        # 处理地址
        parsed_address = RemoteFactory._parse_address(address)
        logger.debug(f"创建异步远程连接: {parsed_address}")

        # 使用传入的配置或默认配置
        config = remote_config or get_config()

        # 设置传输层配置
        if transport_config is None:
            transport_config = config.transport_config or TransportConfig()
        else:
            # 更新配置中的传输层配置
            config = config.with_transport_config(transport_config)

        # 设置传输层地址
        host, port = parsed_address
        transport_config.host = host
        transport_config.port = port

        # 创建远程通信实例
        remote = AsyncRemote(transport_config, config)

        logger.info(f"异步远程通信实例创建成功: {parsed_address}")
        return remote

    @staticmethod
    def create_sync_remote_from_env() -> Remote:
        """从环境变量创建同步远程通信实例

        环境变量:
        - PYROCKETMQ_SERVER_ADDRESS: 服务器地址
        - PYROCKETMQ_ENVIRONMENT: 环境 (development/production/testing)

        Returns:
            同步远程通信实例
        """
        import os

        # 从环境变量获取配置
        server_address = os.getenv(
            "PYROCKETMQ_SERVER_ADDRESS", "localhost:9876"
        )
        environment = os.getenv("PYROCKETMQ_ENVIRONMENT", "default")

        # 创建配置
        config = get_config(environment)

        return RemoteFactory.create_sync_remote(server_address, config)

    @staticmethod
    def create_async_remote_from_env() -> AsyncRemote:
        """从环境变量创建异步远程通信实例

        环境变量:
        - PYROCKETMQ_SERVER_ADDRESS: 服务器地址
        - PYROCKETMQ_ENVIRONMENT: 环境 (development/production/testing)

        Returns:
            异步远程通信实例
        """
        import os

        # 从环境变量获取配置
        server_address = os.getenv(
            "PYROCKETMQ_SERVER_ADDRESS", "localhost:9876"
        )
        environment = os.getenv("PYROCKETMQ_ENVIRONMENT", "default")

        # 创建配置
        config = get_config(environment)

        return RemoteFactory.create_async_remote(server_address, config)

    @staticmethod
    def _parse_address(address: Union[str, Tuple[str, int]]) -> Tuple[str, int]:
        """解析地址

        Args:
            address: 服务器地址

        Returns:
            解析后的 (host, port) 元组

        Raises:
            ValueError: 地址格式错误
        """
        if isinstance(address, tuple):
            if len(address) != 2:
                raise ValueError(f"地址元组必须包含2个元素: {address}")
            host, port = address
            return str(host), int(port)

        elif isinstance(address, str):
            # 处理 "host:port" 格式
            if ":" not in address:
                raise ValueError(f"地址字符串必须包含端口: {address}")

            parts = address.rsplit(":", 1)
            if len(parts) != 2:
                raise ValueError(f"地址格式错误: {address}")

            host, port_str = parts
            try:
                port = int(port_str)
            except ValueError:
                raise ValueError(f"端口必须是数字: {port_str}")

            return host, port

        else:
            raise ValueError(f"不支持的地址类型: {type(address)}")


# 便捷函数
def create_sync_remote(
    address: Union[str, Tuple[str, int]],
    config: Optional[RemoteConfig] = None,
    transport_config: Optional[TransportConfig] = None,
) -> Remote:
    """创建同步远程通信实例的便捷函数"""
    return RemoteFactory.create_sync_remote(address, config, transport_config)


def create_async_remote(
    address: Union[str, Tuple[str, int]],
    config: Optional[RemoteConfig] = None,
    transport_config: Optional[TransportConfig] = None,
) -> AsyncRemote:
    """创建异步远程通信实例的便捷函数"""
    return RemoteFactory.create_async_remote(address, config, transport_config)


def create_remote_from_env() -> Remote:
    """从环境变量创建同步远程通信实例的便捷函数"""
    return RemoteFactory.create_sync_remote_from_env()


def create_async_remote_from_env() -> AsyncRemote:
    """从环境变量创建异步远程通信实例的便捷函数"""
    return RemoteFactory.create_async_remote_from_env()
