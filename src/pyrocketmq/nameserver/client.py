"""
NameServer 客户端实现
提供同步和异步两种方式与 RocketMQ NameServer 进行通信
"""

from pyrocketmq.model.factory import RemotingCommandBuilder

from ..logging import LoggerFactory
from ..model import RemotingRequestFactory
from ..model.enums import FlagType, LanguageCode, RequestCode
from ..model.utils import is_success_response
from ..remote.async_remote import AsyncRemote
from ..remote.factory import create_async_remote, create_sync_remote
from ..remote.sync_remote import Remote
from .errors import NameServerError, NameServerTimeoutError
from .models import BrokerClusterInfo, TopicRouteData

logger = LoggerFactory.get_logger(__name__)


class SyncNameServerClient:
    """同步 NameServer 客户端

    使用 Remote 类进行同步通信
    """

    def __init__(self, remote: Remote, timeout: float = 30.0) -> None:
        """初始化同步客户端

        Args:
            remote: 远程通信实例
            timeout: 请求超时时间，默认30秒
        """
        self.remote: Remote = remote
        self.timeout: float = timeout

    def connect(self) -> None:
        """建立连接"""
        logger.info("Connecting to NameServer...")
        self.remote.connect()
        logger.info("Connected to NameServer successfully")

    def disconnect(self) -> None:
        """断开连接"""
        logger.info("Disconnecting from NameServer...")
        self.remote.close()
        logger.info("Disconnected from NameServer")

    def is_connected(self) -> bool:
        """检查连接状态"""
        return self.remote.is_connected

    def query_topic_route_info(self, topic: str) -> TopicRouteData:
        """查询 Topic 路由信息

        Args:
            topic: 主题名称

        Returns:
            TopicRouteData: Topic 路由信息

        Raises:
            NameServerError: 查询失败时抛出
            NameServerTimeoutError: 请求超时时抛出
        """
        if not self.is_connected():
            raise NameServerError("Client is not connected")

        try:
            # 创建请求命令
            command = RemotingRequestFactory.create_get_route_info_request(topic)

            logger.debug("Querying topic route info", extra={"topic": topic})

            # 发送RPC请求
            response = self.remote.rpc(command, timeout=self.timeout)

            # 检查响应状态
            if not is_success_response(response):
                error_msg = (
                    response.remark or f"Query failed with code: {response.code}"
                )
                logger.error(
                    "Query topic route info failed",
                    extra={
                        "topic": topic,
                        "error_msg": error_msg,
                        "response_code": response.code,
                    },
                )
                raise NameServerError(f"Query topic route info failed: {error_msg}")

            # 解析响应体
            if not response.body:
                raise NameServerError("Empty response body")

            route_data = TopicRouteData.from_bytes(response.body)
            logger.debug(
                "Successfully queried topic route info", extra={"topic": topic}
            )
            return route_data

        except NameServerTimeoutError:
            raise
        except NameServerError:
            raise
        except Exception as e:
            logger.error(
                "Failed to query topic route info",
                extra={"topic": topic, "error": str(e)},
            )
            raise NameServerError(f"Query topic route info failed: {e}")

    def get_broker_cluster_info(self) -> BrokerClusterInfo:
        """获取 Broker 集群信息

        Returns:
            BrokerClusterInfo: Broker 集群信息

        Raises:
            NameServerError: 查询失败时抛出
            NameServerTimeoutError: 请求超时时抛出
        """
        if not self.is_connected():
            raise NameServerError("Client is not connected")

        try:
            # 创建请求命令
            command = RemotingCommandBuilder(
                code=RequestCode.GET_BROKER_CLUSTER_INFO,
                language=LanguageCode.PYTHON,
                flag=FlagType.RPC_TYPE,
            ).build()

            logger.debug("Getting broker cluster info")

            # 发送RPC请求
            response = self.remote.rpc(command, timeout=self.timeout)

            # 检查响应状态
            if not is_success_response(response):
                error_msg = (
                    response.remark
                    or f"Get broker cluster info failed with code: {response.code}"
                )
                logger.error(
                    "Get broker cluster info failed",
                    extra={"error_msg": error_msg, "response_code": response.code},
                )
                raise NameServerError(f"Get broker cluster info failed: {error_msg}")

            # 解析响应体
            if not response.body:
                raise NameServerError("Empty response body")

            cluster_info = BrokerClusterInfo.from_bytes(response.body)
            logger.debug("Successfully got broker cluster info")
            return cluster_info

        except NameServerTimeoutError:
            raise
        except NameServerError:
            raise
        except Exception as e:
            logger.error("Failed to get broker cluster info", extra={"error": str(e)})
            raise NameServerError(f"Get broker cluster info failed: {e}")

    def __enter__(self) -> "SyncNameServerClient":
        """上下文管理器入口"""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """上下文管理器出口"""
        self.disconnect()


class AsyncNameServerClient:
    """异步 NameServer 客户端

    使用 AsyncRemote 进行异步通信
    """

    def __init__(self, remote: AsyncRemote, timeout: float = 30.0) -> None:
        """初始化异步客户端

        Args:
            remote: 异步远程通信实例
            timeout: 请求超时时间，默认30秒
        """
        self.remote: AsyncRemote = remote
        self.timeout: float = timeout

    async def connect(self) -> None:
        """建立连接"""
        try:
            logger.info("Connecting to NameServer...")
            await self.remote.connect()
            logger.info("Connected to NameServer successfully")
        except Exception as e:
            logger.error("Failed to connect to NameServer", extra={"error": str(e)})
            raise NameServerError(f"Connection failed: {e}")

    async def disconnect(self) -> None:
        """断开连接"""
        try:
            logger.info("Disconnecting from NameServer...")
            await self.remote.close()
            logger.info("Disconnected from NameServer")
        except Exception as e:
            logger.error(
                "Failed to disconnect from NameServer", extra={"error": str(e)}
            )
            raise NameServerError(f"Disconnection failed: {e}")

    def is_connected(self) -> bool:
        """检查连接状态"""
        return self.remote.is_connected

    async def query_topic_route_info(self, topic: str) -> TopicRouteData:
        """查询 Topic 路由信息

        Args:
            topic: 主题名称

        Returns:
            TopicRouteData: Topic 路由信息

        Raises:
            NameServerError: 查询失败时抛出
            NameServerTimeoutError: 请求超时时抛出
        """
        if not self.is_connected():
            raise NameServerError("Client is not connected")

        try:
            # 创建请求命令
            command = RemotingRequestFactory.create_get_route_info_request(topic)

            logger.debug("Querying topic route info", extra={"topic": topic})

            # 发送异步RPC请求
            response = await self.remote.rpc(command, timeout=self.timeout)

            # 检查响应状态
            if not is_success_response(response):
                error_msg = (
                    response.remark or f"Query failed with code: {response.code}"
                )
                logger.error(
                    "Query topic route info failed",
                    extra={
                        "topic": topic,
                        "error_msg": error_msg,
                        "response_code": response.code,
                    },
                )
                raise NameServerError(f"Query topic route info failed: {error_msg}")

            # 解析响应体
            if not response.body:
                raise NameServerError("Empty response body")

            route_data = TopicRouteData.from_bytes(response.body)
            logger.debug(
                "Successfully queried topic route info", extra={"topic": topic}
            )
            return route_data

        except NameServerTimeoutError:
            raise
        except NameServerError:
            raise
        except Exception as e:
            logger.error(
                "Failed to query topic route info",
                extra={"topic": topic, "error": str(e)},
            )
            raise NameServerError(f"Query topic route info failed: {e}")

    async def get_broker_cluster_info(self) -> BrokerClusterInfo:
        """获取 Broker 集群信息

        Returns:
            BrokerClusterInfo: Broker 集群信息

        Raises:
            NameServerError: 查询失败时抛出
            NameServerTimeoutError: 请求超时时抛出
        """
        if not self.is_connected():
            raise NameServerError("Client is not connected")

        try:
            # 创建请求命令
            command = RemotingCommandBuilder(
                code=RequestCode.GET_BROKER_CLUSTER_INFO,
                language=LanguageCode.PYTHON,
                flag=FlagType.RPC_TYPE,
            ).build()

            logger.debug("Getting broker cluster info")

            # 发送异步RPC请求
            response = await self.remote.rpc(command, timeout=self.timeout)

            # 检查响应状态
            if not is_success_response(response):
                error_msg = (
                    response.remark
                    or f"Get broker cluster info failed with code: {response.code}"
                )
                logger.error(
                    "Get broker cluster info failed",
                    extra={"error_msg": error_msg, "response_code": response.code},
                )
                raise NameServerError(f"Get broker cluster info failed: {error_msg}")

            # 解析响应体
            if not response.body:
                raise NameServerError("Empty response body")

            cluster_info = BrokerClusterInfo.from_bytes(response.body)
            logger.debug("Successfully got broker cluster info")
            return cluster_info

        except NameServerTimeoutError:
            raise
        except NameServerError:
            raise
        except Exception as e:
            logger.error("Failed to get broker cluster info", extra={"error": str(e)})
            raise NameServerError(f"Get broker cluster info failed: {e}")

    async def __aenter__(self) -> "AsyncNameServerClient":
        """异步上下文管理器入口"""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """异步上下文管理器出口"""
        await self.disconnect()


def create_sync_client(
    host: str, port: int, timeout: float = 30.0, **kwargs
) -> SyncNameServerClient:
    """创建同步 NameServer 客户端

    Args:
        host: NameServer 主机地址
        port: NameServer 端口
        timeout: 请求超时时间，默认30秒
        **kwargs: 传输配置的其他参数

    Returns:
        SyncNameServerClient: 同步客户端实例
    """
    remote = create_sync_remote((host, port))
    return SyncNameServerClient(remote, timeout)


async def create_async_client(
    host: str, port: int, timeout: float = 30.0, **kwargs
) -> AsyncNameServerClient:
    """创建异步 NameServer 客户端

    Args:
        host: NameServer 主机地址
        port: NameServer 端口
        timeout: 请求超时时间，默认30秒
        **kwargs: 传输配置的其他参数

    Returns:
        AsyncNameServerClient: 异步客户端实例
    """
    remote = create_async_remote((host, port))
    return AsyncNameServerClient(remote, timeout)
