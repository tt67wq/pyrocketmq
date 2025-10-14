"""
测试Remote类的processors机制
"""

import threading
from unittest.mock import patch

import pytest

from pyrocketmq.model import (
    FlagType,
    LanguageCode,
    RemotingCommand,
    RequestCode,
    ResponseCode,
)
from pyrocketmq.remote.config import RemoteConfig
from pyrocketmq.remote.sync_remote import Remote
from pyrocketmq.transport.config import TransportConfig


class TestRemoteProcessors:
    """测试Remote类的processors机制"""

    @pytest.fixture
    def transport_config(self):
        """传输配置"""
        return TransportConfig(host="localhost", port=9876)

    @pytest.fixture
    def remote_config(self):
        """远程通信配置"""
        return RemoteConfig(rpc_timeout=1.0)

    @pytest.fixture
    def remote(self, transport_config, remote_config):
        """Remote实例"""
        return Remote(transport_config, remote_config)

    def test_register_request_processor(self, remote):
        """测试注册请求处理器"""

        def test_processor(request, addr):
            return RemotingCommand(code=ResponseCode.SUCCESS)

        # 注册处理器
        remote.register_request_processor(
            RequestCode.GET_BROKER_CLUSTER_INFO, test_processor
        )

        # 验证处理器已注册
        with remote._processors_lock:
            assert RequestCode.GET_BROKER_CLUSTER_INFO in remote._processors
            assert (
                remote._processors[RequestCode.GET_BROKER_CLUSTER_INFO]
                == test_processor
            )

    def test_unregister_request_processor(self, remote):
        """测试取消注册请求处理器"""

        def test_processor(request, addr):
            return RemotingCommand(code=ResponseCode.SUCCESS)

        # 注册处理器
        remote.register_request_processor(
            RequestCode.GET_BROKER_CLUSTER_INFO, test_processor
        )

        # 验证处理器已注册
        with remote._processors_lock:
            assert RequestCode.GET_BROKER_CLUSTER_INFO in remote._processors

        # 取消注册
        result = remote.unregister_request_processor(
            RequestCode.GET_BROKER_CLUSTER_INFO
        )
        assert result is True

        # 验证处理器已移除
        with remote._processors_lock:
            assert RequestCode.GET_BROKER_CLUSTER_INFO not in remote._processors

        # 再次取消注册应该返回False
        result = remote.unregister_request_processor(
            RequestCode.GET_BROKER_CLUSTER_INFO
        )
        assert result is False

    def test_handle_request_message_with_processor(self, remote):
        """测试处理请求消息（有对应的处理器）"""

        # 创建测试处理器
        def test_processor(request, addr):
            assert request.code == RequestCode.HEART_BEAT
            assert addr == ("localhost", 9876)

            # 创建响应
            response = RemotingCommand(
                code=ResponseCode.SUCCESS,
                language=LanguageCode.PYTHON,
                remark="Heartbeat received",
            )
            return response

        # 注册处理器
        remote.register_request_processor(
            RequestCode.HEART_BEAT, test_processor
        )

        # 模拟发送响应
        with patch.object(remote.transport, "output") as mock_output:
            # 创建请求消息
            request = RemotingCommand(
                code=RequestCode.HEART_BEAT,
                language=LanguageCode.JAVA,
                flag=FlagType.RPC_TYPE,
                opaque=123,
                remark="Heartbeat from broker",
            )

            # 处理请求
            remote._handle_request_message(request)

            # 验证响应已发送
            mock_output.assert_called_once()

            # 验证发送的数据
            sent_data = mock_output.call_args[0][0]
            # 反序列化验证响应内容
            response = remote._serializer.deserialize(sent_data)

            assert response.code == ResponseCode.SUCCESS
            assert response.opaque == 123
            assert response.is_response is True
            assert response.remark == "Heartbeat received"

    def test_handle_request_message_without_processor(self, remote):
        """测试处理请求消息（没有对应的处理器）"""
        # 创建请求消息（没有注册处理器）
        request = RemotingCommand(
            code=RequestCode.HEART_BEAT,
            language=LanguageCode.JAVA,
            flag=FlagType.RPC_TYPE,
            opaque=123,
        )

        # 处理请求（应该只记录警告，不发送响应）
        with patch.object(remote.transport, "output") as mock_output:
            remote._handle_request_message(request)

            # 验证没有发送响应
            mock_output.assert_not_called()

    def test_handle_request_message_processor_returns_none(self, remote):
        """测试处理器返回None的情况"""

        # 创建测试处理器（返回None）
        def test_processor(request, addr):
            # 不返回响应，只记录处理
            return None

        # 注册处理器
        remote.register_request_processor(
            RequestCode.HEART_BEAT, test_processor
        )

        # 创建请求消息
        request = RemotingCommand(
            code=RequestCode.HEART_BEAT,
            language=LanguageCode.JAVA,
            flag=FlagType.RPC_TYPE,
            opaque=123,
        )

        # 处理请求（应该不发送响应）
        with patch.object(remote.transport, "output") as mock_output:
            remote._handle_request_message(request)

            # 验证没有发送响应
            mock_output.assert_not_called()

    def test_handle_request_message_processor_exception(self, remote):
        """测试处理器抛出异常的情况"""

        # 创建测试处理器（抛出异常）
        def test_processor(request, addr):
            raise ValueError("Test error")

        # 注册处理器
        remote.register_request_processor(
            RequestCode.HEART_BEAT, test_processor
        )

        # 创建请求消息
        request = RemotingCommand(
            code=RequestCode.HEART_BEAT,
            language=LanguageCode.JAVA,
            flag=FlagType.RPC_TYPE,
            opaque=123,
        )

        # 处理请求（应该捕获异常并记录错误）
        with patch.object(remote.transport, "output") as mock_output:
            remote._handle_request_message(request)

            # 验证没有发送响应
            mock_output.assert_not_called()

    def test_handle_response_message_still_works(self, remote):
        """测试响应消息处理仍然正常工作"""
        # 创建响应消息
        response = RemotingCommand(
            code=ResponseCode.SUCCESS,
            language=LanguageCode.JAVA,
            flag=FlagType.RESPONSE_TYPE,
            opaque=456,
        )

        # 注册等待者
        event = threading.Event()
        remote._register_waiter(456, event)

        # 处理响应消息
        remote._handle_response_message(response)

        # 验证等待者收到响应
        assert event.is_set()
        saved_response = remote._get_waiter_response(456)
        assert saved_response is not None
        assert saved_response.code == ResponseCode.SUCCESS
        assert saved_response.opaque == 456

    def test_handle_response_distinguishes_request_and_response(self, remote):
        """测试_handle_response能够区分请求和响应消息"""

        # 创建请求处理器
        def test_processor(request, addr):
            return RemotingCommand(code=ResponseCode.SUCCESS)

        remote.register_request_processor(
            RequestCode.HEART_BEAT, test_processor
        )

        # 创建请求消息
        request = RemotingCommand(
            code=RequestCode.HEART_BEAT,
            language=LanguageCode.JAVA,
            flag=FlagType.RPC_TYPE,
            opaque=123,
        )

        # 创建响应消息
        response = RemotingCommand(
            code=ResponseCode.SUCCESS,
            language=LanguageCode.JAVA,
            flag=FlagType.RESPONSE_TYPE,
            opaque=456,
        )

        with (
            patch.object(
                remote, "_handle_request_message"
            ) as mock_handle_request,
            patch.object(
                remote, "_handle_response_message"
            ) as mock_handle_response,
        ):
            # 处理请求消息
            remote._handle_response(request)
            mock_handle_request.assert_called_once_with(request)

            # 处理响应消息
            remote._handle_response(response)
            mock_handle_response.assert_called_once_with(response)

    def test_send_processor_response_sets_correct_fields(self, remote):
        """测试发送处理器响应时设置正确的字段"""
        request = RemotingCommand(
            code=RequestCode.HEART_BEAT,
            language=LanguageCode.JAVA,
            flag=FlagType.RPC_TYPE,
            opaque=789,
        )

        response = RemotingCommand(
            code=ResponseCode.SUCCESS,
            language=LanguageCode.PYTHON,
            remark="Processed",
        )
        # 注意：这里不设置opaque和flag，应该由_send_processor_response设置

        with patch.object(remote.transport, "output") as mock_output:
            remote._send_processor_response(request, response)

            # 验证响应已发送
            mock_output.assert_called_once()

            # 验证响应字段已正确设置
            sent_data = mock_output.call_args[0][0]
            sent_response = remote._serializer.deserialize(sent_data)

            assert sent_response.opaque == 789  # 应该继承请求的opaque
            assert sent_response.is_response is True  # 应该设置为响应
            assert sent_response.code == ResponseCode.SUCCESS
            assert sent_response.remark == "Processed"
