"""Unit tests for trace module."""

import unittest

from pyrocketmq.model.trace import (
    CONTENT_SPLITTER,
    FIELD_SPLITTER,
    MessageType,
    TraceBean,
    TraceContext,
    TraceTransferBean,
    TraceType,
)


class TestTraceContextMarshal2Bean(unittest.TestCase):
    """Test cases for TraceContext.marshal2bean method."""

    def test_pub_type_marshal(self):
        """Test marshaling for PUB trace type."""
        # Create trace bean
        bean = TraceBean(
            topic="test-topic",
            msg_id="msg-id-123",
            offset_msg_id="offset-msg-456",
            tags="tag1,tag2",
            keys="key1,key2",
            store_host="127.0.0.1:10911",
            client_host="127.0.0.1:12345",
            store_time=1234567890,
            retry_times=0,
            body_length=100,
            msg_type=MessageType.NORMAL,
        )

        # Create trace context
        context = TraceContext(
            trace_type=TraceType.PUB,
            timestamp=1234567890,
            region_id="region1",
            region_name="test-region",
            group_name="test-group",
            cost_time=50,
            is_success=True,
            request_id="req-123",
            context_code=0,
            trace_beans=[bean],
        )

        # Marshal to transfer bean
        transfer_bean = context.marshal2bean()

        # Verify transfer bean structure
        self.assertIsInstance(transfer_bean, TraceTransferBean)
        self.assertEqual(transfer_bean.trans_key, ["msg-id-123", "key1,key2"])
        self.assertTrue(transfer_bean.trans_data.endswith(FIELD_SPLITTER))

        # Verify trans_data content
        parts = transfer_bean.trans_data.rstrip(FIELD_SPLITTER).split(CONTENT_SPLITTER)
        self.assertEqual(len(parts), 15)
        self.assertEqual(parts[0], "Pub")  # trace_type
        self.assertEqual(parts[1], "1234567890")  # timestamp
        self.assertEqual(parts[2], "region1")  # region_id
        self.assertEqual(parts[3], "test-group")  # group_name
        self.assertEqual(parts[4], "test-topic")  # topic
        self.assertEqual(parts[5], "msg-id-123")  # msg_id
        self.assertEqual(parts[6], "tag1,tag2")  # tags
        self.assertEqual(parts[7], "key1,key2")  # keys
        self.assertEqual(parts[8], "127.0.0.1:10911")  # store_host
        self.assertEqual(parts[9], "100")  # body_length
        self.assertEqual(parts[10], "50")  # cost_time
        self.assertEqual(parts[11], "0")  # msg_type
        self.assertEqual(parts[12], "offset-msg-456")  # offset_msg_id
        self.assertEqual(parts[13], "true")  # is_success
        self.assertEqual(parts[14], "127.0.0.1:12345")  # client_host

    def test_pub_type_with_percent_separator(self):
        """Test marshaling for PUB type with % separator in group name and topic."""
        bean = TraceBean(
            topic="namespace%test-topic",
            msg_id="msg-123",
            offset_msg_id="",
            tags="",
            keys="",
            store_host="",
            client_host="",
            store_time=0,
            retry_times=0,
            body_length=0,
            msg_type=MessageType.NORMAL,
        )

        context = TraceContext(
            trace_type=TraceType.PUB,
            timestamp=1234567890,
            region_id="",
            region_name="",
            group_name="namespace%test-group",
            cost_time=0,
            is_success=False,
            request_id="",
            context_code=0,
            trace_beans=[bean],
        )

        transfer_bean = context.marshal2bean()
        parts = transfer_bean.trans_data.rstrip(FIELD_SPLITTER).split(CONTENT_SPLITTER)

        self.assertEqual(parts[3], "test-group")  # group_name after %
        self.assertEqual(parts[4], "test-topic")  # topic after %
        self.assertEqual(parts[13], "false")  # is_success

    def test_sub_before_type_marshal(self):
        """Test marshaling for SUB_BEFORE trace type."""
        beans = [
            TraceBean(
                topic="test-topic",
                msg_id="msg-1",
                offset_msg_id="",
                tags="",
                keys="key1",
                store_host="",
                client_host="client-1",
                store_time=0,
                retry_times=0,
                body_length=0,
                msg_type=MessageType.NORMAL,
            ),
            TraceBean(
                topic="test-topic",
                msg_id="msg-2",
                offset_msg_id="",
                tags="",
                keys="key2",
                store_host="",
                client_host="client-2",
                store_time=0,
                retry_times=1,
                body_length=0,
                msg_type=MessageType.NORMAL,
            ),
        ]

        context = TraceContext(
            trace_type=TraceType.SUB_BEFORE,
            timestamp=1234567890,
            region_id="region1",
            region_name="",
            group_name="test-group",
            cost_time=0,
            is_success=False,
            request_id="req-123",
            context_code=0,
            trace_beans=beans,
        )

        transfer_bean = context.marshal2bean()

        # Verify transfer bean structure
        self.assertEqual(transfer_bean.trans_key, ["msg-1", "key1", "msg-2", "key2"])
        self.assertTrue(transfer_bean.trans_data.endswith(FIELD_SPLITTER))

        # Verify trans_data content
        parts = transfer_bean.trans_data.rstrip(FIELD_SPLITTER).split(CONTENT_SPLITTER)
        # Should have 9 fields per bean * 2 beans = 18 fields
        self.assertEqual(len(parts), 18)

        # Check first bean fields
        self.assertEqual(parts[0], "SubBefore")
        self.assertEqual(parts[1], "1234567890")
        self.assertEqual(parts[2], "region1")
        self.assertEqual(parts[3], "test-group")
        self.assertEqual(parts[4], "req-123")
        self.assertEqual(parts[5], "msg-1")
        self.assertEqual(parts[6], "0")  # retry_times
        self.assertEqual(parts[7], "key1")
        self.assertEqual(parts[8], "client-1")

        # Check second bean fields
        self.assertEqual(parts[9], "SubBefore")
        self.assertEqual(parts[10], "1234567890")
        self.assertEqual(parts[11], "region1")
        self.assertEqual(parts[12], "test-group")
        self.assertEqual(parts[13], "req-123")
        self.assertEqual(parts[14], "msg-2")
        self.assertEqual(parts[15], "1")  # retry_times
        self.assertEqual(parts[16], "key2")
        self.assertEqual(parts[17], "client-2")

    def test_sub_after_type_marshal(self):
        """Test marshaling for SUB_AFTER trace type."""
        beans = [
            TraceBean(
                topic="test-topic",
                msg_id="msg-1",
                offset_msg_id="",
                tags="",
                keys="key1,key2",
                store_host="",
                client_host="",
                store_time=0,
                retry_times=0,
                body_length=0,
                msg_type=MessageType.NORMAL,
            ),
        ]

        context = TraceContext(
            trace_type=TraceType.SUB_AFTER,
            timestamp=1234567890,
            region_id="",
            region_name="",
            group_name="test-group",
            cost_time=100,
            is_success=True,
            request_id="req-123",
            context_code=200,
            trace_beans=beans,
        )

        transfer_bean = context.marshal2bean()

        # Verify transfer bean structure
        self.assertEqual(transfer_bean.trans_key, ["msg-1", "key1,key2"])
        self.assertTrue(transfer_bean.trans_data.endswith(FIELD_SPLITTER))

        # Verify trans_data content
        parts = transfer_bean.trans_data.rstrip(FIELD_SPLITTER).split(CONTENT_SPLITTER)
        self.assertEqual(len(parts), 9)
        self.assertEqual(parts[0], "SubAfter")
        self.assertEqual(parts[1], "req-123")
        self.assertEqual(parts[2], "msg-1")
        self.assertEqual(parts[3], "100")  # cost_time
        self.assertEqual(parts[4], "true")  # is_success
        self.assertEqual(parts[5], "key1,key2")
        self.assertEqual(parts[6], "200")  # context_code
        self.assertEqual(parts[7], "1234567890")  # timestamp
        self.assertEqual(parts[8], "test-group")

    def test_empty_optional_fields(self):
        """Test marshaling with empty optional fields."""
        bean = TraceBean(
            topic="",
            msg_id="msg-123",
            offset_msg_id="",
            tags="",
            keys="",
            store_host="",
            client_host="",
            store_time=0,
            retry_times=0,
            body_length=0,
            msg_type=MessageType.NORMAL,
        )

        context = TraceContext(
            trace_type=TraceType.PUB,
            timestamp=1234567890,
            region_id="",
            region_name="",
            group_name="test-group",
            cost_time=0,
            is_success=True,
            request_id="",
            context_code=0,
            trace_beans=[bean],
        )

        transfer_bean = context.marshal2bean()
        parts = transfer_bean.trans_data.rstrip(FIELD_SPLITTER).split(CONTENT_SPLITTER)

        # Verify empty fields are preserved as empty strings
        self.assertEqual(parts[2], "")  # region_id
        self.assertEqual(parts[4], "")  # topic
        self.assertEqual(parts[6], "")  # tags
        self.assertEqual(parts[7], "")  # keys
        self.assertEqual(parts[8], "")  # store_host
        self.assertEqual(parts[12], "")  # offset_msg_id
        self.assertEqual(parts[14], "")  # client_host

    def test_message_type_transact(self):
        """Test marshaling with TRANSACT message type."""
        bean = TraceBean(
            topic="test-topic",
            msg_id="msg-123",
            offset_msg_id="",
            tags="",
            keys="",
            store_host="",
            client_host="",
            store_time=0,
            retry_times=0,
            body_length=0,
            msg_type=MessageType.TRANSACT,
        )

        context = TraceContext(
            trace_type=TraceType.PUB,
            timestamp=1234567890,
            region_id="",
            region_name="",
            group_name="test-group",
            cost_time=0,
            is_success=True,
            request_id="",
            context_code=0,
            trace_beans=[bean],
        )

        transfer_bean = context.marshal2bean()
        parts = transfer_bean.trans_data.rstrip(FIELD_SPLITTER).split(CONTENT_SPLITTER)
        self.assertEqual(parts[11], "1")  # TRANSACT message type value

    def test_multiple_beans_sub_after(self):
        """Test SUB_AFTER with multiple beans."""
        beans = [
            TraceBean(
                topic="test-topic",
                msg_id="msg-1",
                offset_msg_id="",
                tags="",
                keys="key1",
                store_host="",
                client_host="",
                store_time=0,
                retry_times=0,
                body_length=0,
                msg_type=MessageType.NORMAL,
            ),
            TraceBean(
                topic="test-topic",
                msg_id="msg-2",
                offset_msg_id="",
                tags="",
                keys="key2",
                store_host="",
                client_host="",
                store_time=0,
                retry_times=0,
                body_length=0,
                msg_type=MessageType.NORMAL,
            ),
        ]

        context = TraceContext(
            trace_type=TraceType.SUB_AFTER,
            timestamp=1234567890,
            region_id="",
            region_name="",
            group_name="test-group",
            cost_time=100,
            is_success=True,
            request_id="req-123",
            context_code=200,
            trace_beans=beans,
        )

        transfer_bean = context.marshal2bean()

        # Verify trans_key includes all msg_ids and keys
        self.assertEqual(transfer_bean.trans_key, ["msg-1", "key1", "msg-2", "key2"])

        # Verify trans_data has 9 fields per bean = 18 fields
        parts = transfer_bean.trans_data.rstrip(FIELD_SPLITTER).split(CONTENT_SPLITTER)
        self.assertEqual(len(parts), 18)


if __name__ == "__main__":
    unittest.main()
