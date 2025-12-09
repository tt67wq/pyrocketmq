"""Unit tests for max_msg_size feature in trace config module."""

import unittest

from pyrocketmq.trace.config import AccessChannel, TraceConfig


class TestTraceConfigMaxMsgSize(unittest.TestCase):
    """Test cases for max_msg_size in TraceConfig class."""

    def test_default_max_msg_size(self):
        """Test default max_msg_size value."""
        config = TraceConfig(
            trace_topic="RMQ_SYS_TRACE_TOPIC",
            group_name="trace_group",
            access=AccessChannel.LOCAL,
            namesrv_addrs=["127.0.0.1:9876"],
        )
        self.assertEqual(config.max_msg_size, 4 * 1024 * 1024)  # 4MB

    def test_custom_max_msg_size(self):
        """Test custom max_msg_size value."""
        custom_size = 2 * 1024 * 1024  # 2MB
        config = TraceConfig(
            trace_topic="RMQ_SYS_TRACE_TOPIC",
            group_name="trace_group",
            access=AccessChannel.LOCAL,
            namesrv_addrs=["127.0.0.1:9876"],
            max_msg_size=custom_size,
        )
        self.assertEqual(config.max_msg_size, custom_size)

    def test_local_config_with_custom_max_msg_size(self):
        """Test create_local_config with custom max_msg_size."""
        custom_size = 8 * 1024 * 1024  # 8MB
        config = TraceConfig.create_local_config(
            trace_topic="RMQ_SYS_TRACE_TOPIC",
            group_name="trace_group",
            namesrv_addrs=["127.0.0.1:9876"],
            max_batch_size=10,
            max_msg_size=custom_size,
        )
        self.assertEqual(config.max_batch_size, 10)
        self.assertEqual(config.max_msg_size, custom_size)

    def test_cloud_config_with_custom_max_msg_size(self):
        """Test create_cloud_config with custom max_msg_size."""
        custom_size = 6 * 1024 * 1024  # 6MB
        config = TraceConfig.create_cloud_config(
            trace_topic="RMQ_SYS_TRACE_TOPIC",
            group_name="trace_group",
            namesrv_addrs=["mq.cloud.com:8081"],
            max_batch_size=15,
            max_msg_size=custom_size,
        )
        self.assertEqual(config.max_batch_size, 15)
        self.assertEqual(config.max_msg_size, custom_size)

    def test_validation_invalid_max_msg_size_zero(self):
        """Test validation with zero max_msg_size."""
        with self.assertRaises(ValueError) as ctx:
            TraceConfig(
                trace_topic="RMQ_SYS_TRACE_TOPIC",
                group_name="trace_group",
                access=AccessChannel.LOCAL,
                namesrv_addrs=["127.0.0.1:9876"],
                max_msg_size=0,
            )
        self.assertIn("max_msg_size must be greater than 0", str(ctx.exception))

    def test_validation_invalid_max_msg_size_negative(self):
        """Test validation with negative max_msg_size."""
        with self.assertRaises(ValueError) as ctx:
            TraceConfig(
                trace_topic="RMQ_SYS_TRACE_TOPIC",
                group_name="trace_group",
                access=AccessChannel.LOCAL,
                namesrv_addrs=["127.0.0.1:9876"],
                max_msg_size=-1,
            )
        self.assertIn("max_msg_size must be greater than 0", str(ctx.exception))

    def test_large_max_msg_size(self):
        """Test with large max_msg_size value."""
        large_size = 100 * 1024 * 1024  # 100MB
        config = TraceConfig(
            trace_topic="RMQ_SYS_TRACE_TOPIC",
            group_name="trace_group",
            access=AccessChannel.LOCAL,
            namesrv_addrs=["127.0.0.1:9876"],
            max_msg_size=large_size,
        )
        self.assertEqual(config.max_msg_size, large_size)

    def test_all_parameters_combined(self):
        """Test TraceConfig with all custom parameters."""
        config = TraceConfig(
            trace_topic="CustomTraceTopic",
            group_name="CustomTraceGroup",
            access=AccessChannel.CLOUD,
            namesrv_addrs=["mq1.cloud.com:8081", "mq2.cloud.com:8081"],
            max_batch_size=50,
            max_msg_size=10 * 1024 * 1024,  # 10MB
        )

        self.assertEqual(config.trace_topic, "CustomTraceTopic")
        self.assertEqual(config.group_name, "CustomTraceGroup")
        self.assertEqual(config.access, AccessChannel.CLOUD)
        self.assertEqual(len(config.namesrv_addrs), 2)
        self.assertEqual(config.max_batch_size, 50)
        self.assertEqual(config.max_msg_size, 10 * 1024 * 1024)


if __name__ == "__main__":
    unittest.main()
