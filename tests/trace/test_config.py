"""Unit tests for trace config module."""

import unittest

from pyrocketmq.trace.config import AccessChannel, TraceConfig


class TestAccessChannel(unittest.TestCase):
    """Test cases for AccessChannel enum."""

    def test_access_channel_values(self):
        """Test AccessChannel enum values."""
        self.assertEqual(AccessChannel.LOCAL.value, 0)
        self.assertEqual(AccessChannel.CLOUD.value, 1)

    def test_access_channel_names(self):
        """Test AccessChannel enum names."""
        self.assertEqual(AccessChannel.LOCAL.name, "LOCAL")
        self.assertEqual(AccessChannel.CLOUD.name, "CLOUD")


class TestTraceConfig(unittest.TestCase):
    """Test cases for TraceConfig class."""

    def test_create_trace_config_success(self):
        """Test successful creation of TraceConfig."""
        config = TraceConfig(
            trace_topic="RMQ_SYS_TRACE_TOPIC",
            group_name="trace_producer_group",
            access=AccessChannel.LOCAL,
            namesrv_addrs=["127.0.0.1:9876"],
        )

        self.assertEqual(config.trace_topic, "RMQ_SYS_TRACE_TOPIC")
        self.assertEqual(config.group_name, "trace_producer_group")
        self.assertEqual(config.access, AccessChannel.LOCAL)
        self.assertEqual(config.namesrv_addrs, ["127.0.0.1:9876"])

    def test_create_trace_config_with_cloud_access(self):
        """Test creating TraceConfig with cloud access."""
        config = TraceConfig(
            trace_topic="RMQ_SYS_TRACE_TOPIC",
            group_name="trace_consumer_group",
            access=AccessChannel.CLOUD,
            namesrv_addrs=["mq.cn-north-1.myhuaweicloud.com:8081"],
        )

        self.assertEqual(config.access, AccessChannel.CLOUD)

    def test_create_local_config_classmethod(self):
        """Test TraceConfig.create_local_config class method."""
        config = TraceConfig.create_local_config(
            trace_topic="RMQ_SYS_TRACE_TOPIC",
            group_name="trace_group",
            namesrv_addrs=["127.0.0.1:9876", "127.0.0.1:9877"],
        )

        self.assertEqual(config.trace_topic, "RMQ_SYS_TRACE_TOPIC")
        self.assertEqual(config.group_name, "trace_group")
        self.assertEqual(config.access, AccessChannel.LOCAL)
        self.assertEqual(config.namesrv_addrs, ["127.0.0.1:9876", "127.0.0.1:9877"])

    def test_create_cloud_config_classmethod(self):
        """Test TraceConfig.create_cloud_config class method."""
        config = TraceConfig.create_cloud_config(
            trace_topic="RMQ_SYS_TRACE_TOPIC",
            group_name="trace_group",
            namesrv_addrs=["mq.cloud.com:8081"],
        )

        self.assertEqual(config.trace_topic, "RMQ_SYS_TRACE_TOPIC")
        self.assertEqual(config.group_name, "trace_group")
        self.assertEqual(config.access, AccessChannel.CLOUD)
        self.assertEqual(config.namesrv_addrs, ["mq.cloud.com:8081"])

    def test_validation_empty_trace_topic(self):
        """Test validation with empty trace_topic."""
        with self.assertRaises(ValueError) as ctx:
            TraceConfig(
                trace_topic="",
                group_name="trace_group",
                access=AccessChannel.LOCAL,
                namesrv_addrs=["127.0.0.1:9876"],
            )
        self.assertIn("trace_topic cannot be empty", str(ctx.exception))

    def test_validation_empty_group_name(self):
        """Test validation with empty group_name."""
        with self.assertRaises(ValueError) as ctx:
            TraceConfig(
                trace_topic="RMQ_SYS_TRACE_TOPIC",
                group_name="",
                access=AccessChannel.LOCAL,
                namesrv_addrs=["127.0.0.1:9876"],
            )
        self.assertIn("group_name cannot be empty", str(ctx.exception))

    def test_validation_empty_namesrv_addrs(self):
        """Test validation with empty namesrv_addrs."""
        with self.assertRaises(ValueError) as ctx:
            TraceConfig(
                trace_topic="RMQ_SYS_TRACE_TOPIC",
                group_name="trace_group",
                access=AccessChannel.LOCAL,
                namesrv_addrs=[],
            )
        self.assertIn("namesrv_addrs cannot be empty", str(ctx.exception))

    def test_validation_none_values(self):
        """Test validation with None values."""
        with self.assertRaises(ValueError) as ctx:
            TraceConfig(
                trace_topic=None,  # type: ignore
                group_name="trace_group",
                access=AccessChannel.LOCAL,
                namesrv_addrs=["127.0.0.1:9876"],
            )
        self.assertIn("trace_topic cannot be empty", str(ctx.exception))

    def test_multiple_namesrv_addrs(self):
        """Test TraceConfig with multiple nameserver addresses."""
        addrs = ["192.168.1.10:9876", "192.168.1.11:9876", "192.168.1.12:9876"]
        config = TraceConfig(
            trace_topic="RMQ_SYS_TRACE_TOPIC",
            group_name="trace_group",
            access=AccessChannel.LOCAL,
            namesrv_addrs=addrs,
        )

        self.assertEqual(len(config.namesrv_addrs), 3)
        self.assertListEqual(config.namesrv_addrs, addrs)


if __name__ == "__main__":
    unittest.main()
