#! /usr/bin/env python3
# -*- coding: utf-8 -*-
from pyrocketmq import logging
from pyrocketmq.logging import LoggingConfig


def main():
    logging.setup_logging(
        LoggingConfig(level="INFO", json_output=False, file_path="demo.log")
    )
    logger = logging.get_logger(__name__)

    logger.info("Starting application", extra={"user": "John Doe"})


if __name__ == "__main__":
    main()
