#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
日志工具

统一日志级别与输出格式。各模块通过 get_logger() 获取同名 logger，
应用入口通过 setup_logger() 完成一次全局配置。

日志级别与格式来自 config.LOG_LEVEL / config.LOG_FORMAT，
可通过环境变量 LOG_LEVEL、LOG_FORMAT 覆盖。
"""

import logging
import sys
from typing import Optional

from config import LOG_LEVEL, LOG_FORMAT

# 是否已完成根 logger 配置
_CONFIGURED = False

# 文本级别到 logging 级别的映射
_LEVELS = {
    'DEBUG': logging.DEBUG,
    'INFO': logging.INFO,
    'WARNING': logging.WARNING,
    'WARN': logging.WARNING,
    'ERROR': logging.ERROR,
    'CRITICAL': logging.CRITICAL,
}


def _resolve_level(level):
    """把字符串级别转换为 logging 级别，无法识别时回退到 INFO。"""
    if isinstance(level, int):
        return level
    if not level:
        return logging.INFO
    return _LEVELS.get(str(level).upper(), logging.INFO)


def setup_logger(name=None, level=None):
    """
    初始化日志配置并返回 logger

    Args:
        name: logger 名称，通常传 __name__
        level: 日志级别，默认使用 config.LOG_LEVEL

    Returns:
        logging.Logger: 配置好处理器与格式的 logger
    """
    global _CONFIGURED

    if not _CONFIGURED:
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(logging.Formatter(LOG_FORMAT))

        root_logger = logging.getLogger()
        # 避免重复添加处理器（例如多次调用 setup_logger）
        for existing in list(root_logger.handlers):
            root_logger.removeHandler(existing)
        root_logger.addHandler(handler)
        root_logger.setLevel(_resolve_level(level or LOG_LEVEL))
        _CONFIGURED = True

    logger = logging.getLogger(name)
    logger.setLevel(_resolve_level(level or LOG_LEVEL))
    return logger


def get_logger(name=None):
    """
    获取 logger

    首次调用时会完成全局日志配置，因此模块可以独立使用本函数。

    Args:
        name: logger 名称，通常传 __name__

    Returns:
        logging.Logger: logger 实例
    """
    if not _CONFIGURED:
        return setup_logger(name)
    return logging.getLogger(name)
