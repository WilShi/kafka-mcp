#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Kafka MCP服务主应用入口
"""

import uvicorn
from mcp.api.app import create_app
from config import API_HOST, API_PORT, API_DEBUG
from mcp.utils.logger import setup_logger

# 设置日志
logger = setup_logger(__name__)

# 创建FastAPI应用
app = create_app()

if __name__ == "__main__":
    logger.info(f"启动Kafka MCP服务，监听地址: {API_HOST}:{API_PORT}")
    uvicorn.run("app:app", host=API_HOST, port=API_PORT, reload=API_DEBUG)