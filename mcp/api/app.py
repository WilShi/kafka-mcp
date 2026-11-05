#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
MCP服务API应用
提供REST API接口
"""

from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from .models_api import router as models_router
from .admin_api import router as admin_router
from ..utils.logger import setup_logger

# 设置日志
logger = setup_logger(__name__)

def create_app() -> FastAPI:
    """
    创建FastAPI应用
    
    Returns:
        FastAPI: FastAPI应用实例
    """
    # 创建FastAPI应用
    app = FastAPI(
        title="Kafka MCP服务",
        description="基于Kafka Admin Client开发的模型上下文协议(MCP)服务",
        version="1.0.0"
    )
    
    # 配置CORS
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    
    # 注册路由
    app.include_router(models_router, prefix="/api")
    app.include_router(admin_router, prefix="/api")
    
    # 根路由
    @app.get("/")
    async def root():
        return {"message": "欢迎使用Kafka MCP服务"}
    
    # 健康检查
    @app.get("/health")
    async def health_check():
        return {"status": "ok"}
    
    logger.info("FastAPI应用已创建")
    return app