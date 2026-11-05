#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
模型API接口
提供模型注册、版本管理等功能的REST API
"""

from typing import Dict, List, Optional, Any
from fastapi import APIRouter, Depends, HTTPException, Body, Query, Path
from pydantic import BaseModel, Field
from datetime import datetime

from ..models.model import Model, ModelVersion, ModelConfig
from ..admin.kafka_admin import KafkaAdminClient
from ..admin.topic_manager import TopicManager
from ..admin.consumer_manager import ConsumerManager
from ..admin.acl_manager import AclManager
from ..protocol.validator import MCPValidator
from ..utils.logger import get_logger
from config import KAFKA_BOOTSTRAP_SERVERS, get_kafka_client_config

# 创建路由器
router = APIRouter(tags=["模型管理"])

# 设置日志
logger = get_logger(__name__)

# 创建Kafka Admin Client
admin_client = KafkaAdminClient(KAFKA_BOOTSTRAP_SERVERS, get_kafka_client_config() or None)

# 创建主题管理器
topic_manager = TopicManager(admin_client)

# 创建消费者组管理器
consumer_manager = ConsumerManager(admin_client, topic_manager)

# 创建ACL权限管理器
acl_manager = AclManager(admin_client, topic_manager)

# 创建MCP验证器
mcp_validator = MCPValidator()

# 内存中存储模型数据（实际应用中应使用数据库）
models_db: Dict[str, Model] = {}

# 请求和响应模型
class ModelCreateRequest(BaseModel):
    """模型创建请求"""
    model_id: str = Field(..., description="模型ID")
    name: str = Field(..., description="模型名称")
    description: Optional[str] = Field(default=None, description="模型描述")
    version: str = Field(..., description="初始版本号")
    config: Optional[ModelConfig] = Field(default=None, description="主题配置")
    metadata: Optional[Dict[str, Any]] = Field(default=None, description="模型元数据")

class ModelVersionCreateRequest(BaseModel):
    """模型版本创建请求"""
    version: str = Field(..., description="版本号")
    config: Optional[ModelConfig] = Field(default=None, description="主题配置")
    metadata: Optional[Dict[str, Any]] = Field(default=None, description="版本元数据")

class ModelResponse(BaseModel):
    """模型响应"""
    model_id: str
    name: str
    description: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    versions: List[str]
    default_version: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None

class ModelVersionResponse(BaseModel):
    """模型版本响应"""
    version: str
    created_at: datetime
    topic_name: Optional[str] = None
    config: ModelConfig
    metadata: Optional[Dict[str, Any]] = None
    active: bool

class ConsumerGroupLagResponse(BaseModel):
    """消费者组滞后响应"""
    status: str
    total_lag: int
    max_lag: int
    min_lag: int
    avg_lag: int
    partitions: int

@router.post("/models", response_model=ModelResponse, status_code=201)
async def create_model(model_data: ModelCreateRequest):
    """创建新模型"""
    # 检查模型ID是否已存在
    if model_data.model_id in models_db:
        raise HTTPException(status_code=409, detail=f"模型ID '{model_data.model_id}' 已存在")
    
    # 创建模型对象
    model = Model(
        model_id=model_data.model_id,
        name=model_data.name,
        description=model_data.description,
        metadata=model_data.metadata
    )
    
    # 创建初始版本
    version = ModelVersion(
        version=model_data.version,
        config=model_data.config or ModelConfig(),
        metadata=model_data.metadata
    )
    
    # 创建Kafka主题
    topic_name = topic_manager.get_model_topic_name(model_data.model_id, model_data.version)
    version.topic_name = topic_name
    
    # 创建主题
    success = topic_manager.create_model_topic(
        model_data.model_id,
        model_data.version,
        partitions=version.config.partitions,
        replication_factor=version.config.replication_factor,
        config={
            'retention.ms': str(version.config.retention_ms),
            **(version.config.config or {})
        }
    )
    
    if not success:
        raise HTTPException(status_code=500, detail=f"创建模型主题失败")
    
    # 添加版本到模型
    model.add_version(version)
    
    # 保存模型
    models_db[model_data.model_id] = model
    
    # 返回响应
    return ModelResponse(
        model_id=model.model_id,
        name=model.name,
        description=model.description,
        created_at=model.created_at,
        updated_at=model.updated_at,
        versions=[v.version for v in model.versions],
        default_version=model.default_version,
        metadata=model.metadata
    )

@router.get("/models", response_model=List[ModelResponse])
async def list_models():
    """获取所有模型列表"""
    return [
        ModelResponse(
            model_id=model.model_id,
            name=model.name,
            description=model.description,
            created_at=model.created_at,
            updated_at=model.updated_at,
            versions=[v.version for v in model.versions],
            default_version=model.default_version,
            metadata=model.metadata
        )
        for model in models_db.values()
    ]

@router.get("/models/{model_id}", response_model=ModelResponse)
async def get_model(model_id: str = Path(..., description="模型ID")):
    """获取特定模型信息"""
    # 检查模型是否存在
    if model_id not in models_db:
        raise HTTPException(status_code=404, detail=f"模型ID '{model_id}' 不存在")
    
    model = models_db[model_id]
    
    return ModelResponse(
        model_id=model.model_id,
        name=model.name,
        description=model.description,
        created_at=model.created_at,
        updated_at=model.updated_at,
        versions=[v.version for v in model.versions],
        default_version=model.default_version,
        metadata=model.metadata
    )

@router.delete("/models/{model_id}", status_code=204)
async def delete_model(model_id: str = Path(..., description="模型ID")):
    """删除模型"""
    # 检查模型是否存在
    if model_id not in models_db:
        raise HTTPException(status_code=404, detail=f"模型ID '{model_id}' 不存在")
    
    # 删除所有相关主题
    success = topic_manager.delete_model_topic(model_id)
    
    if not success:
        raise HTTPException(status_code=500, detail=f"删除模型主题失败")
    
    # 从数据库中删除模型
    del models_db[model_id]
    
    return None

@router.post("/models/{model_id}/versions", response_model=ModelVersionResponse, status_code=201)
async def create_model_version(model_id: str = Path(..., description="模型ID"), version_data: ModelVersionCreateRequest = Body(...)):
    """创建模型新版本"""
    # 检查模型是否存在
    if model_id not in models_db:
        raise HTTPException(status_code=404, detail=f"模型ID '{model_id}' 不存在")
    
    model = models_db[model_id]
    
    # 检查版本是否已存在
    if model.get_version(version_data.version) is not None:
        raise HTTPException(status_code=409, detail=f"版本 '{version_data.version}' 已存在")
    
    # 创建版本对象
    version = ModelVersion(
        version=version_data.version,
        config=version_data.config or ModelConfig(),
        metadata=version_data.metadata
    )
    
    # 创建Kafka主题
    topic_name = topic_manager.get_model_topic_name(model_id, version_data.version)
    version.topic_name = topic_name
    
    # 创建主题
    success = topic_manager.create_model_topic(
        model_id,
        version_data.version,
        partitions=version.config.partitions,
        replication_factor=version.config.replication_factor,
        config={
            'retention.ms': str(version.config.retention_ms),
            **(version.config.config or {})
        }
    )
    
    if not success:
        raise HTTPException(status_code=500, detail=f"创建模型主题失败")
    
    # 添加版本到模型
    model.add_version(version)
    
    # 返回响应
    return ModelVersionResponse(
        version=version.version,
        created_at=version.created_at,
        topic_name=version.topic_name,
        config=version.config,
        metadata=version.metadata,
        active=version.active
    )

@router.get("/models/{model_id}/versions", response_model=List[ModelVersionResponse])
async def list_model_versions(model_id: str = Path(..., description="模型ID")):
    """获取模型所有版本"""
    # 检查模型是否存在
    if model_id not in models_db:
        raise HTTPException(status_code=404, detail=f"模型ID '{model_id}' 不存在")
    
    model = models_db[model_id]
    
    return [
        ModelVersionResponse(
            version=version.version,
            created_at=version.created_at,
            topic_name=version.topic_name,
            config=version.config,
            metadata=version.metadata,
            active=version.active
        )
        for version in model.versions
    ]

@router.get("/models/{model_id}/versions/{version}", response_model=ModelVersionResponse)
async def get_model_version(model_id: str = Path(..., description="模型ID"), version: str = Path(..., description="版本号")):
    """获取模型特定版本信息"""
    # 检查模型是否存在
    if model_id not in models_db:
        raise HTTPException(status_code=404, detail=f"模型ID '{model_id}' 不存在")
    
    model = models_db[model_id]
    
    # 检查版本是否存在
    model_version = model.get_version(version)
    if model_version is None:
        raise HTTPException(status_code=404, detail=f"版本 '{version}' 不存在")
    
    return ModelVersionResponse(
        version=model_version.version,
        created_at=model_version.created_at,
        topic_name=model_version.topic_name,
        config=model_version.config,
        metadata=model_version.metadata,
        active=model_version.active
    )

@router.delete("/models/{model_id}/versions/{version}", status_code=204)
async def delete_model_version(model_id: str = Path(..., description="模型ID"), version: str = Path(..., description="版本号")):
    """删除模型特定版本"""
    # 检查模型是否存在
    if model_id not in models_db:
        raise HTTPException(status_code=404, detail=f"模型ID '{model_id}' 不存在")
    
    model = models_db[model_id]
    
    # 检查版本是否存在
    if model.get_version(version) is None:
        raise HTTPException(status_code=404, detail=f"版本 '{version}' 不存在")
    
    # 删除主题
    success = topic_manager.delete_model_topic(model_id, version)
    
    if not success:
        raise HTTPException(status_code=500, detail=f"删除模型主题失败")
    
    # 从模型中删除版本
    model.remove_version(version)
    
    return None

@router.post("/models/{model_id}/default-version/{version}", response_model=ModelResponse)
async def set_default_version(model_id: str = Path(..., description="模型ID"), version: str = Path(..., description="版本号")):
    """设置模型默认版本"""
    # 检查模型是否存在
    if model_id not in models_db:
        raise HTTPException(status_code=404, detail=f"模型ID '{model_id}' 不存在")
    
    model = models_db[model_id]
    
    # 检查版本是否存在
    if model.get_version(version) is None:
        raise HTTPException(status_code=404, detail=f"版本 '{version}' 不存在")
    
    # 设置默认版本
    model.set_default_version(version)
    
    return ModelResponse(
        model_id=model.model_id,
        name=model.name,
        description=model.description,
        created_at=model.created_at,
        updated_at=model.updated_at,
        versions=[v.version for v in model.versions],
        default_version=model.default_version,
        metadata=model.metadata
    )

@router.get("/consumers/{group_id}/lag", response_model=Dict[str, ConsumerGroupLagResponse])
async def get_consumer_lag(group_id: str = Path(..., description="消费者组ID"), model_id: Optional[str] = Query(None, description="模型ID"), version: Optional[str] = Query(None, description="版本号")):
    """获取消费者组滞后信息"""
    # 如果指定了模型ID和版本
    if model_id and version:
        # 检查模型是否存在
        if model_id not in models_db:
            raise HTTPException(status_code=404, detail=f"模型ID '{model_id}' 不存在")
        
        model = models_db[model_id]
        
        # 检查版本是否存在
        if model.get_version(version) is None:
            raise HTTPException(status_code=404, detail=f"版本 '{version}' 不存在")
        
        # 获取滞后量摘要
        lag_summary = consumer_manager.get_consumer_lag_summary(group_id, model_id, version)
        
        return {f"{model_id}.{version}": ConsumerGroupLagResponse(**lag_summary)}
    else:
        # 获取所有滞后量
        all_lags = consumer_manager.get_all_consumer_lags(group_id)
        
        # 转换为响应格式
        result = {}
        for model_version, lag in all_lags.items():
            result[model_version] = ConsumerGroupLagResponse(**lag)
        
        return result