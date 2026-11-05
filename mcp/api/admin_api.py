#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
管理API接口
提供Kafka集群管理、主题管理和消费者组管理等功能的REST API
"""

from typing import Dict, List, Optional, Any
from fastapi import APIRouter, Depends, HTTPException, Body, Query, Path
from pydantic import BaseModel, Field

from ..admin.kafka_admin import KafkaAdminClient
from ..admin.topic_manager import TopicManager
from ..admin.consumer_manager import ConsumerManager
from ..admin.acl_manager import AclManager
from ..utils.logger import get_logger
from config import KAFKA_BOOTSTRAP_SERVERS, get_kafka_client_config

# 创建路由器
router = APIRouter(tags=["管理接口"])

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

# 请求和响应模型
class TopicCreateRequest(BaseModel):
    """主题创建请求"""
    name: str = Field(..., description="主题名称")
    num_partitions: int = Field(default=3, description="分区数")
    replication_factor: int = Field(default=3, description="副本因子")
    config: Optional[Dict[str, str]] = Field(default=None, description="主题配置")

class TopicConfigUpdateRequest(BaseModel):
    """主题配置更新请求"""
    config: Dict[str, str] = Field(..., description="要更新的配置")

class AclCreateRequest(BaseModel):
    """ACL创建请求"""
    principal: str = Field(..., description="用户主体，格式为'User:username'或'Group:groupname'")
    operations: List[str] = Field(..., description="操作列表，可包含'READ'、'WRITE'、'CREATE'、'DELETE'、'ALTER'等")
    allow: bool = Field(default=True, description="是否允许操作")

class ConsumerGroupOffsetResetRequest(BaseModel):
    """消费者组偏移量重置请求"""
    offset: str = Field(default="earliest", description="偏移量位置，可选值为'earliest'、'latest'或特定数值")

@router.get("/admin/cluster/metadata")
async def get_cluster_metadata():
    """获取Kafka集群元数据"""
    metadata = admin_client.get_cluster_metadata()
    return metadata

@router.get("/admin/topics")
async def list_topics():
    """获取所有主题列表"""
    topics = admin_client.list_topics()
    return topics

@router.post("/admin/topics", status_code=201)
async def create_topic(topic_data: TopicCreateRequest):
    """创建主题"""
    topic_configs = [{
        'name': topic_data.name,
        'num_partitions': topic_data.num_partitions,
        'replication_factor': topic_data.replication_factor,
        'config': topic_data.config or {}
    }]
    
    result = admin_client.create_topics(topic_configs)
    success = result.get(topic_data.name, False)
    
    if not success:
        raise HTTPException(status_code=500, detail=f"创建主题 {topic_data.name} 失败")
    
    return {"name": topic_data.name, "success": True}

@router.delete("/admin/topics/{topic_name}", status_code=204)
async def delete_topic(topic_name: str = Path(..., description="主题名称")):
    """删除主题"""
    result = admin_client.delete_topics([topic_name])
    success = result.get(topic_name, False)
    
    if not success:
        raise HTTPException(status_code=500, detail=f"删除主题 {topic_name} 失败")
    
    return None

@router.get("/admin/topics/{topic_name}/config")
async def get_topic_config(topic_name: str = Path(..., description="主题名称")):
    """获取主题配置"""
    config = admin_client.get_topic_config(topic_name)
    
    if not config:
        raise HTTPException(status_code=404, detail=f"主题 {topic_name} 不存在或无法获取配置")
    
    return config

@router.put("/admin/topics/{topic_name}/config")
async def update_topic_config(topic_name: str = Path(..., description="主题名称"), config_data: TopicConfigUpdateRequest = Body(...)):
    """更新主题配置"""
    success = admin_client.update_topic_config(topic_name, config_data.config)
    
    if not success:
        raise HTTPException(status_code=500, detail=f"更新主题 {topic_name} 配置失败")
    
    return {"name": topic_name, "success": True}

@router.get("/admin/consumer-groups")
async def list_consumer_groups(prefix: Optional[str] = Query(None, description="消费者组前缀过滤")):
    """获取消费者组列表"""
    groups = consumer_manager.list_consumer_groups(prefix)
    return groups

@router.get("/admin/consumer-groups/{group_id}/offsets")
async def get_consumer_group_offsets(group_id: str = Path(..., description="消费者组ID"), topic: Optional[str] = Query(None, description="主题名称")):
    """获取消费者组偏移量"""
    topics = [topic] if topic else None
    offsets = admin_client.get_consumer_group_offsets(group_id, topics)
    
    if not offsets:
        raise HTTPException(status_code=404, detail=f"消费者组 {group_id} 不存在或未订阅任何主题")
    
    return offsets

@router.post("/admin/consumer-groups/{group_id}/reset")
async def reset_consumer_group_offsets(
    group_id: str = Path(..., description="消费者组ID"), 
    model_id: str = Query(..., description="模型ID"), 
    version: str = Query(..., description="版本号"),
    reset_data: ConsumerGroupOffsetResetRequest = Body(...)
):
    """重置消费者组偏移量"""
    success = consumer_manager.reset_consumer_offsets(group_id, model_id, version, reset_data.offset)
    
    if not success:
        raise HTTPException(status_code=500, detail=f"重置消费者组 {group_id} 偏移量失败")
    
    return {"group_id": group_id, "model_id": model_id, "version": version, "success": True}

@router.post("/admin/models/{model_id}/versions/{version}/acl")
async def create_model_acl(
    model_id: str = Path(..., description="模型ID"), 
    version: str = Path(..., description="版本号"),
    acl_data: AclCreateRequest = Body(...)
):
    """创建模型ACL权限"""
    success = acl_manager.create_model_acl(model_id, version, acl_data.principal, acl_data.operations, acl_data.allow)
    
    if not success:
        raise HTTPException(status_code=500, detail=f"创建模型 {model_id} 版本 {version} 的ACL权限失败")
    
    return {"model_id": model_id, "version": version, "principal": acl_data.principal, "success": True}

@router.delete("/admin/models/{model_id}/versions/{version}/acl")
async def delete_model_acl(
    model_id: str = Path(..., description="模型ID"), 
    version: str = Path(..., description="版本号"),
    principal: Optional[str] = Query(None, description="用户主体")
):
    """删除模型ACL权限"""
    success = acl_manager.delete_model_acl(model_id, version, principal)
    
    if not success:
        raise HTTPException(status_code=500, detail=f"删除模型 {model_id} 版本 {version} 的ACL权限失败")
    
    return {"model_id": model_id, "version": version, "success": True}

@router.get("/admin/models/{model_id}/versions/{version}/acl")
async def list_model_acl(model_id: str = Path(..., description="模型ID"), version: str = Path(..., description="版本号")):
    """获取模型ACL权限列表"""
    acls = acl_manager.list_model_acls(model_id, version)
    return acls

@router.post("/admin/models/{model_id}/versions/{version}/role/{role}")
async def create_role_based_acl(
    model_id: str = Path(..., description="模型ID"), 
    version: str = Path(..., description="版本号"),
    role: str = Path(..., description="角色名称，可选值为'admin'、'producer'、'consumer'")
):
    """基于角色创建ACL权限"""
    success = acl_manager.create_role_based_acl(model_id, version, role)
    
    if not success:
        raise HTTPException(status_code=500, detail=f"为模型 {model_id} 版本 {version} 创建角色 {role} 的ACL权限失败")
    
    return {"model_id": model_id, "version": version, "role": role, "success": True}