#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
模型数据模型
定义MCP服务中的模型实体
"""

from typing import Dict, List, Optional, Any
from datetime import datetime
from pydantic import BaseModel, Field

class ModelConfig(BaseModel):
    """
    模型配置类
    定义模型的Kafka主题配置
    """
    partitions: int = Field(default=3, description="主题分区数")
    replication_factor: int = Field(default=3, description="主题副本因子")
    retention_ms: int = Field(default=604800000, description="数据保留时间(毫秒)")
    config: Optional[Dict[str, str]] = Field(default=None, description="其他Kafka主题配置")

class ModelVersion(BaseModel):
    """
    模型版本类
    定义模型的版本信息
    """
    version: str = Field(..., description="版本号，格式为x.y.z")
    created_at: datetime = Field(default_factory=datetime.now, description="创建时间")
    topic_name: Optional[str] = Field(default=None, description="关联的Kafka主题名称")
    config: ModelConfig = Field(default_factory=ModelConfig, description="版本主题配置")
    metadata: Optional[Dict[str, Any]] = Field(default=None, description="版本元数据")
    active: bool = Field(default=True, description="是否激活")

class Model(BaseModel):
    """
    模型类
    定义MCP服务中的模型实体
    """
    model_id: str = Field(..., description="模型ID，唯一标识符")
    name: str = Field(..., description="模型名称")
    description: Optional[str] = Field(default=None, description="模型描述")
    created_at: datetime = Field(default_factory=datetime.now, description="创建时间")
    updated_at: datetime = Field(default_factory=datetime.now, description="更新时间")
    versions: List[ModelVersion] = Field(default_factory=list, description="版本列表")
    default_version: Optional[str] = Field(default=None, description="默认版本")
    metadata: Optional[Dict[str, Any]] = Field(default=None, description="模型元数据")
    
    def add_version(self, version: ModelVersion) -> None:
        """
        添加版本
        
        Args:
            version: 要添加的版本
        """
        # 检查版本是否已存在
        for existing_version in self.versions:
            if existing_version.version == version.version:
                raise ValueError(f"版本 {version.version} 已存在")
        
        # 添加版本
        self.versions.append(version)
        self.updated_at = datetime.now()
        
        # 如果是第一个版本，设为默认版本
        if len(self.versions) == 1 and self.default_version is None:
            self.default_version = version.version
    
    def get_version(self, version: str) -> Optional[ModelVersion]:
        """
        获取特定版本
        
        Args:
            version: 版本号
            
        Returns:
            Optional[ModelVersion]: 版本对象，如果不存在则返回None
        """
        for v in self.versions:
            if v.version == version:
                return v
        return None
    
    def remove_version(self, version: str) -> bool:
        """
        删除版本
        
        Args:
            version: 要删除的版本号
            
        Returns:
            bool: 是否成功删除
        """
        for i, v in enumerate(self.versions):
            if v.version == version:
                # 如果删除的是默认版本，需要更新默认版本
                if self.default_version == version:
                    if len(self.versions) > 1:
                        # 找到最新的版本作为默认版本
                        remaining_versions = [v for v in self.versions if v.version != version]
                        remaining_versions.sort(key=lambda x: x.created_at, reverse=True)
                        self.default_version = remaining_versions[0].version
                    else:
                        self.default_version = None
                
                # 删除版本
                self.versions.pop(i)
                self.updated_at = datetime.now()
                return True
        
        return False
    
    def set_default_version(self, version: str) -> bool:
        """
        设置默认版本
        
        Args:
            version: 要设为默认的版本号
            
        Returns:
            bool: 是否成功设置
        """
        # 检查版本是否存在
        if self.get_version(version) is None:
            return False
        
        # 设置默认版本
        self.default_version = version
        self.updated_at = datetime.now()
        return True
    
    def to_dict(self) -> Dict[str, Any]:
        """
        转换为字典
        
        Returns:
            Dict[str, Any]: 字典表示
        """
        return self.dict()