#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
ACL权限管理器
负责通过Kafka ACL机制控制模型注册和读写权限
"""

from typing import Dict, List, Optional, Any, Tuple
from confluent_kafka.admin import AclBinding, ResourceType, ResourcePatternType, AclOperation, AclPermissionType
from .kafka_admin import KafkaAdminClient
from .topic_manager import TopicManager
from ..utils.logger import get_logger

class AclManager:
    """
    ACL权限管理器
    负责管理模型主题的访问控制权限
    """
    
    def __init__(self, admin_client: KafkaAdminClient, topic_manager: TopicManager):
        """
        初始化ACL权限管理器
        
        Args:
            admin_client: Kafka Admin Client实例
            topic_manager: 主题管理器实例
        """
        self.logger = get_logger(self.__class__.__name__)
        self.admin_client = admin_client
        self.topic_manager = topic_manager
        self.logger.info("ACL权限管理器已初始化")
    
    def create_model_acl(self, model_id: str, version: str, principal: str, 
                        operations: List[str], allow: bool = True) -> bool:
        """
        创建模型主题的ACL权限
        
        Args:
            model_id: 模型ID
            version: 模型版本
            principal: 用户主体，格式为'User:username'或'Group:groupname'
            operations: 操作列表，可包含'READ'、'WRITE'、'CREATE'、'DELETE'、'ALTER'等
            allow: 是否允许操作，True为允许，False为拒绝
            
        Returns:
            bool: 创建是否成功
        """
        topic_name = self.topic_manager.get_model_topic_name(model_id, version)
        
        # 创建ACL绑定列表
        acl_bindings = []
        permission_type = AclPermissionType.ALLOW if allow else AclPermissionType.DENY
        
        for op_str in operations:
            try:
                # 将字符串操作转换为AclOperation枚举
                operation = getattr(AclOperation, op_str)
                
                # 创建ACL绑定
                acl_binding = AclBinding(
                    ResourceType.TOPIC,
                    topic_name,
                    ResourcePatternType.LITERAL,
                    principal,
                    '*',  # 任意主机
                    operation,
                    permission_type
                )
                
                acl_bindings.append(acl_binding)
            except AttributeError:
                self.logger.error(f"不支持的操作类型: {op_str}")
                return False
        
        try:
            # 创建ACL
            futures = self.admin_client.admin_client.create_acls(acl_bindings)
            
            # 等待操作完成
            for future in futures:
                future.result()  # 等待创建完成
                
            self.logger.info(f"为模型 {model_id} 版本 {version} 创建ACL权限成功，授权给 {principal}")
            return True
        except Exception as e:
            self.logger.error(f"为模型 {model_id} 版本 {version} 创建ACL权限失败: {e}")
            return False
    
    def delete_model_acl(self, model_id: str, version: Optional[str] = None, 
                        principal: Optional[str] = None) -> bool:
        """
        删除模型主题的ACL权限
        
        Args:
            model_id: 模型ID
            version: 模型版本，如果为None则删除所有版本的权限
            principal: 用户主体，如果为None则删除所有用户的权限
            
        Returns:
            bool: 删除是否成功
        """
        # 构建ACL过滤器
        topic_pattern = None
        if version:
            topic_name = self.topic_manager.get_model_topic_name(model_id, version)
            topic_pattern = topic_name
        else:
            # 使用前缀匹配所有版本
            topic_pattern = f"{self.topic_manager.MODEL_TOPIC_PREFIX}{model_id}"
        
        try:
            # 创建ACL过滤器
            acl_filter = AclBinding(
                ResourceType.TOPIC,
                topic_pattern,
                ResourcePatternType.LITERAL if version else ResourcePatternType.PREFIXED,
                principal if principal else '*',
                '*',  # 任意主机
                AclOperation.ANY,  # 任意操作
                AclPermissionType.ANY  # 任意权限类型
            )
            
            # 删除ACL
            futures = self.admin_client.admin_client.delete_acls([acl_filter])
            
            # 等待操作完成
            for future in futures:
                result = future.result()  # 等待删除完成
                if not result:
                    self.logger.warning(f"没有找到匹配的ACL权限")
                    return False
                
            self.logger.info(f"删除模型 {model_id} {'版本 ' + version if version else '所有版本'} 的ACL权限成功")
            return True
        except Exception as e:
            self.logger.error(f"删除模型 {model_id} {'版本 ' + version if version else '所有版本'} 的ACL权限失败: {e}")
            return False
    
    def list_model_acls(self, model_id: Optional[str] = None, version: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        获取模型主题的ACL权限列表
        
        Args:
            model_id: 模型ID，如果为None则获取所有模型的权限
            version: 模型版本，如果为None则获取所有版本的权限
            
        Returns:
            List[Dict[str, Any]]: ACL权限列表
        """
        # 构建ACL过滤器
        topic_pattern = None
        pattern_type = ResourcePatternType.LITERAL
        
        if model_id:
            if version:
                topic_pattern = self.topic_manager.get_model_topic_name(model_id, version)
            else:
                # 使用前缀匹配所有版本
                topic_pattern = f"{self.topic_manager.MODEL_TOPIC_PREFIX}{model_id}"
                pattern_type = ResourcePatternType.PREFIXED
        else:
            # 使用前缀匹配所有模型
            topic_pattern = self.topic_manager.MODEL_TOPIC_PREFIX
            pattern_type = ResourcePatternType.PREFIXED
        
        try:
            # 创建ACL过滤器
            acl_filter = AclBinding(
                ResourceType.TOPIC,
                topic_pattern,
                pattern_type,
                '*',  # 任意用户
                '*',  # 任意主机
                AclOperation.ANY,  # 任意操作
                AclPermissionType.ANY  # 任意权限类型
            )
            
            # 获取ACL列表
            futures = self.admin_client.admin_client.describe_acls(acl_filter)
            
            # 等待操作完成
            result = []
            for future in futures:
                acls = future.result()  # 等待查询完成
                
                # 转换为字典格式
                for acl in acls:
                    result.append({
                        'resource_type': acl.resource_type.name,
                        'resource_name': acl.resource_name,
                        'pattern_type': acl.resource_pattern_type.name,
                        'principal': acl.principal,
                        'host': acl.host,
                        'operation': acl.operation.name,
                        'permission_type': acl.permission_type.name
                    })
            
            return result
        except Exception as e:
            self.logger.error(f"获取ACL权限列表失败: {e}")
            return []
    
    def create_role_based_acl(self, model_id: str, version: str, role: str) -> bool:
        """
        基于角色创建ACL权限
        
        Args:
            model_id: 模型ID
            version: 模型版本
            role: 角色名称，可选值为'admin'、'producer'、'consumer'
            
        Returns:
            bool: 创建是否成功
        """
        # 根据角色确定权限
        principal = f"Group:{role}"
        operations = []
        
        if role == 'admin':
            operations = ['READ', 'WRITE', 'CREATE', 'DELETE', 'ALTER', 'DESCRIBE', 'DESCRIBE_CONFIGS', 'ALTER_CONFIGS']
        elif role == 'producer':
            operations = ['WRITE', 'DESCRIBE']
        elif role == 'consumer':
            operations = ['READ', 'DESCRIBE']
        else:
            self.logger.error(f"不支持的角色: {role}")
            return False
        
        # 创建ACL权限
        return self.create_model_acl(model_id, version, principal, operations)
    
    def check_access(self, model_id: str, version: str, principal: str, operation: str) -> bool:
        """
        检查用户是否有权限执行特定操作
        
        Args:
            model_id: 模型ID
            version: 模型版本
            principal: 用户主体
            operation: 操作名称
            
        Returns:
            bool: 是否有权限
        """
        # 获取模型主题的所有ACL权限
        acls = self.list_model_acls(model_id, version)
        
        # 检查是否有匹配的权限
        for acl in acls:
            if (acl['principal'] == principal or acl['principal'] == '*') and \
               (acl['operation'] == operation or acl['operation'] == 'ALL') and \
               acl['permission_type'] == 'ALLOW':
                return True
        
        return False