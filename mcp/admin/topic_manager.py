#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
主题管理器
负责模型与Kafka主题的绑定关系管理，包括创建模型主题、版本管理等
"""

import json
from typing import Dict, List, Optional, Any, Tuple
from confluent_kafka.admin import NewTopic
from .kafka_admin import KafkaAdminClient
from ..utils.logger import get_logger
from config import (
    MODEL_TOPIC_PREFIX,
    DEFAULT_TOPIC_PARTITIONS,
    DEFAULT_TOPIC_REPLICATION_FACTOR,
    DEFAULT_TOPIC_RETENTION_MS
)

class TopicManager:
    """
    主题管理器
    负责模型与Kafka主题的绑定关系管理
    """
    
    def __init__(self, admin_client: KafkaAdminClient):
        """
        初始化主题管理器
        
        Args:
            admin_client: Kafka Admin Client实例
        """
        self.logger = get_logger(self.__class__.__name__)
        self.admin_client = admin_client
        self.logger.info("主题管理器已初始化")
    
    def get_model_topic_name(self, model_id: str, version: Optional[str] = None) -> str:
        """
        根据模型ID和版本生成主题名称
        
        Args:
            model_id: 模型ID
            version: 模型版本，如果为None则返回模型基础主题名称
            
        Returns:
            str: 主题名称
        """
        base_topic = f"{MODEL_TOPIC_PREFIX}{model_id}"
        if version:
            return f"{base_topic}.v{version}"
        return base_topic
    
    def create_model_topic(self, model_id: str, version: str, 
                          partitions: int = DEFAULT_TOPIC_PARTITIONS,
                          replication_factor: int = DEFAULT_TOPIC_REPLICATION_FACTOR,
                          config: Optional[Dict[str, str]] = None) -> bool:
        """
        创建模型主题
        
        Args:
            model_id: 模型ID
            version: 模型版本
            partitions: 分区数
            replication_factor: 副本因子
            config: 主题配置
            
        Returns:
            bool: 创建是否成功
        """
        topic_name = self.get_model_topic_name(model_id, version)
        
        # 设置默认配置
        topic_config = {
            'retention.ms': str(DEFAULT_TOPIC_RETENTION_MS)
        }
        
        # 合并自定义配置
        if config:
            topic_config.update(config)
        
        # 创建主题配置
        topic_configs = [{
            'name': topic_name,
            'num_partitions': partitions,
            'replication_factor': replication_factor,
            'config': topic_config
        }]
        
        # 执行创建操作
        result = self.admin_client.create_topics(topic_configs)
        success = result.get(topic_name, False)
        
        if success:
            self.logger.info(f"模型 {model_id} 版本 {version} 主题创建成功: {topic_name}")
        else:
            self.logger.error(f"模型 {model_id} 版本 {version} 主题创建失败: {topic_name}")
            
        return success
    
    def create_model_topics_batch(self, models_config: List[Dict[str, Any]]) -> Dict[str, bool]:
        """
        批量创建模型主题
        
        Args:
            models_config: 模型配置列表，每个配置包含model_id, version, partitions, replication_factor和config
            
        Returns:
            Dict[str, bool]: 创建结果，键为模型ID.版本，值为是否成功
        """
        topic_configs = []
        model_topic_map = {}
        
        for config in models_config:
            model_id = config['model_id']
            version = config['version']
            topic_name = self.get_model_topic_name(model_id, version)
            
            # 设置默认配置
            topic_config = {
                'retention.ms': str(config.get('retention_ms', DEFAULT_TOPIC_RETENTION_MS))
            }
            
            # 合并自定义配置
            if 'config' in config:
                topic_config.update(config['config'])
            
            # 创建主题配置
            topic_configs.append({
                'name': topic_name,
                'num_partitions': config.get('partitions', DEFAULT_TOPIC_PARTITIONS),
                'replication_factor': config.get('replication_factor', DEFAULT_TOPIC_REPLICATION_FACTOR),
                'config': topic_config
            })
            
            # 记录模型ID和主题的映射关系
            model_key = f"{model_id}.{version}"
            model_topic_map[model_key] = topic_name
        
        # 执行批量创建操作
        result = self.admin_client.create_topics(topic_configs)
        
        # 转换结果为模型ID.版本格式
        model_result = {}
        for model_key, topic_name in model_topic_map.items():
            model_result[model_key] = result.get(topic_name, False)
            if model_result[model_key]:
                self.logger.info(f"模型 {model_key} 主题创建成功: {topic_name}")
            else:
                self.logger.error(f"模型 {model_key} 主题创建失败: {topic_name}")
                
        return model_result
    
    def delete_model_topic(self, model_id: str, version: Optional[str] = None) -> bool:
        """
        删除模型主题
        
        Args:
            model_id: 模型ID
            version: 模型版本，如果为None则删除所有版本主题
            
        Returns:
            bool: 删除是否成功
        """
        if version:
            # 删除特定版本主题
            topic_name = self.get_model_topic_name(model_id, version)
            result = self.admin_client.delete_topics([topic_name])
            success = result.get(topic_name, False)
            
            if success:
                self.logger.info(f"模型 {model_id} 版本 {version} 主题删除成功: {topic_name}")
            else:
                self.logger.error(f"模型 {model_id} 版本 {version} 主题删除失败: {topic_name}")
                
            return success
        else:
            # 删除所有版本主题
            # 先获取所有主题
            all_topics = self.admin_client.list_topics()
            model_topics = [topic for topic in all_topics.keys() 
                           if topic.startswith(f"{MODEL_TOPIC_PREFIX}{model_id}")]
            
            if not model_topics:
                self.logger.warning(f"未找到模型 {model_id} 的任何主题")
                return True
            
            # 执行批量删除
            result = self.admin_client.delete_topics(model_topics)
            
            # 检查是否全部成功
            success = all(result.get(topic, False) for topic in model_topics)
            
            if success:
                self.logger.info(f"模型 {model_id} 所有主题删除成功")
            else:
                self.logger.error(f"模型 {model_id} 部分主题删除失败")
                
            return success
    
    def get_model_topics(self, model_id: Optional[str] = None) -> Dict[str, List[str]]:
        """
        获取模型主题列表
        
        Args:
            model_id: 模型ID，如果为None则获取所有模型主题
            
        Returns:
            Dict[str, List[str]]: 模型主题映射，键为模型ID，值为版本列表
        """
        # 获取所有主题
        all_topics = self.admin_client.list_topics()
        
        # 筛选模型主题
        model_topics = {}
        prefix = MODEL_TOPIC_PREFIX
        
        for topic in all_topics.keys():
            if not topic.startswith(prefix):
                continue
                
            # 解析模型ID和版本：topic_suffix 形如 <model_id> 或 <model_id>.v<version>
            # 版本号本身可能包含点（如 1.0.0），因此只在第一个点处切分
            topic_suffix = topic[len(prefix):]
            current_model_id, _, remainder = topic_suffix.partition('.')

            if not current_model_id:
                continue

            # 如果指定了模型ID且不匹配，则跳过
            if model_id and current_model_id != model_id:
                continue

            # 添加到结果中
            if current_model_id not in model_topics:
                model_topics[current_model_id] = []

            # 版本段形如 v1.0.0
            if remainder.startswith('v'):
                model_topics[current_model_id].append(remainder[1:])
        
        return model_topics
    
    def update_model_topic_config(self, model_id: str, version: str, config_updates: Dict[str, str]) -> bool:
        """
        更新模型主题配置
        
        Args:
            model_id: 模型ID
            version: 模型版本
            config_updates: 要更新的配置
            
        Returns:
            bool: 更新是否成功
        """
        topic_name = self.get_model_topic_name(model_id, version)
        result = self.admin_client.update_topic_config(topic_name, config_updates)
        
        if result:
            self.logger.info(f"模型 {model_id} 版本 {version} 主题配置更新成功")
        else:
            self.logger.error(f"模型 {model_id} 版本 {version} 主题配置更新失败")
            
        return result
    
    def get_model_topic_config(self, model_id: str, version: str) -> Dict[str, str]:
        """
        获取模型主题配置
        
        Args:
            model_id: 模型ID
            version: 模型版本
            
        Returns:
            Dict[str, str]: 主题配置
        """
        topic_name = self.get_model_topic_name(model_id, version)
        return self.admin_client.get_topic_config(topic_name)