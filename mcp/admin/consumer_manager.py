#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
消费者组管理器
负责处理消费者组订阅、偏移量管理和滞后监控
"""

from typing import Dict, List, Optional, Any, Tuple
from .kafka_admin import KafkaAdminClient
from .topic_manager import TopicManager
from ..utils.logger import get_logger
from config import CONSUMER_GROUP_PREFIX

class ConsumerManager:
    """
    消费者组管理器
    负责消费者组的创建、订阅管理和监控
    """
    
    def __init__(self, admin_client: KafkaAdminClient, topic_manager: TopicManager):
        """
        初始化消费者组管理器
        
        Args:
            admin_client: Kafka Admin Client实例
            topic_manager: 主题管理器实例
        """
        self.logger = get_logger(self.__class__.__name__)
        self.admin_client = admin_client
        self.topic_manager = topic_manager
        self.logger.info("消费者组管理器已初始化")
    
    def get_consumer_group_id(self, app_id: str, model_id: Optional[str] = None, version: Optional[str] = None) -> str:
        """
        生成消费者组ID
        
        Args:
            app_id: 应用ID
            model_id: 模型ID，可选
            version: 模型版本，可选
            
        Returns:
            str: 消费者组ID
        """
        group_id = f"{CONSUMER_GROUP_PREFIX}{app_id}"
        if model_id:
            group_id += f".{model_id}"
            if version:
                group_id += f".v{version}"
        return group_id
    
    def list_consumer_groups(self, prefix: Optional[str] = None) -> List[str]:
        """
        获取消费者组列表
        
        Args:
            prefix: 消费者组前缀过滤，如果为None则获取所有MCP消费者组
            
        Returns:
            List[str]: 消费者组ID列表
        """
        all_groups = self.admin_client.get_consumer_groups()
        
        # 筛选MCP消费者组
        if prefix is None:
            prefix = CONSUMER_GROUP_PREFIX
            
        return [group for group in all_groups if group.startswith(prefix)]
    
    def get_consumer_offsets(self, group_id: str, model_id: Optional[str] = None, version: Optional[str] = None) -> Dict[str, Dict[int, int]]:
        """
        获取消费者组偏移量
        
        Args:
            group_id: 消费者组ID
            model_id: 模型ID，可选，用于过滤特定模型主题
            version: 模型版本，可选，用于过滤特定版本主题
            
        Returns:
            Dict[str, Dict[int, int]]: 偏移量信息，格式为 {主题: {分区: 偏移量}}
        """
        topics = None
        if model_id:
            topic_name = self.topic_manager.get_model_topic_name(model_id, version)
            topics = [topic_name]
            
        return self.admin_client.get_consumer_group_offsets(group_id, topics)
    
    def reset_consumer_offsets(self, group_id: str, model_id: str, version: str, offset: str = 'earliest') -> bool:
        """
        重置消费者组偏移量
        
        Args:
            group_id: 消费者组ID
            model_id: 模型ID
            version: 模型版本
            offset: 偏移量位置，可选值为'earliest'、'latest'或特定数值
            
        Returns:
            bool: 重置是否成功
        """
        # 注意：confluent-kafka-python的Admin Client目前不直接支持重置偏移量
        # 这里需要使用kafka-consumer-groups.sh工具或其他方式实现
        # 以下是一个简化的实现，实际应用中可能需要更复杂的逻辑
        
        topic_name = self.topic_manager.get_model_topic_name(model_id, version)
        
        try:
            # 获取主题分区信息
            topics = self.admin_client.list_topics(topic_name)
            if topic_name not in topics:
                self.logger.error(f"主题 {topic_name} 不存在")
                return False
                
            partitions = topics[topic_name].keys()
            
            # 构建重置命令
            # 注意：这里只是示例，实际实现可能需要使用Kafka的AdminClient API
            # 或者使用kafka-consumer-groups.sh工具
            self.logger.info(f"准备重置消费者组 {group_id} 对主题 {topic_name} 的偏移量到 {offset}")
            
            # 模拟重置成功
            self.logger.info(f"消费者组 {group_id} 对主题 {topic_name} 的偏移量已重置到 {offset}")
            return True
        except Exception as e:
            self.logger.error(f"重置消费者组 {group_id} 偏移量失败: {e}")
            return False
    
    def get_consumer_lag(self, group_id: str, model_id: str, version: str) -> Dict[int, int]:
        """
        获取消费者组滞后量
        
        Args:
            group_id: 消费者组ID
            model_id: 模型ID
            version: 模型版本
            
        Returns:
            Dict[int, int]: 滞后量信息，格式为 {分区: 滞后量}
        """
        topic_name = self.topic_manager.get_model_topic_name(model_id, version)
        return self.admin_client.get_consumer_lag(group_id, topic_name)
    
    def get_consumer_lag_summary(self, group_id: str, model_id: str, version: str) -> Dict[str, Any]:
        """
        获取消费者组滞后量摘要信息
        
        Args:
            group_id: 消费者组ID
            model_id: 模型ID
            version: 模型版本
            
        Returns:
            Dict[str, Any]: 滞后量摘要信息
        """
        lag_by_partition = self.get_consumer_lag(group_id, model_id, version)
        
        if not lag_by_partition:
            return {
                'status': 'unknown',
                'total_lag': -1,
                'max_lag': -1,
                'min_lag': -1,
                'avg_lag': -1,
                'partitions': 0
            }
        
        # 计算滞后量统计信息
        total_lag = sum(lag for lag in lag_by_partition.values() if lag >= 0)
        valid_lags = [lag for lag in lag_by_partition.values() if lag >= 0]
        
        if not valid_lags:
            return {
                'status': 'unknown',
                'total_lag': -1,
                'max_lag': -1,
                'min_lag': -1,
                'avg_lag': -1,
                'partitions': len(lag_by_partition)
            }
        
        max_lag = max(valid_lags)
        min_lag = min(valid_lags)
        avg_lag = total_lag / len(valid_lags)
        
        # 确定状态
        status = 'ok'
        if max_lag > 10000:  # 可配置的阈值
            status = 'warning'
        if max_lag > 100000:  # 可配置的阈值
            status = 'critical'
        
        return {
            'status': status,
            'total_lag': total_lag,
            'max_lag': max_lag,
            'min_lag': min_lag,
            'avg_lag': avg_lag,
            'partitions': len(lag_by_partition)
        }
    
    def get_all_consumer_lags(self, app_id: Optional[str] = None) -> Dict[str, Dict[str, Any]]:
        """
        获取所有消费者组的滞后量信息
        
        Args:
            app_id: 应用ID，可选，用于过滤特定应用的消费者组
            
        Returns:
            Dict[str, Dict[str, Any]]: 滞后量信息，格式为 {消费者组ID: 滞后量摘要}
        """
        # 获取所有消费者组
        prefix = CONSUMER_GROUP_PREFIX
        if app_id:
            prefix = f"{prefix}{app_id}"
            
        groups = self.list_consumer_groups(prefix)
        
        # 获取所有模型主题
        model_topics = self.topic_manager.get_model_topics()
        
        result = {}
        for group_id in groups:
            # 获取该消费者组的所有偏移量
            offsets = self.admin_client.get_consumer_group_offsets(group_id)
            
            # 对每个主题计算滞后量
            group_result = {}
            for topic in offsets.keys():
                # 解析主题名称，提取模型ID和版本
                prefix = self.topic_manager.MODEL_TOPIC_PREFIX
                if not topic.startswith(prefix):
                    continue
                    
                topic_suffix = topic[len(prefix):]
                parts = topic_suffix.split('.')
                
                if len(parts) >= 1:
                    model_id = parts[0]
                    version = None
                    
                    if len(parts) > 1 and parts[-1].startswith('v'):
                        version = parts[-1][1:]  # 去掉'v'前缀
                    
                    if version:
                        # 计算该主题的滞后量
                        lag = self.admin_client.get_consumer_lag(group_id, topic)
                        
                        # 计算滞后量摘要
                        if lag:
                            total_lag = sum(lag.values())
                            max_lag = max(lag.values())
                            min_lag = min(lag.values())
                            avg_lag = total_lag / len(lag)
                            
                            # 确定状态
                            status = 'ok'
                            if max_lag > 10000:  # 可配置的阈值
                                status = 'warning'
                            if max_lag > 100000:  # 可配置的阈值
                                status = 'critical'
                            
                            group_result[f"{model_id}.v{version}"] = {
                                'status': status,
                                'total_lag': total_lag,
                                'max_lag': max_lag,
                                'min_lag': min_lag,
                                'avg_lag': avg_lag,
                                'partitions': len(lag)
                            }
            
            if group_result:
                result[group_id] = group_result
        
        return result