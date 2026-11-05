#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Kafka Admin Client实现
提供Kafka集群管理功能，包括主题、消费者组和ACL权限管理
"""

import logging
from typing import Dict, List, Optional, Any, Tuple
from confluent_kafka.admin import AdminClient, NewTopic, ConfigResource, ResourceType, ConfigEntry
from confluent_kafka import KafkaException, KafkaError
from ..utils.logger import get_logger

class KafkaAdminClient:
    """
    Kafka Admin Client封装类
    提供主题创建、删除、配置修改等管理功能
    """
    
    def __init__(self, bootstrap_servers: str, conf: Optional[Dict[str, Any]] = None):
        """
        初始化Kafka Admin Client
        
        Args:
            bootstrap_servers: Kafka集群连接地址
            conf: 额外的Kafka配置参数
        """
        self.logger = get_logger(self.__class__.__name__)
        
        # 基础配置
        config = {
            'bootstrap.servers': bootstrap_servers
        }
        
        # 合并额外配置
        if conf:
            config.update(conf)
            
        # 创建Admin Client实例
        self.admin_client = AdminClient(config)
        self.logger.info(f"Kafka Admin Client已初始化，连接到: {bootstrap_servers}")
    
    def create_topics(self, topic_configs: List[Dict[str, Any]], timeout: float = 30.0) -> Dict[str, bool]:
        """
        批量创建主题
        
        Args:
            topic_configs: 主题配置列表，每个配置包含name, num_partitions, replication_factor和config
            timeout: 操作超时时间(秒)
            
        Returns:
            Dict[str, bool]: 主题创建结果，键为主题名，值为是否成功
        """
        new_topics = []
        for config in topic_configs:
            new_topics.append(NewTopic(
                config['name'],
                num_partitions=config.get('num_partitions', 1),
                replication_factor=config.get('replication_factor', 1),
                config=config.get('config', {})
            ))
        
        # 执行创建操作
        result = {}
        try:
            futures = self.admin_client.create_topics(new_topics, operation_timeout=timeout)
            
            # 等待操作完成并获取结果
            for topic, future in futures.items():
                try:
                    future.result()  # 等待创建完成
                    result[topic] = True
                    self.logger.info(f"主题 {topic} 创建成功")
                except KafkaException as e:
                    result[topic] = False
                    self.logger.error(f"主题 {topic} 创建失败: {e}")
        except Exception as e:
            self.logger.error(f"创建主题时发生错误: {e}")
            for topic in [t.topic for t in new_topics]:
                result[topic] = False
                
        return result
    
    def delete_topics(self, topics: List[str], timeout: float = 30.0) -> Dict[str, bool]:
        """
        批量删除主题
        
        Args:
            topics: 要删除的主题名列表
            timeout: 操作超时时间(秒)
            
        Returns:
            Dict[str, bool]: 主题删除结果，键为主题名，值为是否成功
        """
        result = {}
        try:
            futures = self.admin_client.delete_topics(topics, operation_timeout=timeout)
            
            # 等待操作完成并获取结果
            for topic, future in futures.items():
                try:
                    future.result()  # 等待删除完成
                    result[topic] = True
                    self.logger.info(f"主题 {topic} 删除成功")
                except KafkaException as e:
                    result[topic] = False
                    self.logger.error(f"主题 {topic} 删除失败: {e}")
        except Exception as e:
            self.logger.error(f"删除主题时发生错误: {e}")
            for topic in topics:
                result[topic] = False
                
        return result
    
    def list_topics(self, timeout: float = 30.0) -> Dict[str, Any]:
        """
        获取所有主题列表
        
        Args:
            timeout: 操作超时时间(秒)
            
        Returns:
            Dict[str, Any]: 主题信息字典
        """
        try:
            metadata = self.admin_client.list_topics(timeout=timeout)
            return {topic: metadata.topics[topic].partitions for topic in metadata.topics}
        except Exception as e:
            self.logger.error(f"获取主题列表失败: {e}")
            return {}
    
    def get_topic_config(self, topic: str, timeout: float = 30.0) -> Dict[str, str]:
        """
        获取主题配置
        
        Args:
            topic: 主题名称
            timeout: 操作超时时间(秒)
            
        Returns:
            Dict[str, str]: 主题配置字典
        """
        config_resource = ConfigResource(ResourceType.TOPIC, topic)
        try:
            futures = self.admin_client.describe_configs([config_resource], request_timeout=timeout)
            
            # 等待操作完成并获取结果
            for res, future in futures.items():
                try:
                    configs = future.result()
                    return {config.name: config.value for config in configs.values() if config.value is not None}
                except KafkaException as e:
                    self.logger.error(f"获取主题 {topic} 配置失败: {e}")
                    return {}
        except Exception as e:
            self.logger.error(f"获取主题配置时发生错误: {e}")
            return {}
    
    def update_topic_config(self, topic: str, config_updates: Dict[str, str], timeout: float = 30.0) -> bool:
        """
        更新主题配置
        
        Args:
            topic: 主题名称
            config_updates: 要更新的配置字典
            timeout: 操作超时时间(秒)
            
        Returns:
            bool: 更新是否成功
        """
        config_resource = ConfigResource(ResourceType.TOPIC, topic)
        config_entries = []
        
        for key, value in config_updates.items():
            config_entries.append(ConfigEntry(key, value))
            
        config_resource.set_config(config_entries)
        
        try:
            futures = self.admin_client.alter_configs([config_resource], request_timeout=timeout)
            
            # 等待操作完成并获取结果
            for res, future in futures.items():
                try:
                    future.result()  # 等待更新完成
                    self.logger.info(f"主题 {topic} 配置更新成功")
                    return True
                except KafkaException as e:
                    self.logger.error(f"主题 {topic} 配置更新失败: {e}")
                    return False
        except Exception as e:
            self.logger.error(f"更新主题配置时发生错误: {e}")
            return False
    
    def get_consumer_groups(self, timeout: float = 30.0) -> List[str]:
        """
        获取所有消费者组列表
        
        Args:
            timeout: 操作超时时间(秒)
            
        Returns:
            List[str]: 消费者组ID列表
        """
        try:
            groups = self.admin_client.list_consumer_groups(timeout=timeout)
            return [group.group_id for group in groups.valid]
        except Exception as e:
            self.logger.error(f"获取消费者组列表失败: {e}")
            return []
    
    def get_consumer_group_offsets(self, group_id: str, topics: Optional[List[str]] = None, timeout: float = 30.0) -> Dict[str, Dict[int, int]]:
        """
        获取消费者组的偏移量信息
        
        Args:
            group_id: 消费者组ID
            topics: 要查询的主题列表，如果为None则查询所有主题
            timeout: 操作超时时间(秒)
            
        Returns:
            Dict[str, Dict[int, int]]: 偏移量信息，格式为 {主题: {分区: 偏移量}}
        """
        try:
            offsets = self.admin_client.list_consumer_group_offsets(group_id, topics, request_timeout=timeout)
            result = {}
            
            for topic_partition, offset in offsets.items():
                if topic_partition.topic not in result:
                    result[topic_partition.topic] = {}
                result[topic_partition.topic][topic_partition.partition] = offset.offset
                
            return result
        except Exception as e:
            self.logger.error(f"获取消费者组 {group_id} 偏移量失败: {e}")
            return {}
    
    def get_topic_watermarks(self, topic: str, timeout: float = 30.0) -> Dict[int, Tuple[int, int]]:
        """
        获取主题的水位线信息（最小和最大偏移量）
        
        Args:
            topic: 主题名称
            timeout: 操作超时时间(秒)
            
        Returns:
            Dict[int, Tuple[int, int]]: 水位线信息，格式为 {分区: (最小偏移量, 最大偏移量)}
        """
        try:
            metadata = self.admin_client.list_topics(topic, timeout=timeout)
            if topic not in metadata.topics:
                self.logger.error(f"主题 {topic} 不存在")
                return {}
                
            partitions = metadata.topics[topic].partitions
            result = {}
            
            for partition in partitions.keys():
                try:
                    low, high = self.admin_client.get_watermark_offsets(f"{topic}:{partition}", timeout=timeout)
                    result[partition] = (low, high)
                except Exception as e:
                    self.logger.error(f"获取主题 {topic} 分区 {partition} 水位线失败: {e}")
            
            return result
        except Exception as e:
            self.logger.error(f"获取主题水位线失败: {e}")
            return {}
    
    def get_consumer_lag(self, group_id: str, topic: str, timeout: float = 30.0) -> Dict[int, int]:
        """
        计算消费者组的滞后量（Lag）
        
        Args:
            group_id: 消费者组ID
            topic: 主题名称
            timeout: 操作超时时间(秒)
            
        Returns:
            Dict[int, int]: 滞后量信息，格式为 {分区: 滞后量}
        """
        try:
            # 获取消费者组当前偏移量
            offsets = self.get_consumer_group_offsets(group_id, [topic], timeout)
            if not offsets or topic not in offsets:
                self.logger.error(f"消费者组 {group_id} 未订阅主题 {topic}")
                return {}
                
            # 获取主题水位线
            watermarks = self.get_topic_watermarks(topic, timeout)
            if not watermarks:
                return {}
                
            # 计算滞后量
            result = {}
            for partition, offset in offsets[topic].items():
                if partition in watermarks:
                    _, high = watermarks[partition]
                    lag = high - offset if offset >= 0 else -1
                    result[partition] = lag
            
            return result
        except Exception as e:
            self.logger.error(f"计算消费者组 {group_id} 主题 {topic} 滞后量失败: {e}")
            return {}
    
    def get_cluster_metadata(self, timeout: float = 30.0) -> Dict[str, Any]:
        """
        获取Kafka集群元数据
        
        Args:
            timeout: 操作超时时间(秒)
            
        Returns:
            Dict[str, Any]: 集群元数据信息
        """
        try:
            metadata = self.admin_client.list_topics(timeout=timeout)
            
            # 提取Broker信息
            brokers = {}
            for broker_id, broker in metadata.brokers.items():
                brokers[broker_id] = {
                    'host': broker.host,
                    'port': broker.port
                }
            
            # 提取主题信息
            topics = {}
            for topic_name, topic in metadata.topics.items():
                topics[topic_name] = {
                    'partitions': len(topic.partitions),
                    'error': topic.error.str() if topic.error else None
                }
            
            return {
                'cluster_id': metadata.cluster_id,
                'controller_id': metadata.controller_id,
                'brokers': brokers,
                'topics': topics
            }
        except Exception as e:
            self.logger.error(f"获取集群元数据失败: {e}")
            return {}