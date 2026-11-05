#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
MCP生产者客户端
负责发送符合MCP协议的消息到Kafka主题
"""

import time
from typing import Dict, Any, Optional, Union, List
from confluent_kafka import Producer
from ..protocol.serializer import Serializer, SerializationFormat
from ..protocol.validator import MCPValidator
from ..admin.topic_manager import TopicManager
from ..utils.logger import get_logger

class MCPProducer:
    """
    MCP生产者客户端
    用于发送符合MCP协议的消息到Kafka主题
    """
    
    def __init__(self, bootstrap_servers: str, topic_manager: Optional[TopicManager] = None,
                 serialization_format: str = "json", conf: Optional[Dict[str, Any]] = None):
        """
        初始化MCP生产者
        
        Args:
            bootstrap_servers: Kafka集群连接地址
            topic_manager: 主题管理器实例，用于获取主题名称
            serialization_format: 序列化格式，可选值为'json'或'protobuf'
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
            
        # 创建Kafka生产者
        self.producer = Producer(config)
        
        # 创建序列化器和验证器
        self.serializer = Serializer(default_format=serialization_format)
        self.validator = MCPValidator()
        
        # 保存主题管理器
        self.topic_manager = topic_manager
        
        self.logger.info(f"MCP生产者已初始化，连接到: {bootstrap_servers}")
    
    def send(self, model_id: str, version: str, content: Any, 
             content_type: Optional[str] = None, metadata: Optional[Dict[str, Any]] = None,
             key: Optional[bytes] = None, partition: Optional[int] = None,
             timestamp: Optional[int] = None, headers: Optional[List[tuple]] = None,
             on_delivery: Optional[callable] = None) -> bool:
        """
        发送MCP消息
        
        Args:
            model_id: 模型ID
            version: 模型版本
            content: 消息内容
            content_type: 内容类型
            metadata: 元数据
            key: 消息键
            partition: 分区号
            timestamp: 时间戳
            headers: 消息头
            on_delivery: 发送回调函数
            
        Returns:
            bool: 发送是否成功
        """
        # 构建MCP消息
        message = {
            'model_id': model_id,
            'version': version,
            'timestamp': timestamp or int(time.time() * 1000),
            'content': content
        }
        
        # 添加可选字段
        if content_type:
            message['content_type'] = content_type
            
        if metadata:
            message['metadata'] = metadata
        
        # 验证消息
        validation_result = self.validator.validate(message)
        if not validation_result.valid:
            self.logger.error(f"消息验证失败: {validation_result.errors}")
            return False
        
        # 确定主题名称
        if self.topic_manager:
            topic = self.topic_manager.get_model_topic_name(model_id, version)
        else:
            # 如果没有提供主题管理器，则使用默认命名规则
            topic = f"mcp.model.{model_id}.v{version}"
        
        # 序列化消息
        try:
            value, format_used = self.serializer.serialize(message)
            self.logger.debug(f"消息已序列化，格式: {format_used}")
        except Exception as e:
            self.logger.error(f"消息序列化失败: {e}")
            return False
        
        # 发送消息
        try:
            self.producer.produce(
                topic=topic,
                value=value,
                key=key,
                partition=partition,
                timestamp=timestamp,
                headers=headers,
                on_delivery=on_delivery
            )
            # 立即刷新，确保消息被发送
            self.producer.flush(timeout=1.0)
            self.logger.info(f"消息已发送到主题 {topic}")
            return True
        except Exception as e:
            self.logger.error(f"消息发送失败: {e}")
            return False
    
    def send_batch(self, messages: List[Dict[str, Any]], 
                  on_delivery: Optional[callable] = None) -> Dict[str, bool]:
        """
        批量发送MCP消息
        
        Args:
            messages: 消息列表，每个消息必须包含model_id和version字段
            on_delivery: 发送回调函数
            
        Returns:
            Dict[str, bool]: 发送结果，键为消息ID，值为是否成功
        """
        results = {}
        
        # 验证所有消息
        validation_results = self.validator.validate_batch(messages)
        
        # 发送有效的消息
        for i, (message, validation_result) in enumerate(zip(messages, validation_results)):
            message_id = f"{message.get('model_id', 'unknown')}.{message.get('version', 'unknown')}.{i}"
            
            if not validation_result.valid:
                self.logger.error(f"消息 {message_id} 验证失败: {validation_result.errors}")
                results[message_id] = False
                continue
            
            # 确定主题名称
            model_id = message['model_id']
            version = message['version']
            
            if self.topic_manager:
                topic = self.topic_manager.get_model_topic_name(model_id, version)
            else:
                # 如果没有提供主题管理器，则使用默认命名规则
                topic = f"mcp.model.{model_id}.v{version}"
            
            # 序列化消息
            try:
                value, format_used = self.serializer.serialize(message)
            except Exception as e:
                self.logger.error(f"消息 {message_id} 序列化失败: {e}")
                results[message_id] = False
                continue
            
            # 发送消息
            try:
                self.producer.produce(
                    topic=topic,
                    value=value,
                    on_delivery=on_delivery
                )
                results[message_id] = True
            except Exception as e:
                self.logger.error(f"消息 {message_id} 发送失败: {e}")
                results[message_id] = False
        
        # 刷新生产者，确保所有消息被发送
        self.producer.flush(timeout=5.0)
        
        return results
    
    def flush(self, timeout: float = 5.0) -> None:
        """
        刷新生产者，确保所有消息被发送
        
        Args:
            timeout: 超时时间(秒)
        """
        self.producer.flush(timeout=timeout)
        
    def close(self) -> None:
        """
        关闭生产者
        """
        self.producer.flush()
        # 注意：confluent-kafka-python的Producer没有显式的close方法
        # 但是在对象被垃圾回收时会自动关闭
        self.logger.info("MCP生产者已关闭")