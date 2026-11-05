#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
MCP消费者客户端
负责从Kafka主题接收符合MCP协议的消息
"""

from typing import Dict, Any, Optional, Union, List, Callable
from confluent_kafka import Consumer, KafkaError, KafkaException
from ..protocol.serializer import Serializer, SerializationFormat
from ..protocol.validator import MCPValidator, ValidationResult
from ..admin.topic_manager import TopicManager
from ..admin.consumer_manager import ConsumerManager
from ..utils.logger import get_logger

class MCPConsumer:
    """
    MCP消费者客户端
    用于从Kafka主题接收符合MCP协议的消息
    """
    
    def __init__(self, bootstrap_servers: str, group_id: str,
                 topic_manager: Optional[TopicManager] = None,
                 consumer_manager: Optional[ConsumerManager] = None,
                 serialization_format: str = "json", 
                 conf: Optional[Dict[str, Any]] = None,
                 auto_offset_reset: str = "earliest"):
        """
        初始化MCP消费者
        
        Args:
            bootstrap_servers: Kafka集群连接地址
            group_id: 消费者组ID
            topic_manager: 主题管理器实例，用于获取主题名称
            consumer_manager: 消费者组管理器实例
            serialization_format: 序列化格式，可选值为'json'或'protobuf'
            conf: 额外的Kafka配置参数
            auto_offset_reset: 偏移量重置策略，可选值为'earliest'或'latest'
        """
        self.logger = get_logger(self.__class__.__name__)
        
        # 基础配置
        config = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': auto_offset_reset,
            'enable.auto.commit': True
        }
        
        # 合并额外配置
        if conf:
            config.update(conf)
            
        # 创建Kafka消费者
        self.consumer = Consumer(config)
        
        # 创建序列化器和验证器
        self.serializer = Serializer(default_format=serialization_format)
        self.validator = MCPValidator()
        
        # 保存主题管理器和消费者组管理器
        self.topic_manager = topic_manager
        self.consumer_manager = consumer_manager
        self.group_id = group_id
        
        # 订阅的主题列表
        self.subscribed_topics = []
        
        self.logger.info(f"MCP消费者已初始化，连接到: {bootstrap_servers}，消费者组: {group_id}")
    
    def subscribe_model(self, model_id: str, version: str) -> bool:
        """
        订阅特定模型版本的主题
        
        Args:
            model_id: 模型ID
            version: 模型版本
            
        Returns:
            bool: 订阅是否成功
        """
        # 确定主题名称
        if self.topic_manager:
            topic = self.topic_manager.get_model_topic_name(model_id, version)
        else:
            # 如果没有提供主题管理器，则使用默认命名规则
            topic = f"mcp.model.{model_id}.v{version}"
        
        # 添加到订阅列表
        if topic not in self.subscribed_topics:
            self.subscribed_topics.append(topic)
            
            # 更新订阅
            try:
                self.consumer.subscribe(self.subscribed_topics)
                self.logger.info(f"已订阅主题: {topic}")
                return True
            except KafkaException as e:
                self.logger.error(f"订阅主题 {topic} 失败: {e}")
                # 从列表中移除
                self.subscribed_topics.remove(topic)
                return False
        else:
            self.logger.warning(f"主题 {topic} 已经订阅")
            return True
    
    def unsubscribe_model(self, model_id: str, version: str) -> bool:
        """
        取消订阅特定模型版本的主题
        
        Args:
            model_id: 模型ID
            version: 模型版本
            
        Returns:
            bool: 取消订阅是否成功
        """
        # 确定主题名称
        if self.topic_manager:
            topic = self.topic_manager.get_model_topic_name(model_id, version)
        else:
            # 如果没有提供主题管理器，则使用默认命名规则
            topic = f"mcp.model.{model_id}.v{version}"
        
        # 从订阅列表中移除
        if topic in self.subscribed_topics:
            self.subscribed_topics.remove(topic)
            
            # 如果还有其他订阅，则更新订阅列表
            if self.subscribed_topics:
                try:
                    self.consumer.subscribe(self.subscribed_topics)
                    self.logger.info(f"已取消订阅主题: {topic}")
                    return True
                except KafkaException as e:
                    self.logger.error(f"更新订阅失败: {e}")
                    # 恢复列表
                    self.subscribed_topics.append(topic)
                    return False
            else:
                # 如果没有其他订阅，则取消所有订阅
                self.consumer.unsubscribe()
                self.logger.info(f"已取消所有订阅")
                return True
        else:
            self.logger.warning(f"主题 {topic} 未订阅")
            return True
    
    def unsubscribe_all(self) -> bool:
        """
        取消所有订阅
        
        Returns:
            bool: 取消订阅是否成功
        """
        try:
            self.consumer.unsubscribe()
            self.subscribed_topics = []
            self.logger.info("已取消所有订阅")
            return True
        except KafkaException as e:
            self.logger.error(f"取消订阅失败: {e}")
            return False
    
    def poll(self, timeout: float = 1.0) -> Optional[Dict[str, Any]]:
        """
        轮询获取一条消息
        
        Args:
            timeout: 轮询超时时间(秒)
            
        Returns:
            Optional[Dict[str, Any]]: 解析后的MCP消息，如果没有消息则返回None
        """
        # 轮询消息
        msg = self.consumer.poll(timeout=timeout)
        
        # 如果没有消息或发生错误
        if msg is None:
            return None
            
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # 到达分区末尾，不是真正的错误
                self.logger.debug(f"到达分区末尾: {msg.topic()} [{msg.partition()}]")
            else:
                self.logger.error(f"消息错误: {msg.error()}")
            return None
        
        # 反序列化消息
        try:
            message, format_used = self.serializer.deserialize(msg.value())
            self.logger.debug(f"消息已反序列化，格式: {format_used}")
        except Exception as e:
            self.logger.error(f"消息反序列化失败: {e}")
            return None
        
        # 验证消息
        validation_result = self.validator.validate(message)
        if not validation_result.valid:
            self.logger.warning(f"接收到无效的MCP消息: {validation_result.errors}")
            # 返回消息但添加验证结果
            message['_validation_result'] = {
                'valid': False,
                'errors': validation_result.errors
            }
        else:
            # 添加验证成功标记
            message['_validation_result'] = {'valid': True}
        
        # 添加Kafka消息元数据
        message['_kafka_metadata'] = {
            'topic': msg.topic(),
            'partition': msg.partition(),
            'offset': msg.offset(),
            'timestamp': msg.timestamp(),
            'key': msg.key().decode('utf-8') if msg.key() else None,
            'headers': msg.headers()
        }
        
        return message
    
    def consume(self, num_messages: int = 1, timeout: float = 1.0) -> List[Dict[str, Any]]:
        """
        批量消费消息
        
        Args:
            num_messages: 要消费的消息数量
            timeout: 每条消息的轮询超时时间(秒)
            
        Returns:
            List[Dict[str, Any]]: 解析后的MCP消息列表
        """
        messages = []
        for _ in range(num_messages):
            message = self.poll(timeout)
            if message:
                messages.append(message)
            else:
                # 如果没有更多消息，则退出循环
                break
                
        return messages
    
    def commit(self, message: Optional[Dict[str, Any]] = None, asynchronous: bool = False) -> bool:
        """
        提交偏移量
        
        Args:
            message: 要提交偏移量的消息，如果为None则提交所有已处理的消息
            asynchronous: 是否异步提交
            
        Returns:
            bool: 提交是否成功
        """
        try:
            if message and '_kafka_metadata' in message:
                # 提交特定消息的偏移量
                metadata = message['_kafka_metadata']
                self.consumer.commit(asynchronous=asynchronous)
            else:
                # 提交所有已处理的消息的偏移量
                self.consumer.commit(asynchronous=asynchronous)
                
            return True
        except KafkaException as e:
            self.logger.error(f"提交偏移量失败: {e}")
            return False
    
    def seek(self, model_id: str, version: str, offset: str = "earliest") -> bool:
        """
        设置消费位置
        
        Args:
            model_id: 模型ID
            version: 模型版本
            offset: 偏移量位置，可选值为'earliest'、'latest'或特定数值
            
        Returns:
            bool: 设置是否成功
        """
        # 确定主题名称
        if self.topic_manager:
            topic = self.topic_manager.get_model_topic_name(model_id, version)
        else:
            # 如果没有提供主题管理器，则使用默认命名规则
            topic = f"mcp.model.{model_id}.v{version}"
        
        # 如果使用消费者组管理器，则通过管理器重置偏移量
        if self.consumer_manager:
            success = self.consumer_manager.reset_consumer_offsets(
                self.group_id, model_id, version, offset
            )
            if success:
                self.logger.info(f"已重置消费者组 {self.group_id} 在主题 {topic} 的偏移量到 {offset}")
                return True
            else:
                self.logger.error(f"重置消费者组 {self.group_id} 在主题 {topic} 的偏移量失败")
                return False
        else:
            # 如果没有提供消费者组管理器，则使用seek方法
            # 注意：这种方式只对当前消费者实例有效，不会影响整个消费者组
            try:
                # 获取主题的分区信息
                metadata = self.consumer.list_topics(topic)
                if topic not in metadata.topics:
                    self.logger.error(f"主题 {topic} 不存在")
                    return False
                    
                partitions = metadata.topics[topic].partitions
                
                # 对每个分区设置偏移量
                for partition_id in partitions:
                    if offset == "earliest":
                        self.consumer.seek_to_beginning(f"{topic}:{partition_id}")
                    elif offset == "latest":
                        self.consumer.seek_to_end(f"{topic}:{partition_id}")
                    else:
                        try:
                            # 尝试将offset转换为整数
                            offset_value = int(offset)
                            self.consumer.seek(f"{topic}:{partition_id}", offset_value)
                        except ValueError:
                            self.logger.error(f"无效的偏移量值: {offset}")
                            return False
                
                self.logger.info(f"已设置主题 {topic} 的消费位置到 {offset}")
                return True
            except KafkaException as e:
                self.logger.error(f"设置消费位置失败: {e}")
                return False
    
    def get_lag(self, model_id: str, version: str) -> Dict[str, Any]:
        """
        获取消费滞后信息
        
        Args:
            model_id: 模型ID
            version: 模型版本
            
        Returns:
            Dict[str, Any]: 滞后信息
        """
        if self.consumer_manager:
            # 使用消费者组管理器获取滞后信息
            lag_summary = self.consumer_manager.get_consumer_lag_summary(
                self.group_id, model_id, version
            )
            return lag_summary
        else:
            self.logger.warning("未提供消费者组管理器，无法获取滞后信息")
            return {
                'status': 'unknown',
                'total_lag': -1,
                'max_lag': -1,
                'min_lag': -1,
                'avg_lag': -1,
                'partitions': 0
            }
    
    def close(self) -> None:
        """
        关闭消费者
        """
        self.consumer.close()
        self.logger.info("MCP消费者已关闭")