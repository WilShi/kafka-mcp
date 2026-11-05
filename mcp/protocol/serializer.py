#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
协议序列化器
负责MCP消息的序列化和反序列化，支持JSON和Protobuf格式
"""

import json
from typing import Dict, Any, Optional, Union, Tuple
from enum import Enum
from ..utils.logger import get_logger

try:
    import google.protobuf.json_format as pb_json
    import google.protobuf.message as pb_message
    PROTOBUF_AVAILABLE = True
except ImportError:
    PROTOBUF_AVAILABLE = False

class SerializationFormat(str, Enum):
    """序列化格式枚举"""
    JSON = "json"
    PROTOBUF = "protobuf"

class Serializer:
    """MCP消息序列化器"""
    
    def __init__(self, default_format: str = "json"):
        """
        初始化序列化器
        
        Args:
            default_format: 默认序列化格式，可选值为'json'或'protobuf'
        """
        self.logger = get_logger(self.__class__.__name__)
        
        # 验证默认格式
        try:
            self.default_format = SerializationFormat(default_format.lower())
        except ValueError:
            self.logger.warning(f"不支持的序列化格式: {default_format}，使用默认的JSON格式")
            self.default_format = SerializationFormat.JSON
            
        # 检查Protobuf是否可用
        if self.default_format == SerializationFormat.PROTOBUF and not PROTOBUF_AVAILABLE:
            self.logger.warning("Protobuf库未安装，将使用JSON格式作为默认格式")
            self.default_format = SerializationFormat.JSON
            
        self.logger.info(f"序列化器已初始化，默认格式: {self.default_format.value}")
    
    def serialize(self, message: Union[Dict[str, Any], pb_message.Message], 
                format: Optional[str] = None) -> Tuple[bytes, str]:
        """
        序列化消息
        
        Args:
            message: 要序列化的消息，可以是字典或Protobuf消息对象
            format: 序列化格式，可选值为'json'或'protobuf'，如果为None则使用默认格式
            
        Returns:
            Tuple[bytes, str]: 序列化后的字节数据和使用的格式
        """
        # 确定序列化格式
        if format is None:
            format = self.default_format
        else:
            try:
                format = SerializationFormat(format.lower())
            except ValueError:
                self.logger.warning(f"不支持的序列化格式: {format}，使用默认的格式")
                format = self.default_format
        
        # 执行序列化
        if format == SerializationFormat.JSON:
            return self._serialize_json(message), format.value
        elif format == SerializationFormat.PROTOBUF:
            return self._serialize_protobuf(message), format.value
        else:
            raise ValueError(f"不支持的序列化格式: {format}")
    
    def deserialize(self, data: bytes, format: Optional[str] = None, 
                  message_type: Optional[Any] = None) -> Tuple[Any, str]:
        """
        反序列化消息
        
        Args:
            data: 要反序列化的字节数据
            format: 序列化格式，可选值为'json'或'protobuf'，如果为None则使用默认格式
            message_type: Protobuf消息类型，仅在format为'protobuf'时需要
            
        Returns:
            Tuple[Any, str]: 反序列化后的消息对象和使用的格式
        """
        # 确定序列化格式
        if format is None:
            format = self.default_format
        else:
            try:
                format = SerializationFormat(format.lower())
            except ValueError:
                self.logger.warning(f"不支持的序列化格式: {format}，使用默认的格式")
                format = self.default_format
        
        # 执行反序列化
        if format == SerializationFormat.JSON:
            return self._deserialize_json(data), format.value
        elif format == SerializationFormat.PROTOBUF:
            if message_type is None:
                raise ValueError("使用Protobuf格式反序列化时必须提供message_type参数")
            return self._deserialize_protobuf(data, message_type), format.value
        else:
            raise ValueError(f"不支持的序列化格式: {format}")
    
    def _serialize_json(self, message: Union[Dict[str, Any], pb_message.Message]) -> bytes:
        """
        使用JSON格式序列化消息
        
        Args:
            message: 要序列化的消息，可以是字典或Protobuf消息对象
            
        Returns:
            bytes: 序列化后的字节数据
        """
        # 如果是Protobuf消息对象，先转换为字典
        if PROTOBUF_AVAILABLE and isinstance(message, pb_message.Message):
            message_dict = pb_json.MessageToDict(
                message, 
                preserving_proto_field_name=True,
                including_default_value_fields=True
            )
        else:
            message_dict = message
            
        # 序列化为JSON字符串，然后转换为字节
        return json.dumps(message_dict, ensure_ascii=False).encode('utf-8')
    
    def _deserialize_json(self, data: bytes) -> Dict[str, Any]:
        """
        使用JSON格式反序列化消息
        
        Args:
            data: 要反序列化的字节数据
            
        Returns:
            Dict[str, Any]: 反序列化后的字典
        """
        # 将字节转换为字符串，然后解析JSON
        return json.loads(data.decode('utf-8'))
    
    def _serialize_protobuf(self, message: Union[Dict[str, Any], pb_message.Message]) -> bytes:
        """
        使用Protobuf格式序列化消息
        
        Args:
            message: 要序列化的消息，可以是字典或Protobuf消息对象
            
        Returns:
            bytes: 序列化后的字节数据
        """
        if not PROTOBUF_AVAILABLE:
            raise ImportError("Protobuf库未安装，无法使用Protobuf格式序列化")
            
        # 如果是字典，需要先转换为Protobuf消息对象
        if isinstance(message, dict):
            raise ValueError("使用Protobuf格式序列化字典时，必须先将字典转换为Protobuf消息对象")
            
        # 序列化Protobuf消息对象
        return message.SerializeToString()
    
    def _deserialize_protobuf(self, data: bytes, message_type: Any) -> pb_message.Message:
        """
        使用Protobuf格式反序列化消息
        
        Args:
            data: 要反序列化的字节数据
            message_type: Protobuf消息类型
            
        Returns:
            pb_message.Message: 反序列化后的Protobuf消息对象
        """
        if not PROTOBUF_AVAILABLE:
            raise ImportError("Protobuf库未安装，无法使用Protobuf格式反序列化")
            
        # 创建消息对象并解析
        message = message_type()
        message.ParseFromString(data)
        return message