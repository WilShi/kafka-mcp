#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
协议验证器
负责验证MCP消息的格式和内容是否符合规范
"""

from typing import Dict, Any, List, Optional, Union, Tuple
from enum import Enum
from ..utils.logger import get_logger

class ValidationResult:
    """验证结果类"""
    
    def __init__(self, valid: bool, errors: Optional[List[str]] = None):
        """
        初始化验证结果
        
        Args:
            valid: 是否有效
            errors: 错误信息列表
        """
        self.valid = valid
        self.errors = errors or []
    
    def __bool__(self) -> bool:
        """允许直接在条件表达式中使用验证结果对象"""
        return self.valid

class MCPValidator:
    """MCP协议验证器"""
    
    # 必需字段列表
    REQUIRED_FIELDS = ['model_id', 'version']
    
    # 可选字段列表
    OPTIONAL_FIELDS = ['timestamp', 'metadata', 'content_type', 'content']
    
    # 字段类型映射
    FIELD_TYPES = {
        'model_id': str,
        'version': str,
        'timestamp': (int, float),
        'metadata': dict,
        'content_type': str,
        'content': (dict, list, str, bytes)
    }
    
    def __init__(self):
        """初始化验证器"""
        self.logger = get_logger(self.__class__.__name__)
        self.logger.info("MCP协议验证器已初始化")
    
    def validate(self, message: Dict[str, Any]) -> ValidationResult:
        """
        验证MCP消息
        
        Args:
            message: 要验证的消息字典
            
        Returns:
            ValidationResult: 验证结果
        """
        errors = []
        
        # 验证必需字段
        for field in self.REQUIRED_FIELDS:
            if field not in message:
                errors.append(f"缺少必需字段: {field}")
        
        # 如果缺少必需字段，直接返回错误
        if errors:
            return ValidationResult(False, errors)
        
        # 验证字段类型
        for field, value in message.items():
            # 跳过未知字段
            if field not in self.REQUIRED_FIELDS and field not in self.OPTIONAL_FIELDS:
                continue
                
            expected_type = self.FIELD_TYPES.get(field)
            if expected_type and not isinstance(value, expected_type):
                if isinstance(expected_type, tuple):
                    type_names = ' 或 '.join([t.__name__ for t in expected_type])
                    errors.append(f"字段 {field} 类型错误，期望 {type_names}，实际为 {type(value).__name__}")
                else:
                    errors.append(f"字段 {field} 类型错误，期望 {expected_type.__name__}，实际为 {type(value).__name__}")
        
        # 验证模型ID和版本格式
        if 'model_id' in message and not self._validate_model_id(message['model_id']):
            errors.append(f"模型ID格式无效: {message['model_id']}")
            
        if 'version' in message and not self._validate_version(message['version']):
            errors.append(f"版本格式无效: {message['version']}")
        
        # 返回验证结果
        valid = len(errors) == 0
        return ValidationResult(valid, errors)
    
    def _validate_model_id(self, model_id: str) -> bool:
        """
        验证模型ID格式
        
        Args:
            model_id: 模型ID
            
        Returns:
            bool: 是否有效
        """
        # 模型ID应该是字母、数字、下划线和连字符组成的字符串
        if not isinstance(model_id, str):
            return False
            
        if len(model_id) < 1 or len(model_id) > 64:
            return False
            
        import re
        return bool(re.match(r'^[a-zA-Z0-9_\-\.]+$', model_id))
    
    def _validate_version(self, version: str) -> bool:
        """
        验证版本格式
        
        Args:
            version: 版本字符串
            
        Returns:
            bool: 是否有效
        """
        # 版本应该是形如x.y.z的字符串，其中x、y、z都是非负整数
        if not isinstance(version, str):
            return False
            
        import re
        return bool(re.match(r'^\d+(\.\d+)*$', version))
    
    def validate_batch(self, messages: List[Dict[str, Any]]) -> List[ValidationResult]:
        """
        批量验证MCP消息
        
        Args:
            messages: 要验证的消息列表
            
        Returns:
            List[ValidationResult]: 验证结果列表
        """
        return [self.validate(message) for message in messages]