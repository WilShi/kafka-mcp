#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""协议校验与序列化单元测试（不依赖 Kafka）。"""

import unittest

from mcp.protocol.serializer import Serializer, SerializationFormat
from mcp.protocol.validator import MCPValidator, ValidationResult


class TestMCPValidator(unittest.TestCase):
    def setUp(self):
        self.validator = MCPValidator()

    def test_valid_message(self):
        result = self.validator.validate({
            'model_id': 'risk-score',
            'version': '1.0.0',
            'timestamp': 1700000000000,
            'content': {'score': 0.9},
        })
        self.assertIsInstance(result, ValidationResult)
        self.assertTrue(result)
        self.assertEqual(result.errors, [])

    def test_missing_required_fields(self):
        result = self.validator.validate({})
        self.assertFalse(result)
        self.assertEqual(len(result.errors), 2)

    def test_invalid_version_format(self):
        result = self.validator.validate({'model_id': 'm1', 'version': 'v1'})
        self.assertFalse(result)
        self.assertTrue(any('版本格式无效' in e for e in result.errors))

    def test_invalid_model_id(self):
        result = self.validator.validate({'model_id': '模型/一', 'version': '1.0.0'})
        self.assertFalse(result)
        self.assertTrue(any('模型ID格式无效' in e for e in result.errors))

    def test_wrong_field_type(self):
        result = self.validator.validate({'model_id': 123, 'version': '1.0.0'})
        self.assertFalse(result)

    def test_validate_batch(self):
        results = self.validator.validate_batch([
            {'model_id': 'm1', 'version': '1.0.0'},
            {'model_id': 'm2'},
        ])
        self.assertEqual(len(results), 2)
        self.assertTrue(results[0])
        self.assertFalse(results[1])


class TestSerializer(unittest.TestCase):
    def test_json_roundtrip(self):
        serializer = Serializer(default_format='json')
        data, used = serializer.serialize({'model_id': 'm1', 'version': '1.0.0'})
        self.assertEqual(used, 'json')
        self.assertIsInstance(data, bytes)

        message, used = serializer.deserialize(data)
        self.assertEqual(used, 'json')
        self.assertEqual(message['model_id'], 'm1')

    def test_unsupported_format_falls_back_to_json(self):
        serializer = Serializer(default_format='yaml')
        self.assertEqual(serializer.default_format, SerializationFormat.JSON)

    def test_deserialize_invalid_json_raises(self):
        serializer = Serializer()
        with self.assertRaises(Exception):
            serializer.deserialize(b'not-json')


if __name__ == '__main__':
    unittest.main()
