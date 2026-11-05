#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""HTTP API 冒烟测试：只覆盖不依赖真实 Kafka 的接口。"""

import unittest

from fastapi.testclient import TestClient

from mcp.api.app import create_app


class TestHTTPAPI(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client = TestClient(create_app())

    def test_health(self):
        response = self.client.get('/health')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {'status': 'ok'})

    def test_root(self):
        response = self.client.get('/')
        self.assertEqual(response.status_code, 200)
        self.assertIn('message', response.json())

    def test_list_models(self):
        response = self.client.get('/api/models')
        self.assertEqual(response.status_code, 200)
        self.assertIsInstance(response.json(), list)

    def test_get_unknown_model(self):
        response = self.client.get('/api/models/not-exist')
        self.assertEqual(response.status_code, 404)

    def test_openapi_documents_admin_routes(self):
        response = self.client.get('/openapi.json')
        self.assertEqual(response.status_code, 200)
        paths = response.json()['paths']
        self.assertIn('/api/admin/topics', paths)
        self.assertIn('/api/models', paths)


if __name__ == '__main__':
    unittest.main()
