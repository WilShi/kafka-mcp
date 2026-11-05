#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""MCP stdio 服务协议测试：不依赖 Kafka 与真实 MCP 客户端。"""

import io
import json
import unittest

import mcp_server


class TestMCPProtocol(unittest.TestCase):
    def test_initialize(self):
        response = mcp_server.handle_message({
            'jsonrpc': '2.0',
            'id': 1,
            'method': 'initialize',
            'params': {'protocolVersion': '2024-11-05', 'capabilities': {}},
        })
        self.assertEqual(response['id'], 1)
        self.assertEqual(response['result']['serverInfo']['name'], 'kafka-mcp')
        self.assertIn('tools', response['result']['capabilities'])

    def test_initialized_notification_has_no_response(self):
        response = mcp_server.handle_message({
            'jsonrpc': '2.0',
            'method': 'notifications/initialized',
        })
        self.assertIsNone(response)

    def test_tools_list(self):
        response = mcp_server.handle_message({'jsonrpc': '2.0', 'id': 2, 'method': 'tools/list'})
        tools = response['result']['tools']
        names = {tool['name'] for tool in tools}
        for expected in (
            'kafka_cluster_metadata',
            'kafka_create_topic',
            'model_register',
            'model_version_create',
            'model_acl_create_role',
            'mcp_validate_message',
        ):
            self.assertIn(expected, names)
        for tool in tools:
            self.assertEqual(tool['inputSchema']['type'], 'object')

    def test_required_parameters_are_marked(self):
        tools = {tool['name']: tool for tool in mcp_server._list_tools()}
        required = tools['model_register']['inputSchema'].get('required', [])
        self.assertIn('model_id', required)
        self.assertIn('name', required)
        self.assertIn('version', required)

    def test_call_validate_tool(self):
        response = mcp_server.handle_message({
            'jsonrpc': '2.0',
            'id': 3,
            'method': 'tools/call',
            'params': {
                'name': 'mcp_validate_message',
                'arguments': {'message': {'model_id': 'm1', 'version': '1.0.0'}},
            },
        })
        self.assertFalse(response['result']['isError'])
        payload = json.loads(response['result']['content'][0]['text'])
        self.assertTrue(payload['valid'])

    def test_call_unknown_tool(self):
        response = mcp_server.handle_message({
            'jsonrpc': '2.0',
            'id': 4,
            'method': 'tools/call',
            'params': {'name': 'no_such_tool', 'arguments': {}},
        })
        self.assertEqual(response['error']['code'], -32602)

    def test_unknown_method(self):
        response = mcp_server.handle_message({'jsonrpc': '2.0', 'id': 5, 'method': 'foo/bar'})
        self.assertEqual(response['error']['code'], -32601)

    def test_serve_pipeline(self):
        stdin = io.StringIO(
            json.dumps({'jsonrpc': '2.0', 'id': 1, 'method': 'initialize', 'params': {}}) + '\n' +
            json.dumps({'jsonrpc': '2.0', 'id': 2, 'method': 'ping'}) + '\n' +
            'not-json\n'
        )
        stdout = io.StringIO()
        mcp_server.serve(stdin=stdin, stdout=stdout)
        lines = [line for line in stdout.getvalue().splitlines() if line.strip()]
        self.assertEqual(len(lines), 3)
        self.assertEqual(json.loads(lines[1])['result'], {})
        self.assertEqual(json.loads(lines[2])['error']['code'], -32700)


if __name__ == '__main__':
    unittest.main()
