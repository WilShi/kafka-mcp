#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""主题管理器单元测试：使用伪 Admin Client，不连接真实 Kafka。"""

import unittest

from mcp.admin.topic_manager import TopicManager


class FakeAdminClient:
    """仅实现 TopicManager 依赖的方法。"""

    def __init__(self, existing_topics=None, fail_topics=None):
        self.existing_topics = list(existing_topics or [])
        self.fail_topics = set(fail_topics or [])
        self.created = []
        self.deleted = []
        self.updated = {}

    def create_topics(self, topic_configs, timeout=30.0):
        result = {}
        for config in topic_configs:
            self.created.append(config)
            result[config['name']] = config['name'] not in self.fail_topics
        return result

    def delete_topics(self, topics, timeout=30.0):
        self.deleted.extend(topics)
        return {topic: topic not in self.fail_topics for topic in topics}

    def list_topics(self, timeout=30.0):
        return {topic: {} for topic in self.existing_topics}

    def update_topic_config(self, topic, config_updates, timeout=30.0):
        self.updated[topic] = config_updates
        return topic not in self.fail_topics

    def get_topic_config(self, topic, timeout=30.0):
        return {'retention.ms': '604800000'}


class TestTopicManager(unittest.TestCase):
    def setUp(self):
        self.admin = FakeAdminClient()
        self.manager = TopicManager(self.admin)

    def test_topic_name(self):
        self.assertEqual(self.manager.get_model_topic_name('demo'), 'mcp.model.demo')
        self.assertEqual(
            self.manager.get_model_topic_name('demo', '1.0.0'),
            'mcp.model.demo.v1.0.0',
        )

    def test_create_model_topic(self):
        success = self.manager.create_model_topic('demo', '1.0.0', partitions=6, replication_factor=3)
        self.assertTrue(success)
        created = self.admin.created[0]
        self.assertEqual(created['name'], 'mcp.model.demo.v1.0.0')
        self.assertEqual(created['num_partitions'], 6)
        self.assertEqual(created['replication_factor'], 3)
        self.assertIn('retention.ms', created['config'])

    def test_create_model_topic_failure(self):
        admin = FakeAdminClient(fail_topics=['mcp.model.demo.v1.0.0'])
        manager = TopicManager(admin)
        self.assertFalse(manager.create_model_topic('demo', '1.0.0'))

    def test_create_model_topics_batch(self):
        result = self.manager.create_model_topics_batch([
            {'model_id': 'a', 'version': '1.0.0', 'partitions': 2},
            {'model_id': 'b', 'version': '2.0.0', 'partitions': 4},
        ])
        self.assertTrue(result['a.1.0.0'])
        self.assertTrue(result['b.2.0.0'])
        self.assertEqual(len(self.admin.created), 2)

    def test_delete_single_version(self):
        self.assertTrue(self.manager.delete_model_topic('demo', '1.0.0'))
        self.assertEqual(self.admin.deleted, ['mcp.model.demo.v1.0.0'])

    def test_delete_all_versions(self):
        admin = FakeAdminClient(existing_topics=[
            'mcp.model.demo.v1.0.0',
            'mcp.model.demo.v2.0.0',
            'other.topic',
        ])
        manager = TopicManager(admin)
        self.assertTrue(manager.delete_model_topic('demo'))
        self.assertEqual(sorted(admin.deleted), ['mcp.model.demo.v1.0.0', 'mcp.model.demo.v2.0.0'])

    def test_get_model_topics(self):
        admin = FakeAdminClient(existing_topics=[
            'mcp.model.demo.v1.0.0',
            'mcp.model.demo.v1.1.0',
            'unrelated',
        ])
        manager = TopicManager(admin)
        topics = manager.get_model_topics()
        self.assertEqual(sorted(topics['demo']), ['1.0.0', '1.1.0'])

    def test_update_model_topic_config(self):
        self.assertTrue(self.manager.update_model_topic_config('demo', '1.0.0', {'retention.ms': '1000'}))
        self.assertEqual(
            self.admin.updated['mcp.model.demo.v1.0.0'],
            {'retention.ms': '1000'},
        )


if __name__ == '__main__':
    unittest.main()
