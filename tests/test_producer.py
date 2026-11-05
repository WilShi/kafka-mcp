import unittest
from unittest.mock import MagicMock, patch
from mcp.client.producer import MCPProducer

class TestMCPProducer(unittest.TestCase):
    def setUp(self):
        # Patch Producer, Serializer, MCPValidator, TopicManager
        patcher_producer = patch('mcp.client.producer.Producer')
        patcher_serializer = patch('mcp.client.producer.Serializer')
        patcher_validator = patch('mcp.client.producer.MCPValidator')
        patcher_topic_manager = patch('mcp.client.producer.TopicManager')
        self.mock_producer = patcher_producer.start()
        self.mock_serializer = patcher_serializer.start()
        self.mock_validator = patcher_validator.start()
        self.mock_topic_manager = patcher_topic_manager.start()
        self.addCleanup(patcher_producer.stop)
        self.addCleanup(patcher_serializer.stop)
        self.addCleanup(patcher_validator.stop)
        self.addCleanup(patcher_topic_manager.stop)

        # Mock logger
        patcher_logger = patch('mcp.client.producer.get_logger')
        self.mock_logger = patcher_logger.start()
        self.addCleanup(patcher_logger.stop)

        # 实例化MCPProducer
        self.producer = MCPProducer('localhost:9092')
        self.producer.producer = MagicMock()
        self.producer.serializer = MagicMock()
        self.producer.validator = MagicMock()
        self.producer.topic_manager = MagicMock()

    def test_send_success(self):
        # 模拟验证通过
        self.producer.validator.validate.return_value.valid = True
        # 模拟序列化成功
        self.producer.serializer.serialize.return_value = (b'data', 'json')
        # 模拟produce和flush无异常
        self.producer.producer.produce.return_value = None
        self.producer.producer.flush.return_value = None
        # 模拟topic_manager
        self.producer.topic_manager.get_model_topic_name.return_value = 'test_topic'
        result = self.producer.send('model1', '1', {'foo': 'bar'})
        self.assertTrue(result)
        self.producer.producer.produce.assert_called_once()
        self.producer.producer.flush.assert_called_once()

    def test_send_validation_fail(self):
        self.producer.validator.validate.return_value.valid = False
        self.producer.validator.validate.return_value.errors = ['error']
        result = self.producer.send('model1', '1', {'foo': 'bar'})
        self.assertFalse(result)
        self.producer.producer.produce.assert_not_called()

    def test_send_serialize_fail(self):
        self.producer.validator.validate.return_value.valid = True
        self.producer.serializer.serialize.side_effect = Exception('serialize error')
        result = self.producer.send('model1', '1', {'foo': 'bar'})
        self.assertFalse(result)
        self.producer.producer.produce.assert_not_called()

    def test_send_produce_fail(self):
        self.producer.validator.validate.return_value.valid = True
        self.producer.serializer.serialize.return_value = (b'data', 'json')
        self.producer.producer.produce.side_effect = Exception('produce error')
        result = self.producer.send('model1', '1', {'foo': 'bar'})
        self.assertFalse(result)
        self.producer.producer.flush.assert_not_called()

    def test_send_batch(self):
        messages = [
            {'model_id': 'm1', 'version': '1', 'content': 'a'},
            {'model_id': 'm2', 'version': '2', 'content': 'b'}
        ]
        # validator.validate_batch返回两个valid对象
        valid_result = MagicMock()
        valid_result.valid = True
        self.producer.validator.validate_batch.return_value = [valid_result, valid_result]
        self.producer.serializer.serialize.return_value = (b'data', 'json')
        self.producer.producer.produce.return_value = None
        self.producer.producer.flush.return_value = None
        self.producer.topic_manager.get_model_topic_name.side_effect = lambda m, v: f"topic_{m}_{v}"
        results = self.producer.send_batch(messages)
        self.assertEqual(results['m1.1.0'], True)
        self.assertEqual(results['m2.2.1'], True)
        self.assertEqual(self.producer.producer.produce.call_count, 2)
        self.producer.producer.flush.assert_called_once()

    def test_send_batch_partial_fail(self):
        messages = [
            {'model_id': 'm1', 'version': '1', 'content': 'a'},
            {'model_id': 'm2', 'version': '2', 'content': 'b'}
        ]
        valid = MagicMock(); valid.valid = True
        invalid = MagicMock(); invalid.valid = False; invalid.errors = ['fail']
        self.producer.validator.validate_batch.return_value = [valid, invalid]
        self.producer.serializer.serialize.return_value = (b'data', 'json')
        self.producer.producer.produce.return_value = None
        self.producer.producer.flush.return_value = None
        self.producer.topic_manager.get_model_topic_name.side_effect = lambda m, v: f"topic_{m}_{v}"
        results = self.producer.send_batch(messages)
        self.assertEqual(results['m1.1.0'], True)
        self.assertEqual(results['m2.2.1'], False)
        self.assertEqual(self.producer.producer.produce.call_count, 1)
        self.producer.producer.flush.assert_called_once()

    def test_flush_and_close(self):
        self.producer.producer.flush = MagicMock()
        self.producer.flush(timeout=2.0)
        self.producer.producer.flush.assert_called_with(timeout=2.0)
        self.producer.close()
        self.producer.producer.flush.assert_called()

if __name__ == '__main__':
    unittest.main()