# Kafka MCP服务配置文件

import os
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# Kafka集群配置
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_SECURITY_PROTOCOL = os.getenv('KAFKA_SECURITY_PROTOCOL', 'PLAINTEXT')
KAFKA_SASL_MECHANISM = os.getenv('KAFKA_SASL_MECHANISM', None)
KAFKA_SASL_USERNAME = os.getenv('KAFKA_SASL_USERNAME', None)
KAFKA_SASL_PASSWORD = os.getenv('KAFKA_SASL_PASSWORD', None)

# 主题配置默认值
DEFAULT_TOPIC_PARTITIONS = int(os.getenv('DEFAULT_TOPIC_PARTITIONS', 3))
DEFAULT_TOPIC_REPLICATION_FACTOR = int(os.getenv('DEFAULT_TOPIC_REPLICATION_FACTOR', 3))
DEFAULT_TOPIC_RETENTION_MS = int(os.getenv('DEFAULT_TOPIC_RETENTION_MS', 604800000))  # 7天

# 模型主题命名规则
MODEL_TOPIC_PREFIX = os.getenv('MODEL_TOPIC_PREFIX', 'mcp.model.')

# API服务配置
API_HOST = os.getenv('API_HOST', '0.0.0.0')
API_PORT = int(os.getenv('API_PORT', 8000))
API_DEBUG = os.getenv('API_DEBUG', 'False').lower() == 'true'

# 日志配置
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
LOG_FORMAT = os.getenv('LOG_FORMAT', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# 协议配置
SUPPORTED_SERIALIZATION_FORMATS = ['json', 'protobuf']
DEFAULT_SERIALIZATION_FORMAT = os.getenv('DEFAULT_SERIALIZATION_FORMAT', 'json')

# 消费者组配置
CONSUMER_GROUP_PREFIX = os.getenv('CONSUMER_GROUP_PREFIX', 'mcp.consumer.')


def get_kafka_client_config() -> dict:
    """
    根据环境变量生成 confluent-kafka 客户端的安全相关配置

    Returns:
        dict: 可直接传给 KafkaAdminClient / MCPProducer / Consumer 的 conf 字典，
              PLAINTEXT 且未配置 SASL 时返回空字典
    """
    conf = {}

    protocol = (KAFKA_SECURITY_PROTOCOL or 'PLAINTEXT').upper()
    if protocol != 'PLAINTEXT':
        conf['security.protocol'] = protocol

    if KAFKA_SASL_MECHANISM:
        conf['sasl.mechanism'] = KAFKA_SASL_MECHANISM
    if KAFKA_SASL_USERNAME:
        conf['sasl.username'] = KAFKA_SASL_USERNAME
    if KAFKA_SASL_PASSWORD:
        conf['sasl.password'] = KAFKA_SASL_PASSWORD

    return conf