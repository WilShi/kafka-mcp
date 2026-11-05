#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Kafka MCP Server

基于标准输入输出（stdio）的 Model Context Protocol 服务，使用 JSON-RPC 2.0
协议与 MCP 客户端（Claude Desktop、Cursor 等）通信，把 Kafka Admin Client 的
能力暴露为一组 MCP 工具。

用法：
    python mcp_server.py

通常不需要手动启动，由 MCP 客户端根据配置自动拉起。
注意：stdio 模式下 stdout 专用于协议帧，日志一律输出到 stderr。
"""

import json
import os
import sys
import traceback
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

# 保证从任意工作目录启动都能导入项目内的包
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# MCP 客户端拉起进程时工作目录通常不是项目根目录，
# 这里显式加载项目目录下的 .env；客户端配置里的 env 优先级更高。
try:
    from dotenv import load_dotenv

    load_dotenv(os.path.join(PROJECT_ROOT, ".env"))
except ImportError:  # python-dotenv 未安装时仅依赖外部环境变量
    pass

from config import (  # noqa: E402
    KAFKA_BOOTSTRAP_SERVERS,
    DEFAULT_SERIALIZATION_FORMAT,
    get_kafka_client_config,
)
from mcp.utils.logger import get_logger  # noqa: E402

SERVER_NAME = "kafka-mcp"
SERVER_VERSION = "1.0.0"
PROTOCOL_VERSION = "2024-11-05"

logger = get_logger("kafka_mcp.server")

# 延迟初始化的 Kafka 管理对象与模型注册表
_STATE = None


# ---------------------------------------------------------------------------
# Kafka 连接与状态
# ---------------------------------------------------------------------------

def _kafka_conf() -> Dict[str, Any]:
    """根据环境变量构造 confluent-kafka 的安全相关配置。"""
    return get_kafka_client_config()


def _state() -> Dict[str, Any]:
    """首次调用时初始化 Kafka 客户端与各管理器。"""
    global _STATE
    if _STATE is not None:
        return _STATE

    try:
        from mcp.admin.kafka_admin import KafkaAdminClient
        from mcp.admin.topic_manager import TopicManager
        from mcp.admin.consumer_manager import ConsumerManager
        from mcp.admin.acl_manager import AclManager
    except ImportError as exc:  # pragma: no cover - 依赖缺失时给出可读提示
        raise RuntimeError(
            "无法导入 Kafka 组件，请先安装依赖：pip install -r requirements.txt（%s）" % exc
        )

    conf = _kafka_conf()
    admin_client = KafkaAdminClient(KAFKA_BOOTSTRAP_SERVERS, conf or None)
    topic_manager = TopicManager(admin_client)

    _STATE = {
        "admin_client": admin_client,
        "topic_manager": topic_manager,
        "consumer_manager": ConsumerManager(admin_client, topic_manager),
        "acl_manager": AclManager(admin_client, topic_manager),
        # 模型注册表（进程内），与 REST 服务保持一致
        "models": {},
    }
    logger.info("Kafka 管理器初始化完成，bootstrap.servers=%s", KAFKA_BOOTSTRAP_SERVERS)
    return _STATE


def _producer():
    """惰性创建并复用 MCPProducer。"""
    state = _state()
    if "producer" not in state:
        from mcp.client.producer import MCPProducer

        state["producer"] = MCPProducer(
            KAFKA_BOOTSTRAP_SERVERS,
            state["topic_manager"],
            serialization_format=DEFAULT_SERIALIZATION_FORMAT,
            conf=_kafka_conf() or None,
        )
    return state["producer"]


# ---------------------------------------------------------------------------
# 工具注册
# ---------------------------------------------------------------------------

TOOLS: List[Dict[str, Any]] = []
_TOOL_HANDLERS: Dict[str, Callable[[Dict[str, Any]], Any]] = {}


def _build_schema(parameters: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    properties = {}
    required = []
    for name, spec in parameters.items():
        prop = {
            "type": spec.get("type", "string"),
            "description": spec.get("description", ""),
        }
        if spec.get("enum"):
            prop["enum"] = list(spec["enum"])
        if "default" in spec:
            prop["default"] = spec["default"]
        if spec.get("items"):
            prop["items"] = spec["items"]
        properties[name] = prop
        if spec.get("required"):
            required.append(name)
    schema = {"type": "object", "properties": properties}
    if required:
        schema["required"] = required
    return schema


def tool(name: str, description: str, parameters: Optional[Dict[str, Dict[str, Any]]] = None):
    """注册一个 MCP 工具。"""

    def decorator(func):
        if name in _TOOL_HANDLERS:
            raise RuntimeError("重复注册的工具名: %s" % name)
        TOOLS.append(
            {
                "name": name,
                "description": description,
                "inputSchema": _build_schema(parameters or {}),
            }
        )
        _TOOL_HANDLERS[name] = func
        return func

    return decorator


def _require(args: Dict[str, Any], key: str) -> Any:
    value = args.get(key)
    if value is None or value == "":
        raise ValueError("缺少必需参数: %s" % key)
    return value


def _jsonable(value: Any) -> Any:
    """把返回值转换成可 JSON 序列化的结构。"""
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_jsonable(v) for v in value]
    if hasattr(value, "model_dump"):
        return _jsonable(value.model_dump())
    if hasattr(value, "dict"):
        return _jsonable(value.dict())
    return str(value)


def _model_summary(model) -> Dict[str, Any]:
    return {
        "model_id": model.model_id,
        "name": model.name,
        "description": model.description,
        "created_at": _jsonable(model.created_at),
        "updated_at": _jsonable(model.updated_at),
        "versions": [v.version for v in model.versions],
        "default_version": model.default_version,
        "metadata": _jsonable(model.metadata),
    }


def _version_summary(version) -> Dict[str, Any]:
    return {
        "version": version.version,
        "created_at": _jsonable(version.created_at),
        "topic_name": version.topic_name,
        "config": _jsonable(version.config),
        "metadata": _jsonable(version.metadata),
        "active": version.active,
    }


# ---------------------------------------------------------------------------
# 集群与主题
# ---------------------------------------------------------------------------

@tool(
    "kafka_cluster_metadata",
    "获取 Kafka 集群元数据：集群 ID、Controller ID、Broker 列表与主题概览。",
)
def kafka_cluster_metadata(args: Dict[str, Any]) -> Any:
    return _state()["admin_client"].get_cluster_metadata()


@tool("kafka_list_topics", "列出集群中的所有主题及其分区信息。")
def kafka_list_topics(args: Dict[str, Any]) -> Any:
    return _state()["admin_client"].list_topics()


@tool(
    "kafka_create_topic",
    "创建一个 Kafka 主题（不限于模型主题），可指定分区数、副本因子与主题配置。",
    {
        "topic": {"type": "string", "description": "主题名称", "required": True},
        "num_partitions": {"type": "integer", "description": "分区数", "default": 3},
        "replication_factor": {"type": "integer", "description": "副本因子", "default": 3},
        "config": {
            "type": "object",
            "description": "主题配置，例如 {\"retention.ms\": \"604800000\"}",
        },
    },
)
def kafka_create_topic(args: Dict[str, Any]) -> Any:
    topic = _require(args, "topic")
    result = _state()["admin_client"].create_topics(
        [
            {
                "name": topic,
                "num_partitions": int(args.get("num_partitions", 3)),
                "replication_factor": int(args.get("replication_factor", 3)),
                "config": args.get("config") or {},
            }
        ]
    )
    return {"topic": topic, "success": bool(result.get(topic, False))}


@tool(
    "kafka_delete_topic",
    "删除一个 Kafka 主题。该操作不可恢复，请谨慎调用。",
    {"topic": {"type": "string", "description": "主题名称", "required": True}},
)
def kafka_delete_topic(args: Dict[str, Any]) -> Any:
    topic = _require(args, "topic")
    result = _state()["admin_client"].delete_topics([topic])
    return {"topic": topic, "success": bool(result.get(topic, False))}


@tool(
    "kafka_get_topic_config",
    "读取指定主题的全部配置项。",
    {"topic": {"type": "string", "description": "主题名称", "required": True}},
)
def kafka_get_topic_config(args: Dict[str, Any]) -> Any:
    return _state()["admin_client"].get_topic_config(_require(args, "topic"))


@tool(
    "kafka_set_topic_config",
    "修改指定主题的配置项，例如 retention.ms、max.message.bytes、cleanup.policy。",
    {
        "topic": {"type": "string", "description": "主题名称", "required": True},
        "config": {
            "type": "object",
            "description": "要更新的配置键值对",
            "required": True,
        },
    },
)
def kafka_set_topic_config(args: Dict[str, Any]) -> Any:
    topic = _require(args, "topic")
    config = _require(args, "config")
    if not isinstance(config, dict):
        raise ValueError("config 必须是对象")
    success = _state()["admin_client"].update_topic_config(topic, config)
    return {"topic": topic, "success": bool(success)}


# ---------------------------------------------------------------------------
# 消费者组
# ---------------------------------------------------------------------------

@tool(
    "kafka_list_consumer_groups",
    "列出消费者组；不传 prefix 时按 CONSUMER_GROUP_PREFIX 过滤出本服务的消费者组。",
    {"prefix": {"type": "string", "description": "消费者组 ID 前缀"}},
)
def kafka_list_consumer_groups(args: Dict[str, Any]) -> Any:
    return _state()["consumer_manager"].list_consumer_groups(args.get("prefix"))


@tool(
    "kafka_get_group_offsets",
    "查询消费者组的提交偏移量，返回 {主题: {分区: 偏移量}}。",
    {
        "group_id": {"type": "string", "description": "消费者组 ID", "required": True},
        "topic": {"type": "string", "description": "只查询指定主题，省略则查询全部"},
    },
)
def kafka_get_group_offsets(args: Dict[str, Any]) -> Any:
    group_id = _require(args, "group_id")
    topic = args.get("topic")
    topics = [topic] if topic else None
    return _state()["admin_client"].get_consumer_group_offsets(group_id, topics)


@tool(
    "kafka_get_consumer_lag",
    "查询消费者组在某个模型版本主题上的滞后量摘要（总量/最大/最小/平均值与健康状态）。",
    {
        "group_id": {"type": "string", "description": "消费者组 ID", "required": True},
        "model_id": {"type": "string", "description": "模型 ID", "required": True},
        "version": {"type": "string", "description": "模型版本号", "required": True},
    },
)
def kafka_get_consumer_lag(args: Dict[str, Any]) -> Any:
    manager = _state()["consumer_manager"]
    return manager.get_consumer_lag_summary(
        _require(args, "group_id"), _require(args, "model_id"), _require(args, "version")
    )


@tool(
    "kafka_reset_consumer_offsets",
    "重置消费者组在指定模型版本主题上的偏移量（earliest / latest）。",
    {
        "group_id": {"type": "string", "description": "消费者组 ID", "required": True},
        "model_id": {"type": "string", "description": "模型 ID", "required": True},
        "version": {"type": "string", "description": "模型版本号", "required": True},
        "offset": {
            "type": "string",
            "description": "重置位置",
            "enum": ["earliest", "latest"],
            "default": "earliest",
        },
    },
)
def kafka_reset_consumer_offsets(args: Dict[str, Any]) -> Any:
    manager = _state()["consumer_manager"]
    success = manager.reset_consumer_offsets(
        _require(args, "group_id"),
        _require(args, "model_id"),
        _require(args, "version"),
        args.get("offset", "earliest"),
    )
    return {"success": bool(success)}


# ---------------------------------------------------------------------------
# 模型与版本
# ---------------------------------------------------------------------------

@tool(
    "model_register",
    "注册模型：创建模型实体，并为其初始版本创建 Kafka 主题（mcp.model.<model_id>.v<version>）。",
    {
        "model_id": {"type": "string", "description": "模型 ID，唯一标识", "required": True},
        "name": {"type": "string", "description": "模型名称", "required": True},
        "version": {"type": "string", "description": "初始版本号，如 1.0.0", "required": True},
        "description": {"type": "string", "description": "模型描述"},
        "partitions": {"type": "integer", "description": "主题分区数", "default": 3},
        "replication_factor": {"type": "integer", "description": "副本因子", "default": 3},
        "retention_ms": {"type": "integer", "description": "数据保留时长（毫秒）"},
        "metadata": {"type": "object", "description": "模型元数据"},
    },
)
def model_register(args: Dict[str, Any]) -> Any:
    from mcp.models.model import Model, ModelConfig, ModelVersion

    state = _state()
    model_id = _require(args, "model_id")
    if model_id in state["models"]:
        raise ValueError("模型 ID 已存在: %s" % model_id)

    config_kwargs = {}
    for key in ("partitions", "replication_factor", "retention_ms"):
        if args.get(key) is not None:
            config_kwargs[key] = int(args[key])
    model_config = ModelConfig(**config_kwargs)

    model = Model(
        model_id=model_id,
        name=_require(args, "name"),
        description=args.get("description"),
        metadata=args.get("metadata"),
    )
    version = ModelVersion(
        version=str(_require(args, "version")),
        config=model_config,
        metadata=args.get("metadata"),
    )
    topic_name = state["topic_manager"].get_model_topic_name(model_id, version.version)
    version.topic_name = topic_name

    success = state["topic_manager"].create_model_topic(
        model_id,
        version.version,
        partitions=version.config.partitions,
        replication_factor=version.config.replication_factor,
        config={"retention.ms": str(version.config.retention_ms)},
    )
    if not success:
        raise RuntimeError("创建模型主题失败: %s" % topic_name)

    model.add_version(version)
    state["models"][model_id] = model
    summary = _model_summary(model)
    summary["topic_name"] = topic_name
    return summary


@tool("model_list", "列出当前进程中已注册的全部模型及其版本。")
def model_list(args: Dict[str, Any]) -> Any:
    return [_model_summary(m) for m in _state()["models"].values()]


@tool(
    "model_get",
    "获取单个模型的详细信息（含版本列表与默认版本）。",
    {"model_id": {"type": "string", "description": "模型 ID", "required": True}},
)
def model_get(args: Dict[str, Any]) -> Any:
    model_id = _require(args, "model_id")
    model = _state()["models"].get(model_id)
    if model is None:
        raise ValueError("模型不存在: %s" % model_id)
    return _model_summary(model)


@tool(
    "model_delete",
    "删除模型及其全部版本对应的 Kafka 主题。该操作不可恢复。",
    {"model_id": {"type": "string", "description": "模型 ID", "required": True}},
)
def model_delete(args: Dict[str, Any]) -> Any:
    state = _state()
    model_id = _require(args, "model_id")
    if model_id not in state["models"]:
        raise ValueError("模型不存在: %s" % model_id)
    success = state["topic_manager"].delete_model_topic(model_id)
    if not success:
        raise RuntimeError("删除模型主题失败: %s" % model_id)
    del state["models"][model_id]
    return {"model_id": model_id, "deleted": True}


@tool(
    "model_version_create",
    "为已有模型创建新版本，并创建隔离的新主题（支持独立分区/副本/保留策略）。",
    {
        "model_id": {"type": "string", "description": "模型 ID", "required": True},
        "version": {"type": "string", "description": "新版本号", "required": True},
        "partitions": {"type": "integer", "description": "主题分区数"},
        "replication_factor": {"type": "integer", "description": "副本因子"},
        "retention_ms": {"type": "integer", "description": "数据保留时长（毫秒）"},
        "metadata": {"type": "object", "description": "版本元数据"},
    },
)
def model_version_create(args: Dict[str, Any]) -> Any:
    from mcp.models.model import ModelConfig, ModelVersion

    state = _state()
    model_id = _require(args, "model_id")
    model = state["models"].get(model_id)
    if model is None:
        raise ValueError("模型不存在: %s" % model_id)

    version_str = str(_require(args, "version"))
    if model.get_version(version_str) is not None:
        raise ValueError("版本已存在: %s" % version_str)

    config_kwargs = {}
    for key in ("partitions", "replication_factor", "retention_ms"):
        if args.get(key) is not None:
            config_kwargs[key] = int(args[key])

    version = ModelVersion(
        version=version_str,
        config=ModelConfig(**config_kwargs),
        metadata=args.get("metadata"),
    )
    topic_name = state["topic_manager"].get_model_topic_name(model_id, version_str)
    version.topic_name = topic_name

    success = state["topic_manager"].create_model_topic(
        model_id,
        version_str,
        partitions=version.config.partitions,
        replication_factor=version.config.replication_factor,
        config={"retention.ms": str(version.config.retention_ms)},
    )
    if not success:
        raise RuntimeError("创建模型版本主题失败: %s" % topic_name)

    model.add_version(version)
    return _version_summary(version)


@tool(
    "model_version_list",
    "列出指定模型的全部版本。",
    {"model_id": {"type": "string", "description": "模型 ID", "required": True}},
)
def model_version_list(args: Dict[str, Any]) -> Any:
    model_id = _require(args, "model_id")
    model = _state()["models"].get(model_id)
    if model is None:
        raise ValueError("模型不存在: %s" % model_id)
    return [_version_summary(v) for v in model.versions]


@tool(
    "model_version_delete",
    "删除模型的指定版本及其主题。",
    {
        "model_id": {"type": "string", "description": "模型 ID", "required": True},
        "version": {"type": "string", "description": "版本号", "required": True},
    },
)
def model_version_delete(args: Dict[str, Any]) -> Any:
    state = _state()
    model_id = _require(args, "model_id")
    version = str(_require(args, "version"))
    model = state["models"].get(model_id)
    if model is None:
        raise ValueError("模型不存在: %s" % model_id)
    if model.get_version(version) is None:
        raise ValueError("版本不存在: %s" % version)

    success = state["topic_manager"].delete_model_topic(model_id, version)
    if not success:
        raise RuntimeError("删除模型版本主题失败: %s" % version)
    model.remove_version(version)
    return {"model_id": model_id, "version": version, "deleted": True}


@tool(
    "model_set_default_version",
    "把模型的某个版本设为默认版本（用于版本回滚/切换）。",
    {
        "model_id": {"type": "string", "description": "模型 ID", "required": True},
        "version": {"type": "string", "description": "版本号", "required": True},
    },
)
def model_set_default_version(args: Dict[str, Any]) -> Any:
    model_id = _require(args, "model_id")
    version = str(_require(args, "version"))
    model = _state()["models"].get(model_id)
    if model is None:
        raise ValueError("模型不存在: %s" % model_id)
    if not model.set_default_version(version):
        raise ValueError("版本不存在: %s" % version)
    return _model_summary(model)


# ---------------------------------------------------------------------------
# 消息与协议
# ---------------------------------------------------------------------------

@tool(
    "model_publish",
    "向指定模型版本的主题发送一条 MCP 协议消息（JSON 序列化，发送后立即 flush）。",
    {
        "model_id": {"type": "string", "description": "模型 ID", "required": True},
        "version": {"type": "string", "description": "版本号", "required": True},
        "content": {"type": "object", "description": "消息内容（对象或数组）", "required": True},
        "content_type": {"type": "string", "description": "内容类型，如 application/json"},
        "metadata": {"type": "object", "description": "消息元数据"},
    },
)
def model_publish(args: Dict[str, Any]) -> Any:
    success = _producer().send(
        _require(args, "model_id"),
        str(_require(args, "version")),
        _require(args, "content"),
        content_type=args.get("content_type"),
        metadata=args.get("metadata"),
    )
    if not success:
        raise RuntimeError("消息发送失败（协议校验、序列化或投递失败）")
    return {"sent": True}


@tool(
    "mcp_validate_message",
    "校验一条 MCP 协议消息是否符合规范（model_id / version 必填，version 需为 x.y.z）。",
    {
        "message": {
            "type": "object",
            "description": "待校验的消息对象",
            "required": True,
        }
    },
)
def mcp_validate_message(args: Dict[str, Any]) -> Any:
    from mcp.protocol.validator import MCPValidator

    message = _require(args, "message")
    if not isinstance(message, dict):
        raise ValueError("message 必须是对象")
    result = MCPValidator().validate(message)
    return {"valid": bool(result), "errors": list(result.errors)}


# ---------------------------------------------------------------------------
# ACL
# ---------------------------------------------------------------------------

@tool(
    "model_acl_create",
    "为模型版本主题创建 ACL 授权。",
    {
        "model_id": {"type": "string", "description": "模型 ID", "required": True},
        "version": {"type": "string", "description": "版本号", "required": True},
        "principal": {"type": "string", "description": "主体，如 User:alice", "required": True},
        "operations": {
            "type": "array",
            "description": "操作列表，如 [\"READ\", \"WRITE\"]",
            "items": {"type": "string"},
            "required": True,
        },
        "allow": {"type": "boolean", "description": "true 为允许，false 为拒绝", "default": True},
    },
)
def model_acl_create(args: Dict[str, Any]) -> Any:
    operations = _require(args, "operations")
    success = _state()["acl_manager"].create_model_acl(
        _require(args, "model_id"),
        str(_require(args, "version")),
        _require(args, "principal"),
        operations,
        bool(args.get("allow", True)),
    )
    return {"success": bool(success)}


@tool(
    "model_acl_list",
    "列出模型版本主题上的 ACL。",
    {
        "model_id": {"type": "string", "description": "模型 ID", "required": True},
        "version": {"type": "string", "description": "版本号", "required": True},
    },
)
def model_acl_list(args: Dict[str, Any]) -> Any:
    return _state()["acl_manager"].list_model_acls(
        _require(args, "model_id"), str(_require(args, "version"))
    )


@tool(
    "model_acl_delete",
    "删除模型版本主题上的 ACL；不传 principal 时删除该主题的全部 ACL。",
    {
        "model_id": {"type": "string", "description": "模型 ID", "required": True},
        "version": {"type": "string", "description": "版本号", "required": True},
        "principal": {"type": "string", "description": "主体，如 User:alice"},
    },
)
def model_acl_delete(args: Dict[str, Any]) -> Any:
    success = _state()["acl_manager"].delete_model_acl(
        _require(args, "model_id"),
        str(_require(args, "version")),
        args.get("principal"),
    )
    return {"success": bool(success)}


@tool(
    "model_acl_create_role",
    "按角色为模型版本主题批量授权。",
    {
        "model_id": {"type": "string", "description": "模型 ID", "required": True},
        "version": {"type": "string", "description": "版本号", "required": True},
        "role": {
            "type": "string",
            "description": "角色",
            "enum": ["admin", "producer", "consumer"],
            "required": True,
        },
    },
)
def model_acl_create_role(args: Dict[str, Any]) -> Any:
    role = _require(args, "role")
    if role not in ("admin", "producer", "consumer"):
        raise ValueError("不支持的角色: %s（可选 admin / producer / consumer）" % role)
    success = _state()["acl_manager"].create_role_based_acl(
        _require(args, "model_id"), str(_require(args, "version")), role
    )
    return {"success": bool(success), "role": role}


# ---------------------------------------------------------------------------
# JSON-RPC / MCP 协议处理
# ---------------------------------------------------------------------------

def _response(request_id: Any, result: Any) -> Dict[str, Any]:
    return {"jsonrpc": "2.0", "id": request_id, "result": result}


def _error(request_id: Any, code: int, message: str) -> Dict[str, Any]:
    return {"jsonrpc": "2.0", "id": request_id, "error": {"code": code, "message": message}}


def _list_tools() -> List[Dict[str, Any]]:
    return [
        {"name": t["name"], "description": t["description"], "inputSchema": t["inputSchema"]}
        for t in TOOLS
    ]


def _call_tool(params: Dict[str, Any]) -> Dict[str, Any]:
    name = params.get("name")
    arguments = params.get("arguments") or {}
    if name not in _TOOL_HANDLERS:
        return _error(params.get("__id__"), -32602, "未知工具: %s" % name)
    try:
        result = _TOOL_HANDLERS[name](arguments)
        text = json.dumps(_jsonable(result), ensure_ascii=False, indent=2)
        return {"content": [{"type": "text", "text": text}], "isError": False}
    except Exception as exc:  # 工具执行失败通过 isError 返回给客户端
        logger.error("工具 %s 执行失败: %s\n%s", name, exc, traceback.format_exc())
        return {
            "content": [{"type": "text", "text": "工具执行失败: %s" % exc}],
            "isError": True,
        }


def handle_message(message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """处理单条 JSON-RPC 消息，返回响应（通知类消息返回 None）。"""
    if not isinstance(message, dict):
        return _error(None, -32600, "Invalid Request")

    request_id = message.get("id")
    method = message.get("method")
    params = message.get("params") or {}

    if method is None:
        return _error(request_id, -32600, "Invalid Request")

    if method == "initialize":
        client_version = params.get("protocolVersion")
        return _response(
            request_id,
            {
                "protocolVersion": client_version or PROTOCOL_VERSION,
                "capabilities": {"tools": {"listChanged": False}},
                "serverInfo": {"name": SERVER_NAME, "version": SERVER_VERSION},
            },
        )

    if method == "notifications/initialized" or (isinstance(method, str) and method.startswith("notifications/")):
        return None

    if method == "ping":
        return _response(request_id, {})

    if method == "tools/list":
        return _response(request_id, {"tools": _list_tools()})

    if method == "tools/call":
        if not isinstance(params, dict):
            return _error(request_id, -32602, "Invalid params")
        if params.get("name") not in _TOOL_HANDLERS:
            return _error(request_id, -32602, "未知工具: %s" % params.get("name"))
        return _response(request_id, _call_tool(params))

    if method == "resources/list":
        return _response(request_id, {"resources": []})

    if method == "prompts/list":
        return _response(request_id, {"prompts": []})

    return _error(request_id, -32601, "Method not found: %s" % method)


def _write_message(stream, payload: Any) -> None:
    stream.write(json.dumps(payload, ensure_ascii=False) + "\n")
    stream.flush()


def serve(stdin=None, stdout=None) -> int:
    """stdio 主循环：逐行读取 JSON-RPC 请求，逐行写回响应。"""
    stdin = stdin or sys.stdin
    stdout = stdout or sys.stdout
    logger.info("%s %s 已启动，等待 MCP 客户端请求", SERVER_NAME, SERVER_VERSION)

    for raw_line in stdin:
        line = raw_line.strip()
        if not line:
            continue
        try:
            message = json.loads(line)
        except json.JSONDecodeError as exc:
            _write_message(stdout, _error(None, -32700, "Parse error: %s" % exc))
            continue

        if isinstance(message, list):
            responses = [r for r in (handle_message(m) for m in message) if r is not None]
            if responses:
                _write_message(stdout, responses)
            continue

        response = handle_message(message)
        if response is not None:
            _write_message(stdout, response)

    return 0


def main() -> int:
    try:
        return serve()
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    sys.exit(main())
