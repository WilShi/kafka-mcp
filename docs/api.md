# Kafka MCP 服务 HTTP API

服务默认监听 `http://0.0.0.0:8000`（可通过 `API_HOST` / `API_PORT` 覆盖），所有业务接口以 `/api` 为前缀，返回 JSON。

- 交互式文档：启动服务后访问 `/docs`（Swagger UI）或 `/redoc`
- 健康检查：`GET /health` → `{"status": "ok"}`
- 根路径：`GET /` → `{"message": "欢迎使用Kafka MCP服务"}`

## 错误码约定

| 状态码 | 含义 |
| --- | --- |
| 201 | 资源创建成功 |
| 204 | 删除成功，无响应体 |
| 404 | 模型 / 版本 / 主题 / 消费者组不存在 |
| 409 | 模型 ID 或版本号已存在 |
| 500 | 底层 Kafka 操作失败 |

## 模型与版本管理

### POST /api/models

注册模型，并为其初始版本创建 Kafka 主题（主题名：`MODEL_TOPIC_PREFIX + model_id + .v + version`）。

请求体：

```json
{
  "model_id": "risk-score",
  "name": "风控评分模型",
  "description": "可选",
  "version": "1.0.0",
  "config": {
    "partitions": 3,
    "replication_factor": 3,
    "retention_ms": 604800000,
    "config": {"cleanup.policy": "delete"}
  },
  "metadata": {"owner": "ml-platform"}
}
```

响应（201）：

```json
{
  "model_id": "risk-score",
  "name": "风控评分模型",
  "description": "可选",
  "created_at": "2026-01-01T10:00:00",
  "updated_at": "2026-01-01T10:00:00",
  "versions": ["1.0.0"],
  "default_version": "1.0.0",
  "metadata": {"owner": "ml-platform"}
}
```

### GET /api/models

返回全部模型（数组，元素结构同上）。

### GET /api/models/{model_id}

返回单个模型，不存在时 404。

### DELETE /api/models/{model_id}

删除模型及其所有版本对应的 Kafka 主题，成功返回 204。

### POST /api/models/{model_id}/versions

创建新版本并创建对应主题。

```json
{
  "version": "1.1.0",
  "config": {"partitions": 6, "replication_factor": 3, "retention_ms": 604800000},
  "metadata": {"note": "rollout"}
}
```

响应（201）：

```json
{
  "version": "1.1.0",
  "created_at": "2026-01-02T09:00:00",
  "topic_name": "mcp.model.risk-score.v1.1.0",
  "config": {"partitions": 6, "replication_factor": 3, "retention_ms": 604800000, "config": null},
  "metadata": {"note": "rollout"},
  "active": true
}
```

### GET /api/models/{model_id}/versions

返回模型全部版本（数组）。

### GET /api/models/{model_id}/versions/{version}

返回单个版本。

### DELETE /api/models/{model_id}/versions/{version}

删除该版本及其主题，成功返回 204。

### POST /api/models/{model_id}/default-version/{version}

把指定版本设为模型默认版本，返回更新后的模型对象。

## 消费者组监控

### GET /api/consumers/{group_id}/lag

查询消费者组滞后量。

- 不带查询参数：返回该组下所有模型版本主题的滞后摘要
- 带 `?model_id=risk-score&version=1.0.0`：只返回该主题的摘要

响应：

```json
{
  "risk-score.1.0.0": {
    "status": "ok",
    "total_lag": 120,
    "max_lag": 80,
    "min_lag": 40,
    "avg_lag": 60,
    "partitions": 2
  }
}
```

`status` 取值：`ok`、`warning`（单分区滞后 > 10000）、`critical`（单分区滞后 > 100000）、`unknown`（无可用偏移量）。

## 集群与主题管理

### GET /api/admin/cluster/metadata

返回集群 ID、Controller ID、Broker 列表与主题概览：

```json
{
  "cluster_id": "demo-cluster",
  "controller_id": 1,
  "brokers": {"1": {"host": "localhost", "port": 9092}},
  "topics": {"mcp.model.risk-score.v1.0.0": {"partitions": 3, "error": null}}
}
```

### GET /api/admin/topics

返回主题名到分区信息的映射。

### POST /api/admin/topics

创建任意主题（不限于模型主题）。

```json
{
  "name": "audit.log",
  "num_partitions": 3,
  "replication_factor": 3,
  "config": {"retention.ms": "604800000"}
}
```

响应（201）：`{"name": "audit.log", "success": true}`

### DELETE /api/admin/topics/{topic_name}

删除主题，成功返回 204。

### GET /api/admin/topics/{topic_name}/config

返回主题配置字典；主题不存在或读取失败时 404。

### PUT /api/admin/topics/{topic_name}/config

```json
{"config": {"retention.ms": "86400000", "max.message.bytes": "1048576"}}
```

成功返回 `{"name": "audit.log", "success": true}`。

### GET /api/admin/consumer-groups?prefix=mcp.consumer.

返回消费者组 ID 列表；`prefix` 缺省时按 `CONSUMER_GROUP_PREFIX` 过滤。

### GET /api/admin/consumer-groups/{group_id}/offsets?topic=audit.log

返回 `{"主题": {"分区": 偏移量}}`；组不存在或未订阅时 404。

### POST /api/admin/consumer-groups/{group_id}/reset?model_id=risk-score&version=1.0.0

重置消费者组在指定模型版本主题上的偏移量。

```json
{"offset": "earliest"}
```

`offset` 支持 `earliest`、`latest` 或具体偏移量数值。

## ACL 权限管理

### POST /api/admin/models/{model_id}/versions/{version}/acl

```json
{
  "principal": "User:alice",
  "operations": ["READ", "WRITE", "DESCRIBE"],
  "allow": true
}
```

### GET /api/admin/models/{model_id}/versions/{version}/acl

返回该模型版本主题上的 ACL 列表。

### DELETE /api/admin/models/{model_id}/versions/{version}/acl?principal=User:alice

删除 ACL；不传 `principal` 时删除该主题上的全部 ACL。

### POST /api/admin/models/{model_id}/versions/{version}/role/{role}

按角色批量授权，`role` 可选：

| 角色 | 授权操作 |
| --- | --- |
| `admin` | READ / WRITE / CREATE / DELETE / ALTER / DESCRIBE / DESCRIBE_CONFIGS / ALTER_CONFIGS |
| `producer` | WRITE / DESCRIBE |
| `consumer` | READ / DESCRIBE |

## 调用示例

```bash
# 注册模型
curl -X POST http://localhost:8000/api/models \
  -H 'Content-Type: application/json' \
  -d '{"model_id":"risk-score","name":"风控评分模型","version":"1.0.0"}'

# 查看集群元数据
curl http://localhost:8000/api/admin/cluster/metadata

# 查询消费者组滞后
curl http://localhost:8000/api/consumers/my-app/lag
```
