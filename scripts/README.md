# scripts

本目录存放与 MCP 服务运行无关的可选辅助脚本，部署 MCP/HTTP 服务时不需要它们。

## auto_kafka_script.sh

Linux 下用于 Kafka 集群日常运维的交互式脚本，封装了 Kafka 发行版自带的 `kafka-*.sh` 工具：

| 能力 | 说明 |
| --- | --- |
| 主题管理 | 创建 / 删除 / 查看主题，扩分区，按副本分配创建主题 |
| 主题配置 | 增加、查看、删除主题配置项 |
| 集群运维 | 集群状态、分区迁移、积压（backlog）查看、Leader 选举、压测 |
| 安全 | 设置 ACL、创建 SCRAM 用户 |
| 服务管理 | 启停 / 重启本地 Kafka 服务 |

### 前置条件

- Linux 环境（依赖 `sudo`、`ifconfig`/ip、`sed`、`awk`）
- 本机已安装/解压 Kafka 发行版（脚本会自动探测 `KAFKA_DIR`，也可交互输入）
- 启停服务类命令需要 root 或 Kafka 运行用户权限

### 用法

```bash
bash scripts/auto_kafka_script.sh --help   # 查看全部命令
bash scripts/auto_kafka_script.sh          # 交互式菜单
```

> 注意：脚本中部分命令使用 Zookeeper 模式（`--zookeeper`），适用于仍依赖 Zookeeper 的 Kafka 版本；
> Kafka 3.5+ 已移除 Zookeeper 模式，请按需替换为 `--bootstrap-server`。
> 脚本不含任何真实主机地址或凭据，连接信息均在运行时交互输入。
