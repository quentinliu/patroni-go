# patroni-go

Patroni 4.1.5 的 Go 核心重写（MVP）。对 Python 原版做行为对照，保留主链路：选主、故障转移、bootstrap、跟随、降级、REST API、etcd3 DCS。

## 构建

```bash
export PATH=$HOME/sdk/go/bin:$PATH   # 本机 Go 1.23 安装位置
go build -o bin/patroni ./cmd/patroni
```

## 配置

参考 `postgres0.yml`。支持的顶层键：`name`、`scope`、`namespace`、`ttl`、`loop_wait`、`retry_timeout`、`restapi`、`etcd3`、`postgresql`、`bootstrap`。支持 `PATRONI_*` 环境变量覆盖（含下划线嵌套）。

## 运行

```bash
./bin/patroni --validate-config postgres0.yml
./bin/patroni postgres0.yml
```

单二进制，无 Python 运行时依赖。

## REST API（对照 api.py 核心端点）

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/` `/primary` | 主库健康检查（200/503） |
| GET | `/replica` `/read-only` | 副本健康检查 |
| GET | `/liveness` `/health` `/readiness` | 存活/就绪 |
| GET | `/patroni` `/cluster` `/history` `/config` | 状态 JSON |
| GET | `/metrics` | 简单 Prometheus 文本 |
| POST | `/restart` `/failover` `/switchover` `/reinitialize` `/sigterm` `/reload` | 管理动作（需 basic auth 时配置 `restapi.authentication`） |

## MVP 边界（与 Python 版的显式差异）

- **仅 etcd3** DCS（Go 官方客户端，替代 Python 版 1099 行手写 gRPC-gateway hack）
- **无** pg_rewind / 复制槽管理 / 同步复制 / quorum / Citus / standby cluster / failsafe 模式 / watchdog 设备（接口已留）
- 动作同步执行（无 Python 的 AsyncExecutor），bootstrap 仅 initdb 路径，克隆仅 pg_basebackup
- `POST /config` 未实现（501）

## 代码结构

```
cmd/patroni/          daemon 入口（信号、主循环）
internal/config/      YAML + PATRONI_* 环境变量 + 动态配置
internal/dcs/         Cluster 模型 + DCS 接口（对照 dcs/__init__.py）
internal/dcs/etcd3/   etcd v3 后端（对照 dcs/etcd3.py）
internal/postgres/    进程控制/角色检测/配置文件/bootstrap（对照 postgresql/）
internal/ha/          HA 状态机（对照 ha.py _run_cycle 分发顺序）
internal/api/         REST API（对照 api.py）
internal/watchdog/    看门狗接口（默认 Noop）
```

各文件 doc comment 标注了对应的 Python 源位置，便于行为核对。

## 已验证 / 待验证

- ✅ `go build ./... && go vet ./...` 通过；单二进制产出；`--validate-config`、无 etcd 时的 FATAL 路径
- ⏸ 三节点集群选主/切换需在具备 etcd + PostgreSQL 的 Linux 环境端到端验证（本机网络无法下载 etcd）

## 后续路线

1. Linux 环境三节点端到端（etcd + PG16）：initdb bootstrap → 杀主 → 自动切换
2. 补 pg_rewind 与复制槽（时间线分叉处理）
3. `patronictl` CLI（REST API 已齐备，纯客户端）
4. 其余 DCS 后端（consul/zookeeper 均有成熟 Go 客户端）
