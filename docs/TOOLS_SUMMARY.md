# Airflow MCP Tools 总结

本文档基于项目根目录 `openapi.json` 解析生成，汇总 Airflow REST API 暴露的 MCP 工具。

---

## 概览

| 项目 | 数量 |
|------|------|
| **工具总数** | 73 |
| **GET** | 42 |
| **POST** | 12 |
| **PATCH** | 16 |
| **DELETE** | 7 |
| **分类 (tag)** | 18 |

> 使用 `--safe` 启动 MCP 时，仅会暴露 GET 类工具（只读）。

---

## 按分类索引

| 分类 | 工具数 | 说明 |
|------|--------|------|
| [Config](#config) | 1 | 配置 |
| [Connection](#connection) | 6 | 连接管理 |
| [DAG](#dag) | 11 | DAG 与任务定义 |
| [DAGRun](#dagrun) | 9 | DAG 运行与触发 |
| [DagWarning](#dagwarning) | 1 | DAG 告警 |
| [Dataset](#dataset) | 3 | 数据集 |
| [EventLog](#eventlog) | 2 | 事件日志 |
| [ImportError](#importerror) | 2 | 导入错误 |
| [Monitoring](#monitoring) | 2 | 健康与版本 |
| [Permission](#permission) | 1 | 权限 |
| [Plugin](#plugin) | 1 | 插件 |
| [Pool](#pool) | 5 | 资源池 |
| [Provider](#provider) | 1 | Provider |
| [Role](#role) | 5 | 角色 |
| [TaskInstance](#taskinstance) | 11 | 任务实例与日志 |
| [User](#user) | 5 | 用户 |
| [Variable](#variable) | 5 | 变量 |
| [XCom](#xcom) | 2 | XCom |

---

## 各分类工具明细

### Config

| 方法 | 工具名 | 路径 | 说明 |
|------|--------|------|------|
| GET | `get_config` | `/config` | 获取当前配置 |

---

### Connection

| 方法 | 工具名 | 路径 | 说明 |
|------|--------|------|------|
| GET | `get_connections` | `/connections` | 列出连接 |
| GET | `get_connection` | `/connections/{connection_id}` | 获取单个连接 |
| POST | `post_connection` | `/connections` | 创建连接 |
| PATCH | `patch_connection` | `/connections/{connection_id}` | 更新连接 |
| DELETE | `delete_connection` | `/connections/{connection_id}` | 删除连接 |
| POST | `test_connection` | `/connections/test` | 测试连接 |

---

### DAG

| 方法 | 工具名 | 路径 | 说明 |
|------|--------|------|------|
| GET | `get_dags` | `/dags` | 列出 DAG（支持 dag_id_pattern） |
| GET | `get_dag` | `/dags/{dag_id}` | 获取 DAG 基础信息 |
| GET | `get_dag_details` | `/dags/{dag_id}/details` | 获取 DAG 详情（数据量较大） |
| GET | `get_dag_source` | `/dagSources/{file_token}` | 根据 file_token 获取 DAG 源码 |
| GET | `get_tasks` | `/dags/{dag_id}/tasks` | 获取 DAG 下任务列表 |
| GET | `get_task` | `/dags/{dag_id}/tasks/{task_id}` | 获取任务简化信息 |
| PATCH | `patch_dag` | `/dags/{dag_id}` | 更新单个 DAG |
| PATCH | `patch_dags` | `/dags` | 按 dag_id_pattern 批量更新 DAG（可用 `~` 表示全部） |
| POST | `post_clear_task_instances` | `/dags/{dag_id}/clearTaskInstances` | 按日期范围清除任务实例 |
| POST | `post_set_task_instances_state` | `/dags/{dag_id}/updateTaskInstancesState` | 批量更新任务实例状态 |
| DELETE | `delete_dag` | `/dags/{dag_id}` | 删除 DAG 及其元数据（不可恢复，日志不删） |

---

### DAGRun

| 方法 | 工具名 | 路径 | 说明 |
|------|--------|------|------|
| GET | `get_dag_runs` | `/dags/{dag_id}/dagRuns` | 列出 DAG 运行（dag_id 可用 `~` 表示全部） |
| GET | `get_dag_run` | `/dags/{dag_id}/dagRuns/{dag_run_id}` | 获取单次 DAG 运行 |
| POST | `get_dag_runs_batch` | `/dags/~/dagRuns/list` | 批量查询 DAG 运行（POST 避免 URL 过长） |
| POST | `post_dag_run` | `/dags/{dag_id}/dagRuns` | 触发一次 DAG 运行 |
| PATCH | `update_dag_run_state` | `/dags/{dag_id}/dagRuns/{dag_run_id}` | 修改 DAG 运行状态 |
| PATCH | `set_dag_run_note` | `/dags/{dag_id}/dagRuns/{dag_run_id}/setNote` | 设置 DAG 运行备注 |
| POST | `clear_dag_run` | `/dags/{dag_id}/dagRuns/{dag_run_id}/clear` | 清除 DAG 运行 |
| DELETE | `delete_dag_run` | `/dags/{dag_id}/dagRuns/{dag_run_id}` | 删除 DAG 运行 |
| GET | `get_upstream_dataset_events` | `/dags/{dag_id}/dagRuns/{dag_run_id}/upstreamDatasetEvents` | 获取该次运行的上游数据集事件 |

---

### DagWarning

| 方法 | 工具名 | 路径 | 说明 |
|------|--------|------|------|
| GET | `get_dag_warnings` | `/dagWarnings` | 列出 DAG 告警 |

---

### Dataset

| 方法 | 工具名 | 路径 | 说明 |
|------|--------|------|------|
| GET | `get_datasets` | `/datasets` | 列出数据集 |
| GET | `get_dataset` | `/datasets/{uri}` | 按 URI 获取数据集 |
| GET | `get_dataset_events` | `/datasets/events` | 获取数据集事件 |

---

### EventLog

| 方法 | 工具名 | 路径 | 说明 |
|------|--------|------|------|
| GET | `get_event_logs` | `/eventLogs` | 列出事件日志 |
| GET | `get_event_log` | `/eventLogs/{event_log_id}` | 获取单条事件日志 |

---

### ImportError

| 方法 | 工具名 | 路径 | 说明 |
|------|--------|------|------|
| GET | `get_import_errors` | `/importErrors` | 列出导入错误 |
| GET | `get_import_error` | `/importErrors/{import_error_id}` | 获取单个导入错误 |

---

### Monitoring

| 方法 | 工具名 | 路径 | 说明 |
|------|--------|------|------|
| GET | `get_health` | `/health` | 元数据库与调度器健康状态 |
| GET | `get_version` | `/version` | 版本信息 |

---

### Permission

| 方法 | 工具名 | 路径 | 说明 |
|------|--------|------|------|
| GET | `get_permissions` | `/permissions` | 列出权限 |

---

### Plugin

| 方法 | 工具名 | 路径 | 说明 |
|------|--------|------|------|
| GET | `get_plugins` | `/plugins` | 已加载插件列表 |

---

### Pool

| 方法 | 工具名 | 路径 | 说明 |
|------|--------|------|------|
| GET | `get_pools` | `/pools` | 列出资源池 |
| GET | `get_pool` | `/pools/{pool_name}` | 获取单个池 |
| POST | `post_pool` | `/pools` | 创建池 |
| PATCH | `patch_pool` | `/pools/{pool_name}` | 更新池 |
| DELETE | `delete_pool` | `/pools/{pool_name}` | 删除池 |

---

### Provider

| 方法 | 工具名 | 路径 | 说明 |
|------|--------|------|------|
| GET | `get_providers` | `/providers` | Provider 列表 |

---

### Role

| 方法 | 工具名 | 路径 | 说明 |
|------|--------|------|------|
| GET | `get_roles` | `/roles` | 角色列表 |
| GET | `get_role` | `/roles/{role_name}` | 获取角色 |
| POST | `post_role` | `/roles` | 创建角色 |
| PATCH | `patch_role` | `/roles/{role_name}` | 更新角色 |
| DELETE | `delete_role` | `/roles/{role_name}` | 删除角色 |

---

### TaskInstance

| 方法 | 工具名 | 路径 | 说明 |
|------|--------|------|------|
| GET | `get_task_instances` | `/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances` | 列出任务实例（dag_id/dag_run_id 可用 `~`） |
| POST | `get_task_instances_batch` | `/dags/~/dagRuns/~/taskInstances/list` | 批量列出任务实例 |
| GET | `get_task_instance` | `/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}` | 获取任务实例 |
| GET | `get_mapped_task_instances` | `.../taskInstances/{task_id}/listMapped` | 映射任务实例列表 |
| GET | `get_mapped_task_instance` | `.../taskInstances/{task_id}/{map_index}` | 单个映射任务实例 |
| GET | `get_log` | `.../taskInstances/{task_id}/logs/{task_try_number}` | 任务日志 |
| GET | `get_extra_links` | `.../taskInstances/{task_id}/links` | 任务实例扩展链接 |
| PATCH | `patch_task_instance` | `.../taskInstances/{task_id}` | 更新任务实例状态 |
| PATCH | `patch_mapped_task_instance` | `.../taskInstances/{task_id}/{map_index}` | 更新映射任务实例状态 |
| PATCH | `set_task_instance_note` | `.../taskInstances/{task_id}/setNote` | 设置任务实例备注 |
| PATCH | `set_mapped_task_instance_note` | `.../taskInstances/{task_id}/{map_index}/setNote` | 设置映射任务实例备注 |

---

### User

| 方法 | 工具名 | 路径 | 说明 |
|------|--------|------|------|
| GET | `get_users` | `/users` | 用户列表 |
| GET | `get_user` | `/users/{username}` | 获取用户 |
| POST | `post_user` | `/users` | 创建用户 |
| PATCH | `patch_user` | `/users/{username}` | 更新用户 |
| DELETE | `delete_user` | `/users/{username}` | 删除用户 |

---

### Variable

| 方法 | 工具名 | 路径 | 说明 |
|------|--------|------|------|
| GET | `get_variables` | `/variables` | 变量列表（不含值） |
| GET | `get_variable` | `/variables/{variable_key}` | 按 key 获取变量 |
| POST | `post_variables` | `/variables` | 创建变量 |
| PATCH | `patch_variable` | `/variables/{variable_key}` | 更新变量 |
| DELETE | `delete_variable` | `/variables/{variable_key}` | 删除变量 |

---

### XCom

| 方法 | 工具名 | 路径 | 说明 |
|------|--------|------|------|
| GET | `get_xcom_entries` | `.../taskInstances/{task_id}/xcomEntries` | 列出 XCom（dag_id/dag_run_id/task_id 可用 `~`，不含大值） |
| GET | `get_xcom_entry` | `.../taskInstances/{task_id}/xcomEntries/{xcom_key}` | 获取单个 XCom 条目 |

---

## 说明

- **数据来源**：根目录 `openapi.json`（与 MCP 运行时从 Airflow 拉取的 spec 可能不同）。
- **Safe 模式**：仅 GET 会作为 MCP 工具暴露；POST/PATCH/DELETE 需使用 `--unsafe`。
- **路径占位符**：`{dag_id}`、`{dag_run_id}`、`{task_id}` 等需在调用时替换为实际值；部分接口支持 `~` 表示“全部”。
