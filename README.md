# 实时数据入湖平台

面向流式数据接入、实时转换、增量合并与湖上物化查询的实时数据入湖治理平台。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/            每个业务模块一个页面
│   ├── src/api/              统一请求封装
│   ├── src/stores/           会话与筛选状态
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/          每个业务模块一组接口
│   ├── app/services/         业务规则与状态流转
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 主题注册 | `topic_registry` | 数据主题 | 主题编号、主题名称、消息格式 |
| 流式计算 | `flink_job` | 流式任务 | 任务编号、消费主题、转换逻辑 |
| 消息接入 | `kafka_connector` | 接入配置 | 配置编号、集群地址、消费组名 |
| Schema演进 | `schema_evolve` | 演进规则 | 规则编号、关联主题、新增字段 |
| 增量合并 | `upsert_merge` | 合并策略 | 策略编号、目标表名、合并键列 |
| 湖上分区 | `lake_partition` | 分区定义 | 分区编号、目标表名、分区列名 |
| 流式质量 | `stream_quality` | 质量规则 | 规则编号、监控主题、校验类型 |
| Iceberg管理 | `iceberg_table` | 表元数据 | 表编号、表名称、存储格式 |
| 水位追踪 | `watermark_track` | 水位标记 | 标记编号、数据主题、分区编号 |
| 死信处理 | `dead_letter` | 死信记录 | 记录编号、原始主题、原始分区 |
| 物化查询 | `materialized_view` | 物化视图 | 视图编号、查询SQL、基表名 |
| CDC捕获 | `change_capture` | CDC配置 | 配置编号、源数据库、源表名 |
| 合并清理 | `compact_policy` | 清理策略 | 策略编号、目标表名、触发条件 |
| 流式血缘 | `stream_lineage` | 血缘记录 | 记录编号、上游主题、下游表名 |
| 流式补数 | `backfill_stream` | 补数任务 | 任务编号、目标表名、起始偏移 |
| 下沉分发 | `sink_dispatch` | 分发规则 | 规则编号、湖表名、目标系统 |
| 数据生命周期 | `ttl_policy` | 生命周期 | 策略编号、目标表名、热数据保留 |
| 实时指标 | `realtime_metrics` | 指标定义 | 指标编号、指标名称、聚合窗口 |

## 死信重放映射

死信处理不再只是把记录标成「已重放」：重放时会落到一条**死信重放映射数据**上，
说明这条消息来自哪个上游分区、进入哪个下游转换节点和下游表。

- **映射数据**：`dead_letter_replay_mappings`，保留映射编号、批次、版本、死信编号、
  原始主题、原始分区、转换节点、下游表名、血缘记录编号、解析结果、主重放与映射状态。
- **血缘边**：`dead_letter_replay_edges`，与映射成对写入，构成「上游分区 → 下游转换节点」的边；
  流式血缘页读取当前有效版本后直接画到 DAG 上（虚线）。
- **批次**：`dead_letter_replay_batches`，记录处理路线、版本、计划数、已确认数与中断位置。

处理路线的取舍：

| 路线 | 行为 | 血缘边 |
| --- | --- | --- |
| 首个节点接收 | 每条死信只写首个可接收的转换节点 | 每个分区 1 条主重放边 |
| 完整映射形成 | 每条死信为所有可接收节点各写一条 | 每个分区 N 条边，仅 1 条主重放 |

换路线会整体收口并提升版本，旧版本映射与边标记失效，未完成批次标记「已作废」，
因此任一时刻只有一套有效边集；同路线再次生成视为续跑，只补未完成项。

中断与恢复：生成/恢复都支持 `interrupt_after`，已确认的映射和边先落库；
恢复时按「死信编号 + 转换节点」比对，只补齐缺口，不重写已确认数据。

主要接口（前缀 `/api/dead_letter`）：

| 接口 | 说明 |
| --- | --- |
| `POST /replay/generate` | 解析订单主题死信并生成映射，支持 `route`、`interrupt_after`、`force` |
| `POST /replay/resume` | 恢复中断批次，保留已确认项、只补齐未完成项 |
| `GET /replay/mappings` | 读取映射数据，可按版本过滤 |
| `GET /replay/batches` | 批次列表与中断位置 |
| `GET /replay/check` | 校验主重放唯一、映射与边成对写入、待补齐数量 |
| `GET /replay/export` / `POST /replay/import` | 导出/上传重放清单（CSV；上传也接受打包 zip） |
| `GET /package` | 打包下载死信清单、映射、血缘边与批次清单 |
| `GET /api/stream_lineage/replay-mappings` | 另一个业务入口，读取同一份映射数据 |

页面：死信处理页可生成/恢复/校验/导入导出映射，流式血缘页展示同一份映射及其边；
两页读取同一份数据，结果保持一致。

## 约定

- 每个模块的前端页面在 `frontend/src/views/<模块>/index.vue`，后端接口在
  `backend/app/routers/<模块>.py`，业务规则在 `backend/app/services/<模块>.py`。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

领域：数据工程
子方向：数仓与数据湖
