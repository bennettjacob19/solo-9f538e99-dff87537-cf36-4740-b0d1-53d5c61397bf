# SHA-256 文件校验平台

基于 **Flask + SQLite + Redis Streams** 的制品校验网页与双 worker 服务。

## 功能

- 浏览器上传 **不超过 2 MiB** 的文件，并填写预期 SHA-256（64 位十六进制）。
- 页面分组展示：待校验/校验中（pending/processing）、校验通过（passed）、
  摘要不符（failed），并显示实际摘要、尝试次数、入队次数与错误信息。
- **只有 `passed` 的制品可以下载**；其它状态访问下载接口返回 403。
- 两个 worker 副本以同一消费组（`verifiers`）的不同 consumer 身份运行，
  共享分片、互为冗余。

## 可靠性设计（与需求逐条对应）

1. **已受理上传不会丢失。** 文件落盘与 SQLite 中 `pending` 行的提交先于任何
   Redis 操作；web 进程内置 reconciler 线程，对所有未绑定 stream 消息的
   pending 行反复 `XADD`。入队失败（Redis 宕机）只记录带指数退避的失败计数，
   行保持 pending，恢复后自动派发；web 重启后从数据库继续。
2. **worker 失联可接续。** worker 仅在结论事务提交后才 `XACK`。被 kill -9 时
   消息留在消费组 PEL 中；另一个 worker 通过 `XAUTOCLAIM`（空闲超过
   `CLAIM_MIN_IDLE_MS`，默认 10s）接管。接管时用条件 UPDATE 认领，且只接受
   与当前行绑定消息 ID 一致的消息，避免旧消息误改新任务。
3. **重复处理不产生第二份结论、不改写已确认结果。** 所有状态迁移都是带
   `WHERE status = ?` 条件的事务 UPDATE：
   - `claim_for_processing` 只接受 `pending` 行；
   - `finalize_artifact` 只接受仍是本 worker 持有的 `processing` 行；
   - 终态（passed/failed）行对任何重复/接管消息都直接 ACK 丢弃，永不更新。
4. **读取失败 ≠ 摘要不符。** 哈希过程中的 `OSError`（文件缺失/权限/IO 错误）
   会让行退回 `pending` 并做指数退避重试（保留错误信息），ACK 旧消息后才解绑
   stream 绑定，保证同一作业任何时刻至多一条在途消息；只有成功读取且摘要不同
   才写 `failed`。
5. **重启保持状态与下载资格。** 结论存于 SQLite（WAL 模式，持久卷），
   Redis 开启 AOF。即使 Redis 数据丢失，reconciler 也会扫描卡在
   `processing` 且 PEL 中已无对应消息的行，重置为 pending 重新派发。
6. **下载资格只看数据库确认状态**，不依赖内存或队列状态。

## 目录结构

```
app/
  config.py       环境变量配置
  db.py           SQLite 模式与全部条件状态迁移（幂等核心）
  queue.py        Redis Streams：组、XADD、XREADGROUP、XAUTOCLAIM、XACK
  checksums.py    SHA-256 流式计算与格式校验
  reconciler.py   web 内线程：补发入队失败的任务 + 回收孤儿 processing 行
  worker.py       校验 worker 进程
  web.py          Flask 页面、上传、下载（waitress 承载 WSGI）
  templates/      单页界面
tests/
  e2e_test.py        14 项端到端场景（见下）
  orphan_reap_test.py Redis 数据丢失后的孤儿回收专项
```

## docker compose 启动

```bash
docker compose up --build
# 打开 http://localhost:8000
```

服务组成：`redis`（AOF 持久化 + 健康检查）、`web`（8000 端口）、
`worker`（`deploy.replicas: 2`，即两个独立 consumer）。数据分别存于
`app-data` 与 `redis-data` 卷。

### 主要环境变量

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `MAX_CONTENT_LENGTH` | `2097152` | 上传上限（字节） |
| `CLAIM_MIN_IDLE_MS` | `10000` | XAUTOCLAIM 接管的空闲阈值 |
| `PROCESSING_STALE_SECONDS` | `120` | 多久的 processing 行参与孤儿扫描 |
| `DISPATCH_INTERVAL_SECONDS` | `2` | reconciler 轮询间隔 |
| `SIMULATE_READ_FAILURE` | 未设置 | 设为 `1` 时 worker 模拟文件读取失败（测试用） |
| `PRE_FINALIZE_DELAY_MS` | `0` | 结论写入前延时（测试 kill 窗口用） |

## 本地（无 Docker）验证

仓库测试在本机直接运行编译版 redis-server 与多进程，覆盖与 compose 相同的
代码路径：

```bash
python3 -m venv --without-pip .venv        # 本机若缺 pip 先 bootstrap
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python tests/e2e_test.py
.venv/bin/python tests/orphan_reap_test.py
```

`tests/e2e_test.py` 的 14 项检查：

- T1 正常上传 → passed → 可下载且字节一致
- T2 Redis 宕机时上传仍被受理；行保持 pending 且记录入队失败；恢复后自动派发通过
- T3 摘要不符 → failed，下载 403，记录实际摘要与 mismatch 原因
- T4 超过 2 MiB 请求被拒且不留数据库行
- T5 读取失败只重试不判失败；恢复后重试通过
- T6 worker 在 ACK 前被 SIGKILL，另一 worker XAUTOCLAIM 接管且只产出一份结论
- T7 手工重投同一消息，已确认行零变化（updated_at 不变、无重复行）
- T8 web/worker/redis 全部重启后状态与下载资格保持
- T9 稳态下消费组 PEL 为空（无悬挂消息）

## 已知限制 / 遗留项

- 单机 SQLite，未做读写分离或多实例共享；容器化部署依赖共享卷，多 web 副本
  场景需改用 Postgres 等网络数据库。
- 消费组仅在 Redis 可用后由 reconciler/worker 幂等创建（`BUSYGROUP` 视为正常）。
- 无鉴权与用户隔离，适用于内网/演示场景；公网部署需加认证与传输加密。
- 读失败为无限重试（带退避上限 60s），没有死信队列与人工介入界面。
- 上传文件名经 `secure_filename` 清洗，制品内容原样保存、不做病毒扫描。
