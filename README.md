# Multi-Center DICOM Ingest & Withdrawal API

多中心 DICOM 入库平台。解决的核心问题：**设备在不同中心复用同一个 DICOM UID**
时，分割/引用结果可能错误地指向别家中心的影像。本平台对每个来源中心独立、确定
性地重映射所有实例标识，使跨中心数据在物理上无法串联。

在入库能力之上，平台还提供**按中心的批量撤除（withdrawal）**：影像研究数据工程师
可以撤除误收影像，同时保住留存分割对象的引用关系，并支持撤除状态查询、失败清理
重试与同 UID 同字节重新入库。

技术栈：**FastAPI + pydicom + SQLite（WAL 模式）**，单容器即可运行，交付
Docker Compose 环境与交互式 API 文档（Swagger UI）。

---

## 1. 核心保证

| 需求 | 实现方式 |
| --- | --- |
| 按“中心 + 原 UID”稳定生成新 UID | `new_uid = "2.25." + uuid5(固定命名空间, center + "|" + 原UID).int`，纯函数，无需查库、跨进程/重启不变（`app/uid.py`） |
| 跨中心同原 UID 不碰撞 | 中心标识参与哈希输入；同原 UID 在不同中心必然得到不同新 UID，引用永不跨中心 |
| 统一改写范围 | 顶层 Study/Series/SOP Instance UID、文件头 `MediaStorageSOPInstanceUID`、任意序列深度嵌套的 `ReferencedSOPInstanceUID`、序列内 `SeriesInstanceUID`、`FrameOfReferenceUID`、`ReferencedFrameOfReferenceUID`（递归遍历，`app/dicom_io.py`） |
| 不改写的内容 | SOP Class UID / Transfer Syntax UID 等“类别型”UID、Modality 等属性、**PixelData 像素字节**（写入后回读断言验证） |
| 仅接受 Explicit VR Little Endian | 校验文件头传输语法必须为 `1.2.840.10008.1.2.1`，否则整批 422 拒绝 |
| 引用先到、影像后到 | 引用先入库时在 `uid_mappings` 预留映射并在 `instance_references` 标记未解析；后续同中心影像到达后批量重新解析；不同中心到达的影像不会解析它 |
| 同中心同原 SOP 同字节重传 | 命中 `(center, original_sop_uid, sha256)` 唯一约束，直接复用已存对象，不产生重复行/重复文件 |
| 异字节冲突 | 同中心同原 SOP UID 但 SHA-256 不同 → 整批 409 拒绝 |
| 任一文件失败整批不入库 | 全部文件先在事务外解析；入库在单个 `BEGIN IMMEDIATE` 事务内完成，失败回滚，并清理已落盘的临时对象，**不存在半批可下载** |
| 并发安全 | SQLite WAL + 写事务互斥；唯一约束兜底；12 路并发实测全部 201 |
| 重启不变 | 映射为纯函数可离线复算，数据库与对象文件落盘到挂载卷 |
| 批量上限 | 每批最多 20 份文件（可配置） |

---

## 1b. 批量撤除（withdrawal）保证

| 需求 | 实现方式 |
| --- | --- |
| 按中心批量撤除，1–20 个已入库实例 ID | `POST /api/v1/withdrawals`，入参 `center_id` + `instance_ids`，去重后校验数量与归属（`app/withdrawal.py`） |
| 集合外实例引用待删对象则整次拒绝并列出阻挡引用 | 在同一写事务内查找「源实例不在集合、目标实例在集合」的引用；命中即整批回滚为 `rejected`，响应 409 并在 `blockers` 中逐条列出阻挡引用（源/目标的原 UID 与新 UID） |
| 集合内互引可一起撤除 | 源与目标都在集合内的引用不算阻挡，随实例一并删除（双向互引 SR 场景） |
| 检查与撤除按提交时最新数据共同生效、并发入库不漏检 | 阻挡检查与硬删除全部在一个 `BEGIN IMMEDIATE` 事务内完成，与入库写事务互斥；并发入库要么先提交并被检查命中（阻挡），要么后提交且其引用保持未解析，不存在「已解析引用指向被删目标」的中间态 |
| 撤除后不可查询或下载 | `instances` 行硬删除，`GET /instances` 自然查不到、`download` 返回 404；撤除前的批次记录仍可查询 |
| UID 映射保留 | 只删除 `instances`/`instance_references`，`uid_mappings` 行保留，仅把无存活实例的 SOP 映射 `present` 翻回 0 |
| 指向已撤除目标的新引用先记未解析，目标重入库后恢复 | 撤除时把指向被删目标的存活引用置 `resolved=0`；之后新对象引用该 UID 因 `present=0` 记为未解析；目标重新入库时入库事务的统一重解析逻辑自动恢复 |
| 同 UID 同字节允许重新上传 | 硬删除释放唯一约束，重传走正常入库，确定性 UID 与存储路径保持不变 |
| 文件删除失败可重试、重启后继续清理 | 撤除先提交 DB，对象文件由 `withdrawal_items` 工作队列异步清理；每项记 `pending/done/error`、错误信息与尝试次数；后台线程定期重试，进程启动时先扫一遍（重启续清），也可 `POST /cleanup/retry` 立即重试 |
| 迟到清理不得误删重传文件或无关实例 | 每次 unlink 前在写事务内做**属主复核**：仅当该存储路径已无任何存活实例属主才删除；重传重建同路径后，迟到清理标记 `done` 并写 `skip_reason` 跳过，绝不 unlink；路径还做了存储根目录越界校验 |
| 撤除状态与清理错误查询 | `GET /withdrawals`、`GET /withdrawals/{id}`（含每项清理状态）、`GET /cleanup/errors` |

撤除与入库共用同一个 SQLite 写锁，因此「检查—删除」和「并发入库」严格串行化，
这是并发不漏检的根本保证。

---

## 2. 目录结构

```
app/
  config.py        环境变量配置（数据目录、UID 根、批量上限、清理间隔等）
  uid.py           确定性 UID 重映射（uuid5 + 2.25 根）
  dicom_io.py      DICOM 解析/校验、递归 UID 改写、序列化后回读校验
  db.py            SQLite schema、WAL 连接、即时写事务
  service.py       批次原子入库、重传复用、字节冲突、引用解析
  withdrawal.py    批量撤除、引用阻挡检查、属主安全的文件清理与后台重试
  main.py          FastAPI 路由与响应模型（入库 + 撤除 + 清理）
scripts/
  generate_samples.py  生成合成样例（跨中心碰撞 / 引用先到 / 非法文件）
  demo.sh              对运行中的服务执行完整 HTTP 演示（含撤除/阻挡/重入库）
tests/             pytest（含并发、原子性、嵌套改写、重启稳定性、撤除与清理）
samples/generated/ 合成 DICOM 样例（构建时生成）
Dockerfile, docker-compose.yml, docker/entrypoint.sh
```

---

## 3. 快速开始（Docker Compose）

```bash
# 1. 生成合成样例（需要本机有 pydicom；也可直接用仓库内已生成的文件）
python3 scripts/generate_samples.py

# 2. 构建并启动
docker compose up --build -d

# 3. 健康检查
curl http://localhost:8000/healthz
# {"status":"ok"}

# 4. 打开交互式 API 文档
#    浏览器访问 http://localhost:8000/docs
```

在 Swagger UI 中演示上传：

1. 展开 `POST /api/v1/batches` → **Try it out**
2. `center_id` 填 `center-a`
3. `files` 选择 `samples/generated/cross_center_uid_collision/center-a/image_001.dcm`
4. **Execute**，响应 201，返回每个文件的原/新 UID 映射
5. 用 `GET /api/v1/batches/{batch_id}` 查询批次，用
   `GET /api/v1/instances/{id}/download` 下载改写后的 DICOM

### 在 Swagger UI 中演示撤除、引用阻挡与重新入库

1. 先调 `POST /api/v1/batches`，用一个中心（如 `center-w`）同时上传一张影像和一个
   引用它的 SR 对象（`demo.sh` 第 9 步会生成这两个文件到临时目录；也可自行用
   `scripts/generate_samples.py` 的 `make_ct_image` / `make_sr_with_reference` 生成）。
2. 调 `GET /api/v1/instances?center_id=center-w` 拿到影像与 SR 的实例 `id`。
3. 展开 `POST /api/v1/withdrawals` → **Try it out**，请求体只填影像 id：
   ```json
   {"center_id": "center-w", "instance_ids": [<影像 id>]}
   ```
   **Execute** 后返回 **409**，`detail.withdrawal.blockers` 列出 SR 的阻挡引用；
   两个实例依然存活（`GET /instances` 仍能查到）。
4. 把两个 id 一起放进 `instance_ids` 再次 **Execute**，返回 **200**、`status=committed`。
   再查 `GET /instances` 已为空，对旧 id 调 `download` 返回 **404**。
5. 调 `GET /api/v1/withdrawals/{withdrawal_id}` 查看每个撤除项的 `cleanup_status`；
   调 `GET /api/v1/cleanup/errors?center_id=center-w` 查看待清理/错误项（正常为空）。
6. 重新上传 SR（其目标影像已撤除）→ `GET /references/pending?resolved=false`
   看到一条未解析引用；再重新上传同 UID 同字节的影像 → 引用恢复解析、列表清空。
7. 若文件清理曾失败，调 `POST /api/v1/cleanup/retry` 立即重试；属主复核保证
   重传文件不会被迟到清理误删。

### 一键脚本演示（覆盖全部需求场景）

```bash
BASE_URL=http://localhost:8000 bash scripts/demo.sh
```

脚本依次演示：跨中心 UID 碰撞、引用先到、跨中心不解析、同中心影像到达后解析、
同字节重传复用、异字节整批 409、批次查询与下载校验像素不变、非法文件整批 422、
**撤除引用阻挡（409 + blockers）、集合内互引一起撤除、撤除后不可查/不可下载、
撤除状态与清理错误查询、指向已撤除目标的新引用未解析后随重入库恢复、同 UID
同字节重传、文件清理失败重试与迟到清理对重传文件的保护**。

### 不用 Docker 的本地运行

```bash
pip install -r requirements.txt
python3 scripts/generate_samples.py
DICOM_INGEST_DATA_DIR=./data uvicorn app.main:app --host 0.0.0.0 --port 8000
```

---

## 4. HTTP API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/v1/batches` | multipart 表单：`center_id` + `files`（1–20），原子入库 |
| GET | `/api/v1/batches` | 批次列表，可按 `center_id` 过滤 |
| GET | `/api/v1/batches/{batch_id}` | 批次详情 + 每个文件的原/新 UID 映射 |
| GET | `/api/v1/instances` | 实例检索（`center_id` / `original_sop_uid` / `new_sop_uid`），撤除后自动消失 |
| GET | `/api/v1/instances/{id}/download` | 下载改写后的 DICOM（`application/dicom`），撤除后 404 |
| GET | `/api/v1/references/pending` | 某中心的引用解析状态（`resolved=true/false`） |
| POST | `/api/v1/withdrawals` | 按中心原子撤除 1–20 个实例；被阻挡返回 409 并列 `blockers` |
| GET | `/api/v1/withdrawals` | 撤除请求列表（含 committed 与 rejected），可按中心过滤 |
| GET | `/api/v1/withdrawals/{id}` | 撤除详情 + 每个实例的文件清理状态 |
| GET | `/api/v1/cleanup/errors` | 仍为 pending/error 的文件清理项，可按中心过滤 |
| POST | `/api/v1/cleanup/retry` | 立即触发一次待清理/失败项重试，返回处理汇总 |
| GET | `/healthz` | 存活探针 |

错误语义：`422` = 文件/参数不合法（含非法 DICOM、传输语法不符、超 20 份、撤除
参数非法）；`409` = 与既有状态冲突（异字节同 UID、撤除被集合外引用阻挡）；
`404` = 批次/实例/撤除单不存在，或撤除 ID 属于其他中心/已被撤除。

---

## 5. 合成样例说明

`scripts/generate_samples.py` 生成的全部对象均为带 DICM 前导、文件头和真实
PixelData 的合法 Explicit VR Little Endian 文件：

- `cross_center_uid_collision/center-{a,b}/image_001.dcm`
  两个中心**复用完全相同的 Study/Series/SOP UID**，但像素值分别为 1000 与
  2000（异字节）。入库后得到两个不同的新 SOP UID，像素各自保持不变。
- `reference_first/struct_001.dcm`
  SR 对象，含三层嵌套
  （`ReferencedSOPSequence → ReferencedSeriesSequence → ReferencedSOPSequence`），
  引用一个尚未上传的影像 SOP。先上传它会产生一条未解析引用。
- `reference_first/image_001.dcm`
  被引用的影像。同中心上传后引用自动解析；别的中心上传同原 UID 影像不会解析。
- `invalid/not_a_dicom.dcm`
  非 DICOM 字节，用于演示整批拒绝。

---

## 6. 数据模型要点

- `uid_mappings`：每个 `(center, 原UID)` 一行，`present` 标识携带该 SOP 的实体
  是否已到达，支撑引用先到；撤除时该行保留，仅在无存活实例时把 `present` 置 0。
- `instances`：每个入库对象一行，含原/新三套 UID、SHA-256、存储相对路径，
  `(center, original_sop_uid, sha256)` 与 `new_sop_uid` 均唯一。撤除会硬删除对应行，
  因此撤除后不可查询/下载，且同 UID 同字节可重新入库。
- `instance_references`：引用关系与解析状态，按中心隔离。撤除删除被撤实例自身的
  引用，并把指向被删目标的存活引用置为未解析。
- `batches`：仅记录成功提交的批次；被拒绝的批次不留任何记录或对象。
- `withdrawal_requests`：每次撤除请求一行，`committed` 与被阻挡的 `rejected` 都留档，
  被阻挡时 `blockers` 以 JSON 保存完整阻挡引用列表。
- `withdrawal_items`：已撤除实例的快照（含 `stored_path`），同时是异步文件清理的
  工作队列，记录 `pending/done/error`、错误信息、尝试次数与跳过原因。

所有表在容器启动时通过 `CREATE TABLE IF NOT EXISTS` 幂等建表（新版本仅新增两张
撤除相关表，对既有库为安全的向前迁移）。

---

## 7. 运行测试

```bash
pip install -r requirements.txt -r requirements-dev.txt
pytest -q
# 34 passed
```

测试覆盖：UID 纯函数性质、跨中心碰撞隔离、嵌套序列改写、像素/传输语法保持、
引用先到后解析与跨中心不解析、同字节重传复用、异字节/非法文件整批回滚、
20 份上限、非 Explicit VR 拒绝、8 路并发与 6 路并发重传收敛；以及撤除专项：
撤除后不可查/不可下载且 UID 映射保留、集合内互引同撤、集合外引用阻挡并列出
blockers、撤除与入库并发不漏检、新引用未解析后随重入库恢复、同 UID 同字节重传、
文件清理失败重试与重启续清、迟到清理保护重传文件、撤除参数校验与重复撤除。

---

## 8. 配置项（环境变量）

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `DICOM_INGEST_DATA_DIR` | `/data` | SQLite 库与对象文件根目录（Compose 已挂卷） |
| `DICOM_INGEST_MAX_FILES` | `20` | 单批文件数上限 |
| `DICOM_INGEST_MAX_FILE_BYTES` | `52428800` | 单文件大小上限 |
| `DICOM_INGEST_UID_ROOT` | `2.25` | 新 UID 根（2.25 = UUID 派生 UID 的 ISO/ITU 保留根） |
| `DICOM_INGEST_BUSY_TIMEOUT_MS` | `30000` | SQLite 写锁等待超时 |
| `DICOM_INGEST_MAX_WITHDRAWAL_IDS` | `20` | 单次撤除的实例 ID 数上限 |
| `DICOM_INGEST_CLEANUP_INTERVAL_S` | `10` | 后台文件清理重试的轮询间隔（秒） |
