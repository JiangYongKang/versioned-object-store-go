# 本机可复现的版本化对象存储（Go）

仅使用本地磁盘与本地 WAL 元数据模拟对象存储的**多版本写入、条件写入、分片上传、崩溃恢复与空间回收**。
不依赖任何外部服务、真实凭据或联网资源；并发写入、进程异常中断与重启恢复下的数据结论一致且可解释。

- Go ≥ 1.25（在 Go 1.26 上验证）
- 零第三方依赖，仅标准库

## 快速开始

```bash
# 构建
go build ./...

# 全部测试（并发 / 条件冲突 / 分片异常 / 重启恢复 / GC / WAL 损坏）
go test ./...
go test -race ./...          # 竞态检测
go test ./... -count=1 -v    # 强制重跑并查看每个用例

# 端到端演示（打印对象键、版本标识、分片标识与判定依据）
go run ./cmd/demo -root /tmp/vos-demo
```

## 磁盘布局

```
<root>/
├── lock          # flock 排他锁，禁止两个进程同时打开同一根目录
├── meta.wal      # 追加写、每条 fsync 的元数据 WAL（长度+CRC32+JSON 帧）
├── data/         # 内容寻址的不可变数据块，文件名 = SHA-256(hex)
└── tmp/          # 写入中的暂存块；启动时整体清理（其元数据从未提交）
```

**核心不变量（写入顺序）**：数据块先落盘并 fsync（内容寻址、不可变），**然后**才追加并 fsync
一条元数据 WAL 记录。因此：

- 元数据指向的数据块必然存在（反之即数据损坏，启动时显式报错）；
- 写入在 WAL append 前崩溃，只可能留下无引用块（重启清 `tmp/`，GC 清 `data/` 中的孤儿块）；
- 分片上传的“版本可见 + 上传槽关闭”是**同一条 WAL 记录**，二者同时生效或同时不发生。

## 并发模型

- 进程内：一把互斥锁串行化所有元数据判定（前置条件评估 + 状态迁移 + WAL 追加）。
- 进程间：`<root>/lock` 上的非阻塞排他 `flock`，第二个打开者得到 `storage_locked`。
- 数据块写入在锁外完成（不可变、匿名、内容寻址），发布后再在临界区内提交元数据。
- 读取在持锁期间解析版本并打开全部块句柄；GC 永不删除仍被引用的块，故读取与回收不会互相破坏。

## 版本与条件写入语义

每次 `Put` 都创建一个**新版本 ID**（`v-` + 随机十六进制），永不原地覆盖：

| 操作 | 语义 |
| --- | --- |
| `Put(key)` | 无条件追加新版本，成为当前版本；旧版本仍可按版本 ID 寻址读取，内容不变 |
| `Put(key, IfNoneMatch:true)` | 仅当当前**不存在存活对象**时接受；否则拒绝，原因码 `condition_conflict` |
| `Put(key, IfMatch:etag)` | 仅当当前版本校验和等于 `etag`（`"*"` 表示要求存在）时接受；对象不存在或校验和不符拒绝，原因码 `precondition_failed` |
| `Get(key)` / `Get(key, versionID)` | 读当前版本 / 读指定历史版本 |
| `Delete(key)` | 安装**删除标记**；当前读返回 `not_found`，历史版本全部保留可读 |
| `DeleteVersion(key, versionID)` | 永久移除单个历史版本；若删的是当前版本则同时安装删除标记；其独占的数据块成为 GC 候选 |

两种条件失败使用**不同的哨兵错误**（`ErrConditionConflict` vs `ErrPreconditionFailed`），
可用 `errors.Is` 判定，或用 `objectstore.CodeOf(err)` 取机器可读原因码。

内容相同的不同对象/版本共享同一个数据块（内容寻址去重）；只要还有任一可见版本引用，GC 就不会回收。

## 分片上传状态机

```
                 CreateMultipart
                       │
                       ▼
                 ┌───────────┐   UploadPart(n)   ┌─────────────┐
                 │ INITIATED │◄──────────────────│ PART STORED │（可乱序、可重复到达）
                 └───────────┘                   └─────────────┘
                    │        ▲                          │
        AbortMultipart       └── 同号分片重复上传 = 原子替换旧分片
                    ▼
              ┌─────────┐        CompleteMultipart（分片号严格升序、无重复）
              │ ABORTED │                │
              └─────────┘                ▼
                                  单条 WAL 原子提交：
                                  新版本可见 + 上传槽关闭
                                            │
                                            ▼
                                       ┌────────┐
                                       │  DONE  │（槽位消失）
                                       └────────┘
```

规则与拒绝原因：

| 场景 | 行为 | 原因码 |
| --- | --- | --- |
| 分片号越界（`<1` 或 `>max_parts`） | 拒绝 | `invalid_part_number` |
| 分片乱序到达 | 允许（按分片号存储，Complete 时排序） | — |
| 同一分片号重复上传 | 允许，**原子替换**；旧块变为无引用 GC 候选 | — |
| Complete 列表非严格升序或有重复 | 拒绝 | `invalid_part_order` |
| Complete 引用从未上传的分片号 | 拒绝 | `invalid_part_number` |
| 非末片 `< min_part_size` | 拒绝（末片豁免，与 S3 一致） | `part_too_small` |
| Complete 后重复 Complete | 明确拒绝（槽位已消失，不会产生第二个版本） | `upload_not_found` |
| Abort 后重复 Abort / 对已完成上传 Abort | 明确拒绝 | `upload_not_found` |
| 分片写入时进程崩溃 | 暂存块在下次启动时从 `tmp/` 清除；未产生可见对象 | — |
| 进行中的上传在重启后 | **保留**，可继续传分片并 Complete；Abort 则不留可见对象与残留 | — |

分片对象读取时按分片顺序拼接各块；对象级标识为“有序分片（号+校验和+大小）”的确定性复合值。

## 崩溃恢复

打开根目录时执行：

1. 获取排他 `flock`；
2. 清空 `tmp/`（暂存块不可能已提交元数据）；
3. 重放 `meta.wal`：帧格式为 `uint32 长度 | uint32 CRC32 | JSON 负载`。
   - **尾部残缺帧**（崩溃导致的半写）安全截断丢弃；
   - 尾部以外的长度/CRC/JSON/状态一致性错误判定为 `corrupted_metadata`，拒绝启动；
4. 校验所有被元数据引用的数据块均存在，缺失即 `corrupted_data`，拒绝启动
   （不允许“引用缺失数据”的悬挂条目，也不允许“无元数据的数据”长期存在——后者由 GC 回收）；
5. 恢复进行中的分片上传（可继续），随后进入正常服务。

## 空间回收（GC）

`RunGC`（或后台定时扫描，见配置）执行 mark-sweep：

1. 持锁构建**引用集**：所有未删除版本（含历史版本与多版本分片块）+ 所有进行中上传的分片块；
2. 列举 `data/`，对不在引用集中且修改时间早于 `now - gc_unref_age`（宽限期，防与在途写入竞争）的块，
   在删除前再次持锁确认无引用后删除；
3. 仍被读取的版本：其块句柄已在持锁期间打开，且引用集始终包含该块，读取必然完成；
4. 任何可见对象/版本的块永不删除；被覆盖历史版本、删除版本、中止上传、被替换分片的独占块才会被回收。

## 可配置项

`objectstore.Config`（JSON 示例见 `configs/config.example.json`，可用 `objectstore.LoadConfig` 读取）：

| 字段 | 默认值 | 说明 |
| --- | --- | --- |
| `root_dir` | —（必填） | 存储根目录 |
| `min_part_size` | `5 MiB` | 非末片最小分片字节数（末片豁免） |
| `max_parts` | `10000` | 最大分片号（合法范围 1..max_parts） |
| `gc_interval` | `1m` | 后台 GC 周期；`0` 关闭定时 GC（仍可手动 `RunGC`） |
| `gc_unref_age` | `30s` | 无引用块的回收宽限期 |
| `enable_gc` | `true` | 是否启用后台 GC |

代码内可用 Option 覆盖：`WithMinPartSize` / `WithMaxParts` / `WithGCInterval` /
`WithGCUnrefAge` / `WithLogger`。

## 错误原因码

| 哨兵错误 | code | 含义 |
| --- | --- | --- |
| `ErrNotFound` | `not_found` | 当前版本不存在（含删除标记） |
| `ErrVersionNotFound` | `version_not_found` | 指定版本 ID 从不存在 |
| `ErrVersionDeleted` | `version_deleted` | 指定版本已被 `DeleteVersion` 移除 |
| `ErrPreconditionFailed` | `precondition_failed` | If-Match 前置条件不成立 |
| `ErrConditionConflict` | `condition_conflict` | If-None-Match:* 与现存对象冲突 |
| `ErrUploadNotFound` | `upload_not_found` | 上传不存在 / 已完成 / 已中止 |
| `ErrInvalidPartNumber` | `invalid_part_number` | 分片号越界或未上传 |
| `ErrPartTooSmall` | `part_too_small` | 非末片小于最小分片大小 |
| `ErrInvalidPartOrder` | `invalid_part_order` | Complete 列表未严格升序或重复 |
| `ErrCorruptedMetadata` | `corrupted_metadata` | WAL 中间帧损坏或记录不一致 |
| `ErrCorruptedData` | `corrupted_data` | 元数据引用的数据块缺失 |
| `ErrStorageLocked` | `storage_locked` | 根目录已被其他进程锁定 |

## 日志

默认日志输出到标准输出，每条关键判定都带对象键、版本标识 / 分片标识（uploadID + partNumber）、
校验和与 `basis=` 判定依据，例如：

```
PUT key="a" version=v-.. checksum=9fbf.. size=19 basis=if-match=9fbf.. -> committed
COND key="a" basis=if-none-match:* current=v-.. -> rejected: objectstore: condition_conflict
MP_PART upload=mp-.. key="big" part=2 checksum=60c3.. size=4 -> stored
MP_COMPLETE upload=mp-.. key="big" version=v-.. parts=2 total_size=8 -> committed atomically
GC block=60c3.. basis=unreferenced older_than=1ms -> removed
BOOT root=.. versions_keys=2 uploads_inprogress=1 recovery=ok
```

可用 `WithLogger` 注入自定义日志函数（测试中被静默）。
