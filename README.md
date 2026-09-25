# versioned-object-store-go

一套本机可复现的版本化对象存储服务：以本地磁盘存放对象数据、以本地 JSON 元数据文件记录版本与分片上传状态，模拟对象、多版本与分片上传语义。不依赖任何外部服务、真实凭据或联网资源。

## 构建与验证

```sh
go build ./...        # 编译
go vet ./...          # 静态检查
go test ./...         # 全部测试
go test -race ./...   # 竞态检测（并发写入 / GC 交错场景）
go test -v ./...      # 查看运行日志：对象键、版本号、分片号与判定依据
```

## 磁盘布局

```
<Dir>/
  meta.json                  元数据（写 .tmp + fsync + rename 原子提交）
  data/<sha1(key)>/<verID>   对象版本数据（先写 .tmp-<verID> 再 rename 就位）
  uploads/<uploadID>/part-N  进行中的分片（先写 .tmp 再 rename 就位）
```

**提交顺序保证**：数据文件先完整落盘（fsync + rename），元数据后原子提交。
崩溃只可能留下"有数据、无元数据"的孤儿文件（恢复/GC 可回收），
绝不留下"元数据引用了缺失数据"的悬挂引用或半写版本。

## 版本与条件写入语义

- 每次 `Put` 产生一个全局单调递增的版本号（`v00000001`…），同一键的并发写入
  由互斥锁串行化，各自获得独立版本号，互不覆盖、不留半写数据。
- `Get(key)` 返回当前版本；`GetVersion(key, versionID)` 按版本号寻址，
  被覆盖、被删除的历史版本内容不变、始终可读。
- `Delete(key)` 写入删除标记（tombstone）作为新的当前版本：`Get` 返回
  `ErrDeleted`，历史版本仍可寻址读取，重复删除返回 `ErrNotFound`。
- 条件写入前置条件（`PutOptions`）：
  - `IfAbsent`：要求键当前不存在（或当前版本为删除标记）；
    冲突返回 `*PreconditionError{Kind: PreconditionKeyExists}`。
  - `ExpectVersion`：要求键的当前版本号与之相等；
    冲突返回 `*PreconditionError{Kind: PreconditionVersionMismatch}`，
    并携带 `Expected`/`Actual` 便于排查。
  - 两类失败均可用 `errors.Is(err, ErrPreconditionFailed)` 判定、
    `errors.As` 取出 `*PreconditionError` 区分原因；被拒绝的写入不产生任何版本。

## 分片上传状态机

```
            CreateUpload
                 │
                 ▼
            ┌──────────┐  UploadPart(乱序/重复均可，重复为幂等覆盖)
            │ initiated │◄─────────────────────────────┐
            └──────────┘                               │
             │          │                              │
   Complete  │          │ Abort                        │ UploadPart
             ▼          ▼                              │
       ┌──────────┐  ┌────────┐                        │
       │ completed │  │ aborted │───────────────────────┘(拒绝)
       └──────────┘  └────────┘
```

- 分片编号合法范围为 `[1, MaxParts]`，越界返回 `ErrPartNumberInvalid`。
- `CompleteUpload(parts)` 按给定编号顺序拼接为新版本：
  - 引用未上传分片 → `ErrPartMissing`（指明缺失编号）；
  - 重复完成 → 幂等返回首次完成的版本号，不产生新版本；
  - 已中止 → `ErrUploadAborted`。
- `AbortUpload`：重复中止幂等成功；已完成 → `ErrUploadCompleted`。
- 终态（completed/aborted）会话上的 `UploadPart` 分别返回
  `ErrUploadCompleted` / `ErrUploadAborted`。
- 失败或中止的上传不产生任何可见对象；其分片目录在状态转换时删除，
  崩溃残留由重启恢复或 `GC` 清除，不会留下无法回收的分片。

## 崩溃恢复与空间回收

- `Open` 时执行恢复：丢弃未提交的 `meta.json.tmp`；删除无元数据引用的
  数据文件；清除已终结/未知上传会话的目录及 initiated 会话中未登记的分片文件；
  元数据引用了缺失数据文件的版本条目被移除（防御性，正常流程不会触发）。
- `GC` 只删除无任何元数据引用的文件，因此不影响任何可见对象与版本；
  正在被读取的版本其数据文件始终被引用，回收期间读取可正常完成。
  返回 `GCReport`（删除的数据文件数、上传目录数、回收字节数）。

## 可配置项（Config）

| 字段       | 说明                                       | 默认值          |
|------------|--------------------------------------------|-----------------|
| `Dir`      | 本地数据根目录                             | 必填            |
| `MaxParts` | 单次分片上传允许的最大分片数               | 10000           |
| `Logger`   | 运行日志（`log/slog`）                     | `slog.Default()`|

## 测试覆盖

- `store_test.go`：版本寻址、条件写入冲突原因区分、并发无条件写入
  （版本互不覆盖）、并发 `IfAbsent`（仅一个成功）、删除后历史可读。
- `multipart_test.go`：乱序/重复分片、越界分片、缺失分片、重复完成幂等、
  重复中止幂等、终态拒绝、中止后无可见对象与残留分片。
- `recovery_test.go`：重启后已提交状态保持、崩溃残留清理、悬挂版本条目移除、
  GC 与读取/写入并发交错下的状态自洽。
