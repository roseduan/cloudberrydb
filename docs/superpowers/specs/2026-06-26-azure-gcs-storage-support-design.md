# 设计：database 侧新增 Azure / GCS 存储支持

- 日期：2026-06-26
- 范围：仅 database 仓库（`contrib/`），不改 Gopher 仓库
- 参照模板：**QINGSTOR**（最纯粹的 S3 系对象存储，无特殊分支，照抄其在各模块的接线方式）

## 1. 背景与目标

datalake_fdw 与 dfs-tablespace-ext 通过 gopher client 访问云存储。Gopher（umbrella 版
`Gopher/src/client/gopher.h`）的 `UFS_TYPE` 枚举已包含 `AZURE` 与 `GCS`，且后端已有实现：

```c
typedef enum UFS_TYPE {
    QINGSTOR = 0, HUAWEI, AZURE, GCS, S3A, S3AV2, QCLOUD, OSS, KSYUN,
    QINIU, UCLOUD, HDFS, SWIFT, FTP, UFS_LOCAL, UFS_SHAREMEM, UFS_UNKNOWN
} UFS_TYPE;
```

目标：在 database 侧把用户可见的 `azure` / `gcs` 协议接到 gopher 的 `AZURE` / `GCS`，
覆盖 **native 对象存储路径**（对象存储外表 + 云表空间），使其与既有 OSS 系协议（以
QINGSTOR 为模板）行为一致。**不接入 iceberg/hudi 路径**（与 QINGSTOR/HUAWEI 等一致，见 §4.3）。

## 2. 架构：两段式协议体系

database 用户语义层各自有协议枚举/字符串，再在一处桥接到 gopher 的 `UFS_TYPE`：

```
用户 SQL 选项 (protocol='azure'|'gcs')
        │
        ▼
[模块本地协议字符串/枚举] ── 桥接函数 ──▶ gopherConfig.ufs_type = AZURE | GCS
        │                                          │
   选项校验/报错列表                          gopherConnect()  → Gopher 后端（不改）
```

Azure / GCS 复用既有对象存储字段，**不新增任何选项**：

| 用户选项 | Azure | GCS |
|---|---|---|
| `protocol` | `azure` | `gcs` |
| user mapping `accesskey` | 存储账户名 | HMAC access key |
| user mapping `secretkey` | 账户密钥 | HMAC secret |
| server `host` | `<account>.blob.core.windows.net` | `storage.googleapis.com` |
| server `region` | 可选 | 可选 |
| server `ishttps`/`isvirtual`/`listv2` | 复用 | 复用 |
| foreign table / tablespace 路径 | `/<container>/path` | `/<bucket>/path` |

> 注：具体字段语义（账户名走 host、密钥走 user mapping）以 Gopher 后端实际读取为准；
> database 侧只负责把既有 OSS 字段原样透传，与 QINGSTOR 完全相同。

## 3. 覆盖范围（三个入口，全部纳入）

1. **datalake_fdw 对象存储外表** —— 核心路径（需新增 native ufs_type 接线）
2. **dfs-tablespace-ext 云表空间** —— 第二个独立存储入口（需新增 ufs_type 接线）
3. **iceberg / hudi 路径 —— 明确不支持**：Azure/GCS 与 QINGSTOR/HUAWEI/COS/KS3/OSS 一样，
   仅作 native 对象存储后端，**不接入 iceberg 的 Hadoop connector 路径**（详见 §4.3）

## 4. 详细改动

### 4.1 datalake_fdw 对象存储路径

**(a) `src/datalake_type.h`** —— `DLProt` 枚举 OSS 家族区新增（紧随 QINGSTORE 之后，保持 OSS 连续区间）：

```c
DL_OSS_PROTOCOL_QINGSTORE,
DL_OSS_PROTOCOL_AZURE,   /* 新增 */
DL_OSS_PROTOCOL_GCS,     /* 新增 */
DL_OSS_PROTOCOL_S3A,
...
```

**(b) `src/datalake_def.h`**
- 新增字符串常量：
  ```c
  #define DATALAKE_OSS_PROTOCOL_AZURE "azure"
  #define DATALAKE_OSS_PROTOCOL_GCS   "gcs"
  ```
- 扩展 `PROTOCOL_IS_OSS(protocol)` 宏，纳入 `DL_OSS_PROTOCOL_AZURE`、`DL_OSS_PROTOCOL_GCS`
  （使 `iceberg_write.cpp` 等以 OSS 家族判定的写路径一致对待）。

**(c) `src/datalake_option.c`**
- `protocol_mapping[]`（~164 行）新增两行，照 QINGSTORE 写法：
  ```c
  {DATALAKE_OSS_PROTOCOL_AZURE, DL_OSS_PROTOCOL_AZURE},
  {DATALAKE_OSS_PROTOCOL_GCS,   DL_OSS_PROTOCOL_GCS},
  ```
- 更新 1198–1199 行报错文案，追加 `azure, gcs`。
- 校验路由无需改：`check_server_option` 的 `default` 分支已把非 HDFS/FTP 协议归入
  `IsValidOSSServerOption`，`datalakeGetOptions` 也走 OSS `else` 分支。一旦
  `datalakeGetProtocol` 不再返回 `DL_INVALID_PROTOCOL`，整套 OSS 校验/解析自动复用。

**(d) `src/common/fileSystemWrapper.cpp` `datalakeCreateGopherConfig`** —— 在 OSS `else`
块内（QINGSTOR 分支旁）新增字符串→枚举桥接：

```c
else if (pg_strcasecmp(strConvertLow(options->gopherType), "azure") == 0)
{
    conf->ufs_type = AZURE;
}
else if (pg_strcasecmp(strConvertLow(options->gopherType), "gcs") == 0)
{
    conf->ufs_type = GCS;
}
```

复用同块后续已有的 bucket / access_key / secret_key / endpoint / region /
useVirtualHost / useHttps / useListV2 组装逻辑，无额外字段。

### 4.2 dfs-tablespace-ext 云表空间

表驱动，最简洁。参照 QINGSTOR（此模块字符串为 `qingstor`）：

**(a) `dfs_option.h`**（~30–40 行）新增：
```c
#define DFS_OSS_PROTOCOL_AZURE "azure"
#define DFS_OSS_PROTOCOL_GCS   "gcs"
```

**(b) `dfs_option.c`** `dfsProtocols[]`（51 行）新增两行：
```c
{DFS_OSS_PROTOCOL_AZURE, AZURE},
{DFS_OSS_PROTOCOL_GCS,   GCS},
```

`validateProtocol()` / `getProtocolList()`（报错列表）/ `OptionGetProtocolType()`（字符串→枚举）
均派生自此表，自动生效。`remotefile_connection.c:gopherCreateConfig()` 通过
`config->ufs_type = OptionGetProtocolType(...)` 自动透传，无需改。

### 4.3 iceberg / hudi 路径 —— 明确不支持（零改动）

**决策：Azure/GCS 只作 native 对象存储后端，不接入 iceberg 的 Hadoop connector 路径**，
与 QINGSTOR/HUAWEI/COS/KS3/OSS 完全一致。

依据——database 有两套不同粒度的存储抽象：

- **对象存储外表 / 表空间路径（细粒度 native）**：每个厂商对应一个 liboss2 native 后端
  （`QINGSTOR`/`HUAWEI`/`QCLOUD`/`OSS`/`KSYUN`/`S3A`… 各自独立，QINGSTOR ≠ S3，见
  `Gopher .../protocol.cpp` 的 `case QINGSTOR`、`OssWorker.cpp` 判 `"qingstor"`）。
  在 C 里直接设 `ufs_type`，**不读任何 conf**。本次 Azure(native)/GCS(native) 加在此路径
  （§4.1 / §4.2）。
- **iceberg 路径（粗粒度 Hadoop connector）**：卷类型只有 `s3` / `s3v2` / `abfss` / `hdfs`
  （`iceberg_volume_option.c:19-28`）。`example/s3.conf` 即 Hadoop `S3AFileSystem`
  （`fs.s3a.impl` + `fs.s3a.endpoint`），**只服务 s3 / s3 兼容端点**。QINGSTOR/HUAWEI/COS
  /KS3/OSS 等 native 厂商本就**不在 iceberg 路径支持范围**，Azure/GCS 同样不纳入。

因此本路径**零代码改动**，且明确**不做**：
- 不在 `iceberg_volume_option.c` 为 Azure/GCS 新增卷类型；不动既有 `abfss`。
- 不新建 `azure.conf` / `gcs.conf`；不在 `iceberg_catalog_fdw.c`(~1120)、
  `dlproxy/protocol.c`(5 处)、`dlproxy/iceberg.c` 加 gcs/azure 的 catalogType 分支。
- `iceberg_catalog_fdw.c` 的 Polaris storageType 映射（~1632）虽已识别 gcs/azure 字样，
  但属既有遗留，本次不依赖、不扩展。

文档需写明：Azure/GCS 仅支持普通对象存储外表与云表空间，**不支持 iceberg/hudi 外表**。

### 4.4 文档 / 示例 / 测试

- `contrib/datalake_fdw/README.md`、`contrib/dfs-tablespace-ext` 文档：协议列表补
  azure/gcs；并写明 **仅普通对象存储外表 / 云表空间支持，不支持 iceberg/hudi 外表**。
- `contrib/datalake_fdw/example/`：加 azure/gcs server 配置样例（对象存储外表用法）。
- 回归测试：新增建 SERVER / 外表 / TABLESPACE 的**语法与校验**用例（不连真实云端，
  验证协议被接受、错误协议仍报错、报错列表含 azure/gcs）。端到端读写测试依赖云凭证，
  归入手动/集成测试，不进 `make installcheck`。

## 5. 验证

- 编译：`make -C contrib/datalake_fdw`、`make -C contrib/dfs-tablespace-ext` 通过
  （依赖 umbrella 版 `gopher.h` 含 AZURE/GCS 枚举）。
- 语法回归：`CREATE SERVER ... OPTIONS(protocol 'azure')` 与 `'gcs'` 成功；非法协议报错
  文案含 azure/gcs；`CREATE TABLESPACE` 同理。
- 单元层面：`datalakeGetProtocol("azure")` 返回 `DL_OSS_PROTOCOL_AZURE`，
  `datalakeCreateGopherConfig` 产出 `ufs_type == AZURE`；dfs 的 `OptionGetProtocolType` 同理。

## 6. 风险与依赖

- **iceberg/hudi 不支持（已定）**：Azure/GCS 不接入 iceberg 的 Hadoop connector 路径，
  与 QINGSTOR/HUAWEI/COS/KS3/OSS 一致。用户在 iceberg/hudi 外表上指定 azure/gcs 不在
  支持范围，需用对象存储外表或云表空间。
- **QINGSTOR/HUAWEI/COS 等是 liboss2 独立 native 后端**，不兼容 S3、不使用 s3.conf；
  本次 azure(native)/gcs(native) 与它们同列于对象存储外表/表空间两条路径。
- **字段语义未在 database 侧二次校验**：账户名/容器经 host/bucket 透传，错误配置只会在
  Gopher 后端连接时报错，与现有 OSS 协议行为一致。
- **命名不统一（既有现状）**：datalake_fdw 用 `s3/ali`，dfs-tablespace-ext 用 `s3a/oss`。
  本次两模块的新协议统一用 `azure`/`gcs`，不改既有命名。

## 7. 明确不做（YAGNI）

- 不改 Gopher 仓库（枚举与后端已就绪）。
- **不接入 iceberg/hudi 路径**（Hadoop connector）：Azure/GCS 仅作 native 对象存储后端，
  与 QINGSTOR/HUAWEI/COS/KS3/OSS 同等待遇。不动 `iceberg_volume_fdw` / `iceberg_catalog_fdw`
  / `dlproxy`，不新建 `azure.conf` / `gcs.conf`。
- 不为 Azure/GCS 增设专属选项（SAS token、service-account JSON 等），与 S3 系保持对称；
  如后端需要，后续按需扩展。
- 不动 `hive_connector` / `unionstore_ext` / `gpcloud`（不走 gopher UFS）。
