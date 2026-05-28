# datalake_fdw 与 Iceberg 完整使用指南

## 1. 概述

`datalake_fdw` 是 HashData Lightning 的数据湖访问扩展，提供两大能力：

- **FDW 外部表**：直接读写 S3/HDFS/Hive 上的 Parquet、ORC、Avro、Text、CSV 文件
- **Iceberg 表**：通过 Catalog + Volume + Iceberg Table 三层架构，支持事务性的 ACID 读写、Schema 演化、快照隔离和 VACUUM 压缩

```
                    datalake_fdw
                    /          \
           FDW 外部表         Iceberg 表
         (只读/追加写)       (完整 ACID)
          |                    |
    +-----------+        +-----+------+
    | 协议+格式  |        | Catalog    Volume |
    | S3/HDFS   |        | (元数据)  (存储)   |
    | ORC/Parquet|        +-----+------+
    | Avro/CSV  |              |
    +-----------+         Iceberg Table
```

---

## 2. 安装

```sql
CREATE EXTENSION IF NOT EXISTS datalake_fdw;
CREATE EXTENSION IF NOT EXISTS hive_connector;   -- Hive 集成（可选）
```

安装 `datalake_fdw` 时自动注册以下 FDW：

| FDW 名称 | 用途 |
|----------|------|
| `datalake_fdw` | 通用外部表（S3/HDFS 文件） |
| `iceberg_catalog_fdw` | Iceberg Catalog 管理 |
| `iceberg_volume_fdw` | Iceberg Volume（存储层）管理 |

---

## 3. FDW 外部表（非 Iceberg）

### 3.1 S3 外部表

```sql
-- 创建 S3 Server
CREATE SERVER s3_server
    FOREIGN DATA WRAPPER datalake_fdw
    OPTIONS (
        host 'lakehouse:9100',
        protocol 's3',
        isvirtual 'false',        -- MinIO 需要 false（path-style）
        ishttps 'false'
    );

CREATE USER MAPPING FOR gpadmin
    SERVER s3_server
    OPTIONS (user 'gpadmin', accesskey 'admin', secretkey 'password');

-- 创建外部表（Parquet 格式）
CREATE FOREIGN TABLE s3_sales (id int, name text, amount numeric(10,2))
    SERVER s3_server
    OPTIONS (filePath '/bucket/sales/parquet/', format 'parquet');

-- 查询
SELECT * FROM s3_sales WHERE id > 100;
```

### 3.2 HDFS 外部表

```sql
CREATE SERVER hdfs_server
    FOREIGN DATA WRAPPER datalake_fdw
    OPTIONS (
        protocol 'hdfs',
        hdfs_namenodes 'namenode-host',
        hdfs_port '8020',
        hdfs_auth_method 'simple',
        hadoop_rpc_protection 'authentication'
    );

CREATE USER MAPPING FOR gpadmin
    SERVER hdfs_server
    OPTIONS (user 'gpadmin');

-- Text 格式
CREATE FOREIGN TABLE hdfs_logs (line text)
    SERVER hdfs_server
    OPTIONS (filePath 'hdfs://namenode:8020/logs/2024/', format 'text');

-- ORC 格式
CREATE FOREIGN TABLE hdfs_events (id int, event_type text, ts timestamp)
    SERVER hdfs_server
    OPTIONS (filePath 'hdfs://namenode:8020/events/', format 'orc');
```

### 3.3 支持的文件格式

| 格式 | 读 | 写 | OPTIONS 示例 |
|------|:--:|:--:|-------------|
| Parquet | ✓ | ✓ | `format 'parquet'` |
| ORC | ✓ | ✓ | `format 'orc'` |
| Avro | ✓ | - | `format 'avro'` |
| Text | ✓ | ✓ | `format 'text'` |
| CSV | ✓ | ✓ | `format 'csv', delimiter ',', quote '"'` |

外部表通用 OPTIONS：

| 选项 | 说明 |
|------|------|
| `filePath` | 数据文件路径（支持目录） |
| `format` | 文件格式 |
| `compression` | 文件压缩（`gzip` / `snappy` / `zstd`，按 format 而定） |

CSV / Text 额外 OPTIONS（语义对齐 PG `COPY`）：

| 选项 | 说明 |
|------|------|
| `delimiter` | 字段分隔符 |
| `quote` | 引号字符（csv） |
| `escape` | 转义字符 |
| `null` | NULL 字面量 |
| `header` | 是否包含表头（`true`/`false`） |
| `newline` | 行结束符（`lf`/`cr`/`crlf`） |
| `force_not_null` | 这些列即使为空也不视为 NULL |
| `force_null` | 这些列若空则视为 NULL |
| `encoding` | 文件字符集 |
| `fill_missing_fields` | 缺字段时填 NULL（默认报错） |

### 3.4 Hive 表同步

通过 `hive_connector` 扩展从 Hive Metastore 同步表定义：

```sql
-- 同步单张 Hive 表到本地 schema
SELECT sync_hive_table(
    'hive_cluster',      -- Hive 集群配置段名
    'default',           -- Hive 数据库
    'orders',            -- Hive 表名
    'paa_cluster',       -- HDFS 配置段名
    'myschema.orders',   -- 本地目标表（schema.table）
    'hive_server'        -- FDW Server 名
);

-- 同步整个 Hive 数据库
SELECT sync_hive_database(
    'hive_cluster', 'default', 'paa_cluster',
    'hive_schema', 'hive_server'
);
```

同步后可直接 `SELECT * FROM myschema.orders` 查询 Hive 数据。

---

## 4. Iceberg 表

### 4.1 架构

Iceberg 表采用三层架构，每一层独立配置、可组合：

```
Catalog (元数据管理)     支持: Polaris / Hive / Builtin
    +
Volume  (存储访问)       支持: S3 / HDFS / ABFSS（Azure）
    +
Iceberg Table (数据表)   支持: CREATE / INSERT / UPDATE / DELETE / VACUUM
```

> **关于实现机制**：`CREATE ICEBERG TABLE …` 使用的是原生 Table Access Method
> （`CREATE ACCESS METHOD iceberg`），表对象本身是普通 PostgreSQL relation，
> 不是 FOREIGN TABLE。`iceberg_catalog_fdw` 与 `iceberg_volume_fdw` 仅作为元数据
> 与存储层的接入对象（FOREIGN CATALOG / FOREIGN VOLUME）使用。这意味着
> 对 Iceberg 表的查询计划走 PG 原生 executor，不经过 dlproxy；FDW 路径只在
> 通用外部表（第 3 节）和 catalog/volume 元数据交互时使用。

### 4.2 Catalog 配置

#### Builtin Catalog（内建，无需外部服务）

```sql
CREATE SERVER my_cat_srv FOREIGN DATA WRAPPER iceberg_catalog_fdw;
CREATE USER MAPPING FOR current_user SERVER my_cat_srv;
CREATE FOREIGN CATALOG my_catalog SERVER my_cat_srv;
```

#### Hive Catalog

```sql
CREATE SERVER hive_cat_srv FOREIGN DATA WRAPPER iceberg_catalog_fdw
OPTIONS (
    type 'hive',
    url 'thrift://hive-metastore:9083'
);
CREATE USER MAPPING FOR current_user SERVER hive_cat_srv;
CREATE FOREIGN CATALOG hive_catalog SERVER hive_cat_srv
OPTIONS (
    catalog_name 'hive_location',
    default_namespace 'default',
    warehouse_location_prefix 's3://warehouse/hive/'
);
```

Kerberos 认证时需额外 User Mapping 选项：

| 选项 | 说明 |
|------|------|
| `krb_service_principal` | Hive 服务端 principal |
| `krb_client_principal` | 客户端 principal |
| `krb_client_keytab` | Keytab 文件路径 |

#### Polaris Catalog（REST API）

```sql
CREATE SERVER polaris_cat_srv FOREIGN DATA WRAPPER iceberg_catalog_fdw
OPTIONS (
    type 'polaris',
    url 'http://polaris:8181/api/catalog'
);
CREATE USER MAPPING FOR current_user SERVER polaris_cat_srv
OPTIONS (
    client_id 'my_client_id',
    client_secret 'my_client_secret',
    scope 'PRINCIPAL_ROLE:ALL'
);
CREATE FOREIGN CATALOG polaris_catalog SERVER polaris_cat_srv
OPTIONS (
    catalog_name 'production',
    default_namespace 'public',
    enable_metadata_cache 'true',
    metadata_cache_ttl '300'
);
```

#### Hadoop Catalog（`type='hadoop'`）

不依赖外部元数据服务，直接基于文件系统语义在 `warehouse_location_prefix` 指
向的目录里维护 Iceberg metadata，配合 `iceberg_volume_fdw` 上的 S3 / HDFS
即可工作。底层走 Iceberg `HadoopCatalog`。

```sql
-- Hadoop Catalog + S3 (MinIO) Volume
CREATE SERVER hd_cat_srv FOREIGN DATA WRAPPER iceberg_catalog_fdw
    OPTIONS (type 'hadoop');
CREATE USER MAPPING FOR current_user SERVER hd_cat_srv;
CREATE FOREIGN CATALOG hd_cat SERVER hd_cat_srv
    OPTIONS (warehouse_location_prefix 's3a://warehouse/iceberg/');

CREATE SERVER hd_vol_srv FOREIGN DATA WRAPPER iceberg_volume_fdw
    OPTIONS (type 's3', endpoint 'http://lakehouse:9100',
             region 'us-east-1', bucket_name 'warehouse',
             path_style_access 'true');
CREATE USER MAPPING FOR current_user SERVER hd_vol_srv
    OPTIONS (access_key_id 'admin', secret_access_key 'password');
CREATE FOREIGN VOLUME hd_vol SERVER hd_vol_srv
    OPTIONS (base_path '/iceberg/', allow_writes 'true');
```

```sql
-- Hadoop Catalog + HDFS Volume
CREATE SERVER hd_hdfs_cat_srv FOREIGN DATA WRAPPER iceberg_catalog_fdw
    OPTIONS (type 'hadoop');
CREATE USER MAPPING FOR current_user SERVER hd_hdfs_cat_srv;
CREATE FOREIGN CATALOG hd_hdfs_cat SERVER hd_hdfs_cat_srv
    OPTIONS (warehouse_location_prefix 'hdfs://lakehouse:8020/iceberg/');

CREATE SERVER hd_hdfs_vol_srv FOREIGN DATA WRAPPER iceberg_volume_fdw
    OPTIONS (type 'hdfs', endpoint 'hdfs://lakehouse:8020');
CREATE USER MAPPING FOR current_user SERVER hd_hdfs_vol_srv
    OPTIONS (username 'gpadmin');
CREATE FOREIGN VOLUME hd_hdfs_vol SERVER hd_hdfs_vol_srv
    OPTIONS (base_path '/iceberg/', allow_writes 'true');
```

特点：
- 无需 `url`、`catalog_name` —— `warehouse_location_prefix` 必填且要带协议（`s3a://` 或 `hdfs://`）
- 表的 metadata.json 与数据文件位于 `<warehouse_location_prefix>/<namespace>/<table>/`
- 同一份表可被 Spark / Trino / Flink 通过 Iceberg HadoopCatalog 直读
- Iceberg 提交沿用 HDFS / S3 的原子 rename 语义；当存储不支持原子 rename（部分对象存储）需配合 lock 服务，本实现不内置 lock

#### S3 Catalog（`type='s3'`）

与 Hadoop Catalog 同走 Iceberg `HadoopCatalog` 实现，但内部包装为 `IcebergS3Catalog`：把 `warehouse_location_prefix` 拆成 `fs.defaultFS` + `fs.prefix` 后再传给 Iceberg，方便走 S3 vended-credentials 与 endpoint 内外双地址。配套 Volume 必须是 `type='s3'`。

```sql
CREATE SERVER s3_cat_srv FOREIGN DATA WRAPPER iceberg_catalog_fdw
    OPTIONS (type 's3');
CREATE USER MAPPING FOR current_user SERVER s3_cat_srv;
CREATE FOREIGN CATALOG s3_cat SERVER s3_cat_srv
    OPTIONS (warehouse_location_prefix 's3a://warehouse/iceberg_s3/');

CREATE SERVER s3_vol_srv FOREIGN DATA WRAPPER iceberg_volume_fdw
    OPTIONS (type 's3', endpoint 'http://lakehouse:9100',
             region 'us-east-1', bucket_name 'warehouse',
             path_style_access 'true');
CREATE USER MAPPING FOR current_user SERVER s3_vol_srv
    OPTIONS (access_key_id 'admin', secret_access_key 'password');
CREATE FOREIGN VOLUME s3_vol SERVER s3_vol_srv
    OPTIONS (base_path '/iceberg_s3/', allow_writes 'true');
```

`type='hadoop'` vs `type='s3'` 的细微区别：

| 维度 | `hadoop` | `s3` |
|------|----------|------|
| 适用 Volume | s3 / hdfs | 仅 s3 |
| `warehouse_location_prefix` 要求 | 含协议（`s3a://...` 或 `hdfs://...`） | 含协议（`s3a://...`） |
| 内部 Java 实现 | `IcebergHadoopCatalog` | `IcebergS3Catalog`（拆 fs.defaultFS / fs.prefix） |
| 适合场景 | 单存储（HDFS-only 或 S3-only），最简配置 | S3 多端点 / KMS / vended credentials |

#### Catalog × Volume 兼容矩阵

| Catalog `type` | 兼容 Volume `type` | 备注 |
|----------------|------------------|------|
| `builtin` | s3 / hdfs | 内嵌目录服务 |
| `hive` | s3 / hdfs | 元数据走 Hive Metastore |
| `polaris` | 由 Polaris 服务端下发 | 通常不显式建 Volume |
| `hadoop` | s3 / hdfs | warehouse_location_prefix 必填 |
| `s3` | s3 | 仅对象存储 |

#### Catalog 选项汇总

| 选项 | 适用 | 说明 |
|------|------|------|
| `type` | 全部（SERVER） | `builtin` / `hive` / `polaris` / `hadoop` / `s3` |
| `url` | hive / polaris（SERVER） | hive: `thrift://host:9083`；polaris: REST 地址 |
| `catalog_name` | Hive / Polaris | 远端 catalog 名称 |
| `default_namespace` | 全部 | 默认命名空间 |
| `enable_metadata_cache` | 全部 | 启用元数据缓存 |
| `metadata_cache_ttl` | 全部 | 缓存 TTL（秒） |
| `auto_refresh_metadata` | 全部 | 自动刷新 |
| `warehouse_location_prefix` | Hive / Polaris / Hadoop / S3 | 仓库路径前缀；hadoop / s3 必填且带协议 |
| `allow_exists` | Polaris | 远端已存在时不报错（默认 true） |

### 4.3 Volume 配置

#### S3 / MinIO Volume

```sql
CREATE SERVER s3_vol_srv FOREIGN DATA WRAPPER iceberg_volume_fdw
OPTIONS (
    type 's3',
    endpoint 'http://minio:9000',
    region 'us-east-1',
    bucket_name 'warehouse',
    path_style_access 'true'        -- MinIO 必须为 true
);
CREATE USER MAPPING FOR current_user SERVER s3_vol_srv
OPTIONS (access_key_id 'AKIAEXAMPLE', secret_access_key 'SECRET');
CREATE FOREIGN VOLUME my_volume SERVER s3_vol_srv
OPTIONS (base_path '/warehouse/', allow_writes 'true');
```

#### HDFS Volume

```sql
-- 单节点 + Simple 认证
CREATE SERVER hdfs_vol_srv FOREIGN DATA WRAPPER iceberg_volume_fdw
OPTIONS (
    type 'hdfs',
    hdfs_namenodes '192.168.1.10:9000',
    hdfs_auth_method 'simple',
    hadoop_rpc_protection 'authentication'
);
CREATE USER MAPPING FOR current_user SERVER hdfs_vol_srv
OPTIONS (username 'gpadmin');
CREATE FOREIGN VOLUME hdfs_vol SERVER hdfs_vol_srv
OPTIONS (base_path '/iceberg-warehouse/', allow_writes 'true');

-- HA 模式 + Kerberos
CREATE SERVER hdfs_ha_vol_srv FOREIGN DATA WRAPPER iceberg_volume_fdw
OPTIONS (
    type 'hdfs',
    hdfs_namenodes 'mycluster',
    hdfs_auth_method 'kerberos',
    hadoop_rpc_protection 'privacy',
    is_ha_supported 'true',
    dfs_nameservices 'mycluster',
    dfs_ha_namenodes 'nn1,nn2',
    dfs_namenode_rpc_address '192.168.1.10:9000,192.168.1.11:9000',
    dfs_client_failover_proxy_provider
        'org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider'
);
CREATE USER MAPPING FOR current_user SERVER hdfs_ha_vol_srv
OPTIONS (
    username 'gpadmin',
    krb_principal 'gpadmin/master@REALM.COM',
    krb_principal_keytab '/home/gpadmin/gpadmin.keytab'
);
```

Volume 选项汇总（按对象级分层）：

**SERVER OPTIONS**

| 选项 | 适用 type | 说明 |
|------|----------|------|
| `type` | 全部 | `s3` / `hdfs` / `abfss` |
| `endpoint` | s3 | 对象存储 endpoint（含协议） |
| `region` | s3 | 区域 |
| `bucket_name` | s3 | 桶名 |
| `path_style_access` | s3 | path-style（MinIO 必须 `true`） |
| `endpoint_internal` | s3（Polaris） | Polaris 服务端访问的内部 endpoint |
| `sts_endpoint` / `sts_unavailable` | s3（Polaris） | STS endpoint 与禁用开关 |
| `role_arn` / `external_id` / `user_arn` | s3（Polaris） | Vended-credentials 角色信息 |
| `current_kms_key` / `allowed_kms_keys` | s3（Polaris） | SSE-KMS 密钥 |
| `hdfs_namenodes` | hdfs | NameNode 主机或 nameservice |
| `hdfs_auth_method` | hdfs | `simple` / `kerberos` |
| `hadoop_rpc_protection` | hdfs | `authentication` / `integrity` / `privacy` |
| `is_ha_supported` | hdfs | 是否启用 HA |
| `dfs_nameservices` / `dfs_ha_namenodes` / `dfs_namenode_rpc_address` / `dfs_client_failover_proxy_provider` | hdfs (HA) | HA 路由配置 |
| `tenant_id` / `multi_tenant_app_name` / `consent_url` / `hierarchical` | abfss | Azure Data Lake Gen2 多租户访问 |

**USER MAPPING OPTIONS**

| 选项 | 适用 | 说明 |
|------|------|------|
| `username` | 全部 | 操作系统用户 |
| `access_key_id` / `secret_access_key` | s3 | 静态 AK/SK |
| `krb_principal` / `krb_principal_keytab` | hdfs(kerberos) | Kerberos 凭据 |

**CREATE FOREIGN VOLUME OPTIONS**

| 选项 | 说明 |
|------|------|
| `base_path` | 存储基础路径 |
| `allow_writes` | 允许写操作（默认 false） |
| `enable_caching` | 启用缓存（默认 false） |

### 4.4 创建 Iceberg 表

```sql
-- 设置默认 Catalog 和 Volume（避免每条 DDL 都指定）
SET iceberg_default_catalog = 'my_catalog';
SET iceberg_default_volume  = 'my_volume';

-- 基本建表
CREATE ICEBERG TABLE orders (
    id         BIGINT NOT NULL,
    product    VARCHAR(100),
    amount     DECIMAL(15,2) NOT NULL,
    order_date DATE NOT NULL,
    CONSTRAINT pk_orders PRIMARY KEY (id, order_date)
);

-- 显式指定 Catalog + Volume + 选项
CREATE ICEBERG TABLE sales (
    id BIGINT, region TEXT, amount NUMERIC(12,2)
)
CATALOG hive_catalog
VOLUME s3_volume
OPTIONS (
    namespace 'analytics',
    table_name 'sales_2024',
    base_location '/data/sales/'
);
```

**CATALOG / VOLUME 指定规则**：
- 可省略 → 使用 `iceberg_default_catalog` / `iceberg_default_volume` GUC
- Polaris Catalog 可不指定 Volume（存储配置由 Polaris 服务端下发）
- Builtin / Hive Catalog 必须指定 Volume

**数据路径组成**（新表）：
```
protocol://bucket/base_path/base_location/namespace/tablename
```

**CREATE ICEBERG TABLE OPTIONS 全集**：

| OPTION | 类型 | 说明 |
|--------|------|------|
| `catalog` | string | 指定 Catalog（与子句 `CATALOG` 等价；二选一） |
| `namespace` / `namespace_name` | string | 命名空间 |
| `table_name` | string | 在 catalog 中暴露的表名（默认与 PG 表名一致） |
| `base_location` | string | 表数据目录（相对 Volume 的 `base_path`） |
| `autovacuum_enabled` | bool | 是否参与 `datalake.iceberg_autovacuum` 自动 VACUUM（默认 `true`） |

### 4.5 支持的数据类型

| PostgreSQL 类型 | Iceberg 类型 | 说明 |
|----------------|-------------|------|
| `boolean` | boolean | |
| `smallint` | int | |
| `integer` | int | |
| `bigint` | long | |
| `real` | float | |
| `double precision` | double | |
| `decimal(p,s)` / `numeric(p,s)` | decimal(p,s) | **必须指定精度** |
| `text` / `varchar(n)` | string | |
| `date` | date | |
| `timestamp` | timestamp | 无时区 |
| `timestamptz` | timestamptz | 带时区 |
| `bytea` | binary | |

> **重要**：`numeric` 不指定精度时，Iceberg（Parquet 编码）会报错。务必写成 `numeric(p,s)`。

### 4.6 DML 操作

```sql
-- INSERT
INSERT INTO orders VALUES (1, 'Widget', 99.99, '2024-01-15');
INSERT INTO orders SELECT * FROM staging_orders;

-- UPDATE
UPDATE orders SET amount = amount * 1.1 WHERE region = 'US-West';

-- DELETE
DELETE FROM orders WHERE order_date < '2023-01-01';

-- SELECT（支持谓词下推 + 列裁剪）
SELECT region, SUM(amount) FROM orders
WHERE order_date >= '2024-01-01'
GROUP BY region;
```

**UPDATE / DELETE 实现**：基于 Iceberg v2 position-delete（merge-on-read）。
单条 UPDATE / DELETE 不重写数据文件，只产生 position-delete 记录；后续 SELECT
通过 anti-join 消除被删行。累积位置删除过多后由 VACUUM 触发 compaction 合并。

> ⚠️ **TRUNCATE 当前是 no-op**（`iceberg_relation_nontransactional_truncate`
> 实现为空），调用不会报错但表内容不会被清空。需要清空请用
> `DELETE FROM <table>` 或 `DROP TABLE` 后重建。

### 4.7 Schema 演化

当前版本**所有 `ALTER TABLE` 子命令在 Iceberg 表上均不支持**，执行会报错：

```
ERROR:  ALTER TABLE is not supported on Iceberg tables
HINT:   Use Spark/Trino/Flink to perform Iceberg schema evolution, or recreate the table.
```

需要做 Schema 演化，请在 Spark/Trino/Flink 端通过 Iceberg DDL 完成，或在 Cloudberry 端 `DROP TABLE` 后重建。

Iceberg 通过 **field-id 映射**支持不同 schema 版本的文件并存读取。若通过外部引擎添加了新列，datalake_fdw 可正确读取旧文件（该列显示为 NULL）。

**当前 ALTER 支持状态**：

| 子命令 | 状态 | 说明 |
|--------|------|------|
| `ADD COLUMN` | ❌ 不支持，会报错 | 请通过 Spark/Trino/Flink 的 Iceberg DDL 添加列 |
| `DROP COLUMN` | ❌ 不支持，会报错 | 同上 |
| `RENAME COLUMN` | ❌ 不支持，会报错 | 通过 `RenameStmt` 路径同样被拦截 |
| `ALTER COLUMN TYPE` | ❌ 不支持，会报错 | 曾导致 PG rewrite 路径踩到 Iceberg AM 空 stub（SIGSEGV，issue #334） |
| `SET / DROP NOT NULL` / `SET / DROP DEFAULT` / `ADD CONSTRAINT` / `OWNER TO` / `SET STORAGE` 等 | ❌ 不支持，会报错 | 均不传播到 Iceberg metadata |

> 实现层面：所有 `AlterTableStmt` 子命令以及 `RenameStmt(OBJECT_COLUMN)` 都在 `datalake_fdw.c` 的 `datalake_ProcessUtility` 里被显式拦截，报 `ERRCODE_FEATURE_NOT_SUPPORTED`。

### 4.8 VACUUM（压缩）

Iceberg 表通过 VACUUM 触发数据文件压缩（compaction）：

```sql
-- 基本用法
VACUUM orders;

-- 调整压缩参数
SET datalake.iceberg_vacuum_compact_min_input_files = 5;    -- 最少 5 个文件才触发合并
SET datalake.iceberg_vacuum_rewrite_target_file_size_mb = 256; -- 目标文件大小 256MB
VACUUM orders;
```

VACUUM 实际做的事：
- **合并小文件为大文件**（compaction）—— 当数据文件数 ≥
  `iceberg_vacuum_compact_min_input_files` 时触发，输出文件接近
  `iceberg_vacuum_rewrite_target_file_size_mb`
- **写新快照** —— compaction 完成后产生新的 metadata.json，旧快照仍保留

VACUUM **不做**的事（容易误解）：

| 项 | 现状 | 由谁负责 |
|----|------|---------|
| 删除已被 position-delete 标记的行的物理数据 | 通过 compaction 间接重写（被删行不写出），不是即时删除 | VACUUM compaction |
| 显式 `expireSnapshots()` 清理过期快照 | 当前代码无此调用 | 由后台异步 deletion queue 按 `iceberg_max_snapshot_age` (默认 5 天) 处理 |
| `removeOrphanFiles()` 扫描并删除孤儿文件 | 当前代码无此调用 | 不会自动清理；如有遗留请用 Spark/Trino 的 `remove_orphan_files` 程序 |
| 清理过期的 metadata.json | 当前代码无此调用 | 同上，需外部工具 |

换言之，**VACUUM 只做 compaction**；旧快照、过期数据文件、孤儿文件的回收依赖
后台 deletion queue（受 `iceberg_max_snapshot_age` /
`iceberg_max_file_removals_per_vacuum` 控制）以及外部维护任务。

> **注意**：VACUUM 不能在事务块或 PL/pgSQL 函数内执行。

### 4.9 自动 VACUUM

```sql
-- 启用自动 VACUUM（需 superuser，修改后需 reload）
ALTER SYSTEM SET datalake.iceberg_autovacuum = on;
ALTER SYSTEM SET datalake.iceberg_autovacuum_naptime = 600;  -- 每 10 分钟检查
SELECT pg_reload_conf();
```

### 4.10 内置工具函数（`iceberg_toolkit`）

`datalake_fdw` 注册了一组 SQL 函数用于直接调用 catalog/volume 操作或检查表元数据。所有函数定义在 `iceberg_toolkit` schema 下。

| 函数 | 说明 | 典型用途 |
|------|------|---------|
| `iceberg_toolkit.catalog_fdw(server_name text, op text, args jsonb) → jsonb` | 在指定 catalog server 上执行底层操作（列出 namespace/表、创建 namespace 等） | 排查 catalog 连通性 |
| `iceberg_toolkit.volume_fdw(server_name text, op text, args jsonb) → jsonb` | 在指定 volume server 上执行底层存储操作 | 列目录、读对象元数据 |
| `iceberg_toolkit.create_table(catalog text, namespace text, table_name text, schema_def text, …)` | 不通过 SQL DDL，直接调用 catalog 注册一张 Iceberg 表 | 快速接入已有数据目录 |
| `iceberg_toolkit.get_fragments(table_oid oid) → setof record` | 列出某张表的 scan fragment（数据文件、起止 offset） | 分析数据布局、定位倾斜 |
| `iceberg_toolkit.polaris_list_catalogs(server_name text) → setof text` | 列出 Polaris 服务端可见 catalog | 配置阶段排查 |
| `iceberg_toolkit.polaris_list_namespaces(server_name text, catalog text) → setof text` | 列出 Polaris catalog 下的 namespace | 同上 |

```sql
-- 例：列出 Polaris catalog
SELECT * FROM iceberg_toolkit.polaris_list_catalogs('polaris_cat_srv');

-- 例：查看一张表的 fragment 分布
SELECT * FROM iceberg_toolkit.get_fragments('orders'::regclass);
```

> 这些函数提供"绕开 DDL"的检查能力，主要用于排障；正常业务请走 `CREATE ICEBERG TABLE` / `INSERT` / `SELECT`。

### 4.11 当前不支持的功能 / 已知限制

#### 4.11.1 完全未实现

| 功能 | 备注 |
|------|------|
| `PARTITION BY`（identity / bucket / truncate / year/month/day/hour） | 表统一按文件分布。可由 Spark 等引擎写入分区表后再被 datalake_fdw 读取 |
| Time travel（`FOR SYSTEM_TIME AS OF` / `FOR SYSTEM_VERSION AS OF`） | 当前总是读最新快照 |
| `format-version=1/2`、`write.format.default`、`write.parquet.compression-codec`、`write.distribution-mode` 等 Iceberg 表级 property | 由 catalog 默认值决定，无 SQL 入口可覆盖 |
| 跨表分布式事务 | 单表 ACID；多表写入不构成原子事务 |
| Row-level security / 列级权限传递到物理 Iceberg 数据 | 仅在 PG 入口生效，物理文件无访问控制 |
| 生成列（GENERATED COLUMN）、表达式 DEFAULT 写入 Iceberg | PG 端语法可写但不会反映到 Iceberg schema |

#### 4.11.2 静默 no-op（不会报错但实际无效）

| 操作 | 实际行为 |
|------|---------|
| `TRUNCATE <iceberg_table>` | `iceberg_relation_nontransactional_truncate` 实现为空；表内容不会被清空。需要清空请用 `DELETE FROM` 或 `DROP TABLE` 重建 |
| `CREATE INDEX ... ON <iceberg_table>` | `iceberg_index_build_range_scan` 直接返回 0；索引对象创建后无任何条目 |
| `ANALYZE <iceberg_table>` | 抛 NOTICE：`ANALYZE is a no-op for Iceberg tables; planner stats come from Iceberg catalog metadata`。统计来自 manifest |
| `PRIMARY KEY` / `UNIQUE` / `FOREIGN KEY` / `CHECK` 约束 | DDL 接受但无 Iceberg 端唯一索引承载，运行时不强制 |

#### 4.11.3 显式拒绝

| 操作 | 错误 |
|------|------|
| 所有 `ALTER TABLE` 子命令（`ADD / DROP / RENAME COLUMN`、`ALTER COLUMN TYPE`、`SET/DROP NOT NULL`、`ADD CONSTRAINT` 等） | `ERRCODE_FEATURE_NOT_SUPPORTED`: `ALTER TABLE is not supported on Iceberg tables`（datalake_fdw.c） |
| `RENAME COLUMN`（通过 `ALTER TABLE … RENAME COLUMN`） | `ERRCODE_FEATURE_NOT_SUPPORTED`: `RENAME COLUMN is not supported on Iceberg tables`（datalake_fdw.c） |
| TID range scan（如 `WHERE ctid <@ '...'` 之类的范围 TID 谓词） | `ERRCODE_FEATURE_NOT_SUPPORTED`: `not supported`（pg_iceberg_am_handler.c:496） |
| ANALYZE 内部的 `analyze_next_block` / `analyze_next_tuple` API | `ERRCODE_INTERNAL_ERROR`: `API not supported for iceberg relations`（pg_iceberg_am_handler.c:668） |

#### 4.11.4 并发写入约束

同一张 Iceberg 表的并发提交走 **CAS + 重试**，最多重试 10 次（`TRACKER_MAX_COMMIT_RETRIES`）；超过会以 `ERRCODE_T_R_SERIALIZATION_FAILURE` 报错：

```
ERROR:  failed to commit iceberg metadata for table <oid> after <N> retries
        due to concurrent updates
```

实务建议：
- 高并发写场景把单笔写入做大（一次 INSERT 多行），减少 commit 次数
- 重试前会清理本地缓存并以最新 snapshot 重 base；外部 catalog（Hive/Polaris）不会重复触发 agent commit，避免重复落数据

需要更强 Iceberg 能力（PARTITION BY、time travel、表 property、跨引擎一致 schema 演化）时，建议结合 Spark/Trino/Flink 直接对 Iceberg 表写元数据，由 datalake_fdw 做读侧消费。

---

## 5. 谓词下推与列裁剪

`datalake_fdw` 自动将 WHERE 条件下推到存储层，减少网络传输和 IO：

```sql
-- 下推生效的操作符：=, >, <, >=, <=, !=, IS NULL, IS NOT NULL, LIKE, IN
SELECT * FROM orders WHERE id = 12345;       -- 等值下推
SELECT * FROM orders WHERE amount > 100.00;  -- 范围下推
SELECT * FROM orders WHERE region IN ('US', 'EU');  -- IN 下推

-- 列裁剪：只读取查询需要的列
SELECT id, amount FROM orders;  -- 只读 id, amount 两列的数据
```

验证下推效果：
```sql
EXPLAIN SELECT * FROM orders WHERE id = 12345;
-- 计划中应显示 Filter 被推到外部表层

-- 对比关闭下推
SET datalake.disable_filter_pushdown = on;
EXPLAIN SELECT * FROM orders WHERE id = 12345;
-- Filter 在上层执行，scan 返回全部行
RESET datalake.disable_filter_pushdown;
```

---

## 6. GUC 参考

### 会话级（USERSET，`SET` 可改）

| GUC | 默认值 | 说明 |
|-----|--------|------|
| `iceberg_default_catalog` | `''` | 默认 Catalog 名称 |
| `iceberg_default_volume` | `''` | 默认 Volume 名称 |
| `datalake.disable_filter_pushdown` | `off` | 关闭谓词下推 |
| `datalake.disable_cache_file` | `off` | 关闭文件缓存 |
| `datalake.enable_iceberg_fragment_cache` | `on` | Iceberg fragment 缓存 |
| `datalake.iceberg_vacuum_compact_min_input_files` | `5` | VACUUM 最少输入文件数 |
| `datalake.iceberg_vacuum_rewrite_target_file_size_mb` | `512` | VACUUM 目标文件大小（MB） |
| `datalake.iceberg_postion_deletes_threshold` | `100000` | Position delete 阈值（100K – 10M） |
| `datalake.hudi_log_merger_threshold` | `512` | Hudi log merge 阈值（MB，128 – 10240） |
| `datalake.hudi_log_scale_factor` | `1.3` | Hudi 临时文件膨胀系数（1 – 10） |
| `datalake.external_table_limit_segment_num` | `0` | 限制参与扫描的 segment 数（0=不限） |
| `datalake.external_table_debug` | `off` | 调试模式 |
| `datalake.external_table_new_text` | `off` | 启用新版 text 解析 |
| `datalake.external_table_ignore_hidden_file` | `off` | 忽略隐藏文件/目录（`.` 开头） |
| `datalake.enable_set_hdfs_user` | `on` | 从 user mapping 设置 HDFS 用户 |
| `datalake.enable_list_in_master` | `on` | HDFS list 仅在 master 执行 |
| `datalake.enable_get_block_location` | `off` | 读取 HDFS block location |
| `datalake.agent_server_url` | `http://localhost:3888` | datalake_agent 地址 |
| `datalake.skip_create_polaris_catalog` | `off` | 跳过 Polaris catalog 自动创建 |

### Superuser 级（SUSET）

| GUC | 默认值 | 说明 |
|-----|--------|------|
| `datalake.iceberg_max_snapshot_age` | `432000`（5 天） | 快照最大保留时间（秒） |
| `datalake.iceberg_max_file_removals_per_vacuum` | `100000` | 单次 VACUUM 最大删除文件数 |
| `datalake.iceberg_max_compactions_per_vacuum` | `100` | 单次 VACUUM 最大压缩操作数 |

### 配置文件级（SIGHUP，`pg_reload_conf()` 生效）

| GUC | 默认值 | 说明 |
|-----|--------|------|
| `datalake.iceberg_autovacuum` | `off` | 启用自动 VACUUM |
| `datalake.iceberg_autovacuum_naptime` | `600` | 自动 VACUUM 间隔（秒） |
| `datalake.iceberg_log_autovacuum_min_duration` | `600000` | 记录超过此时长（ms）的自动 VACUUM；`-1` 全不记录、`0` 全部记录 |

---

## 7. 端到端示例

### 7.1 S3 + Builtin Catalog（最简配置）

```sql
-- 1. 扩展
CREATE EXTENSION IF NOT EXISTS datalake_fdw;

-- 2. Catalog（内建，无外部依赖）
CREATE SERVER cat FOREIGN DATA WRAPPER iceberg_catalog_fdw;
CREATE USER MAPPING FOR current_user SERVER cat;
CREATE FOREIGN CATALOG my_cat SERVER cat;
SET iceberg_default_catalog = 'my_cat';

-- 3. Volume（S3/MinIO）
CREATE SERVER vol FOREIGN DATA WRAPPER iceberg_volume_fdw
OPTIONS (type 's3', endpoint 'http://minio:9000', region 'us-east-1',
         bucket_name 'warehouse', path_style_access 'true');
CREATE USER MAPPING FOR current_user SERVER vol
OPTIONS (access_key_id 'admin', secret_access_key 'password');
CREATE FOREIGN VOLUME my_vol SERVER vol OPTIONS (base_path '/data/');
SET iceberg_default_volume = 'my_vol';

-- 4. 建表 + 使用
CREATE ICEBERG TABLE users (id int, name text, score numeric(10,2));
INSERT INTO users VALUES (1, 'Alice', 95.5), (2, 'Bob', 87.0);
SELECT * FROM users WHERE score > 90;
UPDATE users SET score = 96.0 WHERE id = 1;
VACUUM users;
DROP TABLE users;

-- 5. 清理
DROP VOLUME my_vol;
DROP USER MAPPING FOR current_user SERVER vol;
DROP SERVER vol;
DROP CATALOG my_cat;
DROP USER MAPPING FOR current_user SERVER cat;
DROP SERVER cat;
```

### 7.2 Hive 外部表查询

```sql
CREATE EXTENSION IF NOT EXISTS datalake_fdw;
CREATE EXTENSION IF NOT EXISTS hive_connector;

-- Hive Server（用于 sync_hive_table）
CREATE SERVER hive_server
    FOREIGN DATA WRAPPER datalake_fdw
    OPTIONS (protocol 'hdfs', hdfs_namenodes 'namenode',
             hdfs_port '8020', hdfs_auth_method 'simple',
             hadoop_rpc_protection 'authentication');
CREATE USER MAPPING FOR gpadmin SERVER hive_server OPTIONS (user 'gpadmin');

-- 从 Hive Metastore 同步表
SELECT sync_hive_table('hive_cluster', 'default', 'orders',
                       'paa_cluster', 'hive.orders', 'hive_server');

-- 直接查询
SELECT * FROM hive.orders WHERE order_date > '2024-01-01';
```

---

## 8. 对象清理顺序

创建和删除需遵循依赖顺序：

```
创建：Extension → Server → User Mapping → Catalog/Volume → Iceberg Table
删除：Iceberg Table → Volume → Catalog → User Mapping → Server
```

```sql
-- 删除示例（从里到外）
DROP TABLE my_iceberg_table;
DROP VOLUME my_volume;
DROP USER MAPPING FOR current_user SERVER vol_server;
DROP SERVER vol_server;
DROP CATALOG my_catalog;
DROP USER MAPPING FOR current_user SERVER cat_server;
DROP SERVER cat_server;
```

或使用 `CASCADE`：
```sql
DROP SERVER cat_server CASCADE;  -- 级联删除所有依赖对象
```

---

## 9. 常见问题

### 9.1 配置类

| 问题 / 错误信息 | 原因 | 解决 |
|----------------|------|------|
| `numeric` 列建表报错 | Parquet 编码要求精度 | 改为 `numeric(p,s)` |
| VACUUM 在函数内报错 | PG 限制 | 在函数外执行 VACUUM |
| Hive Catalog 读回为空 | Hive Metastore 配置 | 检查 `url` 格式：`thrift://host:port` |
| 下推不生效 | GUC 被关闭 | `RESET datalake.disable_filter_pushdown` |
| Polaris 401 错误 | OAuth 认证失败 | 检查 `client_id` / `client_secret` / `scope` |
| 外部表文件找不到 | `filePath` 路径错误 | 确认包含 bucket 前缀或绝对路径 |

### 9.2 Iceberg 表运行时错误对照

| 错误信息（出处） | 含义 | 排查方向 |
|-----------------|------|---------|
| `iceberg table "%s.%s" does not exist in external catalog "%s"` | catalog 中未注册该表 | 确认远端 catalog 名 / namespace / table 拼写；Polaris 检查 `default_namespace` |
| `failed to resolve iceberg table location for relation "%s"` | catalog 拿不到表位置 | catalog 服务连通性、`base_location` 是否被改动、metadata 是否损坏 |
| `failed to load iceberg table metadata for relation %u` | metadata.json 读不下来 | 检查 Volume 凭据、`base_path`；若使用对象存储确认 endpoint |
| `external catalog "%s" returned empty table location for "%s.%s"` | catalog 返回空 location | catalog 实现 bug 或 namespace 入库异常；可用 `iceberg_toolkit.catalog_fdw` 直查 |
| `empty iceberg table location suffix` | builtin catalog 解析出空路径 | 表名 / namespace 含特殊字符；规整后重建 |
| `iceberg metadata catalog is not available on this segment` | 在 QE 上调用了 QD-only 的元数据接口 | 检查是否在 PL/pgSQL 中误用了元数据函数；改为在 QD 上执行 |
| `foreign catalog with OID %u does not exist` | catalog 对象引用失效 | 排查是否 DROP 后未重建；`pg_foreign_catalog` 中确认存在 |
| `foreign volume with OID %u does not exist` | volume 对象引用失效 | 同上 |
| `must be superuser to create iceberg metadata table` | 非 superuser 触发首次元数据初始化 | 由 superuser 先执行一次任意 Iceberg DDL 完成初始化 |
| `API not supported for iceberg relations` | 调用了 heap-specific API（如 part of CTID-only path） | 多见于第三方扩展直接调内部 API；汇报到 issue |
| `invalid value for boolean option "%s": "%s"` | OPTION 取值不是布尔字面量 | 用 `true` / `false`，不要用 `0/1` 或 `yes/no` |
| `failed to commit iceberg metadata for table %u after %d retries due to concurrent updates` | 并发写入冲突 CAS 重试 10 次仍失败（详见 4.11.4） | 把多笔写合并成一笔事务、降低并发写并发度，或排查是否有外部引擎同时在写同一张表 |
| `not supported`（来自 pg_iceberg_am_handler.c:496） | 触发了 TID range scan 路径 | 改用基于普通列的谓词，避免 `ctid <@ ...` 之类查询 |
| `API not supported for iceberg relations` | 调用了 heap-only API（多见于第三方扩展或 ANALYZE 内部 block API） | 见 4.11.3；或避免该扩展直接走 iceberg 表 |
| `ANALYZE is a no-op for Iceberg tables; planner stats come from Iceberg catalog metadata` | 不是错误，只是 NOTICE | 统计信息来自 Iceberg manifest，不需要 ANALYZE 采样 |
