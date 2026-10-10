# datalake_fdw 与 Iceberg 完整使用指南

## 1. 概述

`datalake_fdw` 是 HashData Lightning 的数据湖访问扩展，提供两大能力：

- **FDW 外部表**：直接读写 S3/HDFS/Hive 上的 Parquet、ORC、Avro、Text、CSV 文件
- **Iceberg 表**：通过 Catalog + Volume + Iceberg Table 三层架构，支持事务性的 ACID 读写、schema 兼容读（field-id 映射）、快照隔离和 VACUUM 压缩

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
Catalog (元数据管理)     支持: Builtin / Hive / Polaris / Hadoop / S3
    +
Volume  (存储访问)       支持: S3 / HDFS
    +
Iceberg Table (数据表)   支持: CREATE / INSERT / UPDATE / DELETE / VACUUM
```

> **关于实现机制**：`CREATE ICEBERG TABLE …` 使用的是原生 Table Access Method
> （`CREATE ACCESS METHOD iceberg`），表对象本身是普通 PostgreSQL relation，
> 不是 FOREIGN TABLE。`iceberg_catalog_fdw` 与 `iceberg_volume_fdw` 仅作为元数据
> 与存储层的接入对象（FOREIGN CATALOG / FOREIGN VOLUME）使用。

#### 为什么 Catalog 与 Volume 都要配连接信息（设计说明）

某些组合下两边的连接信息看起来重复——例如 hadoop catalog 的
`warehouse_location_prefix 'hdfs://lakehouse:8020/…'` 已含 NameNode 地址，HDFS volume
又要配一遍 `hdfs_namenodes`。这是**元数据面与数据面分离**的有意设计：

| 对象 | 回答的问题 | 消费方 |
|------|-----------|--------|
| Catalog | 表在哪注册、metadata.json 指针由谁管 | QD → datalake_agent（元数据面：建表 / 加载 / 提交 / fragments） |
| Volume | 数据文件的字节怎么读写、用什么凭据 | 各 segment 的 gopher（数据面：SELECT / INSERT 的文件 IO） |

分离带来的能力：

- **自由组合**：catalog 与 volume 是两条独立的轴（见下文兼容矩阵）；同一 catalog
  下的不同表可以落在不同 volume（不同桶 / 不同集群 / 不同凭据）上
- **凭据域天然不同**：catalog 侧是 HMS Kerberos / Polaris OAuth，volume 侧是
  S3 AK/SK / HDFS Kerberos，混在一个对象上无法表达"元数据与数据分属不同权限体系"
- **可以只配一边**：Polaris catalog 由服务端下发存储凭据（vended credentials），
  无需建 volume——印证了"连接信息的归宿在 volume，catalog 原则上只管元数据"

短期的重叠点（hadoop / s3 catalog 的 `warehouse_location_prefix` 里携带的
host / bucket 必须与 volume 的 `hdfs_namenodes` / `bucket_name` 保持一致）来源于
两个平面各自独立取配置；配置文件模式（`server_name`，见 4.3）落地后，这部分
重复会进一步收敛为"两边引用同一个 conf 段"。

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

Hive Catalog 的 User Mapping 选项：

| 选项 | 说明 |
|------|------|
| `username` | 操作系统用户 |
| `auth_method` | 认证方式（`simple` / `kerberos`） |
| `krb_service_principal` | Hive 服务端 principal（kerberos） |
| `krb_client_principal` | 客户端 principal（kerberos） |
| `krb_client_keytab` | Keytab 文件路径（kerberos） |

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
    metadata_cache '5'
);
```

#### 通用 REST Catalog（`type='rest'`）

面向实现了 Iceberg REST 规范的存储平台（如星辰天合 XSKY OSG）。与 Polaris 的
区别在于：Polaris 是带专有 admin API 的特定实现，`rest` 走标准协议，不假设任何
厂商扩展。

**与其他 catalog 最大的不同：不建 Volume。** REST catalog 在建表时为每张表分配
自己的存储位置，并按请求下发临时凭证；一个静态配置的 Volume 既无法提供位置、
也无法提供凭证，写进去反而会用固定 bucket 覆盖掉平台分配的那个。所以这类表
`CREATE ICEBERG TABLE` 不带 `VOLUME`，写了会直接报错。

```sql
CREATE SERVER xsky_srv FOREIGN DATA WRAPPER iceberg_catalog_fdw
OPTIONS (
    type 'rest',
    url 'http://10.14.8.253:8060/iceberg',   -- Iceberg REST 基址
    endpoint 'http://10.14.8.253:8060',      -- 对象存储地址
    region 'us-east-1',
    signing_name 's3tables',                 -- SigV4 的 service 名
    path_style_access 'true'
);

CREATE USER MAPPING FOR current_user SERVER xsky_srv
OPTIONS (
    auth_method 'sigv4',
    access_key_id '<AK>',
    secret_access_key '<SK>'
);

CREATE FOREIGN CATALOG xsky_cat SERVER xsky_srv
OPTIONS (
    catalog_name 'ice-tds',       -- 平台上的表桶名
    default_namespace 'default',
    prefix 'ice-tds'              -- REST 路由前缀，通常与 catalog_name 相同
);

-- 建表：没有 VOLUME 子句
CREATE ICEBERG TABLE orders (id bigint, amount numeric) CATALOG xsky_cat;
```

REST Catalog 的 SERVER 选项：

| 选项 | 必填 | 说明 |
|------|------|------|
| `url` | 是 | Iceberg REST 基址，`http://` 或 `https://`，不能带 userinfo |
| `region` | sigv4 时必填 | 签名用的 region |
| `signing_name` | 否 | SigV4 的 service 名；不填则用默认。XSKY 用 `s3tables` |
| `endpoint` | 否 | 对象存储地址；不填时从 `url` 推导（仅当 catalog 与对象存储同址时成立，建议显式写） |
| `path_style_access` | 否 | 默认 `true` |
| `maintenance_api` | 否 | 平台的维护接口类型，目前只支持 `s3tables`；见 4.8 |
| `vended_credentials` | 否 | 整套部署的默认值，语义见下面 USER MAPPING 表中的同名选项；USER MAPPING 写了以它为准 |
| `polaris_server_realm` | 否 | **仅当这个 REST catalog 是 Polaris 部署时才需要**。作为 `Polaris-Realm` 请求头发出；不写则不发该头（其他平台不认识 realm，凭空造一个头不是无害的）。配了非默认 realm 的 Polaris 在缺这个头时会用 404 `MissingOrInvalidRealm` 拒掉 OAuth 取 token 的请求 |

> **用 `type='rest'` 接 Polaris**：Polaris 是符合 Iceberg REST 规范的 catalog，
> 用通用 rest 类型接得上，认证走 `auth_method 'oauth2'`。好处是**不需要配
> FOREIGN VOLUME**——位置由 catalog 分配、凭证由它按表下发，而 `type='polaris'`
> 目前仍要求配 volume。若该 Polaris 用了非默认 realm，补上 `polaris_server_realm`。

USER MAPPING 选项：

| 选项 | 说明 |
|------|------|
| `auth_method` | `sigv4` / `oauth2` / `none` |
| `access_key_id` / `secret_access_key` | sigv4 认证，必须成对出现 |
| `client_id` / `client_secret` | oauth2 认证，必须成对出现 |
| `vended_credentials` | 默认 `on`。关闭后改用上面的静态密钥读写数据文件，隔离性更弱，因此只能显式关闭，不会自动回退。关闭还有一个副作用：扫描不必再向 catalog 索要凭证，配合 `metadata_cache` 可做到零请求，见下文 |

> **`vended_credentials` 两个层级都能写**：SERVER 上是整套部署的默认值，
> USER MAPPING 上是这个用户的取值，**写了就以 USER MAPPING 为准**。两边都不写
> 默认 `on`。取值拼错（比如写成 `no`）会在 DDL 当场报错，不会被当成 `on` 放过去。
>
> 关成 `off` 却没有配静态密钥，会在解析时直接报错
> `vended_credentials is "off" but no static credentials are configured`——
> 无论这个 `off` 写在哪一层。

> **凭证有效期**：平台下发的临时凭证有寿命，XSKY S3 Tables 与 Polaris 均为
> 1 小时，并在响应里给出 `s3.session-token-expires-at-ms`。dlagent 缓存这组
> 凭证时会读取该字段，在到期前就主动丢弃，因此缓存命中不会发出过期凭证。仍然
> 可能失败的是**单条语句本身跑满一小时**——凭证在语句执行途中过期，重跑即可。

> **扫描路径不再每次问 catalog**。一次扫描要向 catalog 要两样东西：表当前的
> head，以及读数据用的存储凭证（segment 上没有 volume，凭证必须随 fragment 一起
> 下发）。这两件事各有各的缓存，互不影响：
>
> | 要的东西 | 缓存在哪 | 由谁控制 |
> |----------|----------|----------|
> | 当前 head（metadata.json 位置） | PG 侧 `pg_iceberg_metadata` | 表选项 / catalog 选项 `metadata_cache`，同名同单位（见 4.8） |
> | 存储凭证 + FileIO | dlagent 进程内，按 (连接, 表) 分条 | `dlagent.iceberg.file-io-cache-ttl`（默认 5 分钟） |
>
> 凭证缓存命中时，这次扫描**一个 catalog 请求都不发**——和 Polaris 表一样。
> `file-io-cache-ttl` 只是上限（权限被回收后最多多久才察觉）；下限由凭证自带的
> 过期时间兜底，所以把它调大不会导致发出过期凭证。缓存按表的 bucket 记账，
> 表被删掉重建（REST catalog 会分配新 bucket）时下一次扫描会自动重新解析。

> **想彻底不碰 catalog：`vended_credentials off` + `metadata_cache`**。上表两
> 样东西各有一条免请求的路子，同时走通就是**每次查询零 catalog 请求**：
>
> - **凭证**：关掉 `vended_credentials` 后用的是 USER MAPPING 上的静态密钥，
>   本来就在本地，不需要问任何人；
> - **head**：`metadata_cache` 设成正数后，TTL 内直接用 PG 侧记着的那个；
> - **bucket**：从 head 字符串里现成解析出来（每张表的桶名就写在它的
>   metadata.json 路径里），也不需要问。
>
> 三样都不用问，这条链路就和读一张普通 Parquet 外表一样了。代价要说清楚，
> 而且两条都不是这个组合新引入的，只是叠加后更明显：
>
> | 代价 | 来自哪个开关 | 具体表现 |
> |------|-------------|---------|
> | 看不到别的引擎的提交 | `metadata_cache` | TTL 到期前读不到 Spark 等外部引擎的新快照（本集群自己的写入不受影响，始终立刻可见） |
> | 凭证长期且跨表 | `vended off` | 用的是一组固定的静态密钥，不是按表下发的临时凭证，权限隔离更弱 |
>
> 所以这是一次明确的取舍：拿**新鲜度**和**隔离性**换请求数。适合数据由本集群
> 自己写、或者外部更新频率远低于 TTL 的场景；如果表会被别的引擎频繁改写，
> 或者合规上要求按表隔离凭证，就不要这么配。

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
    OPTIONS (warehouse_location_prefix 's3://warehouse/iceberg/');

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
    OPTIONS (type 'hdfs', hdfs_namenodes 'lakehouse:8020',
             hdfs_auth_method 'simple');
CREATE USER MAPPING FOR current_user SERVER hd_hdfs_vol_srv
    OPTIONS (username 'gpadmin');
CREATE FOREIGN VOLUME hd_hdfs_vol SERVER hd_hdfs_vol_srv
    OPTIONS (base_path '/iceberg/', allow_writes 'true');
```

特点：
- 无需 `url`、`catalog_name` —— `warehouse_location_prefix` 必填且要带协议（`s3://` 或 `hdfs://`）
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
    OPTIONS (warehouse_location_prefix 's3://warehouse/iceberg_s3/');

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
| `warehouse_location_prefix` 要求 | 含协议（`s3://...` 或 `hdfs://...`） | 含协议（`s3://...`） |
| 内部 Java 实现 | `IcebergHadoopCatalog` | `IcebergS3Catalog`（拆 fs.defaultFS / fs.prefix） |
| 适合场景 | 单存储（HDFS-only 或 S3-only），最简配置 | S3 多端点 / KMS / vended credentials |

#### Catalog × Volume 兼容矩阵

| Catalog `type` | 兼容 Volume `type` | 备注 |
|----------------|------------------|------|
| `builtin` | s3 / hdfs | 内嵌目录服务 |
| `hive` | s3 / hdfs | 元数据走 Hive Metastore |
| `polaris` | s3 | 表数据物理路径由 Polaris 服务端下发，但**仍必须建 Volume**：QE 通过 Volume 的 endpoint 与 AK/SK 读写数据文件 |
| `rest` | **不建 Volume** | 位置与凭证均由平台按表下发；写 VOLUME 会被拒绝 |
| `hadoop` | s3 / hdfs | warehouse_location_prefix 必填 |
| `s3` | s3 | 仅对象存储 |

#### Catalog 选项汇总

| 选项 | 适用 | 说明 |
|------|------|------|
| `type` | 全部（SERVER） | `builtin` / `hive` / `polaris` / `rest` / `hadoop` / `s3` |
| `url` | hive / polaris / rest（SERVER） | hive: `thrift://host:9083`；polaris / rest: REST 地址 |
| `polaris_server_realm` | Polaris（SERVER） | 发往 Polaris 的 `Polaris-Realm` 请求头；不设置时默认 `POLARIS`，需与服务端配置的 realm 一致（如 `default`） |
| `server_name` | hive（SERVER） | 配置文件模式：未在 SQL OPTIONS 里写的连接配置从 `gphive.conf` 对应段补齐，SQL 里写了的优先（见 4.3 末尾"配置文件模式"） |
| `catalog_name` | Hive / Polaris / REST | 远端 catalog 名称；REST 下即平台上的表桶名 |
| `default_namespace` | 全部 | 默认命名空间 |
| `metadata_cache` | 全部 | head 缓存 TTL（**分钟**），该 catalog 下所有表的默认值，单表可用同名表选项覆盖；`0` 或不写 = 不缓存，每次都取最新 head。它只影响**别的引擎**的提交多久可见——本集群自己的写入始终立刻可见；也与 manifest / metadata.json 的文件缓存无关（那由 gopher 管）。旧选项 `enable_metadata_cache` / `metadata_cache_ttl`（秒）/ `auto_refresh_metadata` 已废弃：写了不报错、无效果（它们在任何已发布版本中都从未生效过）；迁移时把 `metadata_cache_ttl '300'` 改写为 `metadata_cache '5'` |
| `warehouse_location_prefix` | Hive / Polaris / Hadoop / S3 | 仓库路径前缀；hadoop / s3 必填且带协议。REST 不用：位置由平台分配 |
| `prefix` | REST（FOREIGN CATALOG） | REST 路由前缀，默认取 `catalog_name` |

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
-- hdfs_namenodes 支持 host:port 拼接；也可拆成 hdfs_namenodes '192.168.1.10' + hdfs_port '9000'
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

-- HA 模式 + Kerberos（HA 时 hdfs_namenodes 填 nameservice 名，不带端口）
CREATE SERVER hdfs_ha_vol_srv FOREIGN DATA WRAPPER iceberg_volume_fdw
OPTIONS (
    type 'hdfs',
    hdfs_namenodes 'mycluster',
    hdfs_auth_method 'kerberos',
    krb_principal 'gpadmin/master@REALM.COM',
    krb_principal_keytab '/home/gpadmin/gpadmin.keytab',
    krb_service_principal 'hdfs/namenode@REALM.COM',
    hadoop_rpc_protection 'privacy',
    is_ha_supported 'true',
    dfs_nameservices 'mycluster',
    dfs_ha_namenodes 'nn1,nn2',
    dfs_namenode_rpc_address '192.168.1.10:9000,192.168.1.11:9000',
    dfs_client_failover_proxy_provider
        'org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider'
);
CREATE USER MAPPING FOR current_user SERVER hdfs_ha_vol_srv
OPTIONS (username 'gpadmin');
```

Volume 选项汇总（按对象级分层）：

**SERVER OPTIONS**

| 选项 | 适用 type | 说明 |
|------|----------|------|
| `type` | 全部 | `s3` / `s3v2` / `hdfs` |
| `server_name` | 全部 | 配置文件模式：未在 SQL OPTIONS 里写的连接配置从 `s3.conf` / `gphdfs.conf` 的对应段补齐，SQL 里写了的优先（详见本节末"配置文件模式"） |
| `endpoint` | s3 | 对象存储 endpoint（含协议）；hdfs 不使用此选项 |
| `region` | s3 | 区域 |
| `bucket_name` | s3 | 桶名 |
| `path_style_access` | s3 | path-style（MinIO 必须 `true`） |
| `endpoint_internal` | s3（Polaris） | Polaris 服务端访问的内部 endpoint |
| `sts_endpoint` / `sts_unavailable` | s3（Polaris） | STS endpoint 与禁用开关 |
| `role_arn` / `external_id` / `user_arn` | s3（Polaris） | Vended-credentials 角色信息 |
| `current_kms_key` / `allowed_kms_keys` | s3（Polaris） | SSE-KMS 密钥 |
| `hdfs_namenodes` | hdfs | NameNode 地址，支持 `host` 或 `host:port` 拼接（拼接中的端口优先于 `hdfs_port`）；HA 模式填 nameservice 名（不带端口） |
| `hdfs_port` | hdfs | NameNode RPC 端口（默认 8020；用拼接写法或 HA 模式时不需要） |
| `hdfs_auth_method` | hdfs | `simple` / `kerberos` |
| `krb_principal` / `krb_principal_keytab` | hdfs (kerberos) | 客户端 Kerberos 凭据 |
| `krb_service_principal` | hdfs (kerberos) | NameNode 服务端 principal |
| `hadoop_rpc_protection` | hdfs | `authentication` / `integrity` / `privacy` |
| `data_transfer_protocol` | hdfs | 是否启用 SASL data transfer（`true` / `false`） |
| `dfs_client_use_datanode_hostname` | hdfs | 是否用 DataNode 主机名（而非 IP）建立数据连接（`true` / `false`）；DataNode 在 NAT / 容器网络后时需置 `true` |
| `is_ha_supported` | hdfs | 是否启用 HA |
| `dfs_nameservices` / `dfs_ha_namenodes` / `dfs_namenode_rpc_address` / `dfs_client_failover_proxy_provider` | hdfs (HA) | HA 路由配置 |

**USER MAPPING OPTIONS**

| 选项 | 适用 | 说明 |
|------|------|------|
| `username` | 全部 | 操作系统用户 |
| `access_key_id` / `secret_access_key` | s3 | 静态 AK/SK |

**CREATE FOREIGN VOLUME OPTIONS**

| 选项 | 说明 |
|------|------|
| `base_path` | 存储基础路径 |
| `allow_writes` | 允许写操作（默认 false；当前版本解析但未生效） |
| `enable_caching` | 启用缓存（默认 false；当前版本解析但未生效） |

#### 配置文件模式（server_name 与 s3.conf / gphdfs.conf / gphive.conf）

除了在 SERVER OPTIONS 里 inline 写连接配置，也可以在 catalog / volume 的 SERVER 上指定
`server_name '<段名>'`，把连接信息放进配置文件。

**优先级（与 Hadoop 配置文件的习惯一致）**：SQL OPTIONS 优先于配置文件，按键合并——
某个键在 SQL OPTIONS 里写了就用 SQL 的值；没写的键回退到 conf 段里的值。conf 文件
相当于站点级默认值，SQL 相当于按对象覆盖。例如 conf 段里有完整连接信息时 DDL 只需
`server_name`；想为某个 SERVER 单独换一个 endpoint，则在该 SERVER 的 OPTIONS 里
加 `endpoint`，其余键继续取自 conf。

**键名所见即所得**：conf 文件里的键名与 SQL OPTIONS 的选项名**完全相同**（哪一级对象的
选项就叫什么名字，含 USER MAPPING 的凭据键）。旧版 Hadoop 风格键名
（`fs.s3a.*`、`fs.gopher.*`、`uris`、`hdfs_namenode_host`、点号 `dfs.*` 等）在
iceberg 路径**不再支持，读到即报错**，错误信息会列出对应的新键名（通用外部表
FDW（第 3 节）的旧格式仍兼容，新旧键同段共存时以新键为准）。

文件与对象的对应关系：

| SERVER 类型 | 配置文件 | 触发条件 |
|------------|---------|---------|
| `iceberg_catalog_fdw`（type=hive） | `gphive.conf` | SERVER OPTIONS 含 `server_name` |
| `iceberg_volume_fdw`（type=s3/s3v2） | `s3.conf` | 同上 |
| `iceberg_volume_fdw`（type=hdfs） | `gphdfs.conf` | 同上 |

**位置与格式**：两处各放一份——
- **QD / 各 segment 实例的数据目录**（与 hive_connector 读 `gphive.conf` 的约定一致），
  供数据库侧（QD/QE）读取；
- **datalake_agent 进程的工作目录**，供 agent 读取。

格式为 YAML，顶层 key 即段名（也就是 `server_name` 的取值），一个文件可以放多个段。

**gphive.conf**（样例：`contrib/datalake_fdw/example/gphive.conf`）——
键名 = catalog SERVER / USER MAPPING 的选项名：

```yaml
hive_cluster:
    url: thrift://192.168.50.22:9083           # = SERVER 选项 url
    auth_method: simple                        # = USER MAPPING 选项（simple / kerberos）
    krb_service_principal: hive/host@REALM     # kerberos 时
    krb_client_principal: user/host@REALM
    krb_client_keytab: /path/to/user.keytab
```

> 旧键 `uris` 已废弃，读到报错，改用 `url`。

**s3.conf**（样例：`contrib/datalake_fdw/example/s3.conf`）——
键名 = volume SERVER / USER MAPPING 的选项名：

```yaml
s3_cluster:
    type: s3                                   # = SERVER 选项 type（s3 / s3v2）
    endpoint: http://127.0.0.1:9000            # = SERVER 选项 endpoint
    region: us-east-1                          # = SERVER 选项 region
    path_style_access: true                    # = SERVER 选项（MinIO 必须 true）
    access_key_id: admin                       # = USER MAPPING 选项
    secret_access_key: password                # = USER MAPPING 选项
    # bucket_name: warehouse                   # 可写；通常放 DDL（volume 身份信息）
```

> 旧键 `fs.s3a.endpoint` / `fs.s3a.access.key` / `fs.s3a.secret.key` /
> `fs.s3a.endpoint.region` / `fs.s3a.path.style.access` / `fs.gopher.ufs_type`
> 已废弃，读到报错；对应新键依次为 `endpoint` / `access_key_id` /
> `secret_access_key` / `region` / `path_style_access` / `type`。
> bucket 与路径前缀通常仍由表的 location（`warehouse_location_prefix` /
> volume `base_path`）解析得到，conf 里的 `bucket_name` 只作缺省回退。

**gphdfs.conf**（样例：`contrib/datalake_fdw/example/gphdfs.conf`）——
键名 = volume SERVER 的 hdfs 选项名：

```yaml
paa_cluster:
    hdfs_namenodes: 192.168.1.10               # = SERVER 选项（host 或 host:port；HA 填 nameservice 名）
    hdfs_port: 9000                            # = SERVER 选项（拼接写法 / HA 时不需要）
    hdfs_auth_method: simple                   # simple / kerberos
    hadoop_rpc_protection: authentication
    # kerberos 时：
    # krb_principal: gpadmin/master@REALM.COM
    # krb_principal_keytab: /home/gpadmin/gpadmin.keytab
    # krb_service_principal: hdfs/namenode@REALM.COM
    # HA 时：
    # is_ha_supported: true
    # dfs_nameservices: mycluster
    # dfs_ha_namenodes: nn1,nn2
    # dfs_namenode_rpc_address: 192.168.1.10:9000,192.168.1.11:9000
    # dfs_client_failover_proxy_provider: org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider
```

> 旧键 `hdfs_namenode_host` / `hdfs_namenode_port` 与点号写法
> `dfs.nameservices` / `dfs.ha.namenodes` / `dfs.namenode.rpc-address` /
> `dfs.client.failover.proxy.provider.<ns>` 已废弃，读到报错；
> 对应新键为 `hdfs_namenodes` / `hdfs_port` 与下划线的同名 `dfs_*` 键。

三个文件中未列出的未知键仍会作为 Hadoop 配置原样透传，但命中上述废弃键
（含 `fs.s3a.` / `fs.gopher.` 前缀与点号 `dfs.` 前缀）一律报错，不会静默透传。

> 注意（conf 文件在各链路的生效范围不同）：
> - **通用外部表（FDW，第 3 节）**：conf 文件全程参与，包括数据读取——QE 发起的读请求携带
>   conf 文件名与 `hive_cluster_name` / `hdfs_cluster_name`（即 conf 段名，见 3.4 节
>   `sync_hive_table('hive_cluster', …, 'paa_cluster', …)` 的两个集群名），由本机
>   datalake_agent 读取 conf 完成存储访问。该路径仍兼容旧键名。
> - **Iceberg 表**：设计语义是设了 `server_name` 后 **QD、QE 与 agent 用同一套合并
>   结果**（SQL OPTIONS 覆盖 conf 段，缺省回退 conf）。当前实现中 agent 侧
>   （建表 / fragments / commit / 统计）已按此工作；QE 上 gopher 直读/直写数据文件的
>   conf 读取尚未接入（暂仍依赖 inline 连接选项），按该语义打通的改造进行中。

#### gopher 配置详解

gopher 是数据湖访问的原生 IO 客户端，出现在两个平面，连接配置来源不同：

| 平面 | 进程 | 连接配置来源 |
|------|------|-------------|
| QE 数据面 | segment 侧 gopher 服务（经本地 socket 承接 SELECT / INSERT 的数据文件读写） | volume SERVER 的 inline 选项；`server_name` 模式下按设计用同一套「SQL 覆盖 conf」合并结果（QE 侧接入改造中，暂仍需 inline） |
| agent 元数据面 | datalake_agent 内的 GopherFileIO（读写 metadata.json / manifest；`gopher.enabled=true` 时启用） | inline 选项；设了 `server_name` 时按「SQL 覆盖 conf」合并 |

**agent 端基线**（datalake_agent 的 `application.properties`）：

```properties
gopher.enabled=false          # 总开关（当前构建默认 false：元数据面回退 ResolvingFileIO，
                              # 即 s3 走 S3FileIO、其余走 HadoopFileIO）
gopher.worker-path=           # gopher socket 路径，通常留空——由数据库端随每个请求自动注入
gopher.connect-path=
gopher.cache-strategy=GOPHER_CACHE
gopher.log-level=info
gopher.liboss2-log-level=info
gopher.block-size=16777216
gopher.buffer-size=16384
```

`gopher.*` 键全部由系统自动换算，**用户不需要也不应该手写**；下面用例子说明
"用户写什么 → 系统换算成什么"，便于排障时核对。

**例 1：gopher 连接 S3（inline，最常用）**

用户只写普通的 volume DDL：

```sql
CREATE SERVER s3_vol_srv FOREIGN DATA WRAPPER iceberg_volume_fdw
    OPTIONS (type 's3', endpoint 'http://192.168.50.30:9000',
             region 'us-east-1', bucket_name 'warehouse',
             path_style_access 'true');
CREATE USER MAPPING FOR current_user SERVER s3_vol_srv
    OPTIONS (access_key_id 'admin', secret_access_key 'password');
```

系统换算出的 gopher 连接配置：

| 用户写的 | 换算后的键 | 值 |
|---------|-----------|----|
| `endpoint 'http://192.168.50.30:9000'` | `gopher.endpoint` | `192.168.50.30:9000`（scheme 剥掉） |
| 同上（`http` → 非 https） | `gopher.useHttps` | `false` |
| `bucket_name 'warehouse'` | `gopher.bucket` | `warehouse` |
| `region 'us-east-1'` | `gopher.region` | `us-east-1` |
| `path_style_access 'true'` | `gopher.useVirtualHost` | `false`（取反） |
| `access_key_id` / `secret_access_key` | `gopher.access_key` / `gopher.secret_key` | `admin` / `password` |
| `type 's3'` | `gopher.ufs_type` | `s3` |

> 当前限制：QE 数据面不会从 endpoint 的 scheme 推导 `useHttps`（仅 agent 元数据面会），
> 因此 inline `endpoint` 请使用 `http://`；`https://` 端点在数据面暂不会启用 TLS。

**例 2：gopher 连接 S3（conf-file 模式）**

设了 `server_name` 后，连接配置按「SQL OPTIONS 覆盖 conf 段」合并；conf 段写全
连接信息时，inline 连接选项可全部省略（`bucket_name` 属于 volume 身份信息而非
连接配置，仍写在 DDL 里）。`s3.conf` 按上文"位置与格式"部署到数据库实例数据目录
与 agent 工作目录，**键名与 SQL 选项名相同**：

```yaml
minio_prod:
    type: s3
    endpoint: http://192.168.50.30:9000
    region: us-east-1
    path_style_access: true
    access_key_id: admin
    secret_access_key: password
```

DDL 只需引用段名：

```sql
CREATE SERVER s3_vol_srv FOREIGN DATA WRAPPER iceberg_volume_fdw
    OPTIONS (type 's3', server_name 'minio_prod', bucket_name 'warehouse');
```

换算关系与例 1 完全相同——conf 键名就是 SQL 选项名，不存在第二套对照表;
`type: s3` 让 agent 设置 `fs.defaultFS = s3://<bucket>`，bucket 取自表
location / DDL。若想按 SERVER 覆盖某一项（如临时切换 endpoint），直接在
OPTIONS 里写 `endpoint '…'` 即可，其余键继续取自 conf。

> 当前实现状态：agent 侧已按 `server_name` 合并 conf；QE 数据面 gopher 连接的
> conf 读取尚未接入（暂仍需 inline 连接选项），打通改造进行中。

**例 3：gopher 连接 HDFS（simple 认证）**

```sql
CREATE SERVER hdfs_vol_srv FOREIGN DATA WRAPPER iceberg_volume_fdw
    OPTIONS (type 'hdfs', hdfs_namenodes '192.168.50.10:8020',
             hdfs_auth_method 'simple');
CREATE USER MAPPING FOR current_user SERVER hdfs_vol_srv
    OPTIONS (username 'gpadmin');
```

| 用户写的 | 换算后的键 | 值 |
|---------|-----------|----|
| `type 'hdfs'` | `gopher.ufs_type` | `hdfs` |
| `hdfs_namenodes '192.168.50.10:8020'` | `gopher.name_node` / `gopher.port` | `192.168.50.10` / `8020` |
| `hdfs_auth_method 'simple'` | `gopher.auth_method` | `simple` |

**例 4：gopher 连接 HDFS（Kerberos，含 keytab 准备步骤）**

第一步，准备 keytab。`krb_principal_keytab` 填的是**各主机本地的文件路径**：
QE 数据面要求每台 segment 主机该路径下都有 keytab，agent 元数据面要求 agent
所在主机（通常是 coordinator）也有。用相同路径分发一份即可：

```bash
# 1. 从 KDC 导出（或向 Kerberos 管理员索取）gpadmin 的 keytab，放到 /tmp/gpadmin.keytab
# 2. 分发到所有主机的同一路径并收紧权限（hostfile 含 coordinator + 全部 segment 主机）
gpssh -f hostfile 'mkdir -p /home/gpadmin/keytab'
gpscp -f hostfile /tmp/gpadmin.keytab =:/home/gpadmin/keytab/gpadmin.keytab
gpssh -f hostfile 'chmod 600 /home/gpadmin/keytab/gpadmin.keytab'
# 3. 确认各主机 /etc/krb5.conf 指向正确的 KDC / realm
```

第二步，DDL 引用该路径：

```sql
CREATE SERVER hdfs_krb_vol_srv FOREIGN DATA WRAPPER iceberg_volume_fdw
OPTIONS (
    type 'hdfs',
    hdfs_namenodes '192.168.50.10:8020',
    hdfs_auth_method 'kerberos',
    krb_principal 'gpadmin@REALM.COM',
    krb_principal_keytab '/home/gpadmin/keytab/gpadmin.keytab',
    hadoop_rpc_protection 'privacy'
);
```

| 用户写的 | 换算后的键 |
|---------|-----------|
| `hdfs_auth_method 'kerberos'` | `gopher.auth_method = kerberos` |
| `krb_principal 'gpadmin@REALM.COM'` | `gopher.krb_principal` |
| `krb_principal_keytab '/home/gpadmin/keytab/gpadmin.keytab'` | `gopher.krb_server_key_file`（各主机本地路径） |
| `hadoop_rpc_protection 'privacy'` | `gopher.hadoop_rpc_protection` |

**例 5：gopher 连接 HDFS（HA）**

```sql
CREATE SERVER hdfs_ha_vol_srv FOREIGN DATA WRAPPER iceberg_volume_fdw
OPTIONS (
    type 'hdfs',
    hdfs_namenodes 'mycluster',              -- HA：填 nameservice 名，不带端口
    hdfs_auth_method 'simple',
    is_ha_supported 'true',
    dfs_nameservices 'mycluster',
    dfs_ha_namenodes 'nn1,nn2',
    dfs_namenode_rpc_address '192.168.50.10:8020,192.168.50.11:8020',
    dfs_client_failover_proxy_provider
        'org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider'
);
```

`is_ha_supported` / `dfs_nameservices` / `dfs_ha_namenodes` /
`dfs_client_failover_proxy_provider` 原样映射为同名 `gopher.dfs_*` 键；
`dfs_namenode_rpc_address` 的逗号列表会按 namenode 逐个展开，交给 gopher 做 failover 路由：

```
dfs.namenode.rpc-address.mycluster.nn1 = 192.168.50.10:8020
dfs.namenode.rpc-address.mycluster.nn2 = 192.168.50.11:8020
```

**例 6：gopher 连接 HDFS（conf-file 模式）**

`gphdfs.conf`（键表见上文模板，键名 = SQL 选项名）按"位置与格式"部署到数据库实例
数据目录与 agent 工作目录，DDL 只需引用段名（语义同例 2：SQL OPTIONS 覆盖 conf，
conf 写全时 inline 连接选项可省）：

```sql
CREATE SERVER hdfs_vol_srv FOREIGN DATA WRAPPER iceberg_volume_fdw
    OPTIONS (type 'hdfs', server_name 'paa_cluster');
```

conf 键的换算与例 3-5 的 inline 选项完全一致（同名同义）：

| gphdfs.conf 键（= SQL 选项） | 换算后的键 | 说明 |
|---------------|-----------|------|
| `hdfs_namenodes` / `hdfs_port` | `gopher.name_node` / `gopher.port` | 同时拼出 `fs.defaultFS = hdfs://<host>:<port>` |
| `hdfs_auth_method` | `hadoop.security.authentication` | gopher 模式下同步 `gopher.auth_method` |
| `krb_principal` / `krb_principal_keytab` | kerberos 登录凭据 | gopher 模式下同步 `gopher.krb_principal` 与 keytab |
| `hadoop_rpc_protection` | `hadoop.rpc.protection` | gopher 模式下同步 `gopher.hadoop_rpc_protection` |
| `data_transfer_protocol` | `dfs.encrypt.data.transfer` | gopher 模式下同步 `gopher.data_transfer_protocol` |
| `is_ha_supported`、`dfs_nameservices`、`dfs_ha_namenodes`、`dfs_namenode_rpc_address`、`dfs_client_failover_proxy_provider` | `gopher.is_ha_supported` / `gopher.dfs_*` | HA 路由配置（展开方式见例 5） |
| —（gopher 模式自动设置） | `gopher.ufs_type=hdfs`；`fs.hdfs.impl` → GopherFileSystem | 使所有 `hdfs://` 路径的 IO 经由 gopher |

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
    table 'sales_2024',
    location '/data/sales/'
);
```

**CATALOG / VOLUME 指定规则**：
- 可省略子句 → 使用 `iceberg_default_catalog` / `iceberg_default_volume` GUC
- 所有 Catalog 类型（含 Polaris）都**必须**有 Volume：Polaris 场景下表数据的物理路径由
  Polaris 服务端下发，但 QE 读写数据文件仍使用 Volume 提供的存储 endpoint 与 AK/SK；
  两者均未指定时建表报错 `no foreign volume specified`

**数据路径组成**（新表）：

- **内部（builtin）catalog**：一律**拍平**到 Volume 的 base_path 本身，不再追加
  数据库 / namespace / 表名三段；同一个 Volume 下所有 builtin 表**共享**该目录，
  Iceberg 再自动追加 `metadata/`、`data/` 子目录：
  ```
  protocol://bucket/base_path/{metadata,data}/…
  ```
  之所以安全：Iceberg 所有文件名都带 UUID（不会同名覆盖），当前 metadata 指针记在
  系统表 `pg_iceberg_metadata`（查表不依赖目录层级），删除链路全程按 metadata 树枚举
  具体文件路径而非"按前缀列目录"。
  > ⚠️ 正因共享目录，**禁止**对 builtin 表跑任何"按前缀列目录再删"的维护
  > （Iceberg `remove_orphan_files`、将来的 compaction autovacuum），否则会把同目录里
  > 别的表的活文件当孤儿删掉；清理必须保持 metadata-tree 范围。builtin 建表也**不允许**
  > 指定 `location`（见下表），路径完全由 Volume base_path 决定。
- **外部 catalog（hive / polaris / hadoop / s3）**：物理路径由外部 catalog 决定，
  通常形如 `<warehouse_location_prefix>/<namespace>/<table>/`。

**CREATE ICEBERG TABLE OPTIONS 全集**：

| OPTION | 类型 | 说明 |
|--------|------|------|
| `catalog` | string | 指定 Catalog（与子句 `CATALOG` 等价；二选一） |
| `namespace` | string | 命名空间 |
| `table` | string | 在 catalog 中暴露的表名（默认与 PG 表名一致） |
| `location` | string | **仅外部 catalog 有意义**。builtin（内部）catalog 建表若指定该项直接报错 `location option is not allowed for builtin iceberg tables`——内部表路径固定为 Volume `base_path` 本身。 |
| `autovacuum_enabled` | bool | 是否参与 `datalake.iceberg_autovacuum` 自动 VACUUM（默认 `true`） |
| `metadata_cache` | int | **仅外部 catalog 有意义**。本表 head 缓存的 TTL，单位**分钟**。不写则继承 catalog 同名选项；写 `0` 表示本表显式不缓存，每次扫描都取最新 head（优先级高于 catalog 选项）。详见 4.8 的优先级表 |

> ⚠️ 未识别的 OPTION **不会报错而是被静默忽略**，注意拼写（如误写 `table_name`、`base_location` 将不生效）。

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
| `char(n)` / `bpchar` | string | Iceberg 无定长字符类型；尾部空格/右填充语义**不保留**（对齐 Snowflake/Spark/Trino） |
| `date` | date | |
| `timestamp` | timestamp | 无时区 |
| `timestamptz` | timestamptz | 映射为带时区语义的 Iceberg `timestamptz`（`isAdjustedToUTC`），存 UTC 瞬时值；Spark/Trino/Flink 按 UTC 正确读取（issue #366，早期误映射为无时区 timestamp） |
| `bytea` | binary | |

> **重要**：`numeric` 不指定精度时，建表会成功（元数据按缺省 `decimal(38,9)` 注册），
> 但 **INSERT 写入时报错**（`The precision of numeric in foreign tables with parquet
> format should be specified explicitly`）。务必写成 `numeric(p,s)`。
>
> 未列出的类型（如 `uuid`、`json`）建表时会发出 WARNING 并按 string 映射，请谨慎使用。

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

#### REST Catalog 上的 VACUUM

REST catalog 的表**不能由本地重写**：存储归平台所有，我们去改文件会和平台自己
的乐观锁竞争。而 Iceberg REST 规范里没有维护接口，所以平台若提供合并能力，一定
是在 catalog 协议之外另开的一套接口。

因此默认行为是拒绝：

```sql
VACUUM orders;
ERROR:  VACUUM is not supported on tables in an external REST catalog
HINT:   If the platform serves a maintenance API, declare it on the catalog
        server (OPTIONS (maintenance_api 's3tables')) and VACUUM will submit
        the job. ...
```

平台若提供了维护接口，在 SERVER 上声明之后，VACUUM 改为**向平台提交合并作业**：

```sql
ALTER SERVER xsky_srv OPTIONS (ADD maintenance_api 's3tables');

VACUUM orders;
NOTICE:  iceberg: compaction submitted for "orders"
DETAIL:  The platform runs the job on its own schedule. Until it finishes and
         this database's metadata is refreshed, reads continue to use the
         files the table has now.
VACUUM
```

几个必须知道的行为:

- **只提交，不等待**。作业在平台侧异步执行(实测数十秒到数分钟)，语句立即返回。
  返回成功只代表"请求被接受"，不代表文件已经合并。
- **不判断阈值**。是否值得合并由平台按自己的参数决定；对一张没什么可合并的表提交
  会正常返回并且什么都不做。
- **每张表每种类型同时只允许一个作业**。重复提交返回 409，我们把它当作提示而非
  错误——"你要的维护正在发生"不该让批量脚本中断：

  ```sql
  VACUUM orders;   -- 上一个还在跑
  NOTICE:  iceberg: a maintenance job is already running for "orders"
  VACUUM
  ```
- **只提交 compaction**，不提交快照过期和孤儿清理——后两者会删文件，而正在进行的
  扫描可能还引用着它们。

- **合并完成后不需要在这边做任何事**。外部 catalog 上的表每次扫描都会去解析该
  catalog 的当前 head（TTL 为 0 时即每次都取最新，设了 TTL 则在 TTL 内复用），
  所以平台一提交合并快照，后续查询就读到合并后的文件。

  生效的 TTL 按下面的优先级取，两个层级同名同单位：

  | 来源 | 单位 | 说明 |
  |------|------|------|
  | 表选项 `metadata_cache` | **分钟** | 写了就以它为准，`0` 表示本表显式不缓存 |
  | catalog 选项 `metadata_cache` | **分钟** | 表没写时的默认；`0` 或不写 = 不缓存 |

> `maintenance_api` 只支持 `s3tables` 一个值，写错会在 `CREATE SERVER` /
> `ALTER SERVER` 时就被拒绝，不会拖到 VACUUM 才报错。

> **注意**：VACUUM 不能在事务块或 PL/pgSQL 函数内执行。

### 4.9 自动 VACUUM

自动 VACUUM 挂在 PG 原生 autovacuum 之后（`datalake.iceberg_autovacuum = on`，
最小间隔 10 分钟）。

> **它目前不做 compaction。** 名字容易让人误会：这个后台任务只消费删除队列——
> `DROP TABLE` / `TRUNCATE` 之后遗留在对象存储上的文件由它异步删除。**小文件合并
> 仍然只在手动 `VACUUM` 时发生。**
>
> 原因是 PG 的 autovacuum 靠死元组和插入计数来挑表，而 Iceberg 表的数据在外部
> 存储上，这两个计数器恒为 0，所以这类表从来不会被选中（AO / PAX 表通过在 DML
> 回调里上报统计解决了同样的问题）。在此之前，请按需要自行安排 `VACUUM`。

调整间隔或关闭（需 superuser，修改后需 reload）：

```sql
ALTER SYSTEM SET datalake.iceberg_autovacuum_naptime = 1200;  -- 改为每 20 分钟检查
ALTER SYSTEM SET datalake.iceberg_autovacuum = off;           -- 如需关闭
SELECT pg_reload_conf();
```

### 4.10 时间旅行（读历史快照）

Iceberg 每次提交都会产生一个快照。`iceberg_snapshot_scan()` 让你把某个历史快照
**当成一张表来查**，无需预先创建任何对象。

```sql
-- 1) 先看有哪些快照可选
SELECT snapshot_id, committed_at, operation, is_current
  FROM iceberg_snapshot_list('orders');

--  snapshot_id     |        committed_at        | operation | is_current
-- -----------------+----------------------------+-----------+------------
--  808164795275195135 | 2026-08-04 12:20:47.211+08 | append    | f
--  1686471219369376260| 2026-08-04 12:20:52.615+08 | append    | t

-- 2) 按快照 ID 读
SELECT * FROM iceberg_snapshot_scan('orders', 808164795275195135);

-- 3) 按时间读：取「该时刻或之前最近的一个快照」
SELECT * FROM iceberg_snapshot_scan('orders', TIMESTAMPTZ '2026-08-04 12:20:50+08');
SELECT * FROM iceberg_snapshot_scan('orders', now() - interval '1 day');

-- 4) 跨快照对比，一条 SQL、零预建对象
SELECT a.id FROM iceberg_snapshot_scan('orders', 808164795275195135) a
  JOIN orders b USING (id) WHERE a.amount <> b.amount;

-- 0 或 NULL::bigint 表示读 HEAD（等价于直接查表）
SELECT count(*) FROM iceberg_snapshot_scan('orders', 0);
```

#### 时间戳参数必须带类型

`iceberg_snapshot_scan` 的第二个参数有 `bigint`（快照 ID）和 `timestamptz`（时刻）两个重载。
一个**不带类型的字面量**两边都匹配不上，PostgreSQL 会报错：

```sql
SELECT * FROM iceberg_snapshot_scan('orders', '2026-08-04 12:20:50+08');
ERROR:  function iceberg_snapshot_scan(unknown, unknown) is not unique
```

写成 `TIMESTAMPTZ '…'`、`'…'::timestamptz` 即可。同理，读 HEAD 时若要传 NULL，
需写 `NULL::bigint`（整数字面量如 `0`、`123` 不受影响，能自行定到 `bigint`）。

#### 允许什么样的参数

两个参数都必须能在解析期折算成确定的值。

| 写法 | 结果 |
|------|------|
| 字面量、`TIMESTAMPTZ '…'`、`'…'::timestamptz` | ✅ |
| `now()`、`current_timestamp`、`now() - interval '1 day'` | ✅ 语句内恒定 |
| `clock_timestamp()`、`random()` | ❌ `arguments must be constant expressions` |
| `$1` 参数、子查询、引用其他表的列 | ❌ 同上 |

拒绝非确定性表达式是必需的：快照选择器会在解析和规划的多个环节各自折算一次，
取值若每次不同，会出现「列集来自一个快照、数据来自另一个快照」的错乱。
这与 Spark 的规则一致（Spark 同样接受 `current_timestamp()`、拒绝 `rand()`）。

#### 找不到对应快照时会报错，不会退回 HEAD

```sql
SELECT * FROM iceberg_snapshot_scan('orders', TIMESTAMPTZ '1999-01-01');
ERROR:  iceberg time travel: "orders" has no snapshot at or before 1999-01-01 00:00:00+08
HINT:  Use iceberg_snapshot_list() to list the available snapshots.
```

快照 ID 不存在同理报错。静默返回当前数据无法与正确结果区分，因此一律显式失败。

#### 返回的是**该快照自己的 schema**

与 Spark / Trino 语义一致：

| 快照之后发生的变更 | 读旧快照时 |
|---|---|
| 加列 | 不出现（那时还不存在） |
| 删列 | **仍可见，数据完整** |
| 重命名 | 显示快照当时的旧列名 |
| 类型提升 | 按快照当时的类型返回 |

> `CHAR(N)`/`VARCHAR(N)` 在 Iceberg 侧统一是 `string`，历史快照读回来是 `text`；
> `fixed[L]` 读回 `bytea`，长度不再强制。

#### 预备语句会把时刻固定下来

用 `PREPARE` / plpgsql 函数体缓存一条带 `now()` 的时间旅行语句时，**时刻在第一次解析时就被
固定成具体快照**，之后每次 `EXECUTE` 都读同一个快照，不会随时间推移滑向新快照。

这是刻意的：解析期确定的结果列集必须和实际读到的数据来自同一个快照，否则 schema 演化后
会返回错乱的数据。Spark 也是在分析期一次性绑定快照。需要「每次都取最新的一天前」时，
不要用预备语句，直接执行 SQL 即可。

#### 可回溯窗口 = 快照保留窗口

时间旅行只能到达 **metadata 里仍然记录着的快照**。Iceberg 的快照不会自动过期，但任何引擎
（最常见是 Spark 的 `CALL <catalog>.system.expire_snapshots(...)`，或 Glue/Tabular 等托管
服务的定时维护）都可能按保留策略把老快照连同其独占的数据文件一起清掉。规划保留策略时把
时间旅行需求算进去：

- **窗口内**：正常读。
- **已过期**：确定性报错（按 id 读报 "snapshot not found"；按时刻读报
  "no snapshot at or before …"），**绝不静默回落到 HEAD**——与 Spark/Trino 行为一致。
- **竞态**：一条已经开始的长查询撞上并发的 expire，可能在读数据文件时失败
  （文件已删）。协议层没有读锁，所有引擎皆然；唯一的防线是让保留窗口盖过最长的读。
- **要长期保留某个快照**：让写入方打 tag（如 Spark
  `ALTER TABLE t CREATE TAG eoy2025 AS OF VERSION ... RETAIN 365 DAYS`），
  expire 不会删除被活 tag/branch 引用的快照。本产品暂不支持按 tag 读（见下），
  但 tag 保住的快照 id 仍可通过 `iceberg_snapshot_list()` 找到并按 id 读取。

本产品自身暂未提供 expire 能力：只由 CBDB 写入的内表快照会持续累积，共享表的过期
由外部引擎的维护任务负责。

#### 尚不支持

`FOR SYSTEM_TIME AS OF` / `FOR VERSION AS OF` 语法糖、branch / tag 引用（`useRef`）、
增量读（`appendsBetween`）暂未提供，请使用上面的函数形式。

### 4.11 内置工具函数（`iceberg_toolkit`）

`datalake_fdw` 注册了一组 SQL 函数用于直接调用 catalog/volume 操作或检查表元数据。所有函数定义在 `iceberg_toolkit` schema 下。

> ⚠️ **`iceberg_toolkit` 是内部开发/排障工具，不是对外产品接口**。函数签名与行为可能随版本变化，
> 不提供兼容性保证；生产业务不要依赖这些函数，正常使用请走 `CREATE ICEBERG TABLE` / DML。

| 函数 | 说明 | 典型用途 |
|------|------|---------|
| `iceberg_toolkit.catalog_fdw(operation text, name_space text, table_name text, catalog_server_name text, catalog_table_name text, volume_server_name text, volume_table_name text, append_json_string text) → text` | 在指定 catalog/volume 对象上执行底层操作（`create_table`、`get_fragment` 等） | 排查 catalog 连通性 |
| `iceberg_toolkit.volume_fdw(operation text, name_space text, table_name text, row_limit integer, catalog_server_name text, catalog_table_name text, volume_server_name text, volume_table_name text) → setof record` | 在指定 volume 上执行底层存储读取 | 直读数据做抽样核对 |
| `iceberg_toolkit.create_table(table_name text, name_space text, catalog_server text, catalog_table text, volume_server text, volume_table text) → text` | `catalog_fdw('create_table', …)` 的便捷封装，不经 SQL DDL 直接注册 Iceberg 表 | 快速接入已有数据目录 |
| `iceberg_toolkit.get_fragments(table_name text, name_space text, catalog_server text, catalog_table text, volume_server text, volume_table text) → text` | `catalog_fdw('get_fragment', …)` 的便捷封装，列出表的 scan fragment | 分析数据布局、定位倾斜 |
| `iceberg_toolkit.polaris_list_catalogs(datalake_agent_url text, polaris_url text, client_id text, client_secret text, scope text DEFAULT 'PRINCIPAL_ROLE:ALL') → text` | 直连 Polaris REST 列出可见 catalog | 配置阶段排查 |
| `iceberg_toolkit.polaris_list_namespaces(datalake_agent_url text, polaris_url text, client_id text, client_secret text, catalog_name text, scope text DEFAULT 'PRINCIPAL_ROLE:ALL') → text` | 列出 Polaris catalog 下的 namespace | 同上 |

```sql
-- 例：列出 Polaris catalog（凭据显式传入，首参为 datalake_agent 地址）
SELECT iceberg_toolkit.polaris_list_catalogs(
    'http://localhost:3888',
    'http://polaris:8181/api/catalog',
    'my_client_id', 'my_client_secret');

-- 例：查看一张表的 fragment 分布
SELECT iceberg_toolkit.get_fragments(
    'orders',        -- 表名
    'public',        -- namespace
    'cat_srv',       -- catalog SERVER 名
    'my_catalog',    -- FOREIGN CATALOG 名
    'vol_srv',       -- volume SERVER 名
    'my_volume');    -- FOREIGN VOLUME 名
```


### 4.12 外部 Catalog 元数据缓存与刷新

外部 catalog（Polaris / Hive / Hadoop / S3）上的表，每次读之前都要先问一次 catalog：「这张表当前的 head 是哪个 metadata.json？」这一问是一次网络往返。这一节讲怎么把它省掉，一共三样东西：

| | 是什么 | 谁来做 |
|---|---|---|
| `metadata_cache` 表选项 | 给「head 是哪个」的答案设一个有效期 | 建表的人 |
| 后台刷新 | 在有效期到之前把答案提前续上；**它是唯一会写这份元数据的东西** | 由 autovacuum 驱动，**默认开启、无需配置** |
| `iceberg_refresh_metadata()` | 出事时手工把某一张表拉回最新 | 运维按需 |

#### 一、`metadata_cache`：给答案设一个有效期

```sql
-- 缓存 5 分钟（单位是分钟）
CREATE ICEBERG TABLE orders (id bigint, amount numeric)
OPTIONS (namespace 'public', table 'orders', metadata_cache '5');

-- 不写这个选项，或者写 0：不缓存，每次读都问 catalog
CREATE ICEBERG TABLE orders_live (id bigint)
OPTIONS (namespace 'public', table 'orders_live');
```

过没过期由 dlagent 判断：读路径把「上次解析到的 head」连同「它的时间戳」一起下推，agent 发现没过 TTL 就把老答案原样还回来（**零次外部往返**），过了就去 catalog 重新解析，回一个新 head 和新时间戳。

（这个选项要和下一节的定时任务一起用才有效果，原因见下。）

**那新时间戳是谁存下来的？谁都不是 —— 读不写元数据。**

这是一条明确的规则：不管事务能不能写、是不是备机，**任何读都不会去动 `pg_ext_aux.pg_iceberg_metadata`**。一条普通 `SELECT` 会解析出当前 head、用它返回正确的数据，然后就结束了，什么也不记。

| 谁在读 | 这次读到的数据 | 时间戳 |
|---|---|---|
| 普通 `SELECT`（默认可读写事务） | 最新 | 不变 |
| `BEGIN; SET TRANSACTION READ ONLY;` 里的读 | 最新 | 不变 |
| 备机（hot standby）上的读 | 最新 | 不变 |

这么定的理由是：一条普通查询不该顺带改动全局共享的目录状态 —— 那样写回会搭在读者的事务上（读事务回滚，刷新也跟着没了）、两个并发读者会去抢同一行、同一条语句还会因为事务恰好是不是只读而行为不同。**写 mirror 现在只有两个入口，且都是显式的**：定时任务调的 `iceberg_refresh_metadata_cache()`，和人工执行的 `iceberg_refresh_metadata()`。

由此得到一个必须知道的结论：

> **`metadata_cache` 要靠刷新器才有意义。** 时间戳只有被刷新推进过，后续的读才会命中缓存。刷新器默认是开着的（autovacuum 驱动，见第三节），所以一般情况下不用做什么；但如果它被关掉、或者 TTL 短到 autovacuum 的节奏跟不上，这个选项就等于没设 —— 读依然正确，只是一次外部往返都省不掉。

`iceberg_polaris_metadata_cache` 冒烟用例把这套规则都钉住了：`stamp_still_null_after_read`、`stamp_seeded_by_refresh`、`stamp_unchanged_past_ttl`、`stamp_unchanged_by_readonly_read`。

#### 二、另一层缓存：文件内容由 gopher 缓存

上面那层 TTL 省掉的是「问 catalog 当前 head 是哪个」这一次 REST / HMS 往返。**文件本身还有独立的一层缓存**，两者省的不是同一笔开销：

| | 省掉什么 | 谁管 |
|---|---|---|
| `metadata_cache` TTL | 问 catalog「head 是谁」的那次往返 | PG 侧的时间戳 + agent 的比较 |
| gopher 文件缓存 | 把 metadata.json / manifest / 数据文件从对象存储拉下来的流量 | Go 层，按路径缓存内容 |

两层是独立的，开启条件也不同：**gopher 这层开箱即用**，什么都不用配；而上面那层 `metadata_cache` 只有在建了刷新任务之后才真正生效（没有任务，时间戳永远是 NULL，每次读都要去问 catalog —— 见上一节）。

Iceberg 读到的每个文件 —— metadata.json、manifest list、manifest、parquet —— 都经 `GopherFileIO` 走 gopher（agent 侧把 `CatalogProperties.FILE_IO_IMPL` 设成 `org.cbdb.iceberg.gopher.client.GopherFileIO`）。**默认就是开着缓存的**：

| 位置 | 配置 | 默认 |
|---|---|---|
| agent | `gopher.cache-strategy`（`application.properties`） | `GOPHER_CACHE` |
| 段上的读路径 | `datalake.disable_cache_file` | `off`（即缓存开启） |

缓存落在每个实例数据目录下的 `gophermeta/`，**有容量上限，按 LRU 淘汰**：

| GUC | 默认 | 改动生效 | 含义 |
|---|---|---|---|
| `pg_gophermeta.gopher_local_capacity_mb` | `1024000`（≈1000 GB） | SIGHUP | 本地磁盘缓存容量上限，写满后按 LRU 淘汰。GUC 自带的建议是**设成磁盘容量的 30%** |
| `pg_gophermeta.gopher_plasma_size_mb` | `0`（**关闭**） | 需重启 | Plasma L1 共享内存读缓存，0 表示不启用；设正值才开 |
| `pg_gophermeta.register_gophermeta` | `on` | 需重启 | 是否启动 gophermeta worker |

也就是默认形态是：**L1 共享内存缓存关闭，L2 本地磁盘缓存开启、容量 1000 GB、LRU 淘汰**。默认那个容量对大多数机器都大于实际磁盘，等于「能存多少存多少」，所以生产上按上面的建议把它调到磁盘的 30% 更稳妥。

（gopher 库内部有 `cached_files` / `cached_blocks` / `used_bytes` / `capacity` / `lru_size` 这些计数器，但目前没有任何 SQL 接口把它们暴露出来，只能看 `gophermeta/` 目录的实际占用。）

**实测**（单表约 40 万行，polaris catalog + MinIO，量的是 MinIO 容器的实际网络流量）：

```
SELECT gp_toolkit.__gopher_free_all_cache();   -- 先清空缓存
select sum(id) from gp_cache_t;   -- 冷读：MinIO 流出 +2MB
select sum(id) from gp_cache_t;   -- 热读：+0
select sum(id) from gp_cache_t;   -- 热读：+0
```

也就是说 TTL 命中的那些读，不光不问 catalog，连对象存储都不碰 —— 同一个 head 指向的是同一个 metadata.json 路径，文件内容直接从本地拿。

**为什么这层缓存不会读到旧数据**：Iceberg 的文件是一次写定、名字唯一的（`00047-<uuid>.metadata.json`、`snap-<id>-1-<uuid>.avro`），同一个路径的内容永远不变，所以按路径缓存内容不存在陈旧问题。会变的只有「当前 head 是哪个」，而那个来自 catalog 服务，不经过 gopher。

要清空或关闭：

```sql
SELECT gp_toolkit.__gopher_free_all_cache();   -- 清空（排查问题时用）
SET datalake.disable_cache_file = on;          -- 关闭段上读路径的文件缓存
```

**hadoop catalog 也验过了**。它是这里唯一值得担心的形态 —— 元数据文件叫 `v1.metadata.json`、`v2.metadata.json`，当前版本来自一个每次提交都会被改写的固定路径，而其它 Iceberg 文件都是一次写定。`iceberg_am_hadoop_hdfs_metadata_cache` 冒烟用例专门盯这一点：把表推进一个版本、再把 mirror 回滚一格，然后要求刷新越过刚被写入预热过的文件缓存、落到新的那个版本文件上。结论是能（`refreshed = t`、head 确实前移）。

> 顺带一个容易踩的点：这张表必须带 `OPTIONS (namespace, table)` 才算"外部表"。判定规则是**有没有指名一张已存在的外部表**，而不是用的哪种 catalog（`pg_iceberg_catalog_helper.c`）—— 不带这两个选项建出来的表 `is_internal = true`，压根不是缓存的候选。

#### 三、后台刷新：默认由 autovacuum 驱动

**默认就开着，什么都不用配。** 刷新挂在 autovacuum worker 上 —— QD 上的 autovacuum worker 既允许写、又不在恢复状态、还本来就会定期巡库，正好是这件事需要的执行环境，所以它和 deletion queue 的消费者共用同一个钩子。

| GUC | 默认 | 含义 |
|---|---|---|
| `datalake_fdw.metadata_refresh_enabled` | `on` | 是否启用 autovacuum 驱动的刷新 |
| `datalake_fdw.metadata_refresh_max_tables_per_cycle` | `100` | 单轮最多发起多少次外部请求 |
| `datalake_fdw.metadata_refresh_ahead_percent` | `70` | TTL 烧掉这个百分比就算到期待刷 |
| `datalake_fdw.metadata_refresh_min_interval` | `30`（秒） | 见下，实际几乎不起作用 |

四个都是 `PGC_SIGHUP`，改完 `SELECT pg_reload_conf()` 生效。

**它会刷哪些表**：外部 catalog 的表，且 `metadata_cache > 0`。就这两条。建表之后时间戳是 NULL，**NULL 按「无限陈旧」处理**，所以第一轮就会把它种上。

反过来说：一张挂着但从来没人查的表，只要设了 `metadata_cache`，也会每个 TTL 被刷一次（一次外部往返）。不想让它占这个开销，就别给它设 `metadata_cache` —— 没有 TTL 的表根本不是候选。

**正在被 DDL 占用的表会被跳过，不会去等**：刷新对表只取 `AccessShareLock`，所以普通的读写（SELECT / INSERT / UPDATE / DELETE）跟它不冲突，照刷不误；真正冲突的是 `ALTER TABLE`、`DROP`、`TRUNCATE`、`VACUUM FULL`、`REINDEX` 这类拿 `AccessExclusiveLock` 的操作。遇到它们刷新器**立刻放弃这张表**，而不是排队等 —— 等下去会把 autovacuum worker 卡住、压着本轮事务的 xmin，还让同一轮里剩下的表陪着一起等。跳过没有代价：候选按最旧优先排，下一轮它就排在最前面。（内核 autovacuum 出于同样的理由也是这么做的。）

**刷不过来会怎样**：什么也不会坏。读路径自己那套 TTL 检查照旧，刷新器关了、没跑到、或者 catalog 连不上，读只是慢一点，**不会读到更旧的数据**。

##### 节奏的限制：TTL 短于几分钟时它跟不上

钩子在每个 autovacuum worker 的生命周期里只触发一次，所以**实际节奏就是 `autovacuum_naptime`（默认 60 秒）**，而不是 `metadata_refresh_min_interval` —— 那个 GUC 只能拦住"同一个 worker 里跑了不止一次"的情况，而 worker 每次都是新 fork 的，计时器跟着归零。于是：

| `metadata_cache` | 默认 naptime 下够不够 |
|---|---|
| `'5'`（5 分钟）及以上 | 够 —— 3.5 分钟时到期待刷，60 秒一轮，来得及 |
| 一两分钟 | **不够** —— 会在两轮之间就过期，由读它的查询自己解析 |

##### TTL 很短时：改用任务调度器

把钩子关掉，改由 task 驱动同一个函数，周期由你定（两个都开的话会对同一批表重复发外部请求）：

```sql
-- 1) 关掉 autovacuum 那条路
ALTER SYSTEM SET datalake_fdw.metadata_refresh_enabled = off;
SELECT pg_reload_conf();

-- 2) 授权：函数默认对 PUBLIC 收回了执行权限
GRANT EXECUTE ON FUNCTION pg_catalog.iceberg_refresh_metadata_cache(int) TO dba_role;

-- 3) 建任务（在有 iceberg 外部表的那个库里建；目录是按库存的）
CREATE TASK iceberg_metadata_refresh SCHEDULE '30 seconds'
  AS $$ SELECT iceberg_refresh_metadata_cache() $$;
```

**`SCHEDULE` 支持哪些写法**（下表都是在本产品上实测过的）：

| 写法 | 含义 | |
|---|---|---|
| `'30 seconds'` / `'5 seconds'` | 每 N 秒 | ✅ 秒级只支持 **1–59** |
| `'60 seconds'`、`'90 seconds'` | — | ❌ `ERROR: invalid schedule` |
| `'1 minute'` | — | ❌ 不认这种写法，一分钟以上一律用 cron |
| `'*/5 * * * *'` | 每 5 分钟 | ✅ |
| `'0 * * * *'` 或 `'@hourly'` | 每小时 | ✅ |
| `'0 3 * * *'` | 每天 03:00 | ✅ |
| `'@daily'` | 每天 00:00 | ✅ |

**周期怎么选**：刷新在 TTL 烧掉 70% 时触发，所以周期取 TTL 的 1/10 ~ 1/5 就很宽裕：

| `metadata_cache` | 建议 `SCHEDULE` |
|---|---|
| `'1'`（1 分钟） | `'10 seconds'` |
| `'5'`（5 分钟） | `'30 seconds'` |
| `'30'`（30 分钟） | `'*/5 * * * *'` |

**查看、修改、停用、删除**：

```sql
-- 跑得怎么样（status 取值：succeeded / failed / running ...）
SELECT t.jobname, h.status, h.start_time, h.return_message
  FROM pg_task t JOIN pg_task_run_history h ON h.jobid = t.jobid
 WHERE t.jobname = 'iceberg_metadata_refresh'
 ORDER BY h.runid DESC LIMIT 5;

ALTER TASK iceberg_metadata_refresh SCHEDULE '*/5 * * * *';   -- 改周期
ALTER TASK iceberg_metadata_refresh NOT ACTIVE;               -- 临时停用
ALTER TASK iceberg_metadata_refresh ACTIVE;                   -- 恢复
DROP  TASK iceberg_metadata_refresh;                          -- 删除
```

任何时候也可以手工跑一次：`SELECT iceberg_refresh_metadata_cache();`（「我刚从 Spark 写完，让 PG 立刻看见」）。

#### 四、两个函数的签名

```sql
iceberg_refresh_metadata_cache(max_tables int DEFAULT 100) RETURNS int

iceberg_refresh_metadata(tbl regclass)
    RETURNS record (refreshed bool, previous_head text, current_head text)
```

| | `iceberg_refresh_metadata_cache` | `iceberg_refresh_metadata` |
|---|---|---|
| 做什么 | 扫全库，把**快到期**的外部表的 head 重新解析并落盘 | 把**指定的这一张**表的 mirror 拉回 catalog 当前 head |
| 参数 | `max_tables` — 单轮最多发起多少次外部请求 | `tbl` — 表名 |
| 返回 | 本轮实际刷新的表数 | 三列：head 有没有移动、移动前、移动后 |
| 看 TTL 吗 | 看，只刷到期的 | **不看**，你点名就刷 |
| 典型用法 | 由 autovacuum 自动调用；TTL 很短时改挂在 task 上 | 出事时手工执行 |
| 权限 | 默认无人可执行，DBA 显式 `GRANT` | 表 owner |
| 位置 | 协调节点（QD） | 协调节点（QD） |

#### 五、表很多的时候（十几二十张表可以跳过这节）

默认值（每轮 100 张、autovacuum 每 60 秒一轮）对几十张表的库绰绰有余，不用调。表上千了再看这里 —— 下面的「周期」在默认形态下就是 `autovacuum_naptime`，换成 task 驱动时就是 task 的周期。

- **超出额度的表不会被漏掉，也不会被饿死**。候选按缓存时间戳**最旧优先**排，刷过的表戳记变新、自动排到队尾，下一轮轮到的就是接下来最旧的一批。1000 张表、`max_tables=100`，是 10 轮轮转覆盖一遍，不是每轮都啃同样那 100 张。（实测：5 张全部到期的表，`max_tables=2` 连跑三轮，依次刷 t1+t2、t3+t4、t5，第四轮 0 张。）
- **吞吐够不够**，一个式子就能判断：

  > `max_tables ÷ 周期  ≥  表数 ÷ TTL`

  1000 张表、`metadata_cache='5'`（300 秒）、`max_tables=100` → 周期要 ≤ 30 秒。默认 60 秒的 naptime 达不到，这种规模就该把 `metadata_refresh_max_tables_per_cycle` 调大到 200，或者改用 task 驱动、把周期压到 30 秒。不够也不出错，排在后面的表先过期，由读到它的那个查询自己解析。
- **优先缩短周期，而不是一味调大 `max_tables`**。一轮是一个事务（每张表一个子事务）：轮跑得越久，QD 的 xmin 被压得越久；单轮超过 64 张表还会撑爆本事务的子事务号缓存（`PGPROC_MAX_CACHED_SUBXIDS`），让这段时间里其它 backend 的快照带上 suboverflow 标记。调度器也**不会让两轮重叠** —— 上一轮没跑完时最多排队一个，跑完立刻补一轮，然后回到正常节奏。

#### 六、单表强制刷新：把被外部维护打坏的表救回来

不走读缓存的那些路径 —— DML、时间旅行、`iceberg_snapshot_list()` —— 读的是 mirror 里记着的那个 metadata.json，这是事务能读到自己写入的前提。当存储平台自己做了 compaction / 快照过期 / 孤儿文件清理，mirror 可能还指着 manifest 已被删掉的那份元数据，这些路径就会报文件不存在：

```
Failed to open file: .../metadata/snap-5961517163737914279-1-....avro
Gopher error: FileDoesNotExistException
```

还在写的表下一次写入就会自动 rebase 回来；只读的表要手工救：

```sql
SELECT * FROM iceberg_refresh_metadata('orders');
 refreshed |               previous_head                |               current_head
-----------+--------------------------------------------+--------------------------------------------
 t         | .../metadata/00047-....metadata.json       | .../metadata/00050-....metadata.json
```

- 返回前后两个 head 而不是一个布尔值，是因为表被打坏之后运维第一个想知道的是「落后了多少」。
- `refreshed` 只表示 head 有没有移动。已经在 head 上时它是 `f`，但缓存时间戳仍会续期。
- 内建 catalog 的表是定义好的 no-op（head 由 PG 自己管），不报错。
- 当前事务里对该表有未提交写入时会拒绝 —— 移动 mirror 会让那笔提交和自己的 rebase 打架。先提交或回滚。

#### 七、升级注意

**扩展本身**：这两个函数随 `datalake_fdw` 1.2 提供。新装的集群 `CREATE EXTENSION` 直接就有；已经装了旧版本的集群执行一次（需要 superuser）：

```sql
ALTER EXTENSION datalake_fdw UPDATE TO '1.2';
```

**跨版本升级集群：必须重新 initdb，不能原地换二进制。** 缓存时间戳那一列 `pg_ext_aux.pg_iceberg_metadata.metadata_cached_at` 是由 `iceberg-cdbinit--1.0.sql` 建的，而那个脚本只在 initdb 时跑一次 —— RPM 升级只会替换磁盘上的脚本文件，不会重新执行它。于是在这个特性之前创建的集群，目录里只有 5 列，而新二进制按第 6 列去读。

这不是"缓存不生效"那么轻，在 release 构建里是**越界读**：`heap_getattr()` 发现 attnum 超过 natts 就走 `getmissingattr()`，去解引用属性数组末尾之后的内存，会不会崩取决于后面恰好是什么字节 —— 协调节点 SIGSEGV、整个集群进恢复，而且每次挂的查询都不一样（issue #439、#443）。

所以处理办法是**在启动这一关就拦住**：`CATALOG_VERSION_NO` 已经随这一列一起提升，这样的集群会直接拒绝启动，报的是清清楚楚的

```
FATAL:  database files are incompatible with server
DETAIL:  The database cluster was initialized with CATALOG_VERSION_NO ...,
         but the server was compiled with CATALOG_VERSION_NO ...
```

而不是读着读着踩到没映射的页。读路径另外还加了 natts 守卫兜底，列不在就当作「从未刷新过」，走已有的 NULL 分支。

一句话：**目录表的结构变更靠 initdb，不靠扩展升级脚本。** `ALTER EXTENSION ... UPDATE` 只负责函数这类扩展自己的对象。

---

### 4.13 当前不支持的功能 / 已知限制

#### 4.13.1 完全未实现

| 功能 | 备注 |
|------|------|
| `PARTITION BY`（identity / bucket / truncate / year/month/day/hour） | 表统一按文件分布。可由 Spark 等引擎写入分区表后再被 datalake_fdw 读取 |
| Time travel 的 **SQL 语法糖**（`FOR SYSTEM_TIME AS OF` / `FOR SYSTEM_VERSION AS OF`） | 时间旅行本身已支持，走函数形式 `iceberg_snapshot_scan()`，见 4.10；语法糖需改内核语法，暂不提供 |
| Time travel 的 branch / tag 引用（`useRef`）、增量读（`appendsBetween` / changelog scan） | 仅支持按 snapshot id / 时间戳定位，见 4.10 |
| `format-version=1/2`、`write.format.default`、`write.parquet.compression-codec`、`write.distribution-mode` 等 Iceberg 表级 property | 由 catalog 默认值决定，无 SQL 入口可覆盖 |
| 跨表分布式事务 | 单表 ACID；多表写入不构成原子事务 |
| Row-level security / 列级权限传递到物理 Iceberg 数据 | 仅在 PG 入口生效，物理文件无访问控制 |
| 生成列（GENERATED COLUMN）、表达式 DEFAULT 写入 Iceberg | PG 端语法可写但不会反映到 Iceberg schema |

#### 4.13.2 静默 no-op（不会报错但实际无效）

| 操作 | 实际行为 |
|------|---------|
| `TRUNCATE <iceberg_table>` | `iceberg_relation_nontransactional_truncate` 实现为空；表内容不会被清空。需要清空请用 `DELETE FROM` 或 `DROP TABLE` 重建 |
| `CREATE INDEX ... ON <iceberg_table>` | `iceberg_index_build_range_scan` 直接返回 0；索引对象创建后无任何条目 |
| `ANALYZE <iceberg_table>` | 非完全 no-op：会从 Iceberg catalog 元数据刷新 `pg_class.reltuples/relpages` 并抛 NOTICE：`ANALYZE on Iceberg tables refreshed pg_class.reltuples/relpages from Iceberg catalog metadata`；列级统计仍来自 manifest，不做采样 |
| `PRIMARY KEY` / `UNIQUE` / `FOREIGN KEY` / `CHECK` 约束 | DDL 接受但无 Iceberg 端唯一索引承载，运行时不强制 |

#### 4.13.3 显式拒绝

| 操作 | 错误 |
|------|------|
| 所有 `ALTER TABLE` 子命令（`ADD / DROP / RENAME COLUMN`、`ALTER COLUMN TYPE`、`SET/DROP NOT NULL`、`ADD CONSTRAINT` 等） | `ERRCODE_FEATURE_NOT_SUPPORTED`: `ALTER TABLE is not supported on Iceberg tables`（datalake_fdw.c） |
| `RENAME COLUMN`（通过 `ALTER TABLE … RENAME COLUMN`） | `ERRCODE_FEATURE_NOT_SUPPORTED`: `RENAME COLUMN is not supported on Iceberg tables`（datalake_fdw.c） |
| TID range scan（如 `WHERE ctid <@ '...'` 之类的范围 TID 谓词） | `ERRCODE_FEATURE_NOT_SUPPORTED`: `not supported`（pg_iceberg_am_handler.c） |
| ANALYZE 内部的 `analyze_next_block` / `analyze_next_tuple` API | `ERRCODE_INTERNAL_ERROR`: `API not supported for iceberg relations`（pg_iceberg_am_handler.c） |

#### 4.13.4 并发写入约束

同一张 Iceberg 表的并发提交走 **CAS + 重试**，最多重试 10 次（`TRACKER_MAX_COMMIT_RETRIES`）；超过会以 `ERRCODE_T_R_SERIALIZATION_FAILURE` 报错：

```
ERROR:  failed to commit iceberg metadata for table <oid> after <N> retries
        due to concurrent updates
```

实务建议：
- 高并发写场景把单笔写入做大（一次 INSERT 多行），减少 commit 次数
- 重试前会清理本地缓存并以最新 snapshot 重 base；外部 catalog（Hive/Polaris）不会重复触发 agent commit，避免重复落数据

需要更强 Iceberg 能力（PARTITION BY、表 property、跨引擎一致 schema 演化）时，建议结合 Spark/Trino/Flink 直接对 Iceberg 表写元数据，由 datalake_fdw 做读侧消费。（时间旅行已原生支持，见 4.10。）

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
| `datalake.iceberg_autovacuum` | `on` | 启用自动 VACUUM |
| `datalake.iceberg_autovacuum_naptime` | `600` | 自动 VACUUM 间隔（秒） |
| `datalake.iceberg_log_autovacuum_min_duration` | `600000` | 记录超过此时长（ms）的自动 VACUUM；`-1` 全不记录、`0` 全部记录 |
| `datalake_fdw.metadata_refresh_ahead_percent` | `70` | 外部 catalog 元数据缓存提前刷新的阈值：TTL 烧掉这个百分比即视为待刷新，见 4.12 |

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
| `numeric` 无精度列 INSERT 报错 | Parquet 编码要求显式精度（建表成功，写入时报错） | 改为 `numeric(p,s)` |
| VACUUM 在函数内报错 | PG 限制 | 在函数外执行 VACUUM |
| Hive Catalog 读回为空 | Hive Metastore 配置 | 检查 `url` 格式：`thrift://host:port` |
| 下推不生效 | GUC 被关闭 | `RESET datalake.disable_filter_pushdown` |
| Polaris 401 错误 | OAuth 认证失败 | 检查 `client_id` / `client_secret` / `scope` |
| 外部表文件找不到 | `filePath` 路径错误 | 确认包含 bucket 前缀或绝对路径 |

### 9.2 Iceberg 表运行时错误对照

| 错误信息（出处） | 含义 | 排查方向 |
|-----------------|------|---------|
| `iceberg table "%s.%s" does not exist in external catalog "%s"` | catalog 中未注册该表 | 确认远端 catalog 名 / namespace / table 拼写；Polaris 检查 `default_namespace` |
| `failed to resolve iceberg table location for relation "%s"` | catalog 拿不到表位置 | catalog 服务连通性、`location` 是否被改动、metadata 是否损坏 |
| `failed to load iceberg table metadata for relation %u` | metadata.json 读不下来 | 检查 Volume 凭据、`base_path`；若使用对象存储确认 endpoint |
| `external catalog "%s" returned empty table location for "%s.%s"` | catalog 返回空 location | catalog 实现 bug 或 namespace 入库异常；可用 `iceberg_toolkit.catalog_fdw` 直查 |
| `location option is not allowed for builtin iceberg tables` | builtin（内部）表建表指定了 `location` OPTION | 内部表路径固定为 Volume `base_path`，去掉 `location` 子句/选项即可；如需自定义路径改用外部 catalog |
| `empty iceberg volume base path for builtin table` | builtin 表解析出的 Volume base_path 为空 | 检查 Volume 的 `base_path` / `bucket_name` 配置 |
| `iceberg metadata catalog is not available on this segment` | 在 QE 上调用了 QD-only 的元数据接口 | 检查是否在 PL/pgSQL 中误用了元数据函数；改为在 QD 上执行 |
| `foreign catalog with OID %u does not exist` | catalog 对象引用失效 | 排查是否 DROP 后未重建；`pg_foreign_catalog` 中确认存在 |
| `foreign volume with OID %u does not exist` | volume 对象引用失效 | 同上 |
| `invalid value for boolean option "%s": "%s"` | OPTION 取值不是布尔字面量 | 用 `true` / `false`，不要用 `0/1` 或 `yes/no` |
| `failed to commit iceberg metadata for table %u after %d retries due to concurrent updates` | 并发写入冲突 CAS 重试 10 次仍失败（详见 4.12.4） | 把多笔写合并成一笔事务、降低并发写并发度，或排查是否有外部引擎同时在写同一张表 |
| `not supported`（来自 pg_iceberg_am_handler.c） | 触发了 TID range scan 路径 | 改用基于普通列的谓词，避免 `ctid <@ ...` 之类查询 |
| `tables on a REST catalog do not use a foreign volume` | REST catalog 建表时写了 `VOLUME` | 去掉 `VOLUME` 子句；位置由平台按表分配 |
| `VACUUM is not supported on tables in an external REST catalog` | 平台未声明维护接口 | 平台若有维护 API，在 SERVER 上加 `maintenance_api 's3tables'`；否则到平台侧发起维护 |
| `invalid value for option "maintenance_api"` | 取值拼写错误 | 目前只支持 `s3tables` |
| `iceberg commit result unknown` | 提交的回复丢失，落库与否无法确认 | **先核实数据是否已在表中再决定是否重跑**——重跑一次已生效的提交会产生重复数据 |
| 凭证过期类错误（`ExpiredToken` / `InvalidAccessKeyId`） | 平台下发的临时凭证已过期（XSKY 约 1 小时） | 重跑该语句；长事务请拆短 |
| `API not supported for iceberg relations` | 调用了 heap-only API（多见于第三方扩展直接调内部 API，或 ANALYZE 内部 block API） | 见 4.12.3；避免该扩展直接作用于 iceberg 表 |
| `ANALYZE on Iceberg tables refreshed pg_class.reltuples/relpages from Iceberg catalog metadata` | 不是错误，只是 NOTICE | ANALYZE 仅刷新 reltuples/relpages（供 ORCA 使用）；列级统计来自 Iceberg manifest，不做采样 |
