# 设计：解耦 Gopher 依赖 —— 存储 Wrapper（开源版接 libhdfs3 + S3 SDK）

- 日期：2026-06-29
- 关联：GitLab issue #305（本方案在 #305 基础上做 4 处增强）
- 范围：database 仓库；闭源组件 = **gopher + liboss2 + iceberg-gopher**；开源依赖 = libhdfs3（将开源）、AWS S3 SDK、Iceberg/Hadoop 官方库

## 1. 背景与目标

`datalake_fdw` 等组件当前所有文件 I/O 都经闭源 **Gopher**（`libgopher.so`）。Gopher 提供统一文件系统抽象 + 本地缓存，但强依赖阻碍开源、部署重、类型泄露深（`gopherConfig`/`gopherFS`/`gopherFile`/`gopherFileInfo` 泄露到 34 个源文件）。

目标：
- **开源版**（无 gopher 源码）：HDFS 走 libhdfs3、对象存储走 AWS S3 SDK，编译链接完全不依赖 `libgopher`；可扩展接入其它对象存储 SDK。
- **商业版**（有 gopher 源码）：保持 Gopher 后端不变，无性能回归、无行为变化。
- **统一代码库**：编译期开关切换，不维护两套分支。
- 覆盖全部 gopher 消费者：datalake_fdw、dfs-tablespace-ext（C++），datalake_agent（Java/iceberg）。

## 2. Gopher 依赖全景（源自 #305 代码扫描）

| 组件 | 位置 | 依赖方式 |
|---|---|---|
| datalake_fdw | `contrib/datalake_fdw` | `-lgopher` + 34 文件 include `gopher.h` |
| pg_gophermeta | `contrib/pg_gophermeta` | `-lgopher` + `gopher_server.h`（gopher 元数据 worker） |
| dfs-tablespace-ext | `contrib/dfs-tablespace-ext` | `-lgopher` + remotefile API |
| datalake_agent (Java) | `contrib/datalake_agent` | Maven 依赖 `iceberg-gopher` |
| iceberg-gopher (Java) | `iceberg-gopher/` | JNI `gopherClient.jar` → `libgopherClient.so` |
| configure.ac | `database/configure.ac` | `--enable-datalake` 强制启用 gophermeta |

耦合细节（#305）：3 处绕过 wrapper 直调 gopher（`gopher_random_file.cpp`、`row_reader.c`、`hudi_logfile_block_reader.c`）；6 个结构体直接存 `gopherFS`/`gopherFile` 句柄；`gopherresowner.c`/`hdw_gopher_cache.c` 硬编码 gopher 缓存 admin。关键发现：`gopherFileInfo` 10 字段中消费者**只用 2 个**（`mPath`、`mLength`）。

## 3. 总体设计：三条对称解耦线

| 层 | 消费者 | 抽象 | 商业(闭源) | 开源 | 构建开关 |
|---|---|---|---|---|---|
| C++ 文件 I/O | datalake_fdw + dfs-tablespace-ext | `FileSystem` SPI + scheme 工厂 | `GopherFileSystem` | libhdfs3 + S3 SDK（可扩展） | `--with-gopher` |
| Java iceberg | datalake_agent | Iceberg `FileIO`/`FileSystem` | `iceberg-gopher` | iceberg-aws + hadoop-aws | maven `-Pgopher` |
| 元数据 worker | pg_gophermeta | （仅商业版构建） | pg_gophermeta | 不编译 | `--with-gopher` |

### 与 #305 的关系
采纳 #305 的全部分析、复用现有 `FileSystem` 类的重构骨架、`datalakeFileInfo` 设计、S3 操作映射、分阶段计划。**4 处增强**（#305 明确列为"不在范围"，但为本项目要求）：

1. **多后端 scheme 工厂**：`#else` 分支不写死单一 S3，改为按 scheme 分发（`hdfs://`→libhdfs3、`s3://`→S3 SDK、其它→可注册），支持别人接入新对象存储 SDK。
2. **HDFS via libhdfs3**：libhdfs3 将开源，OSS 的 hdfs 直连 libhdfs3（#305 将 HDFS 列为不在范围）。
3. **dfs-tablespace-ext 纳入**：同一套 `FileSystem` SPI，`remotefile_connection.c` 改走它（#305 列为后续单独 issue）。
4. **gopher 实现物理隔离**：`gopherFileSystem.cpp` 等放私有目录/子仓，开源镜像剔除，而非留在树里靠 `#ifdef` 跳过（避免开源树里躺着 include `gopher.h` 的死文件）。

## 4. C++ 侧：FileSystem SPI

### 4.1 公共类型（无 gopher 泄露）

```c
/* src/common/datalake_storage.h */
typedef struct datalakeFileInfo {
    char   *path;        /* 文件路径 (palloc'd) */
    int64_t length;      /* 文件大小 bytes */
    bool    isDirectory;
} datalakeFileInfo;
```

### 4.2 抽象基类（重构现有 `FileSystem`，移除 `gopher/gopher.h`）

```cpp
/* src/common/fileSystem.h —— 纯虚基类，不引用任何 gopher 头 */
class FileSystem {
public:
    virtual ~FileSystem() {}
    virtual int createHandle(void *storageOptions) = 0;
    virtual int openFile(const char *path, int flag) = 0;
    virtual int read(void *buff, int64_t size) = 0;
    virtual int write(void *buff, int64_t size) = 0;
    virtual int seek(int64_t position) = 0;
    virtual int closeFile() = 0;
    virtual datalakeFileInfo* listInfo(const char *path, int &count,
                                       int recursive = 1, bool iswrite = false) = 0;
    virtual datalakeFileInfo* getFileInfo(const char *path) = 0;
    virtual void freeListInfo(datalakeFileInfo *list, int count) = 0;
    virtual int destroyHandle() = 0;
    /* gopher 专有（缓存/ufs），native 实现为 no-op/返回 0 */
    virtual int getUfsId() { return 0; }
};
```

公共 C 接口 `fileSystemWrapper.h` 去掉所有 gopher 类型，`datalakeCreateGopherConfig`+`datalakeCreateFileSystem` 合并为单步 `datalakeCreateFileSystem(void *storageOptions)`，返回 `datalakeFileInfo*`。

### 4.3 后端选择（编译期 + scheme 工厂）

```cpp
/* fileSystemWrapper.cpp */
ossFileStream datalakeCreateFileSystem(void *storageOptions)
{
    FileSystem *fs;
#ifdef USE_GOPHER
    fs = new GopherFileSystem();                 /* 商业版：私有模块 */
#else
    fs = createNativeFileSystem(storageOptions); /* 开源版：按 scheme 分发 */
#endif
    fs->createHandle(storageOptions);
    return new ossInternalFileStream(fs);
}
```

```cpp
/* 开源工厂：按 protocol/scheme 选择 native backend，可注册扩展 */
FileSystem* createNativeFileSystem(void *storageOptions) {
    switch (schemeOf(storageOptions)) {
        case DL_SCHEME_HDFS: return new Hdfs3FileSystem();   /* libhdfs3 */
        case DL_SCHEME_S3:   return new S3FileSystem();      /* AWS S3 SDK */
        default:             return lookupRegisteredFs(scheme); /* 第三方注册 */
    }
}
```

**扩展点**：新对象存储 SDK = 实现 `FileSystem` 子类 + 在注册表登记 scheme，无需改上层 40+ 调用者。

### 4.4 native 后端实现

- **`S3FileSystem`（AWS S3 SDK C++）**——映射（源自 #305）：`createHandle`→`Aws::S3::S3Client`（endpoint/ak/sk/bucket/region，支持 path-style/MinIO）；`read`→`GetObject`+Range；`write`→5MB 缓冲 + `UploadPart`/`CompleteMultipartUpload`；`seek`→改偏移；`getFileInfo`→`HeadObject`；`listInfo`→`ListObjectsV2`+分页。**无缓存直连**。
- **`Hdfs3FileSystem`（libhdfs3）**——`hdfsConnect`/`hdfsOpenFile`/`hdfsPread`/`hdfsWrite`/`hdfsGetPathInfo`/`hdfsListDirectory`。复用现有 HDFS 配置（namenode/HA/kerberos）。
- gopher 专有缓存/admin（`gopherresowner` 的 `gopherCreateAdmin`/`AdminClearListResult`、`hdw_gopher_cache` 的 `__gopher_free_all_cache`/`__gopher_cache_free_relation_name`）在 native 下为 no-op/返回 0 的 stub；SQL 函数仍创建（stub），保证接口兼容。

### 4.5 修复 3 处绕过 + 6 个结构体
`row_reader.c`、`gopher_random_file.cpp`→`datalake_random_file.cpp`、`hudi_logfile_block_reader.c` 全部改走 wrapper / `ossFileStream`；6 个结构体的 `gopherFS`/`gopherFile` 字段改为 `ossFileStream`。

## 5. gopher 私有模块隔离

- 私有文件：`gopherFileSystem.h/.cpp`（含 `#include <gopher/gopher.h>`、`-lgopher`），放在私有目录（如 `contrib/datalake_fdw/src/common/gopher/`）由独立私有仓库/子模块提供，开源镜像剔除该目录。
- `fileSystemWrapper.cpp` 的 `#ifdef USE_GOPHER` 分支引用它；开源构建不定义 `USE_GOPHER`，不编译、不链接、不 include。
- 开源树验证：`grep -rl "gopher/gopher.h" contrib/datalake_fdw` 应为空；`nm -D datalake_fdw.so | grep gopher` 无输出；`ldd` 不含 `libgopher`。

## 6. dfs-tablespace-ext 纳入

`remotefile_connection.c` 的 `gopherCreateConfig`/`config->ufs_type = OptionGetProtocolType(...)` 改走同一 `FileSystem` SPI；`--with-gopher` 时用 GopherFileSystem，开源时按 scheme 用 libhdfs3/S3。其协议表（`dfsProtocols[]`）保持，映射到 SPI scheme。

## 7. Java 侧：dlagent / iceberg 解耦

| | 商业版 `-Pgopher` | 开源版（默认） |
|---|---|---|
| FileIO | `GopherFileIO`（iceberg-gopher，私有） | `S3FileIO`(iceberg-aws) / `HadoopFileIO`(hdfs) |
| Hadoop FS | `GopherFileSystem` | `S3AFileSystem`(hadoop-aws) / HDFS Java client |
| 闭源剔除 | — | 整个 **iceberg-gopher**（GopherFileIO/FS + JNI libgopherClient.so） |

- `datalake_agent/pom.xml`：`iceberg-gopher` 设 `<optional>`/profile 限定；默认 profile 加 `iceberg-aws` + `hadoop-aws`。
- FileIO 选择由 database 下发的 iceberg 配置 `io-impl`（及 `fs.s3a.*`/`fs.defaultFS`）驱动。配置发射点（`icebergConfig.c` / catalogType→conf / volume JSON）按构建模式决定发 `GopherFileIO` 还是 `S3FileIO`/`HadoopFileIO`。
- 与既有决定一致：iceberg 路径开源后只支持 S3 + HDFS 原生 FileIO；azure/gcs 不接 iceberg。

## 8. 构建系统

```bash
./configure --enable-datalake               # 开源：不定义 USE_GOPHER，链 libhdfs3 + aws-sdk-cpp，不编 pg_gophermeta
./configure --enable-datalake --with-gopher # 商业：USE_GOPHER=1，链 -lgopher，编 pg_gophermeta
```

```makefile
ifdef USE_GOPHER
  OBJS += src/common/gopher/gopherFileSystem.o
  SHLIB_LINK += -lgopher
else
  OBJS += src/common/s3FileSystem.o src/common/hdfs3FileSystem.o
  SHLIB_LINK += -laws-cpp-sdk-s3 -laws-cpp-sdk-core -lhdfs3
endif
# 格式库始终链接（不依赖 gopher）
SHLIB_LINK += -lparquet -lorc -larchive -lavrocpp -larrow -lavro -lcurl -lyaml ...
```

Maven：`-Pgopher` 含 iceberg-gopher；默认含 iceberg-aws + hadoop-aws。

## 9. 文件变更清单（#305 基础 + 增强）

**新增**：`datalake_storage.h`、`s3FileSystem.h/.cpp`、`hdfs3FileSystem.h/.cpp`（增强）、`nativeFsFactory.cpp`（增强：scheme 工厂+注册表）、`gopher/gopherFileSystem.h/.cpp`（私有）、`datalake_random_file.h/.cpp`。

**修改**：`fileSystem.h`（抽象基类）、`fileSystemWrapper.h/.cpp`（去 gopher 类型、合并 create、scheme 工厂）、`row_reader.c`、`hudi_logfile_block_reader.h/.c`、`utils.h`（6 结构体改 `ossFileStream`）、`gopherresowner.c`、`hdw_gopher_cache.c`（`#ifdef`+stub）、`dfs-tablespace-ext/remotefile_connection.c`（增强）、`Makefile`、`configure.ac`、`datalake_fdw--1.0.sql`、`datalake_agent/pom.xml`（增强）、34 个 include 文件去 `gopher.h`、14 个调用者合并 create。

## 10. 实施阶段

| 阶段 | 内容 | 风险 | 预计 |
|---|---|---|---|
| P1 抽象层 | `datalakeFileInfo`、`FileSystem` 抽象基类、提取 `GopherFileSystem`、重构 wrapper、迁移 14 调用者 | 低 | 3-5d |
| P2 修复绕过 | row_reader / random_file / hudi reader 改走 wrapper | 中 | 2-3d |
| P3 条件编译 | gopherresowner/hdw_gopher_cache `#ifdef`+stub，清 34 个 include，gopher 文件移私有目录 | 低 | 1-2d |
| P4 S3 实现 | `S3FileSystem`（AWS SDK） | 高 | 5-7d |
| P5 HDFS+工厂（增强） | `Hdfs3FileSystem`（libhdfs3）+ scheme 工厂/注册表 | 中 | 2-3d |
| P6 dfs-tablespace-ext（增强） | remotefile 改走 SPI | 低 | 1-2d |
| P7 构建系统 | `--with-gopher`/`-Pgopher`、解耦 gophermeta、Makefile/pom 条件 | 低 | 1-2d |
| P8 Java iceberg | datalake_agent 用 S3FileIO/HadoopFileIO | 低 | 1-2d |
| P9 测试 | 无 gopher 编译验证、MinIO/HDFS 集成、gopher 回归、TPC-H 性能 | 中 | 3-5d |

里程碑：M1 抽象层就位 gopher 回归通过；M2 无 gopher 环境编译链接成功；M3 MinIO+HDFS 读写通过；M4 两模式全测+性能无回归。P1-P3 为纯重构，可在现有 gopher 构建上逐步推进、各自独立验证。

## 11. 测试策略

- **编译验证**：无 gopher 时 `nm -D datalake_fdw.so | grep gopher` 空、`ldd` 不含 libgopher；有 gopher `make installcheck` 全过。
- **S3（MinIO）**：Parquet/ORC/CSV 读写、目录列举、大文件(>5MB) multipart、列投影随机读。
- **HDFS（libhdfs3）**：读写、HA、kerberos。
- **gopher 回归**：`make installcheck`、`iceberg-test.sql`、TPC-H 1GB 性能（基线 ~23s，±5%）。
- **Java**：dlagent 用 S3FileIO 做 iceberg 元数据读写。
- **边界**：空目录、连接/凭证错误、并发读。

## 12. 风险与缓解

| 风险 | 缓解 |
|---|---|
| S3 直连无缓存，性能不如 gopher | Range 请求本身高效；后续可加本地缓存层（不在本次） |
| AWS SDK C++ 体积大 | 只编 s3 模块；考虑静态链接 |
| 重构引入 gopher 路径回归 | P1-P3 纯重构，gopher 构建持续绿灯 |
| gopherresowner 资源清理缺失 | native 保留 ResourceOwner 框架，只跳过 gopher admin |
| scheme 工厂/注册表过度设计 | 先内置 hdfs/s3 两个，注册表只留最小扩展点（YAGNI） |

## 13. 不在范围

- S3 本地文件缓存层（后续性能优化）。
- 运行时动态切换（纯编译期）。
- azure/gcs 接入 iceberg 路径（与既有决定一致）。
- 非 datalake 的其它 gopher 用途（如有）。
