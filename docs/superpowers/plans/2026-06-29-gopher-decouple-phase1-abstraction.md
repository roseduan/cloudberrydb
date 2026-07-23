# Gopher 解耦 Phase 1：C++ 存储抽象层 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 `datalake_fdw` 的存储抽象层从 Gopher 类型解耦：引入存储无关的 `datalakeFileInfo`，把 `FileSystem` 改为抽象基类，现有 gopher 实现下沉为 `GopherFileSystem`，公共头不再泄露 `gopher/gopher.h`。全程保持 `--with-gopher` 构建行为不变、回归通过。

**Architecture:** 纯重构，不引入新后端。`fileSystem.h` 变纯虚基类；`gopherFileSystem.h/.cpp`（私有目录）实现它、封装现有 `gopherFS`/`gopherFile` 调用；`fileSystemWrapper.cpp` 用 `#ifdef USE_GOPHER` 选实现。公共类型从 `gopherFileInfo*` 换成 `datalakeFileInfo*`（仅 path/length/isDirectory）。

**Tech Stack:** C/C++17，PostgreSQL/Cloudberry 扩展（PGXS），libgopher（私有），在 `hd-iceberg-test` 容器构建（`/workspace/database`，端口 7000 demo 集群）。

## Global Constraints

- 本 Phase **只在 `--with-gopher` 构建上做重构**，每个 Task 结束都必须 `make` 通过且 `make installcheck`（datalake_fdw 回归）无新增失败。
- 不改变任何运行时行为（无功能增删）；`datalakeFileInfo` 只含 `path`/`length`/`isDirectory` 三字段（消费者实际只用 path+length）。
- 不得在本 Phase 删除 gopher 链接或引入 S3/libhdfs3（那是后续 Phase）。
- C 代码 tab 缩进、`/* */` 注释；遵循 PostgreSQL 风格与文件头规范。
- 构建/测试在容器内：`docker exec hd-iceberg-test bash -lc '...'`，先 `source /workspace/dist/database/greenplum_path.sh`、`export PATH=/workspace/dist/database/bin:$PATH`。
- 测试周期是**构建级**（非单测）：`cd /workspace/database/contrib/datalake_fdw && make && make install`，再 `make installcheck`。每个 Task 末尾 commit。

---

## File Structure

| 文件 | 责任 | 本 Phase 动作 |
|---|---|---|
| `src/common/datalake_storage.h` | 存储无关公共类型 `datalakeFileInfo` | **新增** |
| `src/common/fileSystem.h` | `FileSystem` 抽象基类（无 gopher 头） | 改写为纯虚 |
| `src/common/gopher/gopherFileSystem.h` | `GopherFileSystem : FileSystem` 声明（私有目录，含 gopher.h） | **新增**（从 fileSystem.h 提取 gopher 部分） |
| `src/common/gopher/gopherFileSystem.cpp` | gopher 实现（从 fileSystem.cpp 迁移） | **新增**（迁移现 fileSystem.cpp 实体） |
| `src/common/fileSystem.cpp` | 删除（实体迁到 gopherFileSystem.cpp） | 删除或留空 |
| `src/common/fileSystemWrapper.h` | 公共 C 接口（无 gopher 类型） | 改：`gopherFileInfo*`→`datalakeFileInfo*`，去 `gopher.h` |
| `src/common/fileSystemWrapper.cpp` | C 接口实现 + `#ifdef USE_GOPHER` 选实现 | 改 |
| Makefile | 编译 `gopher/gopherFileSystem.o`（USE_GOPHER 时） | 改 |
| 消费 `datalakeListDir`/`datalakeGetFileInfo` 结果的文件 | 字段 `.mPath/.mLength`→`.path/.length` | 改 |

> 注：本 Phase 仍始终定义 `USE_GOPHER`（构建带 `--with-gopher`）。`#else` 分支留空桩（编译期不触达），native 实现在后续 Phase 填。

---

### Task 1: 引入 `datalakeFileInfo` 公共类型

**Files:**
- Create: `contrib/datalake_fdw/src/common/datalake_storage.h`

**Interfaces:**
- Produces: `typedef struct datalakeFileInfo { char *path; int64_t length; bool isDirectory; } datalakeFileInfo;`

- [ ] **Step 1: 新建头文件**

```c
/*-------------------------------------------------------------------------
 *
 * datalake_storage.h
 *    Storage-backend-agnostic public types for datalake_fdw, replacing
 *    leakage of Gopher's gopherFileInfo into consumer code.
 *
 * Portions Copyright (c) 2023-2026, HashData Technology Limited.
 *
 * IDENTIFICATION
 *        contrib/datalake_fdw/src/common/datalake_storage.h
 *-------------------------------------------------------------------------
 */
#ifndef DATALAKE_STORAGE_H
#define DATALAKE_STORAGE_H

#include <stdbool.h>
#include <stdint.h>

/*
 * Minimal file metadata used by datalake_fdw consumers. Of Gopher's 10-field
 * gopherFileInfo, only path and length are actually consumed; isDirectory is
 * kept for hidden-file/dir filtering. Backend-agnostic: filled by GopherFileSystem
 * (commercial) or the native S3/HDFS backends (open source).
 */
typedef struct datalakeFileInfo
{
	char	*path;			/* object/file path (palloc'd by caller's context) */
	int64_t	length;			/* size in bytes */
	bool	isDirectory;	/* true if a directory/prefix */
} datalakeFileInfo;

#endif							/* DATALAKE_STORAGE_H */
```

- [ ] **Step 2: 编译验证（头文件可独立通过）**

Run:
```bash
docker exec hd-iceberg-test bash -lc 'cd /workspace/database/contrib/datalake_fdw && /usr/local/toolchain/bin/gcc -fsyntax-only -I../../src/include src/common/datalake_storage.h && echo OK'
```
Expected: `OK`

- [ ] **Step 3: Commit**

```bash
cd /workspace/database && git add contrib/datalake_fdw/src/common/datalake_storage.h
git commit -m "Refactor: add backend-agnostic datalakeFileInfo type"
```

---

### Task 2: `FileSystem` 接口改用 `datalakeFileInfo`（gopher 边界转换）

把抽象层对外的文件信息类型从 `gopherFileInfo*` 换成 `datalakeFileInfo*`，转换发生在 gopher 实现内部（`listInfo`/`getFileInfo` 内把 gopher 结果拷成 datalakeFileInfo）。本 Task 仍在单一 `FileSystem` 类内做（尚未拆基类），保证小步可编译。

**Files:**
- Modify: `contrib/datalake_fdw/src/common/fileSystem.h`
- Modify: `contrib/datalake_fdw/src/common/fileSystem.cpp`
- Modify: `contrib/datalake_fdw/src/common/fileSystemWrapper.h`
- Modify: `contrib/datalake_fdw/src/common/fileSystemWrapper.cpp`
- Modify (消费者，字段改名): `src/datalake_fragment.c`、`src/provider/parquet/read/parquetInputStream.cpp`、`src/provider/avro/read/avroOssInputStream.h`、`src/provider/common/gopher_random_file.cpp`

**Interfaces:**
- Consumes: `datalakeFileInfo`（Task 1）
- Produces:
  - `FileSystem::listInfo(const char*, int&, int, bool) -> datalakeFileInfo*`
  - `FileSystem::getFileInfo(const char*) -> datalakeFileInfo*`
  - `FileSystem::freeListInfo(datalakeFileInfo*, int)`
  - wrapper: `datalakeListDir(...) -> datalakeFileInfo*`、`datalakeGetFileInfo(...) -> datalakeFileInfo*`、`datalakeFreeListDir(ossFileStream, datalakeFileInfo*, int)`

- [ ] **Step 1: 改 `fileSystem.h` 签名**

把 `fileSystem.h` 顶部加 `#include "datalake_storage.h"`，并将三处签名改为：
```cpp
	datalakeFileInfo* listInfo(const char *path, int &count, int recursive = 1, bool iswrite = false);
	datalakeFileInfo* getFileInfo(const char* path);
	void freeListInfo(datalakeFileInfo *list, int count);
```
（其余 gopher 成员暂保留，本 Task 不拆基类。）

- [ ] **Step 2: 改 `fileSystem.cpp` 实现做转换**

在 `listInfo`/`getFileInfo` 里，调用现有 gopher API 得到 `gopherFileInfo*` 后，分配 `datalakeFileInfo` 数组并逐项拷贝（`path = pstrdup(src.mPath)`，`length = src.mLength`，`isDirectory = src.mDirectory`），随后 `gopherFreeFileInfo` 释放 gopher 原结果；返回 datalakeFileInfo 数组。`freeListInfo` 改为释放 datalakeFileInfo 数组（先 `pfree(list[i].path)` 再 `pfree(list)`）。

转换规则（精确、无歧义）：`gopherFileInfo.mPath → datalakeFileInfo.path`（pstrdup），`mLength → length`，`mDirectory → isDirectory`；其余 8 个 gopher 字段丢弃。

- [ ] **Step 3: 改 `fileSystemWrapper.h`**

把三处返回/参数类型 `gopherFileInfo*` 改为 `datalakeFileInfo*`，并在文件顶部加 `#include "datalake_storage.h"`（暂保留 `#include <gopher/gopher.h>`，Task 3 再移除）。

- [ ] **Step 4: 改 `fileSystemWrapper.cpp`**

`datalakeListDir`/`datalakeGetFileInfo`/`datalakeFreeListDir` 的内部类型同步改为 `datalakeFileInfo*`（直接透传 FileSystem 的新返回值，无需再转换）。

- [ ] **Step 5: 改消费者字段名**

在 4 个消费者文件里把对 list/getFileInfo 结果的字段访问改名：`->mPath`/`.mPath` → `->path`/`.path`；`->mLength`/`.mLength` → `->length`/`.length`；`->mDirectory` → `->isDirectory`。先 grep 定位：
```bash
docker exec hd-iceberg-test bash -lc 'cd /workspace/database/contrib/datalake_fdw && grep -rn "mPath\|mLength\|mDirectory" src/datalake_fragment.c src/provider/parquet/read/parquetInputStream.cpp src/provider/avro/read/avroOssInputStream.h src/provider/common/gopher_random_file.cpp'
```
逐处按上面规则替换。

- [ ] **Step 6: 构建 + 回归**

Run:
```bash
docker exec hd-iceberg-test bash -lc 'source /workspace/dist/database/greenplum_path.sh; export PATH=/workspace/dist/database/bin:$PATH; cd /workspace/database/contrib/datalake_fdw && make 2>&1 | tail -15 && make install 2>&1 | tail -3'
```
Expected: 编译链接成功，无 `gopherFileInfo` 相关报错。

- [ ] **Step 7: Commit**

```bash
cd /workspace/database && git add -A contrib/datalake_fdw/src
git commit -m "Refactor: FileSystem exposes datalakeFileInfo instead of gopherFileInfo"
```

---

### Task 3: 拆出 `GopherFileSystem` 子类，`FileSystem` 变纯虚，公共头去 gopher.h

**Files:**
- Modify: `contrib/datalake_fdw/src/common/fileSystem.h`（纯虚基类，去 `gopher/gopher.h`）
- Create: `contrib/datalake_fdw/src/common/gopher/gopherFileSystem.h`
- Create: `contrib/datalake_fdw/src/common/gopher/gopherFileSystem.cpp`（迁移 `fileSystem.cpp` 实体）
- Delete: `contrib/datalake_fdw/src/common/fileSystem.cpp`
- Modify: `contrib/datalake_fdw/src/common/fileSystemWrapper.h`（去 `gopher/gopher.h`）
- Modify: `contrib/datalake_fdw/src/common/fileSystemWrapper.cpp`（`#ifdef USE_GOPHER` new GopherFileSystem）
- Modify: `contrib/datalake_fdw/Makefile`（编 `gopher/gopherFileSystem.o`）

**Interfaces:**
- Consumes: `FileSystem`（Task 2 后的接口）
- Produces: `class GopherFileSystem : public Datalake::Internal::FileSystem`（实现全部纯虚方法 + `createHandle(void*)`）

- [ ] **Step 1: `fileSystem.h` 改纯虚基类**

去掉 `#include "gopher/gopher.h"` 和 `gopherFS fs; gopherFile file;` 私有成员；把所有方法改 `virtual ... = 0;`，加虚析构。保留 `Error` 结构（无 gopher 依赖）与 `datalake_storage.h` include。`gopherCreateHandle(gopherConfig*)` 改为后端无关的 `virtual int createHandle(void *storageOptions) = 0;`。

```cpp
class FileSystem {
public:
	virtual ~FileSystem() {}
	virtual int createHandle(void *storageOptions) = 0;
	virtual int openFile(const char *path, int flag) = 0;
	virtual int write(void *buff, int64_t size) = 0;
	virtual int read(void *buff, int64_t size) = 0;
	virtual int seek(int64_t postion) = 0;
	virtual int closeFile() = 0;
	virtual int getUfsId() = 0;
	virtual datalakeFileInfo* listInfo(const char *path, int &count, int recursive = 1, bool iswrite = false) = 0;
	virtual datalakeFileInfo* getFileInfo(const char* path) = 0;
	virtual int deleteFile(const char *path) = 0;
	virtual void freeListInfo(datalakeFileInfo *list, int count) = 0;
	virtual int destroyHandle() = 0;
};
```

- [ ] **Step 2: 新建 `gopher/gopherFileSystem.h`**

```cpp
/*-------------------------------------------------------------------------
 * gopherFileSystem.h — Gopher-backed FileSystem (commercial build only).
 * PRIVATE: excluded from open-source mirror. Compiled only when USE_GOPHER.
 *-------------------------------------------------------------------------
 */
#ifndef DATALAKE_GOPHER_FILESYSTEM_H
#define DATALAKE_GOPHER_FILESYSTEM_H
#include "gopher/gopher.h"
#include "common/fileSystem.h"

namespace Datalake { namespace Internal {
class GopherFileSystem : public FileSystem {
public:
	GopherFileSystem();
	~GopherFileSystem() override;
	int createHandle(void *storageOptions) override;   /* storageOptions = gopherConfig* */
	int openFile(const char *path, int flag) override;
	int write(void *buff, int64_t size) override;
	int read(void *buff, int64_t size) override;
	int seek(int64_t postion) override;
	int closeFile() override;
	int getUfsId() override;
	datalakeFileInfo* listInfo(const char *path, int &count, int recursive = 1, bool iswrite = false) override;
	datalakeFileInfo* getFileInfo(const char* path) override;
	int deleteFile(const char *path) override;
	void freeListInfo(datalakeFileInfo *list, int count) override;
	int destroyHandle() override;
private:
	static bool checkCanceled(void);
	gopherFS fs;
	gopherFile file;
	std::string filePath;
	bool closed;
};
}}
#endif
```

- [ ] **Step 3: 迁移实现到 `gopher/gopherFileSystem.cpp`**

`git mv contrib/datalake_fdw/src/common/fileSystem.cpp contrib/datalake_fdw/src/common/gopher/gopherFileSystem.cpp`；把类名 `FileSystem::` 全改为 `GopherFileSystem::`；构造/析构改名；`gopherCreateHandle(gopherConfig *conf)` 改为 `createHandle(void *storageOptions)` 并在体内 `gopherConfig *conf = (gopherConfig*)storageOptions;`。include 改为 `#include "common/gopher/gopherFileSystem.h"`。listInfo/getFileInfo/freeListInfo 的 datalakeFileInfo 转换逻辑（Task 2 已写）随之迁入。

- [ ] **Step 4: `fileSystemWrapper.h` 去 gopher.h**

删除 `#include <gopher/gopher.h>`；删除 `datalakeCreateGopherConfig`/`datalakeFreeGopherConfig` 声明（其职责并入 create，见 Task 4，但本 Task 先保留实现于 .cpp 以免破坏调用方——本 Task 仅头文件去类型泄露，create 合并放 Task 4）。本 Task 保持 `datalakeCreateFileSystem(gopherConfig*)` 暂不改签名是不行的（头不能有 gopherConfig）。**因此本 Task 把 `datalakeCreateFileSystem` 暂改为接收 `void *conf`**：
```c
ossFileStream datalakeCreateFileSystem(void *conf);   /* conf = gopherConfig* (commercial) */
```

- [ ] **Step 5: `fileSystemWrapper.cpp` 选实现**

```cpp
#include "common/fileSystem.h"
#ifdef USE_GOPHER
#include "common/gopher/gopherFileSystem.h"
#endif
using namespace Datalake::Internal;

ossFileStream datalakeCreateFileSystem(void *conf)
{
	FileSystem *fsImpl;
#ifdef USE_GOPHER
	fsImpl = new GopherFileSystem();
#else
	elog(ERROR, "datalake_fdw built without a storage backend");
	return NULL;
#endif
	fsImpl->createHandle(conf);
	/* wrap fsImpl into ossInternalFileStream exactly as before */
	...
}
```
`ossInternalFileStream` 内部持有的 `FileSystem*` 现在是基类指针——其余 wrapper 函数（open/read/write/.../listDir/getFileInfo/freeListDir/destroy）通过基类虚调用，无需改逻辑，只把内部 `gopherConfig*`/`gopherFileInfo*` 痕迹清掉。`datalakeCreateGopherConfig`/`datalakeFreeGopherConfig` 的实现（构建 gopherConfig）暂留在 .cpp 内并加 `#ifdef USE_GOPHER`（Task 4 再并入 create）。

- [ ] **Step 6: Makefile 编译私有实现**

把 `OBJS` 里的 `src/common/fileSystem.o` 替换为：
```makefile
ifdef USE_GOPHER
  OBJS += src/common/gopher/gopherFileSystem.o
endif
```

- [ ] **Step 7: 构建 + 回归**

Run:
```bash
docker exec hd-iceberg-test bash -lc 'source /workspace/dist/database/greenplum_path.sh; export PATH=/workspace/dist/database/bin:$PATH; cd /workspace/database/contrib/datalake_fdw && make 2>&1 | tail -20 && make install 2>&1 | tail -3'
docker exec hd-iceberg-test bash -lc 'grep -rl "gopher/gopher.h" /workspace/database/contrib/datalake_fdw/src/common/fileSystem.h /workspace/database/contrib/datalake_fdw/src/common/fileSystemWrapper.h && echo "LEAK" || echo "no leak in public headers"'
```
Expected: 编译成功；公共头无 gopher.h（`no leak in public headers`）。

- [ ] **Step 8: Commit**

```bash
cd /workspace/database && git add -A contrib/datalake_fdw
git commit -m "Refactor: extract GopherFileSystem, make FileSystem abstract, drop gopher.h from public headers"
```

---

### Task 4: 合并 create API（config + connect 一步），迁移调用者

**Files:**
- Modify: `contrib/datalake_fdw/src/common/fileSystemWrapper.h/.cpp`
- Modify: 调用 `datalakeCreateGopherConfig`+`datalakeCreateFileSystem` 的文件（见 spec §9 的 14 处）

**Interfaces:**
- Produces: `ossFileStream datalakeCreateFileSystem(void *storageOptions)` —— 内部完成"options→后端配置→连接"，调用方一行搞定。`datalakeCreateGopherConfig`/`datalakeFreeGopherConfig` 从公共接口移除（gopher 实现内部消化）。

- [ ] **Step 1: 定位现有两步调用**

Run:
```bash
docker exec hd-iceberg-test bash -lc 'cd /workspace/database/contrib/datalake_fdw && grep -rn "datalakeCreateGopherConfig" src/'
```
记录每处（spec 列了 14 处）。

- [ ] **Step 2: 改 create 实现为单步**

`datalakeCreateFileSystem(void *storageOptions)`：内部（`#ifdef USE_GOPHER`）调用原 `datalakeCreateGopherConfig` 逻辑把 `storageOptions`(gopherOptions*) 转 gopherConfig，再 `new GopherFileSystem(); createHandle(conf)`，并在结束/出错时释放 conf。把 `datalakeCreateGopherConfig`/`datalakeFreeGopherConfig` 改为 `static` 内部函数（不再导出）。

- [ ] **Step 3: 迁移 14 个调用者**

每处把
```c
gopherConfig *conf = datalakeCreateGopherConfig(opt);
ossFileStream s = datalakeCreateFileSystem(conf);
... datalakeFreeGopherConfig(conf);
```
改为
```c
ossFileStream s = datalakeCreateFileSystem(opt);
```
（`opt` 为 `gopherOptions*`/`dataLakeOptions->gopher`。删除随后的 `datalakeFreeGopherConfig` 调用。）

- [ ] **Step 4: 构建 + 回归**

Run:
```bash
docker exec hd-iceberg-test bash -lc 'source /workspace/dist/database/greenplum_path.sh; export PATH=/workspace/dist/database/bin:$PATH; cd /workspace/database/contrib/datalake_fdw && make 2>&1 | tail -15 && make install 2>&1 | tail -3'
```
Expected: 全部调用者编译通过，无 `datalakeCreateGopherConfig` 未定义报错。

- [ ] **Step 5: 全量回归测试**

Run:
```bash
docker exec hd-iceberg-test bash -lc 'source /workspace/dist/database/greenplum_path.sh; export PATH=/workspace/dist/database/bin:$PATH; export COORDINATOR_DATA_DIRECTORY=/workspace/database/gpAux/gpdemo/datadirs/qddir/demoDataDir-1; cd /workspace/database/contrib/datalake_fdw && make installcheck 2>&1 | tail -25'
```
Expected: 回归通过（或与重构前同样的 diff 基线，无新增失败）。

- [ ] **Step 6: Commit**

```bash
cd /workspace/database && git add -A contrib/datalake_fdw
git commit -m "Refactor: merge storage config+connect into datalakeCreateFileSystem(storageOptions)"
```

---

## Phase 1 完成判据（M1）

- `--with-gopher` 构建成功、`make installcheck` 回归无新增失败。
- 公共头 `fileSystem.h`/`fileSystemWrapper.h` 不含 `gopher/gopher.h`，不暴露 `gopherConfig`/`gopherFileInfo`。
- gopher 实现集中在私有 `src/common/gopher/gopherFileSystem.{h,cpp}`。
- 上层调用者通过单步 `datalakeCreateFileSystem(storageOptions)` + `datalakeFileInfo` 工作。

## 后续 Plan（各自单独成文）

- **Phase 2**：修复 3 处绕过（`row_reader.c`/`gopher_random_file.cpp`→`datalake_random_file.cpp`/`hudi_logfile_block_reader.c`）+ 6 个结构体 `gopherFS`→`ossFileStream`。
- **Phase 3**：`gopherresowner.c`/`hdw_gopher_cache.c` `#ifdef`+stub，清剩余 34 处 `gopher.h` include，gopher 文件确认在私有目录。
- **Phase 4**：`S3FileSystem`（AWS SDK）。
- **Phase 5**：`Hdfs3FileSystem`（libhdfs3）+ scheme 工厂/注册表。
- **Phase 6**：dfs-tablespace-ext 改走 SPI。
- **Phase 7**：构建系统 `--with-gopher`/`-Pgopher`、解耦 gophermeta。
- **Phase 8**：Java datalake_agent 用 S3FileIO/HadoopFileIO。
- **Phase 9**：无 gopher 编译验证 + MinIO/HDFS 集成 + 性能回归。
