# CAGG 长稳测试框架 — 设计与操作手册

> **English version**: [README.md](./README.md)
> **5 分钟快速开始**: §1
> **最后更新**: 2026-07-09

本目录是 Apache Cloudberry `time_series` 扩展的 **持续聚合 (CAGG) 长稳测试框架**。它在一个 MPP 集群上持续灌入时序写入并周期性验证 view 跟源表逐行等价。

---

## 目录

**入门**
- [§0 设计动机](#0-设计动机)
- [§1 前置条件 + 运行](#1-前置条件--运行)
- [§2 容量规划 (profile 选择)](#2-容量规划-profile-选择)
- [§3 配置参数详解](#3-配置参数详解)

**主动验证**
- [§4 正确性验证逻辑](#4-正确性验证逻辑)
- [§5 性能验收](#5-性能验收view-query-latency)
- [§6 Chaos 注入](#6-chaos-注入)

---

## §0 设计动机

现有 regress / isolation2 测试覆盖了 **功能正确性 + 单次故障恢复**, 但有几类问题只在长时间持续运行下才会暴露:

| 问题类别 | regress 能抓 | 需要长稳 |
|---|:---:|:---:|
| Scheduler 进程内存泄漏 (MemoryContextReset 遗漏) | ✗ | ✓ |
| L1/L2 invalidation log 积压 (千次刷新后行数膨胀) | ✗ | ✓ |
| `bgw_job_stat` 统计累加精度 (total_runs 溢出、duration 漂移) | ✗ | ✓ |
| 多 CAGG 共享源表的 watermark 竞争 | 部分 | ✓ |
| Worker 反复 fork/exit 的资源耗尽 (DSM / fd / PID 循环) | ✗ | ✓ |
| 调度精度漂移 (fixed_schedule 在真实时钟下的累积误差) | ✗ | ✓ |
| 高并发 DML + DDL + refresh 三路交叉的死锁 / 活锁 | isolation2 部分 | ✓ |
| BGW worker 完成 refresh 后无声死亡 (ghost row) | ✗ | ✓ |
| Hypertable chunk 路由在持续 INSERT 下的稳定性 (错路由、fork 越界) | 部分 | ✓ |
| Hypertable 跨 chunk 边界时 CAGG bucket 与 chunk 对齐漂移 | ✗ | ✓ |
| Hypertable per-chunk fork 文件随时间累积 (fd / inode 泄漏、ts_relfilenode 表膨胀) | ✗ | ✓ |

**通过判定** 只盯两件事 —— 其余问题 (FATAL / 段下线 / 死锁 / OOM…) 都会通过系统异常 / 错误日志自然暴露, **不需要单独验证**:

- **数据正确性 (§4)**: **无 bucket 持续发散** (MISMATCH 桶需在 **settle 延迟后复查仍发散** 才 escalate exit 2 —— decidable-region 检查 + targeted delayed re-check,滤掉 watermark 边界的最终一致瞬态;settle 内自愈的瞬态记录不 fail)
- **查询性能 (§5)**: 所有 (db, probe) 的 硬底线 `cv_1hour_24h / source_24h < 1` 任何时候告警 (view 比源表还慢 → 扫描路径退化); `plan_sig` 跨 cycle 不变 (plan drift 告警)
- **可选 chaos (§6)**: chaos 模式下额外验证 fault-recovery 路径, 退出码 +8 if unexpected PANIC / 段未恢复

其它如 RSS / disk_pct 等, 由 `system_metrics.csv` 记录,发生问题肉眼可见, **不作为主动验证项**。中间态信号 (mat 表 lag、L1/L2 大小、watermark 单调性、BGW counter) 不做常规采样 —— 只在 view_correctness MISMATCH 触发时通过现场 dump 捕获 (`view_mismatch_*.snapshot.txt`)。

---

## §1 前置条件 + 运行

soak 框架 **只负责 soak 逻辑**, 不管 CBDB 集群怎么部署。你要先有一个可用的 CBDB 集群 (无论是 docker / sandbox / bare-metal / Kubernetes 部署都行), 然后 soak 在它上面跑。

### 1.1 前置条件 (Prerequisites)

`bash soak.sh` 之前, 你的环境必须满足以下 8 条:

| # | 检查项 | 说明 |
|---|---|---|
| 1 | psql 能连上 coordinator | `psql -h $SOAK_HOST -p $SOAK_PORT -U $SOAK_USER -d postgres -c 'SELECT 1'` |
| 2 | `shared_preload_libraries` 含 `time_series` | `gpconfig -c shared_preload_libraries -v 'time_series' && gpstop -ar` |
| 3 | `time_series` 扩展可装 | time_series.so + .control 在 `$prefix/lib/postgresql/` 和 `$prefix/share/postgresql/extension/` |
| 4 | `gp_inject_fault` 扩展可装 | chaos 必需; `cd gpcontrib/gp_inject_fault && make USE_PGXS=1 install` |
| 5 | PAX access method 已注册 | 自动压缩必需; CBDB 用 `--enable-pax` 编译 |
| 6 | 用户有 CREATEDB 权限 | driver 要建/删 `soak_test_a`/`soak_test_b` |
| 7 | 集群所有 segment status='u' | `SELECT * FROM gp_segment_configuration` |
| 8 | TSBS binary 可执行 | 见下 |

### 1.2 一键验证

```bash
cd contrib/time_series/soak
SOAK_HOST=localhost SOAK_PORT=7000 SOAK_USER=gpadmin \
SOAK_TSBS_BIN=/path/to/tsbs/bin \
  bash tools/verify_env.sh
```

8 项 check 全过 (✓✓✓✓✓✓✓✓) 才能跑 soak。任何一项 ✗ 会告诉你怎么修。

### 1.3 准备 TSBS binary

soak 用 [TSBS](https://github.com/timescale/tsbs) 灌种子数据和持续写入 workload。需要为 **跑 soak 的机器的 CPU/OS** build TSBS:

```bash
git clone https://github.com/timescale/tsbs                  # 或拉公司 fork
cd tsbs
make tsbs_generate_data tsbs_load_timescaledb                # ~1 min
export SOAK_TSBS_BIN=$(pwd)/bin
```

**注意**: TSBS binary 必须跟 **跑 soak.sh 的 OS/arch 一致**。Mac 上 build 的 binary 不能在 Linux 容器内跑 (Mach-O vs ELF)。

### 1.4 跑 soak

```bash
cd contrib/time_series/soak

bash soak.sh                       # 默认 recommended preset: 24h, scale=500, cold-start
bash soak.sh --preset=smoke        # 5 min 框架自检 (~1 GB)
bash soak.sh --duration=4h         # recommended 但只跑 4h
bash soak.sh --warm-start          # 改成 warm-start (预灌 1d 历史)
bash soak.sh --chaos=1             # 开 chaos(二值开关:0=off, 1=on)

# 详细可调参数见 §3.1 (CLI flags) + conf/soak_params.sh (86 个 backstop 默认值)
```

结果落在 `soak/SOAK-<timestamp>/` —— 始终紧挨 `soak.sh` 脚本本身(`SOAK_RESULTS_BASE` 默认取脚本所在目录,无论你从哪 `cd` 运行)。用 `SOAK_RESULTS_BASE=<path>` 覆盖到指定位置。

### 1.5 看结果

```bash
ls -td SOAK-*/ | head -1                     # 找最新的 run
bash soak.sh --report $(ls -td SOAK-*/ | head -1)  # 重新生成汇总报告
```

退出码:
- `0` = clean
- `2` = correctness MISMATCH (有 view-vs-source bug, 严重)
- `4` = perf regression (p95 > baseline × 2.0)
- `8` = chaos 后段未恢复 / inject-reset 数不匹配
- `6`/`10`/`12`/`14` = 组合

详见 §3.3。

---

## §2 容量规划 (preset 选择)

只有两档 preset:

| Preset | scale | 默认时长 | 峰值磁盘 | compress_after / schedule | 适用场景 |
|---|---|---|---|---|---|
| `smoke`       | 100 | 5 min | ~1 GB           | 2 min / 1 min   | **框架自检** — 改完 `soak/*` 代码 5 min 验证框架没坏 |
| `recommended` | 500 | 24 h  | ≤ 100 GB(预算内) | 30 min / 10 min | **产品 baseline** — 找产品 bug、汇报数据 |

别名: `recommended` = `default` / `prod` / `bp`。

- **cold-start(默认)**: 空 cpu 表起步,workload 从 T=0 灌;`--warm-start` 改为预灌 1d 历史(preset 的 `SOAK_SEED_DAYS`)。
- **compress_after / schedule**: chunk 超过 `compress_after` 没再被写,BGW 每 `schedule` 把它压成 PAX。
- **磁盘**: peak ≈ scale × duration × 500 B/row × #DBs + mat 表 + WAL(`compress_after` 越短越小)。recommended 定 scale=500,是因为 4000 在 8h 就冲到 ~89 GB(71%),24h 会撞 `SOAK_DISK_KILL_PCT=90` panic 门。

**怎么选**: 动了 `soak/*` 代码 → `smoke`;跑产品 / 出数据 → `recommended`。想更短更小就在 recommended 上缩,例如 `bash soak.sh --duration=4h --scale=200`。

**override 优先级(高→低)**: `CLI flag > 用户 env (SOAK_X=…) > preset 默认 > conf/soak_params.sh 兜底`。例:`SOAK_COMPRESS_AFTER='5 min' bash soak.sh --duration=2h --chaos=1`。完整 flag 见 §1.4 / §3.1。

---

## §3 配置参数详解

### 3.1 CLI 参数 (soak.sh)

```
bash soak.sh [选项]

  --preset=NAME       smoke | recommended (默认: recommended)
                      recommended 的 aliases: default / prod / bp
                      见 §2 容量规划
  --duration=10m      覆盖 preset 默认 duration
                      支持 s / m / h / d, 如 30m / 24h / 7d
  --scale=N           同时覆盖 SOAK_SEED_SCALE + SOAK_WORKLOAD_SCALE
                      (最常用的 override, 不用 export 两个变量)
  --cold-start        空表起步 (默认, workload 从 T=0 灌)
  --warm-start        预灌 preset 的 SOAK_SEED_DAYS 天历史数据
  --chaos=N           SOAK_CHAOS_LEVEL 的 shortcut, N ∈ {0,1}(二值开关),见 §6
  --port=7000         coordinator 端口
  --db=NAME           单 DB
  --dbs="a b c"       多 DB (默认 "soak_test_a soak_test_b")
  --skip-setup        跳过 DROP/CREATE DB + seed, 在已有环境上 resume
  -h, --help          帮助
```

### 3.2 环境变量

profile 自动设置的 (可被显式 env 覆盖):

| Env | 说明 |
|-----|------|
| `SOAK_SEED_DAYS`              | seed 历史天数 |
| `SOAK_SEED_SCALE`             | seed TSBS 主机数 |
| `SOAK_WORKLOAD_SCALE`         | workload TSBS 主机数 = 写速率 (rows/s/DB) |
| `SOAK_VIEW_CORRECTNESS_EVERY` | view-vs-source auto-fail check 周期, 秒 |
| `SOAK_PERF_PROBE_EVERY`       | 性能 probe 周期, 秒 |
| `SOAK_COMPRESS_AFTER`         | 自动压缩 policy 的 compress_after (interval 字符串, 如 '6 hours') |
| `SOAK_COMPRESS_SCHEDULE`      | 压缩 policy 的 schedule_interval (interval 字符串, 如 '1 hour') |

非 profile 控制的 (默认值适合多数场景):

| Env | 默认 | 说明 |
|-----|------|------|
| `SOAK_HOST`                 | localhost | coordinator hostname |
| `SOAK_PORT`                 | 7000      | coordinator port |
| `SOAK_USER`                 | gpadmin   | DB user |
| `SOAK_DBS`                  | "soak_test_a soak_test_b" | 多 DB 列表 (空格分隔) |
| `SOAK_RESULTS_BASE`         | soak.sh 所在目录 | 结果目录的 base;每次 run 创建 `<base>/SOAK-<timestamp>/` |
| `SOAK_DISK_KILL_PCT`        | 90        | disk% 超此值时安全熔断 SIGINT 杀 driver, 0=关闭 |
| `SOAK_DATA_DIR`             | /home/gpadmin/gpdata | df 监控目标 |
| `SOAK_METRICS_EVERY`        | 60        | system_metrics.sh 采样周期 |
| `SOAK_CHAOS_LEVEL`          | 1         | 二值开关 0/1,见 §6 |
| `SOAK_TSBS_BIN`             | /home/gpadmin/timedb/tsbs/bin | TSBS binary 路径 (Docker 内是 /opt/tsbs/bin) |

### 3.3 退出码 + verdicts 目录(判决与完成度解耦)

**退出码只表达"完成度",不表达"是否达标"**:

```
0 = run 正常结束 (24h 自然到期 或 panic.* 提前终止)
1 = setup 失败 (cluster 挂掉 / extension 装不上 / seed 失败)
3 = 用户中断 (SIGINT / SIGTERM)
```

**判决只有两级**:signals/panic.\*(环境挂了立即退)+ verdicts/fail.\*(SLO 破了但 run 继续),没有中间的 warn 层。

**"是否达标"完全在 `$RESULTS/verdicts/` 目录里看**。每次触发 append 一行(时间戳 + 具体上下文),文件按类别拆分:

```
verdicts/fail.perf.refresh                   # refresh p95 > 2× schedule
verdicts/fail.perf.compress                  # compress p95 > 2× schedule
verdicts/fail.perf.bgw_overdue               # policy overdue 占比过高
verdicts/fail.perf.watermark                 # watermark 持续滞后
verdicts/fail.perf.rss_leak                  # RSS 增长过大
# (计划漂移仅诊断输出到 report.txt + plan_snapshots/,不写 flag)
verdicts/fail.correctness.view_mismatch      # CAGG mat vs live 数据对不上
verdicts/fail.correctness.view_incomplete    # 视图连续 incomplete 过多
verdicts/fail.correctness.invalidation_stuck # L1 长期非空
```

**判断 run 是否正常的两步**:

```
ls $RESULTS/signals/panic.*   非空 → 环境挂了(提前退)
ls $RESULTS/verdicts/fail.*   非空 → SLO 破了(run 可能仍跑完 24h)
两个都空                        → 完美 run
```

事后审计 3 步走:
1. `ls $RESULTS/verdicts/` — 看有几类 SLO 破了
2. `wc -l $RESULTS/verdicts/<file>` — 看某类触发多少次
3. `head/tail $RESULTS/verdicts/<file>` — 看具体上下文

`signals/panic.*` 触发 driver 的**健康闸**: wait 循环每分钟轮询,出现即
提前终止 — 环境已失效的 soak 继续空跑没有意义 (2026-06-09 事故:
库被 isolation2 删掉后空跑了 16 小时,此闸即由此而来)。

**Loop 心跳保护**:oneshot 循环的心跳文件老化超过
`SOAK_LOOP_PERMANENTLY_DEAD_MULT × interval`(默认 10 倍,即 60s loop
就是 10 min)→ 直接升级到 `signals/panic.loop_permanently_dead.<name>`
提前退出。短期抖动只在 soak_log 里留一行,不写 flag。

`report.txt` 里也会汇总 signals/ 和 verdicts/ 内容。

### 3.4 输出物 (soak_results/YYYYMMDD_HHMMSS_<dur>/)

```
├── report.txt                  # tools/report.sh 组装 (可对任意历史目录重跑)
├── manifest.env                # ★ 本轮生效的全部 SOAK_* 参数 + git SHA (三层合并后冻结)
├── signals/                    # ★ 运行时 IPC: chaos_active + panic.* (被 soak.sh 主循环消费)
├── verdicts/                   # ★ 结束时 SLO 审计: 只有 fail.*(见 report.txt)
├── .pids/                      # supervisor 记录的各循环进程组 id
├── view_correctness.csv        # ★ 每 CAGG 窗口一行: ts, db, cagg, window, total_rows, mismatch_count, verdict
├── view_incomplete.csv / view_defer.csv
├── view_correctness.err        # psql 错误 (期望空)
├── perf_probe.csv              # ts, db, probe, exec_ms, plan_ms, plan_sig
├── refresh_durations.csv       # ★ BGW refresh 耗时 (被动 scrape)
├── compress_durations.csv      # ★ BGW compress 耗时 + chunks/bytes
├── watermark_lag.csv           # ★ 每 CAGG 物化进度
├── late_arrival.csv / ddl_churn.csv
├── system_metrics.csv          # ts, disk_pct, rss_total, bgw_workers, ...
├── <loop>.log / <loop>.err     # 每循环的 stdout / stderr
├── view_mismatch_*.snapshot    # ★ MISMATCH 时立即 dump L1/L2/watermark
└── chaos_log.csv               # ★ chaos 模式: 每次 inject/reset
```

### 3.5 架构

```
═══════════════════════════════════ CAGG SOAK 框架架构 ═══════════════════════════════════

  入口                                    参数层 (唯一权威)
 ┌─────────────────────────┐    ┌──────────────────────────────────────┐
 │ bash soak.sh            │    │ conf/soak_params.sh                  │
 │   --preset=smoke|       │    │  共用默认值 (86 个), 平铺纯数据      │
 │   recommended           │    │  + "刻意不参数化"豁免清单            │
 │   [--scale=N]           │    └──────────────┬───────────────────────┘
 │   [--duration=X]        │                   │  source (set-if-unset)
 │   [--cold/--warm-start] │                   ▼
 │   [--chaos=N]           │      优先级: CLI flag > 用户 env > preset 默认 > conf 兜底
 └────────────┬────────────┘
              │
              ▼
 ┌─────────────────────────── soak.sh（纯生命周期）─────────────────────────────┐
 │  1.SETUP          2.MANIFEST      3.RUN            4.WAIT            5.STOP→报告    │
 │  setup/01..03  →  manifest.env →  supervisor   →   健康闸:        →  stop_all      │
 │  + 记录           (冻结全部        start_loops      轮询 panic.*      (按进程组      │
 │    DB OID         参数+gitSHA)       │             + 循环心跳         一发全清)     │
 └──────────────────────────────────────┼─────────────────────────────────────────────┘
                                        │ 扫描 SOAK-LOOP 头自动发现
                                        │ 每循环 = setsid 独立进程组
                                        │ + 启动错峰 + 每周期 .beat 心跳
        ┌────────────┬────────────┬────────────┬───────────┬────────────┐
        ▼            ▼            ▼            ▼           ▼            ▼
 ┌───────────┐┌──────────────┐┌────────────┐┌──────────┐┌──────────┐┌───────────┐
 │ workload/ ││ monitor/     ││ chaos/     ││ judge/   ││ tools/   ││ (扩展点)  │
 │ (写入者)  ││ (采集为主)   ││ (唯一      ││ 业务判定 ││ report   ││ 放带      │
 │           ││              ││  破坏者)   ││ (默认开) ││ verify_  ││ SOAK-LOOP │
 │ tsbs 流式 ││ view_check ◄─┼┤            ││          ││  env     ││ 头的脚本  │
 │  ×2DB     ││ perf_probe   ││ fault_     ││ judged   ││ selftest ││ 即自动接入│
 │ late_arriv││ refresh_scr. ││  catalog   ││ (流式+   │└──────────┘└───────────┘
 │ ddl_churn ││ compress_scr.││  (9故障)   ││  report) │
 │           ││ watermark_lag││    ↓       ││ verdicts/│  读 CSV → fail.*
 │           ││ invalidation ││ chaos_loop ││ 8 verdict┼─ / chaos.*
 │           ││ system_metr. ││  加权随机  ││  全迁入) │
 │           ││ bgw_sched_hlt││  →注入→   ││          │
 └─────┬─────┘└──────┬───────┘│   armed   │└──────────┘
       │ INSERT/DDL  │        │  →重置→   │
       ▼             ▼ SELECT │   静默    │
       │             │        └────┬──────┘
       │             │             ▼ gp_inject_fault
 ┌─────────────────────────────────────────────────────────────┐
 │              CloudberryDB 集群（被测对象）                   │
 │  cpu hypertable ×2DB → 触发器→L1 → BGW refresh → 4×CAGG     │
 │  → watermark → 压缩 policy → PAX chunks                     │
 └─────────────────────────────────────────────────────────────┘
        │ 所有循环只通过文件通信（无进程间变量传递）
        ▼
 ┌────────────────── $RESULTS/（每轮一个目录 = 唯一共享状态）──────────────────────────┐
 │  manifest.env       *.csv (每循环一个, append-only)   signals/ ◄─ 运行时 IPC        │
 │  (本轮参数快照)     view_correctness / watermark_lag /  ├ chaos_active (judge 读)   │
 │  .pids/ + .beat     perf_probe / refresh / compress /   └ panic.* → 立即提前终止    │
 │  (进程组+心跳)      late_arrival / ddl_churn / chaos_log                            │
 │  .db_oid_* (守卫)   view_mismatch_*.snapshot (出事现场) verdicts/ ◄─ SLO 审计       │
 │                                                         └ fail.*  (SLO 破了)       │
 └──────────────────────────────┬──────────────────────────────────────────────────────┘
                                │ 跑完后（或随时对任意历史目录）
                                ▼
 ┌──────────────── tools/report.sh（无状态胶水, ~70 行）────────────────────────────────┐
 │  按 order= 遍历所有 SOAK-LOOP 脚本, 逐个调 `script --report $RESULTS`               │
 │  → report.txt（每个脚本写自己的报告段落）+ flags 总判定                              │
 └───────────────────────────────────────────────────────────────────────────────────┘

 设计原则:
  ① 一个功能 = 一个文件（采集+报告同文件, 加监控 = 放一个文件, 零既有代码修改）
  ② 参数只有一个家（soak_params.sh）; 入口只调容量; manifest.env 记录所有偏离
  ③ monitor 只读 / chaos 独占破坏权 / 进程间只传文件
  ④ 检查不假设后台"何时"完成, 只在同一快照内观察"是否"完成（可判定区域）
  ⑤ panic 即提前终止, 不空跑; 框架也监控自己（循环心跳 → signals/panic.loop_permanently_dead.*）
```

每个循环脚本通过头部一行自描述:

```bash
# SOAK-LOOP: scope=per-db interval=300 interval_var=SOAK_FOO_EVERY order=30
```

并且必须支持 `script.sh --report <results_dir>` 输出自己的报告段落。
**新增监控/负载 = 新增一个文件** — supervisor 自动发现、报告自动
收录, 不需要改任何既有代码。临时禁用某循环:
`SOAK_DISABLE_LOOPS="name1 name2"`; 单循环调频用头部声明的
`interval_var` (0 = 关闭)。

#### 3.5.1 循环分类 (mode × scope)

`SOAK-LOOP:` 头里有两个字段决定 loop 类型:

- **`mode=`**(默认 `oneshot`,可选 `self`)—— 谁负责 while 循环
- **`scope=`**(`per-db` / `global`)—— 每个 db 起一份还是全集群一份

两轴正交组合出 4 种,当前 14 个 loop 全部落在这 4 格里:

|                | `mode=oneshot`(supervisor 循环)| `mode=self`(worker 自循环)|
|---|---|---|
| **`scope=per-db`** | 9 monitor + 2 workload(`bgw_scheduler_health` / `watermark_lag` / `invalidation_log` / `refresh_perf_scrape` / `compress_perf_scrape` / `perf_probe` / `view_correctness_scrape` / `ddl_churn` / `late_arrival`) | `workload/query_load` `workload/workload_via_tsbs` |
| **`scope=global`** | `monitor/system_metrics` | `chaos/chaos_loop` `judge/judged` |

**两种 mode 的差别**:

| | `oneshot`(默认) | `self` |
|---|---|---|
| worker 生命周期 | 每 tick fork,跑完就死 | 24h 只 fork 一次 |
| 谁写 while 循环 | supervisor 生成 wrapper | worker 自己 |
| heartbeat 文件 | ✅ 必须(mtime 判活) | ❌ 不需要(`kill -0 pid` 直接判活) |
| 能保内存状态吗 | 不能 | 能(psql 连接、内部计数器) |
| 适用 | 定时采集 | 高频压力 / 长跑 / 有内部状态 |

**选型规则(三条)**:
1. **worker 需要跨 tick 保状态**(如复用 psql 连接、内部计数器)→ `mode=self`
2. **调用频率 < 5s**(避免每秒 fork)→ `mode=self`
3. **对象与 db 绑定**(CAGG / watermark / bgw_job)→ `scope=per-db`;
   **对象是集群或框架层面**(CPU / chaos / 判决)→ `scope=global`

`ps` 里区分:oneshot 的 wrapper 形如 `bash -c 'touch $4; while ...' _ <interval> <worker.sh> ...`;
self 的 worker 就直接是 `bash <worker.sh> ...`,没有外层 wrapper。

### 3.6 Judge 模块(判定层)

**目录**: `judge/` — 独立的业务判定引擎,和 monitor(采集)分开。

```
judge/
├── judged.sh                     daemon:流式判定循环 + --report
└── verdicts/                     verdict 规则,一条一个文件(8 个,全部已迁移)
    ├── view_correctness.sh       MISMATCH 聚合 + coverage + incomplete
    ├── refresh_perf.sh           refresh p95 vs schedule
    ├── compress_perf.sh          compress p95 vs schedule
    ├── perf_probe.sh             view latency + 加速比 + plan drift
    ├── watermark_lag.sh          watermark 落后 vs 容忍度
    ├── invalidation_log.sh       L1/L2 bloat + drain
    ├── system_metrics.sh         disk peak 软告警 + RSS 泄漏趋势
    └── bgw_scheduler_health.sh   BGW dispatch delay
```

**三层分工**:

| 层 | 职责 |
|---|---|
| **monitor/** | **纯采集** —— 采样 DB 状态,append CSV。已迁移维度的 `--report` 是 no-op(不再判定)。例外:`system_metrics` 保留 `panic.disk_full` 现场早停(框架存活检查,不是业务判定)。 |
| **judge/** | **全部业务判定** —— 读 monitor CSV(+ `chaos_log.csv`),应用 `verdicts/*.sh`,触发 `fail.*` / `chaos.*`。两种判定模式:**流式**(daemon 每 `SOAK_JUDGE_INTERVAL` 秒重新评估,阈值越界当场抓,不用等到收尾)+ **report**(`--report` 打印 verdict 汇总)。 |
| **report.sh** | **不变的 glue** —— 本来就调每个 SOAK-LOOP 脚本的 `--report`;judge 贡献 verdict 段,(已 no-op 的)monitor 不再贡献段。 |

**Verdict 契约** —— 每个 `judge/verdicts/<name>.sh` 定义:
```
judge_run_<name>    <results_dir> <state_dir>   流式 tick(judged 循环每周期调)
judge_report_<name> <results_dir>               打印 report 段(judged --report 调)
```
判定逻辑抽成一个共享 `_judge_eval_<name>` 函数,两个入口都调它:流式那次 touch flag(早发现),report 那次 touch flag + 打印(收尾保证)。flag touch 幂等,重复调安全。

`SOAK_JUDGE_ENABLED=1`(默认开)。**8 个业务 verdict 全部在 judge 层**,每个 monitor 的 `--report` 都是 no-op —— monitor 是**纯采集器**。report.txt 里所有判定段来自 `── judge (business verdicts) ──`,monitor 不出判定段。

**两处刻意留在采集器(实时,不能事后判)**:
- `monitor/system_metrics.sh` 采集时的 `panic.disk_full` 早停(必须秒级触发防爆盘)
- `monitor/view_correctness_scrape.sh` 采集时的 MISMATCH 现场 snapshot + `fail.view_mismatch`(现场状态几秒后被覆盖,必须当场抓)+ `panic.env_lost` / `panic.view_check_dead`

这些是**框架存活 / 现场取证**,不是可事后重算的业务判定,所以留在采集侧。judge 的 `view_correctness` verdict 只做 MISMATCH 计数聚合 + coverage WARN + incomplete 升级。

---

## §4 正确性验证逻辑

正确性验证只有 **一个 auto-fail 信号**: `monitor/view_correctness.sql` —— 端到端验证 `view = source` 不变量。如果它失败, driver 立刻 dump 现场状态用于事后诊断。

| 脚本 | 验证什么 | 频率 | Auto-fail? | 角色 |
|---|---|---|---|---|
| `monitor/view_correctness.sql` | **view = source** 端到端等价 | 每 30 min (profile 默认) | ✅ 任何 MISMATCH → exit 2 | **唯一 auto-fail 信号** |
| `view_mismatch_*.snapshot.txt` | MISMATCH 触发时 dump 现场 L1/L2/watermark | on-trigger | — | 诊断用,事后看 |

### 4.1 view_correctness — 唯一 auto-fail 信号

#### 4.1.1 核心原理

CAGG view 定义在 catalog 上是这样的展开:

```
cv_1hour  ≡  (SELECT * FROM mat_1hour WHERE bucket <  watermark)
             UNION ALL
             (SELECT * FROM direct_view_1hour WHERE bucket >= watermark)
                                                  ↑
                                       live 分支: 实时从 cpu 聚合
```

任何时刻、任何 refresh 进度下, view 返回的行集都应该 **等于直接对源表做完整聚合**。Refresh 慢的话只是 live 分支扛得多一点、mat 分支扛得少一点; 总和不变。

→ `view = source` 是 **结构性不变量**, 等价的违反必是真 bug:
- view 的 UNION ALL filter 写错 (`<` 写成 `<=`)
- live 分支聚合用了跟 mat 不同的算法
- watermark 边界 off-by-one
- mat 物化值算错

#### 4.1.2 实现 —— 单 transaction + REPEATABLE READ

每次调用 `view_correctness.sql`, driver 在 **一个 REPEATABLE READ transaction 内** 跑完所有 4 个 CAGG 的对比。

```sql
SET optimizer = off;
SET timezone = 'UTC';
SET statement_timeout = '3min';

BEGIN ISOLATION LEVEL REPEATABLE READ;

-- 4 个对比, 共享一个 MVCC 快照
SELECT ... FROM <view query> ...;
SELECT ... FROM <source aggregation> ...;
-- ↑ 这两个查询看到的 cpu 行集 100% 相同

COMMIT;
```

#### 4.1.3 对比方式 —— per-(bucket, tags_id) FULL OUTER JOIN

对每个 CAGG, 不止比聚合总和, **逐 (bucket, tags_id) 对比所有 mat 列值**:

```sql
WITH src AS (
  SELECT time_bucket(W, time) AS bucket, tags_id,
         count(*) AS cnt, avg(usage_user) AS avg_user, max(usage_system) AS max_system
    FROM cpu
   WHERE time_bucket(W, time) >= w_start AND time_bucket(W, time) < w_end
   GROUP BY 1, 2
),
v AS (
  SELECT bucket, tags_id, cnt, avg_user, max_system
    FROM cv_1min
   WHERE bucket >= w_start AND bucket < w_end
),
diff AS (
  SELECT count(*) AS total_rows,
         count(*) FILTER (
           WHERE src.bucket IS NULL OR v.bucket IS NULL    -- 任一边缺行
              OR src.cnt    != v.cnt                       -- 计数不一致
              OR abs(src.avg_user - v.avg_user) > 1e-4     -- 浮点容差
              OR src.max_system != v.max_system            -- 整型精确
         ) AS mismatch_count
    FROM src FULL OUTER JOIN v
      ON src.bucket = v.bucket AND src.tags_id = v.tags_id
)
SELECT now(), current_database(), 'cv_1min', '30min',
       total_rows, mismatch_count,
       CASE WHEN mismatch_count = 0 THEN 'match' ELSE 'MISMATCH' END
  FROM diff;
```

FULL OUTER JOIN 保证两边任何 "缺行/多行/值不一致" 都被 mismatch_count 抓到。**MISMATCH=0 当且仅当 view 跟 source 在该窗口逐行一致**。

#### 4.1.4 driver 升级逻辑

```
# 采集器 monitor/view_correctness_scrape.sh(纯采集 + 延迟复查):
if verdict == 'MISMATCH':
    record 差异桶 → view_mismatch_buckets.csv (ts,db,cagg,window,bucket)
    capture 现场快照 (watermark + L1/L2 + 差异行)
# 每个差异桶在首见已过 SOAK_VIEW_RECHECK_SETTLE_SEC 秒、且尚无终态时,单独复查该桶:
for bucket 首见已过 settle 且未终态:
    对"那一个桶"重跑同样的 decidable + 对比
    → view_mismatch_recheck.csv (ts,db,cagg,window,bucket,outcome)
       outcome = still_mismatch(可判定且仍发散)/ resolved(可判定且已一致)/ pending(未到 settle 或不可判定)

# judge/verdicts/view_correctness.sh(读复查结果判定):
CONFIRMED = 有 ≥1 条 still_mismatch 的桶  → 真发散 → fail.view_mismatch(exit 2)
RESOLVED  = 复查已一致、从未 still_mismatch   → 瞬态,不 fail
PENDING   = 见过但还没终态(settle 未到 / 暂不可判定)→ 本轮不判
```

**confirm-before-flag = targeted delayed re-check**(判定在 judge,采集器负责采集 + 延迟复查):decidable-region 检查(排除有 pending L1/L2 的桶)滤掉大部分最终一致噪声,但迟到写的行可能比它的 L1/L2 失效记录**早几秒可见**(watermark 边界竞态,2026-07-09 冷启动 1/336 命中),所以**单次** MISMATCH 仍可能是瞬态。因此采集器只**记下差异桶 + 抓现场**,并在 **`SOAK_VIEW_RECHECK_SETTLE_SEC`(默认 180s)之后单独复查那一个桶**:仍发散且可判定 → 真发散 → `fail.view_mismatch`;settle 内已自愈 → 瞬态,不 fail。
> 为什么不用"跨周期复现":mat 检查窗口随 watermark 滑动,粗采样间隔下,一个持续发散的桶可能只在**一个**周期被观测到(窗口随后滑过它)——"跨 ≥N 周期复现"就永远不触发。按桶的延迟复查与窗口是否再次覆盖**解耦**:复查的是**那一个确切的桶**,不管窗口现在滑到哪;迟到写的瞬态会在 settle 内自愈 → 不 fail。

### 4.2 触发现场 dump

`view_correctness` MISMATCH 触发时, driver 立即 dump:
- `time_series.cagg_invalidation_log` (L1) 全量
- `time_series.cagg_materialization_log` (L2) 全量
- `time_series.cagg_watermark` (per-segment) 全量

落到 `view_mismatch_<db>_<cagg>_<HHMMSS>.snapshot.txt`, 事后 100% 复现 "那一刻 CAGG 视图为什么返回了错值"。这是**唯一**的 L1/L2/watermark 采样路径(不做常规周期性 snapshot)。

### 4.3 覆盖矩阵

| Bug 类型 | view_correctness 抓到? | 备注 |
|---|---|---|
| 触发器漏写 / refresh 丢 bucket | ✅ | view 的 live 分支会重算 → 暴露差异 |
| refresh 残留旧行 | ✅ | |
| `partial_view` 表达式拼错 (列写错值跨列) | ✅ | |
| refresh SPI 写错列顺序 | ✅ | |
| 段间 motion 串错列 | ✅ | |
| BGW worker 物化用错 timezone (bucket 边界漂移) | ✅ | view + source 都强制 UTC |
| view 的 UNION ALL filter 写错 (`<` vs `<=`) | ✅ | |
| live 分支聚合算法跟 mat 不同 | ✅ | |
| watermark 边界 off-by-one 导致 view 漏行/重行 | ✅ | |
| watermark 错位 + mat 错值的复合 bug | ✅ | 两个 bug 在 view 端仍叠加可见 |
| 源表 UPDATE/DELETE 的 invalidation 路径 | ❌ | TSBS append-only 不触发; regression 兜底 |
| `time_bucket()` 函数本身有 bug | ❌ | 两边都调用 = 套套逻辑; regression 单测兜底 |
| 对称错误抵消 (+X / -X 行级 cancel) | ❌ | 极罕见, 不工程化兜底 |
| NaN / Inf 值污染 | ❌ | TSBS 不产生; 留作已知限制 |

> **结论**: `view_correctness` 是 **v1 GA 的唯一正确性 auto-fail 信号**。覆盖了所有 view 构造层面 + mat 物化层面的 bug。中间态 (mat lag、L1/L2 大小、watermark 单调性) 只在 MISMATCH 触发时通过 dump 捕获,不做常规采样。

---

## §5 性能验收 (view query latency)

### 5.1 设计动机

`view_correctness`(§4)只验数据等价(view = source),**不看响应时间** —— view 的 live 分支可能退化成全扫源表做 GROUP BY(用户查 `cv_1hour` 要 8–25 秒),此时 `view_correctness` 和 `bgw_job_stat` 全绿却**完全看不到**。§5 站在用户视角周期性查 view、记录响应时间分布,专抓这类扫描路径退化。

### 5.2 采样点

每 `SOAK_PERF_PROBE_EVERY` 秒 (default 300s) 跑一次, **5 条查询** 每条独立计时:

| probe label | 查询 | 模拟场景 |
|---|---|---|
| `cv_1min_1h`           | `count(*) FROM cv_1min  WHERE bucket >= now()-'1h'`           | BI 点查 (窗口最小) |
| `cv_1hour_24h`         | `count(*) FROM cv_1hour WHERE bucket >= now()-'24h'`          | 运维报表 (scan path) |
| `source_24h`           | `cpu` 上现场 `time_bucket + GROUP BY`, 24h 窗口                | 对照: 源表实时聚合 |
| `cv_1hour_24h_fetch`   | `SELECT bucket, 6 cols FROM cv_1hour ORDER BY ... LIMIT 1000` | **真实 BI dashboard 抓数据** |

前 4 条都是 `count(*)` —— 主要用来抓 **扫描路径退化** (view 退化成扫源表)。第 5 条 `cv_1hour_24h_fetch` 贴近用户真实体感: BI 控件抓最近 24h 数据画 1000 个点。

`source_24h` 作为 **控制组**: 同样 24h 窗口对源表做等价聚合。`cv_1hour_24h` 与它的比值就是 CAGG 的实测加速倍数; 当倍数 **< 1** 时, view 退化到扫源表, 比直接查还慢。

### 5.2.1 阈值规则

| 指标 | 阈值规则 | verdict |
|---|---|---|
| `cv_1hour_24h` vs `source_24h` 比值 | < 1.0 | 🔴 view 比源表还慢, **总告警** |
| `cv_1hour_24h` vs `source_24h` 比值 | (1, 2.0) | ⚠ CAGG 加速比偏低 |
| `plan_sig` (md5 of normalized plan) | 跨 cycle 全程一致 | ✓ |
| `plan_sig` 中途变化且 last-Q exec ≥ 1.5× first-Q | 自相对慢化 | ⚠ PLAN DRIFT + EXCESS SLOWDOWN |

**不维护绝对基线**: 性能数字随 host / preset / scale 变, 一份共享 CSV 是误导大于帮助。Run-to-run 性能对比就手工 diff 两个 results 目录的 `perf_probe.csv`。

### 5.3 实现: EXPLAIN ANALYZE + plan signature

由 `monitor/perf_probe.sh` 执行, 用 `EXPLAIN (ANALYZE, TIMING ON, COSTS OFF, BUFFERS, FORMAT TEXT) <query>` 从 PG 返回的 `Execution Time:` 字段拿**纯服务端执行时间**(不含 psql 启动 / 网络 / fetch),并顺手拿到完整 plan 算 `plan_sig` 检测计划退化。

**CSV 格式**:

```csv
ts,db,probe,exec_ms,plan_ms,plan_sig
2026-05-20 16:05:00,soak_test_a,cv_1min_1h,2.34,0.41,a3f9b2c107e5
2026-05-20 16:05:00,soak_test_a,cv_1hour_24h,12.7,0.52,b1c8e2f4d901
```

- `exec_ms`: server-side execution time
- `plan_ms`: planning time
- `plan_sig`: md5 prefix of normalized EXPLAIN output (cost/rows/timing 都 strip 掉, 只留 shape)

**第一次出现每个 (db, probe) 时, driver 把完整 plan 文本 dump 到 `plan_snapshots/${db}_${probe}_init.txt`**, 作为 baseline plan。后续 cycle 如果 plan_sig 变了, 就能直接 diff baseline plan vs 现在的 plan 看到底改了什么。

### 5.4 边界: 本节不覆盖什么

| 未覆盖 | 为什么不在本节 / 替代方案 |
|---|---|
| **写入路径延迟** (INSERT 慢、chunk 切换抖动) | §5 只测读路径。INSERT 持续慢会在 `workload_*.log` 的 TSBS loader stderr 自然暴露 |
| **瞬时尖刺** (BGW refresh 持续几秒抖动 view) | 5 min 采样间隔抓不到几秒级 spike。实时瞬态请翻同时段的 `system_metrics.csv` (BGW RSS / disk_pct) |
| **冷启动延迟** (mat 表 cold cache) | 长稳 7 天里 cache 早就 warm 完。冷启动测试是 capacity / boot benchmark 的范畴 |
| **并发 view 查询的伸缩性** (10 个用户同时查) | 这是 concurrency benchmark, 不是稳定性。要测请用 pgbench |

### 5.5 关闭采样

```bash
SOAK_PERF_PROBE_EVERY=0 bash soak.sh ...
```

关闭后无法实施 §5 中的延迟/加速比判定 (perf_probe.csv 不存在)。同时 §5.6 / §5.7 的 refresh / compress scrape 也一起停。

### 5.6 Refresh 性能 (BGW 写路径, passive scrape)

§5.1-§5.5 测的都是 **读路径** (用户主动查 view)。但 BGW worker 跑的 `refresh_continuous_aggregate` 才是真正最耗时的工作:

| 操作 | 典型耗时 | 频率 (large profile, 7d) |
|---|---|---|
| view count(*) | 1-50 ms | 5 min × N (2016 次) |
| **refresh cv_1min**  | 50-500 ms | 1 min × 3 CAGGs × 2 DBs (10080 次) |
| **refresh cv_5min**  | 0.1-2 s   | 5 min  (2016 次) |
| **refresh cv_1hour** | 0.2-5 s   | 1 hour (168 次) |

#### 5.6.1 数据来源 — passive scrape

不主动 probe (会增加 cluster 负载), 而是周期性 **scrape** `time_series.job_history`:

```sql
SELECT id, job_id, execution_start, duration, succeeded, is_crashed, error_data
  FROM time_series.job_history
 WHERE proc_name = 'policy_refresh' AND id > $cursor
 ORDER BY id;
```

每次 scrape 用 `.refresh_cursor_$DB` 文件记录已读到的 max(id), 避免重复。Scrape 本身 < 10 ms。

#### 5.6.2 采样间隔 — 5 min

跟 view probe 同步。理由:
- scrape 极便宜 (单次 SELECT 几行, < 10 KB), 5 min 间隔不会 spam
- 5 min 能 catch 1 min schedule 的 cv_1min refresh (5 个 sample/cycle, 足够算 p95)
- 由于是 incremental scrape, 不会漏 — 即使 scrape 间隔比 refresh 间隔长, bgw_job_stat_history 不会丢历史

#### 5.6.3 阈值规则

阈值 **相对 schedule_interval**, 不是绝对值。Key insight: 如果 refresh duration > schedule_interval, scheduler 必须 kill 上一个还在跑的 worker(scheduler-driven termination)。

| Metric | 规则 | Verdict |
|---|---|---|
| refresh p95 (per CAGG) | < schedule_interval | ✓ OK |
| refresh p95 (per CAGG) | [schedule_interval, 2 × schedule_interval] | ⚠ WARN (接近 kill 边界) |
| refresh p95 (per CAGG) | > 2 × schedule_interval | 🔴 **CRITICAL → exit +4** |

per-CAGG schedule_interval (来自 setup/03_caggs.sql):

| CAGG | schedule_interval | p95 WARN 阈值 | p95 CRITICAL 阈值 |
|---|---|---|---|
| cv_1min  | 60 s    | > 60 s    | > 120 s |
| cv_5min  | 300 s   | > 300 s   | > 600 s |
| cv_1hour | 3600 s  | > 3600 s  | > 7200 s |

#### 5.6.4 输出: refresh_durations.csv

```csv
ts,db,id,job_id,cagg_name,execution_start,duration_ms,succeeded,is_crashed,error
2026-05-25T16:05:00Z,soak_test_a,42,7,cv_1hour,2026-05-25T16:00:00Z,1234,true,false,
2026-05-25T16:05:00Z,soak_test_a,43,5,cv_1min, 2026-05-25T16:04:00Z,87,true,false,
```

#### 5.6.5 report.txt 示例

```
── refresh perf (from bgw_job_stat_history) ──
  soak_test_a/cv_1min   n=10080 p50=23.0   p95=120.0  p99=380.0  max=950.0  fail=0 crash=0 kills=12  ✓ OK
  soak_test_a/cv_5min   n=2016  p50=45.0   p95=210.0  p99=560.0  max=1200.0 fail=0 crash=0 kills=0   ✓ OK
  soak_test_a/cv_1hour  n=1008  p50=180.0  p95=580.0  p99=1400.0 max=3200.0 fail=0 crash=0 kills=0   ✓ OK
  ...
```

`kills` 列: refresh duration > schedule_interval 的次数 (scheduler-driven termination)。少量 kills (< 1% of n) 正常, 大量 kills 意味着 BGW 已经 saturated。

### 5.7 Compression 性能 (BGW 写路径)

跟 §5.6 同样 pattern, scrape `proc_name='policy_compression'` 的行。CSV 多两列 (chunks_compressed, bytes_reclaimed):

```csv
ts,db,id,job_id,table_name,execution_start,duration_ms,succeeded,is_crashed,chunks_compressed,bytes_reclaimed,error
2026-05-25T17:00:00Z,soak_test_a,50,12,cpu,...,5400,true,false,3,12500000,
```

#### 5.7.1 阈值

- schedule_interval 来自 preset 的 SOAK_COMPRESS_SCHEDULE (smoke: 1min, recommended: 10min) — 见 §2
- p95 < schedule → ✓ OK; p95 in (schedule, 2×): ⚠ WARN; p95 > 2× → 🔴 CRITICAL (+4)

#### 5.7.2 report.txt 示例

```
── compress perf (from bgw_job_stat_history) ──
  soak_test_a n=28 p50=4500.0 p95=18000.0 p99=42000.0 max=68000.0 fail=0 crash=0 chunks=245 reclaimed=14336.0MB  ✓ OK
  soak_test_b n=28 p50=4400.0 p95=17500.0 p99=41200.0 max=66000.0 fail=0 crash=0 chunks=240 reclaimed=14150.0MB  ✓ OK
```

`chunks` / `reclaimed` 列累计这次 soak 期间总共压缩了多少 chunk、回收了多少 disk。是 v1 GA 一个 **功能性回归测试**: 如果跑完 chunks=0 说明压缩 policy 根本没触发, 大概率是 bug。

---

## §6 Chaos 注入

### 6.1 什么是 chaos —— 只测生产真实故障

Chaos 只注入**生产真会遇到的、非人为的**故障(进程被 OS 杀、集群崩溃、资源耗尽),验证系统扛得住并能恢复。**不测**代码里 `gp_inject_fault` 精确插桩的 race —— 那是**确定性回归**(iso2/regress)的活。一个 fault 只走一条轨道:代码 fault point 是回归语言,不是 chaos 语言。

### 6.2 fault catalog(已实现)

`chaos/fault_catalog.csv`,4 条(列:`fault_name,level,target,mechanism,signal,active_s,recovery_budget_s,weight`):

| Fault | 机制 | 集群 | active_s/budget/weight |
|---|---|---|---|
| refresh_worker_kill | pkill -9 正在跑 refresh 的 BGW(mid-refresh crash 恢复)| 活 | 20 / 120 / 3 |
| compress_worker_kill | pkill -9 正在跑 compress 的 BGW(mid-compress crash)| 活 | 20 / 120 / 3 |
| scheduler_kill | pg_terminate BGW scheduler(launcher 重生)| 活 | 20 / 60 / 2 |
| cluster_crash | pkill -9 全部 postgres → gpstart(crash-consistency 招牌)| **挂→恢复** | 10 / 180 / 1 |

- 全部 fault 都是 `level=1` —— `level` 列保留供未来分级用,当前不参与过滤(见 §6.3)。
- `active_s`:worker kill 用 **poll-then-kill** 抓 in-flight worker 的最长等待(解决旧的"8% lottery"命中率);也参与排除窗口计算(§6.4)。
- `recovery_budget_s`:reset 后允许的恢复时间,超时会在 report.txt 里报告(不写 flag,chaos_log.csv 已有每次的 recovery_s 数据供分析);**同时定义 chaos 排除窗口宽度**。
- 原先混入 catalog 的多条 `gp_inject_fault` error/panic 插桩已移出、转 iso2 回归(backlog;`recompress_reader_double_count.sql` 已有)。

### 6.3 SOAK_CHAOS_LEVEL(二值开关:0 / 1)

| 值 | 选哪些 | 集群 | 用途 |
|---|---|---|---|
| 0(默认)| 无,chaos_loop 立即退出 | — | 普通长稳 / baseline |
| 1 | `fault_catalog.csv` 里全部 4 个 fault | worker kill 期间活;cluster_crash 期间短暂挂 | 常规 chaos + crash-consistency 一起测 |

频率由 `SOAK_CHAOS_AVG_INTERVAL`(默认 900s)独立控制。焦点 smoke 用 `SOAK_CHAOS_ONLY=<name>[,...]` 只跑指定 fault(如 `SOAK_CHAOS_ONLY=cluster_crash` 反复压恢复路径,或 `SOAK_CHAOS_ONLY=refresh_worker_kill,compress_worker_kill,scheduler_kill` 跳过 cluster_crash)。

### 6.4 注入 + 恢复循环

每轮:**加权 pick → inject(记 INJECT)→ recover → measure_recovery(记 RECOVERY_TIME)→ pause(凑 AVG_INTERVAL)**。

- **worker kill**:自愈,BGW 由 scheduler 重生,无需干预。
- **cluster crash → `recover_cluster`**:硬 kill 后裸 `gpstart` 必败,得做"重启会自动清而这里没有"的清理 —— `pkill -9 postgres`(清残留/半启动 coordinator)→ 删 `postmaster.pid` → 删 `/tmp/.s.PGSQL.*`(stale socket,否则 gpstart 报 "instance process running")→ `ipcrm` 掉本用户遗留 SysV shm/sem(否则报 "pre-existing shared memory block still in use")→ 删 `pgsql_tmp`(遗留 spill)→ `gpstart`,**用 live `psql SELECT 1` 判成功而非 gpstart 退出码**,重试 `SOAK_CHAOS_GPSTART_TRIES`(3)次。容器里硬 kill 留的僵尸不被 PID1 reap 属正常、不 block 恢复(它们不占端口/内存/IPC);长跑用 `docker run --init`。

### 6.5 chaos-aware 判定(judge 层)

判定全在 `judge/`(与 §3.6 解耦一致)。核心是**排除窗口**:故障窗口内的越阈 / incomplete 样本**不计 fail** —— 它们测的恰是被注入的扰动;只有窗口外的持续越阈才 `fail.*`。

- **排除窗口 = [INJECT, INJECT + recovery_budget]**。`chaos_loop` 把 RECOVERY_TIME 行的**时间戳直接盖成 `inject + max(active_s + grace, recovery_budget)`**(并非真等那么久),`judge/lib/chaos.sh::chaos_windows_str` 据此产出窗口(ISO8601 字符串比较,无需 epoch;未闭合的 INJECT 补一个 open 窗口,防流式判定误判)。`SOAK_CHAOS_WINDOW_GRACE`(5s)兜 kill 后的瞬态。
- **按 fault × 判官细粒度过滤**(2026-07-21 重构):`chaos_windows_str` 第二参数是**逗号分隔的 fault_name 白名单**,判官声明"我只在意能真的干扰我这个指标的 fault"。没被列进白名单的 fault 窗口对该判官**不可见**,避免不相关的 chaos(比如 `compress_worker_kill` 期间的 `refresh_perf`)过度保护 → 掩盖真 bug。当前矩阵:

  | 判官 | 关心的 fault |
  |---|---|
  | `refresh_perf` | refresh_worker_kill, scheduler_kill, cluster_crash |
  | `compress_perf` | compress_worker_kill, scheduler_kill, cluster_crash |
  | `bgw_scheduler_health` | scheduler_kill, cluster_crash |
  | `watermark_lag` | refresh_worker_kill, scheduler_kill, cluster_crash |
  | `invalidation_log` | refresh_worker_kill, scheduler_kill, cluster_crash |
  | `view_correctness` / `framework_health` (view_check_dead) | cluster_crash |
  | `rss_leak` / `perf_probe` | (不排除,和 chaos 关联弱)|

  `chaos_recovery.sh`:recovery_s 超 budget 或 timeout 会在 report 中打印(不写 flag)——慢但成功的恢复不算 SLO 违反;真正失败(gpstart 非零)由上游 chaos_loop 写 `signals/panic.chaos`。
- **MISMATCH 任何时候都严格 = 0**(不按 chaos 窗口豁免):decidable-region 检查 + targeted delayed re-check(§4)滤掉最终一致瞬态,复查后仍发散到 fail 的就是真发散,天然安全,不需额外 chaos-aware。

Flag 命名空间两级:`signals/panic.*`(早停,framework)/ `verdicts/fail.*`(业务 / SLO)。无中间 warn 层。

### 6.6 验证状态

chaos=1 / chaos=2 均验证通过:worker kill 秒级自愈、集群崩溃 `gpstart` 恢复(5–9s)、崩溃期 incomplete 被排除窗口过滤、无误早停、0 MISMATCH、clean run。

### 6.7 明确不做

`gp_inject_fault` error/panic(归 iso2)· 网络分区(需 netem)· 时钟漂移(难 reliably 注入)· segment crash + gprecoverseg(mirror 恢复难,backlog)· 磁盘满(风险高)· 手工损坏数据文件(属 DR)。
