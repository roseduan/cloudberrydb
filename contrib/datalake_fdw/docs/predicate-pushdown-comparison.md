# Predicate Pushdown: PAX vs datalake_fdw Iceberg Comparison

Date: 2026-04-01

## 1. Feature Comparison

| Feature | PAX | FDW Iceberg | Gap |
|---------|:---:|:----------:|:---:|
| **Qual extraction** | OK: direct `List *quals` | BLOCKED: `plan->qual` is ExprState on segments | **Must fix** |
| **OpExpr (Var OP Const)** | OK | Partial: INT32/INT64/FLOAT/DOUBLE/DATE only | Missing NUMERIC/TEXT/TIMESTAMP |
| **BoolExpr AND** | OK: recursive | OK: flattenQuals recursive | -- |
| **BoolExpr OR** | OK | No | Missing |
| **ScalarArrayOpExpr (IN)** | OK | Partial: Java agent path only, not in Parquet reader | Missing |
| **NullTest (IS NULL)** | OK | No | Missing |
| **BETWEEN** | OK: split into two OpExpr | Partial: depends on executor AND decomposition | Depends on qual form |
| **NUMERIC/DECIMAL** | OK: min/max supported | No: parseFilterQuals skips | Missing |
| **TEXT/VARCHAR** | OK | No: parseFilterQuals skips | Missing |
| **TIMESTAMP** | OK | No: parseFilterQuals skips | Missing |
| **BOOL** | OK | No | Missing |
| **Iceberg metadata pruning** | N/A | OK: Java agent does partition/file pruning | -- |
| **File/Partition skip** | OK: micro-partition stats | OK: Iceberg manifest pruning | -- |
| **Row Group skip (min/max)** | OK: enabled by default | BLOCKED: logic implemented but quals don't reach reader | **Blocked** |
| **Page level skip** | No | No | Both missing |
| **Bloom Filter** | OK: equality queries | No | Missing |
| **Row level pushdown** | OK: disabled by default | No | Missing |
| **Column projection** | OK | OK | -- |
| **MPP qual passing** | OK: local execution, quals distributed with plan | No: `plan->qual` is ExprState not Expr | **Core blocker** |

## 2. Filter Level Comparison

| Filter Level | Granularity | PAX | FDW Iceberg | Notes |
|-------------|-------------|:---:|:----------:|-------|
| **Partition/File pruning** | Skip entire files | OK | OK | FDW via Java agent Iceberg manifest pruning |
| **Row Group / Stripe skip** | Skip data blocks (~128MB) | OK | BLOCKED: logic exists, quals can't reach | FDW core blocker: segments can't get original Expr |
| **Page level skip** | Skip pages within row group (~1MB) | No | No | Both missing, requires reader refactor |
| **Bloom Filter** | Fast exclude for equality queries | OK | No | PAX has it, FDW not implemented |
| **Row level pushdown** | Skip column decoding for non-matching rows | OK (off by default) | No | PAX has it but off by default, FDW not implemented |
| **Column projection** | Only read needed columns | OK | OK | Both implemented |
| **Executor Filter** | Row-by-row WHERE evaluation | OK (fallback) | OK (fallback) | Last defense, both have it |

## 3. Filter Pipeline

```
Coarse-grained                                                    Fine-grained
(large data skip)                                              (per-row check)

+----------------+  +----------------+  +---------+  +----------+  +----------+  +--------------+
|  Partition     |  |  Row Group     |  |  Page   |  |  Bloom   |  |  Row     |  |  Executor    |
|  /File Prune   |->|  /Stripe Skip  |->|  Skip   |->|  Filter  |->|  Filter  |->|  Filter      |
+----------------+  +----------------+  +---------+  +----------+  +----------+  +--------------+
  PAX: OK            PAX: OK            Both: No     PAX: OK      PAX: OK        Both: OK
  FDW: OK            FDW: BLOCKED                    FDW: No      FDW: No
```

FDW currently only has two ends (Partition pruning + Executor fallback).
The 4 middle layers are all missing. Top priority is unblocking **Row Group skip**
(blocker: qual serialization from GetForeignPlan to segment workers).

## Root Cause of FDW Blocker

Cloudberry MPP dispatches plans to segment workers. During dispatch, `plan->qual`
gets compiled from `Expr` trees into `ExprState` nodes (tag 160 = T_GatherState).
The ParquetReader's `parseFilterQuals()` expects `OpExpr`/`Var`/`Const` nodes but
receives unrecognizable ExprState nodes, so 0 predicates are accepted.

**Solution**: In `GetForeignPlan()`, serialize the original `scan_clauses` (which
are still `Expr` trees at plan time) into `fdw_private` as a custom lightweight
format (e.g., list of `{attrno, opno, const_value, const_type}`). Then in
`BeginForeignScan()`, deserialize from `fdw_private` instead of reading `plan->qual`.
This bypasses the ExprState compilation issue entirely.
