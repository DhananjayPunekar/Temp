# Silver → ADV (Gold) Framework: Developer Documentation

Covers every module in `Framework/silver-adv-framework/`. Written from the code as it stands, so where a docstring and the code disagree, this document follows the code and says so.

---

## Contents

1. [Module map](#1-module-map)
2. [Wiring it up (startup)](#2-wiring-it-up-startup)
3. [End-to-end pipeline flow](#3-end-to-end-pipeline-flow)
4. [Control-table configuration](#4-control-table-configuration)
5. [Rules: how transforms are called](#5-rules-how-transforms-are-called)
6. [Derive functions: full reference](#6-derive-functions-full-reference)
7. [Source connectors](#7-source-connectors)
8. [DeltaWriter: how data is written](#8-deltawriter-how-data-is-written)
9. [DQ runner and run log](#9-dq-runner-and-run-log)
10. [Logging and debug switches](#10-logging-and-debug-switches)
11. [Gotchas and observations](#11-gotchas-and-observations)

---

## 1. Module map

All modules live in the Python package `framework` (imports are `from framework.<module> import ...`).

| File | What it contains | Key public names |
|---|---|---|
| `silver_pipeline_executor.py` | The single executor for both Silver and Gold layers: loads sources, runs pre-rules, joins, rules, column mapping, SCD2 in-batch logic, then writes. | `PipelineExecutor`, `GoldPipelineExecutor`, `clear_source_cache()` |
| `gold_pipeline_executor.py` | Compatibility shim; re-exports `GoldPipelineExecutor` from the merged executor. No logic. | `GoldPipelineExecutor` |
| `rule_engine.py` | Base rule engine: applies an ordered list of named rules to a DataFrame. Built-in generic rules. | `RuleEngine` |
| `dv_rule_engine.py` | Data Vault rules injected into `RuleEngine` at startup. | `DVRuleEngine`, `DVRuleEngine.install()` |
| `dv_derive_rule.py` | The **`derive`** rule plus the library of 32 derive functions it can call. | `DERIVATIONS`, `apply_derivation()`, `install_derive_rule()`, `set_gold_fq()` |
| `dv_bootstrap.py` | One call that installs all Gold rules (DV rules + `derive`). | `install_gold_rules()` |
| `dv_derivations.py` | **Legacy.** An older 6-function derive library. Not imported by any other module; `dv_derive_rule.py` supersedes it. | — |
| `source_connector.py` | Connector abstraction + Delta / Parquet / Majesco / Gold connectors and the registry. | `SourceConnector`, `DeltaTableConnector`, `ParquetVolumeConnector`, `MajescoConnector`, `GoldConnector`, `ConnectorRegistry` |
| `silver_connector.py` | Connector that reads Silver tables for the Gold build. | `SilverConnector` |
| `delta_writer.py` | Full / first load, SCD1 MERGE, SCD2 expire + insert; partition handling; concurrency retry; metrics. | `DeltaWriter` |
| `dq_runner.py` | Post-load DQ gate (row count, NULL PK, duplicate PK). | `run_dq()` |
| `run_log.py` | Appends one row per pipeline execution to `<control_fq>.run_log`. | `RunLogger` |

External dependencies referenced but **not** in this folder:

- `config.env_loader._parse_env_file`: used by `dv_derive_rule._auto_resolve_gold_fq`.
- A `settings` object with `bronze_fq`, `silver_fq`, `gold_fq`, `control_fq`, `environment`.
- The registry/loader that reads the `ctl_*` tables and builds the `cfg` dict passed to `execute()` (see §4).

---

## 2. Wiring it up (startup)

Typical Gold notebook bootstrap (names from the module headers):

```python
from framework.source_connector import ConnectorRegistry
from framework.silver_connector import SilverConnector
from framework.dv_bootstrap import install_gold_rules
from framework.silver_pipeline_executor import PipelineExecutor   # or GoldPipelineExecutor

# 1. Connectors
ConnectorRegistry.setup_defaults(S.silver_fq)          # Delta, Parquet, Majesco, Gold
ConnectorRegistry.register(SilverConnector(S.silver_fq))   # needed for source_type='silver'

# 2. Rules: DV rules + the `derive` rule are injected into RuleEngine
install_gold_rules()

# 3. Run a pipeline
executor = PipelineExecutor(spark, S)
result = executor.execute(cfg, S, context, load_type="incremental", layer="gold")
# or: GoldPipelineExecutor(spark, S).execute(cfg, S, context, load_type)
```

Notes:

- `setup_defaults()` **clears** the registry first, so call it before any `register()`.
- `source_type='silver'` only works after `SilverConnector` is registered. Without it, `ConnectorRegistry.get('silver')` raises `No connector registered`.
- `install_gold_rules()` returns the list of rule methods injected. A rule that already exists on `RuleEngine` is **not** overwritten (see `filter_expr`, §5.2).
- `set_gold_fq("catalog.schema")` is optional. `derive_recursive_lookup` otherwise resolves the Gold schema itself from the env file the first time it needs to.

---

## 3. End-to-end pipeline flow

`PipelineExecutor.execute(cfg, settings, context, load_type, layer="silver")` runs these steps in order. `layer="gold"` writes to `settings.gold_fq.<target_table>`; otherwise `settings.silver_fq.<target_table>`.

| # | Stage | What happens |
|---|---|---|
| 1 | **Resolve load type** | `load_type` (or `cfg.load_type_default`, else `incremental`). `full` is applied **once per target table per run**: the first pipeline for a table in a run gets FULL, later pipelines for the same table are switched to INCREMENTAL. The run key is `context.full_load_run_key` → `batch_id` → `main_batch_id` → `job_id` → `__default__`. `context.full_load_tables` (CSV or list) limits FULL to those tables. |
| 2 | **Schema mode** | For an existing Gold table the physical table schema is used; otherwise the `ctl_table_schema` projection is used (`use_ctl_schema`). |
| 3 | **Load each source** (`cfg.sources`) | Connector chosen by `source_type`. Bare names are qualified: `silver` → `silver_fq`, `gold` → `gold_fq`, anything else → `bronze_fq`. The connector applies product / PQB / watermark / debug filters (§7). Sources are cached per `(table, pre-rule signature)` for the run. |
| 4 | **Soft-delete filter** | If the source has a `DATE_DELETED` column (case-insensitive), rows with `DATE_DELETED IS NOT NULL` are dropped. |
| 5 | **Pre-rules** (`pre_rules_json`) | Rules run on this source only, **before** joins. Used to derive join keys (e.g. `QOT_ID`) or constant join columns (e.g. `REC_SRC_NM`). |
| 6 | **Combine sources** | `joins` present → join pipeline starting from `base_source_alias`, joins in `join_seq` order. One source → used as is. Several sources, no joins → `unionByName(allowMissingColumns=True)`. |
| 7 | **Drop inherited `LD_DT`** | Any `LD_DT` from a joined/unioned source is dropped. |
| 8 | **Rules** (`cfg.rules`, from `ctl_rule`) | Run in order. The executor adds `batch_id` and `run_id` to every rule's params. |
| 9 | **Column mapping** (`ctl_column_map`) | Projects to the target columns (§4.5). String outputs are `TRIM(REGEXP_REPLACE(x,'[\r\n\t]',''))`. |
| 10 | **Stamp `LD_DT`** | `LD_DT = current_timestamp()` always. |
| 11 | **Intra-batch `SRC_EXPRN_DT`** | If `scd2_lag_order_cols` is set and both its first column and `SRC_EXPRN_DT` exist: `SRC_EXPRN_DT = LEAD(<first lag col>) OVER (PARTITION BY <first PK> ORDER BY <lag cols> ASC)`. |
| 12 | **LNK SCD2 block** | Only for `LNK_*` targets with `scd_type='scd2'`: dedup on the full PK, rank within `partition_by` (entity grain) by lag cols DESC, set `LD_END_DT = LD_DT` on all but the latest. |
| 13 | **Align to physical schema** | If the target exists: select/cast to its schema; missing columns become NULL; strings cleaned of CR/LF/TAB and trimmed. |
| 14 | **NULL-PK filter** | Rows with any NULL primary-key column are dropped (they could never MERGE correctly). |
| 15 | **In-batch dedup** | SCD2 with `scd2_rank_order_cols`: drop consecutive rows whose business columns are unchanged, recompute `SRC_EXPRN_DT`, set `LD_END_DT = now − 1s` on non-latest rows. Otherwise: keep one row per PK, latest `watermark_col` first (non-deterministic when the watermark column isn't in the DataFrame). |
| 16 | **Write** | `DeltaWriter.write(...)` (§8). |
| 17 | **Run log** | One row to `<control_fq>.run_log` with status SUCCESS / FAILED. |

"Business columns" in step 15 = all columns except the PK and the technical set `REF_ID, SRC_EFF_DT, SRC_EXPRN_DT, LD_DT, LD_END_DT, BTCH_ID, BTCH_DT, ERR_CD, ERR_FLG, REC_SRC_NM, PART_COL`.

---

## 4. Control-table configuration

The executor receives a `cfg` dict. The loader that builds it from the `ctl_*` tables is not in this folder; the mapping below comes from the seed files and from the keys the executor reads.

### 4.1 `ctl_pipeline` → top-level `cfg` keys

| Column | Used by | Meaning |
|---|---|---|
| `pipeline_id` | executor, logs | Unique id, e.g. `AV__GOLD__SAT_QOT_DED`. |
| `product_code` | run log | Product of the pipeline (`AV`, `COMMON`, …). |
| `domain`, `pipeline_name`, `mapping_version`, `updated_ts` | logs / metadata | Descriptive. |
| `target_table` | executor, writer | Bare table name; qualified by layer. |
| `base_source_alias` | join step | Alias of the left-most source for join pipelines. |
| `primary_keys` | NULL-PK filter, dedup, MERGE key | CSV, e.g. `QBE_HASH_QOT_ID`. **Required.** |
| `product_filter_col` | connectors | Source column holding the product code; filtered to `context.products`. |
| `is_common` | connectors | Passed to connectors (only the Parquet connector honours it; see §11). |
| `partition_by` | writer, LNK SCD2 | Logical partition columns (CSV), e.g. `REC_SRC_NM,PART_COL`. For `LNK_*` tables these are also the MERGE key and the entity grain. |
| `physical_partition_by` | writer (Gold) | Physical partition layout; default `REC_SRC_NM,PART_COL`. Read directly from `ctl_pipeline` by `DeltaWriter`. |
| `load_type_default` | step 1 | Used when no `load_type` is passed. |
| `watermark_col` | connectors, dedup, run log | Column(s) for incremental filtering (CSV → `COALESCE`). |
| `watermark_type` | connectors | Kept for signature compatibility. |
| `scd_type` | writer | `scd1` or `scd2`. |
| `scd2_lag_order_cols` | steps 11, 12, 15 | Ascending order for `LEAD`/`LAG`, e.g. `SRC_EFF_DT,BTCH_DT,REF_ID`. First column feeds `SRC_EXPRN_DT`. |
| `scd2_rank_order_cols` | step 15, SCD2 MERGE | Rank order with direction, e.g. `SRC_EFF_DT DESC,BTCH_DT DESC,REF_ID DESC`. |
| `pqb_flag_col` | connectors | Column with the P/Q/B flag; filtered to `context.pqb_flags`. |
| `layer`, `is_active`, `pii_columns`, `decimal_validation_columns` | loader / metadata | Not read by the executor directly (`layer`/`is_active` are read by `DeltaWriter` for `physical_partition_by`). |

### 4.2 `ctl_source` → `cfg["sources"]`

| Column | `cfg` key | Meaning |
|---|---|---|
| `seq` | — | Load order. |
| `alias` | `alias` | Name used in joins and in `ctl_column_map.source_expr` (`QD.DED_AMT`). |
| `source_ref` | `table` | Bare or fully-qualified table name, or a `/Volumes/...` path for Parquet. |
| `source_type` | `source_type` | `silver`, `gold`, `delta`, `majesco`, `parquet` (default `delta`). |
| `pre_rules_json` | `pre_rules_json` or `rules` | JSON list of rules run on this source before joins (§5.4). |

### 4.3 `ctl_join` → `cfg["joins"]`

| Column | Meaning |
|---|---|
| `join_seq` | Join order. All rows with the same `join_seq` form one join. |
| `right_alias` | Source alias joined on the right. |
| `join_type` | `inner`, `left`, … (any Spark join type). |
| `left_col`, `right_col` | Join columns (case-insensitive). The left side is the accumulated DataFrame. |
| `pair_seq` | Multi-column joins: the executor accepts `on_pairs=[{"left":..,"right":..}, ...]`, which the loader builds from rows sharing a `join_seq`. Conditions are ANDed. |

After each join the executor keeps **all left columns** and **only right-side columns whose name does not already exist on the left** (case-insensitive). See §11 for what that means for columns such as `REC_SRC_NM`.

### 4.4 `ctl_rule` → `cfg["rules"]`

| Column | Meaning |
|---|---|
| `seq` | Order of execution. |
| `rule_name` | Rule to run (`record_source`, `derive`, `filter_not_null`, …). Case-insensitive. |
| `params_json` | Parameters for that rule (§5). |

Internally each rule is `{"rule": <rule_name>, "params": <params_json>}`.

### 4.5 `ctl_column_map` → `cfg["column_map"]`

| Column | Used? | Meaning |
|---|---|---|
| `target_column` | yes | Target column name. |
| `source_expr` | yes | Any Spark SQL expression. `ALIAS.COL` is rewritten to the bare column after joins. A bare column name that doesn't exist becomes `NULL`. Literal strings need escaped quotes: `'\'0\''`. |
| `seq`, `role` | metadata | — |
| `col_type`, `transform_fn`, `transform_args` | **not read** | The executor does not call derive functions from the column map. Use `ctl_rule` or `pre_rules_json` instead (§5.3). |

Projection behaviour:

- With `ctl_table_schema` (new tables): every column in the schema is output, cast to its type. Mapped columns use `source_expr`; unmapped columns take the same-named DataFrame column, or `NULL`.
- For existing Gold tables (physical schema mode): only the mapped columns are added/overwritten; alignment to the table happens in step 13.

### 4.6 `ctl_table_schema`

`columns_json` for `table_name`: `{"columns":[{"name":"...","type":"..."}, ...]}` (keys case-insensitive; names upper-cased).

### 4.7 Runtime `context` keys

| Key | Meaning |
|---|---|
| `products` | Product code(s) for the product filter (compared against `UPPER(col)`, so pass upper case). |
| `load_type` | Set by the executor from step 1. |
| `start_date`, `end_date` | Optional inclusive date window for incremental reads. |
| `watermark_ts` | Last successful watermark; used when no date window is given. |
| `pqb_flags` | Allowed P/Q/B values, e.g. `['P','B']`. |
| `debug_filter_column`, `debug_filter_value` | Restrict every source to one value (debug only). |
| `num_of_days_for_cmn_tables` | Day window on `LD_DT` for `SAT_PREM` / `SAT_CMSN` sources. |
| `batch_id`, `run_id` | Logged, and injected into every `ctl_rule` params dict. |
| `full_load_tables`, `full_load_run_key`, `main_batch_id`, `job_id` | Control table-level FULL (step 1). |

---

## 5. Rules: how transforms are called

### 5.1 Base rules (`rule_engine.RuleEngine`)

| Rule | Params | Behaviour |
|---|---|---|
| `audit_columns` | `batch_id`, `run_id`, `source_ref` | Adds `_ingest_ts`, `_batch_id`, `_run_id`, `_source_ref`. |
| `hash_key` | `cols` + `target` (+ `separator`, default `_`) **or** `mappings: {TARGET: SOURCE_COL}` | MD5 (Python UDF, UTF-8). With `cols`: `concat_ws(sep, coalesce(col,''))`, and rows whose input is blank are **dropped**. With `mappings`: rows where the source is NULL or blank are **dropped**, then hashed. |
| `hashdiff` | `cols`, `target`, `separator` (default `\|`) | `md5(concat_ws(sep, coalesce(col,'')))`. |
| `trim_strings` | `cols` | `trim()` in place. |
| `upper_case` | `cols` | `upper()` in place. |
| `coalesce_nulls` | `mappings: {col: default}` | `coalesce(col, default)`. |
| `drop_columns` | `cols` | Drops columns. |
| `cast_decimal` | `cols`, `precision` (18), `scale` (2) | Cast to decimal. |
| `custom_sql` | `select_exprs` (list, required) | `df.selectExpr(*select_exprs)`, which replaces the column set. |
| `filter_expr` | `expr` | `df.filter(expr)`; no-op with a warning if `expr` is missing. |

### 5.2 Data Vault rules (`dv_rule_engine.DVRuleEngine`)

Injected by `DVRuleEngine.install()` / `install_gold_rules()`.

| Rule | Params | Behaviour |
|---|---|---|
| `record_source` | `target` (`REC_SRC_NM`), `value` (`'109'`), `source_col` (optional) | If `source_col` exists: keep only rows where it equals `value`. Then `target = value`. |
| `business_key` | `cols` (required), `target` (required), `separator` (`_`), `upper` (false) | `concat_ws(sep, coalesce(col,''))`. Missing `cols`/`target` → skipped with a warning. |
| `dv_audit` | `rec_src` (`'109'`), `ld_dt` (`LD_DT`), `ld_end_dt` (`LD_END_DT`), `err_flg` (`'0'`), `err_cd` (`'0'`) | Sets `LD_DT = now`, `LD_END_DT = NULL`; adds `REC_SRC_NM`, `ERR_FLG`, `ERR_CD` **only if missing**. |
| `hardcode` | `mappings: {col: literal}` | Sets each column to a literal. |
| `filter_not_null` | `cols` | For each existing column: drop rows where it is NULL **or** `''`. Missing columns are skipped with a warning. |
| `filter_expr` | `expr` | **Not injected**: the base `RuleEngine` already has `filter_expr`, so the base version is used (same behaviour). |

### 5.3 The `derive` rule

Installed by `install_derive_rule()` (via `install_gold_rules()`).

```json
{"fn": "<derive function name>", "target": "<output column>", "args": { ... }}
```

- `fn` is looked up case-insensitively in `DERIVATIONS` (§6). An unknown name raises `ValueError` listing the available functions.
- Missing `fn` or `target` → the rule is skipped with a warning (no error).
- `target` is added or overwritten. Some functions also write extra columns (`derive_cvrg_id` writes `hash_input_col`).
- Any exception is re-raised as `RuntimeError("[dv_derivations] Derivation '<fn>' failed for target '<target>': ...")`.

### 5.4 Where you can call a rule

**A. As a `ctl_rule` row** (runs after joins, on the combined DataFrame):

```sql
INSERT INTO ctl_rule (pipeline_id, seq, rule_name, params_json) VALUES
('AV__GOLD__SAT_QOT_DED', 0, 'record_source', '{"value":"109","target":"REC_SRC_NM"}'),
('AV__GOLD__SAT_QOT_DED', 1, 'derive',
 '{"fn":"derive_limit_type","target":"LMT_TP_CD","args":{"agg_col":"RPEC_C_CARRIER_AGGREGATE","occ_col":"RPEC_C_CARRIER_OCCURRENCE"}}');
```

**B. In `ctl_source.pre_rules_json`** (runs on one source before joins; the only way to derive a **join key**). A JSON list; each element is either form:

```json
[
  {"rule_name": "derive", "params_json": {"fn": "derive_qot_id", "target": "QOT_ID",
                                          "args": {"cols": ["MQP_ENTITY_REFERENCE", "MQP_DATE_MODIFIED"], "separator": "_"}}},
  {"rule": "derive", "params": {"fn": "default_value", "target": "REC_SRC_NM", "args": {"value": "109"}}}
]
```

`params_json` may itself be a JSON string. Elements without a rule name are ignored.

**C. Not from `ctl_column_map`.** `transform_fn` / `transform_args` are not read (§4.5).

---

## 6. Derive functions: full reference

Every function has the signature `fn(df, target, args) -> DataFrame` and is called through the `derive` rule:
`{"fn": "<name>", "target": "<COL>", "args": {...}}`.

### 6.0 Behaviour shared by most functions

- **Column lookup is case-insensitive, and a missing column silently becomes `NULL`** (`_col_or_null`). A typo in a column name does **not** raise an error; the key is simply built without that part. Exceptions: `derive_cntct_type`, `derive_case_value` / `derive_case_concat` conditions, and `derive_sql_expr` reference columns directly and do fail on a missing column.
- **"Blank-safe" concatenation** (used by the key builders): each part is `TRIM(CAST(col AS STRING))`, blank parts become NULL, then `concat_ws(separator, ...)`. NULL/blank parts are **skipped**, so you never get `a__b`, a leading `_` or a trailing `_`. If every part is NULL/blank the result is `''` (empty string), not NULL.
- **`normalize`**: where supported, `normalize: true` applies `LOWER(LTRIM(result))`.

### 6.1 Quick reference

| Function | Typical target | Required args | Optional args (default) |
|---|---|---|---|
| [`derive_concat`](#derive_concat) | any key | `cols` | `separator` (`""`), `normalize` (false) |
| [`derive_qot_id`](#derive_qot_id) | `QOT_ID` (HUB_QOT) | `cols` | `separator` (`""`) |
| [`derive_plcy_id`](#derive_plcy_id) | `PLCY_ID` (HUB_PLCY) | `cols` | `separator` (`""`) |
| [`derive_oppty_id`](#derive_oppty_id) | `OPPTY_ID` (HUB_OPPTY) | `cols` | `separator` (`""`) |
| [`derive_coins_plcy_id`](#derive_coins_plcy_id) | `COINS_PLCY_ID` | `cols` | `separator` (`""`) |
| [`derive_undrly_plcy_id_ndyc`](#derive_undrly_plcy_id_ndyc--derive_undrly_plcy_id_rpec) | `UNDRLY_PLCY_ID` (Excess) | `cols` | `separator` (`""`) |
| [`derive_undrly_plcy_id_rpec`](#derive_undrly_plcy_id_ndyc--derive_undrly_plcy_id_rpec) | `UNDRLY_PLCY_ID` (Quota share) | `cols` | `separator` (`""`) |
| [`derive_lob_id`](#derive_lob_id) | `LOB_ID` | `col` | `normalize` (false) |
| [`derive_cvrg_id`](#derive_cvrg_id) | `CVRG_ID` + hash input | `cols` | `separator` (`_`), `filter_col`, `product_code_col` (`PRODUCT_CODE`), `rec_src_nm_col` (`REC_SRC_NM`), `hash_input_col` (`CVRG_HASH_INPUT`) |
| [`derive_cvrg_id_input`](#derive_cvrg_id_input) | `CVRG_HASH_INPUT` | — | `cvrg_id_col` (`CVRG_ID`), `rec_src_nm_col`, `product_code_col`, `separator` (`_`) |
| [`derive_insd_obj_id_bo`](#derive_insd_obj_id_bo) | `INSD_OBJ_ID` | at least one of `standard_cols` / `excess_cols` / `ev_cols` / `ac_cols` | `separator` (`_`), `ycmc_cols`, `uivc_cols`, `ac_check_cols`, `airport_condition`, `priority`, `normalize` |
| [`derive_insd_obj_type_bo`](#derive_insd_obj_type_bo) | `INSD_OBJ_TP_CD` | — | `risk_type_col`, `excess_value`, `ev_value`, `airport_value`, `airport_condition` |
| [`derive_cntct_pnt_id`](#derive_cntct_pnt_id) | `CNTCT_PNT_ID` | `mad_cols` | `ycmc_cols`, `separator` (`_`) |
| [`derive_cntct_pnt_phys`](#derive_cntct_pnt_phys) | `CNTCT_PNT_ID` (physical) | `mad_cols` (or legacy `cols`) | `ycmc_cols`, `separator` (`_`), `normalize` |
| [`derive_cntct_type`](#derive_cntct_type) | `CNTCT_PNT_TP_CD` | — (fixed columns) | — |
| [`derive_party_id`](#derive_party_id) | `PRTY_ID` | `role_col` + the role's columns | see section, `normalize` |
| [`derive_party_type`](#derive_party_type) | `PRTY_TP_CD` | — | `role_col`, `business_name`, `company_name`, `producer_code`, `first`, `surname`, `underwriter_name`, `contact_name` |
| [`derive_party_role_type`](#derive_party_role_type) | `ROLE_TP_CD` | — | `org_type_col`, `pers_type_col` |
| [`derive_full_name`](#derive_full_name) | `FULL_NM` | — | `first`, `middle`, `surname`, `underwriter_name`, `contact_name` |
| [`derive_limit_type`](#derive_limit_type) | `LMT_TP_CD` | — | `agg_col`, `occ_col`, `layer_col`, `excess_col`, `qbe_col`, `per_layer_col` |
| [`derive_prem_tran_id`](#derive_prem_tran_id) | `PREM_TRAN_ID` | one of 3 modes | `separator` (`_`) |
| [`derive_recursive_lookup`](#derive_recursive_lookup) | any code | `lookup_table`, `lookup_filter_value`, `source_to_lookup` | `lookup_filter_col`, `payload_col`, `return_col`, `product_code_col`, `filter-Eff-Exp` |
| [`default_value`](#default_value) | any | `value` | — |
| [`derive_column_copy`](#derive_column_copy) | any | `col` | `normalize` |
| [`trim_col`](#trim_col) | any | `col` | — |
| [`concat_name`](#concat_name) | name | `cols` | `separator` (`" "`) |
| [`coalesce_cols`](#coalesce_cols) | any | `cols` | — |
| [`derive_null_if_empty`](#derive_null_if_empty) | any | — | `source_col` (= target) |
| [`derive_case_value`](#derive_case_value) | any | `condition` | `true_value`, `false_value` |
| [`derive_case_concat`](#derive_case_concat) | any | `condition` | `true_cols`, `false_cols`, `separator` (`_`) |
| [`derive_mapped_value`](#derive_mapped_value) | any | `source_col`, `mapping` | `default_mode` (`source`) |
| [`derive_sql_expr`](#derive_sql_expr) | any | `expr` | — |

---

### 6.2 Business-key builders

#### `derive_concat`

Generic key concatenation. All the `derive_*_id` wrappers below call this.

| Arg | Req. | Default | Meaning |
|---|---|---|---|
| `cols` | yes | `[]` | Ordered list of source columns. |
| `separator` | no | `""` | Separator. **Default is empty string.** Always pass `"_"` for keys. |
| `normalize` | no | `false` | `LOWER(LTRIM(result))`. Also lowercases the timestamp `T` → `t`. |

Behaviour:

- Each part is trimmed; blank → NULL; NULL parts are skipped (`concat_ws`).
- A column whose type is **`TimestampType`** is formatted as `yyyy-MM-dd'T'HH:mm:ss` (e.g. `2026-01-01T10:00:00`). `DateType` and string columns are only cast to string (a date gives `2026-01-01`; a timestamp stored as a string is not reformatted).
- Case is preserved unless `normalize` is true.

```json
{"fn":"derive_concat","target":"QOT_ID",
 "args":{"cols":["MQP_ENTITY_REFERENCE","MQP_DATE_MODIFIED"],"separator":"_","normalize":false}}
```
→ `Q-1001_2026-01-01T10:00:00`

#### `derive_qot_id`

HUB_QOT business key. `derive_concat` with `normalize` **forced to false** (the `T` stays upper case).

| Arg | Req. | Default | Meaning |
|---|---|---|---|
| `cols` | yes | — | Typically `["MQP_ENTITY_REFERENCE","MQP_DATE_MODIFIED"]`. |
| `separator` | no | `""` | Pass `"_"`. (The docstring says the default is `_`; the code default is `""`.) |

Any `normalize` you pass is ignored.

```json
{"rule_name":"derive","params_json":{"fn":"derive_qot_id","target":"QOT_ID",
  "args":{"cols":["MQP_ENTITY_REFERENCE","MQP_DATE_MODIFIED"],"separator":"_"}}}
```
→ `Q-1001_2026-01-01T10:00:00`

#### `derive_plcy_id`

HUB_PLCY business key. Same as `derive_qot_id` (normalize forced false).

| Arg | Req. | Default | Meaning |
|---|---|---|---|
| `cols` | yes | — | Typically `["MQP_DISPLAY_POLICY_NUMBER","MQP_EFFECTIVE_DATE"]`. |
| `separator` | no | `""` | Pass `"_"`. |

The docstring says "normalize always enabled", but the code sets `normalize=False`, so case is preserved.

```json
{"fn":"derive_plcy_id","target":"PLCY_ID",
 "args":{"cols":["MQP_DISPLAY_POLICY_NUMBER","MQP_EFFECTIVE_DATE"],"separator":"_"}}
```
→ `AV-POL-1_2026-01-01T00:00:00` (if `MQP_EFFECTIVE_DATE` is a timestamp) or `AV-POL-1_2026-01-01` (if it is a date).

#### `derive_oppty_id`

HUB_OPPTY business key. `derive_concat`, normalize forced false.

| Arg | Req. | Default | Meaning |
|---|---|---|---|
| `cols` | yes | — | Opportunity id plus a column that holds the literal suffix `1`. |
| `separator` | no | `""` | Pass `"_"`. |

`cols` must be **column names**: to append `_1`, first create a constant column with `default_value` (e.g. `OPPTY_SFX = '1'`). It does **not** lowercase the id; the reference SQL `LOWER(TRIM(MQP_C_OPPORTUNITY_ID))||'_1'` only matches when the id is already lower case.

```json
[{"rule_name":"derive","params_json":{"fn":"default_value","target":"OPPTY_SFX","args":{"value":"1"}}},
 {"rule_name":"derive","params_json":{"fn":"derive_oppty_id","target":"OPPTY_ID",
   "args":{"cols":["MQP_C_OPPORTUNITY_ID","OPPTY_SFX"],"separator":"_"}}}]
```
→ `OPP-9_1`

#### `derive_coins_plcy_id`

HUB_COINS_PLCY business key. `derive_concat`, normalize forced false.

| Arg | Req. | Default | Meaning |
|---|---|---|---|
| `cols` | yes | — | Typically `["RPEC_C_CARRIER","RPEC_C_QUOTA_POLICY_NUMBER","RPEC_ID"]`. |
| `separator` | no | `""` | Pass `"_"`. |

Blank parts are skipped and case is preserved. The reference SQL keeps empty parts (`COALESCE(x,'')`) and lowercases, so the results differ for blank carriers or upper-case values.

#### `derive_undrly_plcy_id_ndyc` / `derive_undrly_plcy_id_rpec`

HUB_UNDRLY_PLCY keys for Excess (NDYC) and Quota Share (RPEC). Both are `derive_concat` with normalize forced false.

| Arg | Req. | Default | Meaning |
|---|---|---|---|
| `cols` | yes | — | NDYC: `[EX_POLICY_NUMB, EFFECTIVE_DATE, CARRIER, NDYC_ID]`; RPEC: `[QUOTA_POLICY_NUMBER, EFFECTIVE_DATE, CARRIER, RPEC_ID]` (actual column names). |
| `separator` | no | `""` | Pass `"_"`. |

#### `derive_lob_id`

HUB_LOB key. Calls `derive_column_copy`.

| Arg | Req. | Default | Meaning |
|---|---|---|---|
| `col` | yes | — | Source column, e.g. `MQP_C_SUB_PRODUCT_CODE` / `LOB_CODE`. |
| `normalize` | no | `false` | `LOWER(LTRIM(x))`. Pass `true` to match a SQL spec that uses `LOWER(...)`. |

The value is copied **as is**, with no trim. The executor's string clean-up trims the final column on write.

#### `derive_cvrg_id`

HUB_CVRG key and hash input in one rule. Writes **two** columns: `target` and `hash_input_col`.

| Arg | Req. | Default | Meaning |
|---|---|---|---|
| `cols` | yes | `[]` | Ordered parts of the coverage key. |
| `separator` | no | `_` | Separator. |
| `filter_col` | no | — | If this column is NULL/blank, the key is set to NULL (pair with `filter_not_null`). |
| `product_code_col` | no | `PRODUCT_CODE` | Product column used for the BO rule. |
| `rec_src_nm_col` | no | `REC_SRC_NM` | Record-source column. |
| `hash_input_col` | no | `CVRG_HASH_INPUT` | Second output column. |

Behaviour:

1. `target = LOWER(LTRIM(concat_ws(sep, blank-safe parts)))`. Parts are trimmed and blank parts skipped. Always lower case.
2. NULL guard via `filter_col`.
3. `hash_input_col` = `target` if product is `BO`, else `target || sep || REC_SRC_NM` (e.g. `av_hull_h1_109`).

```json
{"fn":"derive_cvrg_id","target":"CVRG_ID","args":{
  "cols":["PRODUCT_CODE","LOB_CODE","C_COVERAGE_CODE","ASLOB","SUBLINE_CODE","DISP_NAME"],
  "separator":"_","filter_col":"C_COVERAGE_CODE","product_code_col":"PRODUCT_CODE"}}
```

Then hash with `hash_key` on `CVRG_HASH_INPUT`.

#### `derive_cvrg_id_input`

Hash input only (for when `CVRG_ID` already exists).

| Arg | Req. | Default | Meaning |
|---|---|---|---|
| `cvrg_id_col` | no | `CVRG_ID` | Coverage id column. |
| `rec_src_nm_col` | no | `REC_SRC_NM` | Record source column. |
| `product_code_col` | no | `PRODUCT_CODE` | Product column. |
| `separator` | no | `_` | Separator. |

Output: `CVRG_ID` for `BO`, otherwise `CVRG_ID_REC_SRC_NM`.

#### `derive_insd_obj_id_bo`

Insured-object key with routing between four paths: **standard** (UIVC), **excess** (YCMC), **EV** (location, MLO) and **AC** (airport, CCX).

| Arg | Req. | Default | Meaning |
|---|---|---|---|
| `standard_cols` | * | `[]` | Parts for the standard path. |
| `excess_cols` | * | `[]` | Parts for the excess path. |
| `ev_cols` | * | `[]` | Parts for the EV path; **also** used as the EV presence check. |
| `ac_cols` | * | `[]` | Parts for the airport path. |
| `separator` | no | `_` | Separator. |
| `ycmc_cols` | no | `YCMC_*` columns in `excess_cols` | Presence check for the excess path. |
| `uivc_cols` | no | `UIVC_*` columns in `standard_cols` | Presence check for the standard path. |
| `ac_check_cols` | no | `CCX_*` columns in `ac_cols` | Presence check for the airport path. |
| `airport_condition` | no | — | SQL boolean; when given, it replaces `ac_check_cols`. |
| `priority` | no | `["excess","ac","ev","standard"]` | Which path wins if several have data. |
| `normalize` | no | `false` | `LOWER(LTRIM(x))`, then restores the ISO `T` (`(\d)t(\d)` → `$1T$2`). |

\* At least one path must be configured.

Behaviour:

- Parts are blank-safe. **`TimestampType` and `DateType`** parts are formatted `yyyy-MM-dd'T'HH:mm:ss` (a date becomes `...T00:00:00`).
- The ID is only built when at least one of the YCMC / UIVC / EV / AC checks has data; otherwise NULL.

#### `derive_cntct_pnt_id`

Contact-point key with a YCMC/MAD switch.

| Arg | Req. | Default | Meaning |
|---|---|---|---|
| `mad_cols` | yes | `[]` | Address parts (MAD). |
| `ycmc_cols` | no | `[]` | Alternative parts (YCMC). |
| `separator` | no | `_` | Separator. |

Uses `ycmc_cols` when the **hard-coded** column `YCMC_C_STATE_CODE` is not NULL, otherwise `mad_cols`. Blank-safe. No `normalize`.

#### `derive_cntct_pnt_phys`

Physical-address key (preferred over `derive_cntct_pnt_id`).

| Arg | Req. | Default | Meaning |
|---|---|---|---|
| `mad_cols` | yes | `[]` | Address parts. Legacy alias: `cols`. |
| `ycmc_cols` | no | `[]` | Alternative parts. |
| `separator` | no | `_` | Separator. |
| `normalize` | no | `false` | `LOWER(LTRIM(x))`. |

Uses `ycmc_cols` only when configured **and** at least one of them is non-blank; otherwise `mad_cols`. Blank parts are **skipped** (`1 main st_boston_...`), whereas SQL built with `COALESCE(TRIM(x),'')` keeps them (`1 main st__boston_...`).

```json
{"fn":"derive_cntct_pnt_phys","target":"CNTCT_PNT_ID","args":{
  "mad_cols":["MAD_LINE_1","MAD_LINE_2","MAD_CITY","MAD_COUNTY","MAD_STATE_CODE","MAD_COUNTRY","MAD_ZIP_CODE"],
  "separator":"_","normalize":true}}
```

#### `derive_party_id`

HUB_PRTY business key, chosen by role.

| Arg | Meaning |
|---|---|
| `role_col` | Column holding the role (compared as `LOWER(TRIM(x))`). |
| `business_name` | Insured organisation name. |
| `first_name`, `middle_name`, `surname` | Insured person name parts. |
| `company_name` | Insurer. |
| `underwriter_name`, `underwriter_code` | Underwriter. |
| `broker_contact_name` | Broker. |
| `producer_code` | Producer (selling agency). |
| `licensed_contact_name` | Licensed individual. |
| `parentagency_name`, `parentagency_number` | Parent agency. |
| `normalize` | `LOWER(LTRIM(x))` (default false). |

| Role value | Result |
|---|---|
| `insured` | `business_name` if non-blank, else `first_middle_surname` (blank-safe, `_`) |
| `insurer` | `company_name` |
| `underwriter` | `underwriter_name_underwriter_code` |
| `broker` | `broker_contact_name` |
| `producer` | `producer_code_broker_contact_name` if `producer_code` is present, else NULL |
| `parentagency` | `parentagency_name_parentagency_number` |
| `licensed individual` / `licensed_individual` / `licensedindividual` | `licensed_contact_name` |
| anything else | NULL |

A blank result becomes NULL. Note: `business_name`, `company_name`, `broker_contact_name` and `licensed_contact_name` are not trimmed by the function.

---

### 6.3 Type / classification functions

#### `derive_party_type`

Returns `Organization` / `Person` / NULL. Args (all column names, all optional): `role_col`, `business_name`, `company_name`, `producer_code`, `first`, `surname`, `underwriter_name`, `contact_name`.

Order: role `insurer`/`producer` → Organization; `insured` → Organization if `business_name` present, else Person; `underwriter`/`broker`/licensed variants → Person; then fallbacks: business/company/producer present → Organization; first or surname → Person; underwriter → Person; contact → Person; else NULL. The `parentagency` role is not listed, so it falls through to the fallbacks.

#### `derive_party_role_type`

| Arg | Default | Meaning |
|---|---|---|
| `org_type_col` | `ORG_TYPE` (only if neither arg is given) | ORG role column. |
| `pers_type_col` | `PERS_TYPE` (only if neither arg is given) | PERS role column. |

If you pass only one of the two, the other side is ignored.

Mapping (case-insensitive, trimmed):

| Source column | Value | Result |
|---|---|---|
| ORG | `insurer` | `Insurer` |
| ORG | `insured` | `Insured` |
| ORG | `producer` | `Selling_Agency` |
| ORG | `parentagency` | `ParentAgency` |
| PERS | `insured` | `Insured` |
| PERS | `underwriter` | `Underwriter` |
| PERS | `broker` | `Broker` |
| PERS | `licensed_individual` | `Licensed_Individual` |

Anything else → NULL.

#### `derive_insd_obj_type_bo`

| Arg | Default |
|---|---|
| `risk_type_col` | `UIVC_C_INSURABLE_RISK_TYPE` |
| `excess_value` | `Excess_&_Surplus_Property` |
| `ev_value` | `Location` |
| `airport_value` | `Property` |
| `airport_condition` | `CCX_AIRPORT_CODE IS NOT NULL` |

Order: `YCMC_C_STATE_CODE` not NULL → `excess_value`; else `airport_condition` → `airport_value`; else `MLO_LOCATION_NO` not NULL → `ev_value`; else the value of `risk_type_col`. `YCMC_C_STATE_CODE` and `MLO_LOCATION_NO` are hard-coded.

#### `derive_cntct_type`

No args. Uses **fixed** columns: `MCN_E_MAIL` → `Electronic_Address`, `MCN_PHONE_1` → `Phone_Number`, `MAD_LINE_1` → `Physical Address` (**space**, not underscore), else NULL. All three columns must exist; this function references them directly and fails otherwise.

#### `derive_limit_type`

First populated column wins:

| Arg | Result |
|---|---|
| `agg_col` | `Aggregate Limit` |
| `occ_col` | `Occurrence Limit` |
| `layer_col` | `Layer Limit` |
| `excess_col` | `Excess Limit` |
| `qbe_col` | `Qbe Limit` |
| `per_layer_col` | `Per Layer Limit` |
| none populated | `Unspecified` |

```json
{"fn":"derive_limit_type","target":"LMT_TP_CD","args":{
  "agg_col":"RPEC_C_CARRIER_AGGREGATE","occ_col":"RPEC_C_CARRIER_OCCURRENCE",
  "layer_col":"NDYC_C_LAYER_LIMIT","excess_col":"NDYC_C_LIMIT",
  "qbe_col":"NDYC_C_QBE_LIMIT","per_layer_col":"NDYC_C_PER_LAYER_LIMIT"}}
```

---

### 6.4 Premium and lookup functions

#### `derive_prem_tran_id`

Blank-safe concatenation of hash-key columns, product-aware. Three modes, checked in this order:

| Mode | Args | Behaviour |
|---|---|---|
| 1. Dynamic | `product_code_col` + `product_column_map` | Per row: picks the column list for `UPPER(TRIM(product))`. Unknown products use the **first** list in the map. |
| 2. Static | `product` + `product_column_map` | Uses the list for that product (case-insensitive). Product not in the map → warning, DataFrame returned **unchanged** (target not created). |
| 3. Legacy | `cols` | Same list for every row. |
| none of the above | — | Warning, DataFrame unchanged. |

`separator` defaults to `_`. Values are cast to string and trimmed (no timestamp formatting).

```json
{"fn":"derive_prem_tran_id","target":"PREM_TRAN_ID","args":{
  "product_code_col":"MQP_PRODUCT_CODE",
  "product_column_map":{
    "BO":["QBE_HASH_PLCY_ID","QBE_HASH_LOB_ID","QBE_HASH_CVRG_ID","QBE_HASH_INSD_OBJ_ID",
          "QBE_HASH_CNTCT_PNT_ID","QBE_HASH_PLCY_PRTY_ROLE_ID","REF_ID","REC_SRC_NM"],
    "EV":["QBE_HASH_PLCY_ID","QBE_HASH_LOB_ID","QBE_HASH_CVRG_ID","ST_PRVNC_CD","TRAN_TP_CD",
          "QBE_HASH_PLCY_PRTY_ROLE_ID","REF_ID","REC_SRC_NM"]},
  "separator":"_"}}
```

#### `derive_recursive_lookup`

Code translation through a JSON reference payload (e.g. source code → Global_Code), with wildcard and effective-date matching. No Python UDF.

| Arg | Req. | Default | Meaning |
|---|---|---|---|
| `lookup_table` | yes | — | Reference table. A bare name is qualified with the Gold schema. If not found, `target` is NULL. |
| `lookup_filter_value` | yes | — | Key of the reference row to use (matched case-insensitively). |
| `source_to_lookup` | yes | — | `{source_col: lookup_field}` (ordered). Or product-keyed: `{"EV": {...}, "BO": {...}}`. |
| `lookup_filter_col` | no | `DATA_KEY_ID` | Column holding the key. |
| `payload_col` | no | `DATA_VAL` | Column holding the JSON payload (object or list of objects). |
| `return_col` | no | `Global_Code` | Field in the payload to return. |
| `product_code_col` | no | `PART_COL` | Product column (only for product-keyed maps). |
| `filter-Eff-Exp` | no | `{}` | `{"<payload eff field>": "<source col>", "<payload exp field>": "<source col>"}`. Canonical keys `effective_date`/`effective_from` and `expiration_date`/`effective_to`; otherwise the first two keys are used. Note the hyphenated arg name. |

Matching rules:

- Only the **first** reference row for `lookup_filter_value` is read (`limit(1)`).
- Blank payload values become `NoValue`, which acts as a **wildcard**; blank source values are also `NoValue`.
- Each payload record must match every key exactly or by wildcard. The best match wins: date-valid first, then the most exact matches.
- With `filter-Eff-Exp`, a record is valid when `rec_eff <= src_eff` and `rec_exp >= src_exp`. Missing source dates count as valid; missing record dates are open-ended. Many date formats are accepted (`M/d/yyyy`, `yyyy-MM-dd`, ISO, …).
- If the best match is not date-valid, the result is NULL.

```json
{"fn":"derive_recursive_lookup","target":"GLBL_LOB_CD","args":{
  "lookup_table":"ref_code_map","lookup_filter_value":"LOB_MAP",
  "source_to_lookup":{"MQP_PRODUCT_CODE":"Product","LOB_CODE":"Lob"},
  "return_col":"Global_Code",
  "filter-Eff-Exp":{"Effective_Date":"MQP_EFFECTIVE_DATE","Expiration_Date":"MQP_EXPIRATION_DATE"}}}
```

---

### 6.5 General-purpose utilities

#### `default_value`
`args: {"value": <literal>}` → `target = lit(value)`. Useful for constant join columns (`REC_SRC_NM = '109'`) or key suffixes.

#### `derive_column_copy`
`args: {"col": "<source>", "normalize": false}` → copy of `col` (NULL if missing); `normalize` → `LOWER(LTRIM(x))`.

#### `trim_col`
`args: {"col": "<source>"}` → `TRIM(CAST(col AS STRING))`.

#### `concat_name`
`args: {"cols": [...], "separator": " "}` → NULLs become `''`, joined with `separator`, repeated whitespace collapsed to one space, trimmed; an empty result is NULL. With a non-space separator, empty parts leave doubled separators.

#### `derive_full_name`
`args: first, middle, surname, underwriter_name, contact_name` (all column names). If first or surname is present → `"first middle surname"` (whitespace collapsed); else underwriter name; else contact name; else NULL. An empty result is NULL.

#### `coalesce_cols`
`args: {"cols": [...]}` → first non-NULL value (missing columns count as NULL). Blank strings are **not** skipped.

#### `derive_null_if_empty`
`args: {"source_col": "<col>"}` (defaults to `target`) → NULL when blank, else the value as a string.

#### `derive_case_value`
`args: {"condition": "<SQL bool>", "true_value": ..., "false_value": ...}` → `CASE WHEN condition THEN true_value ELSE false_value END`.
A value is treated as a **SQL expression** if it contains `(` or `)`, or ` IS `, ` NOT `, `COALESCE` or `CAST` (case-insensitive); otherwise it is a **literal string**. So `"MQP_C_POLICY_TYPE"` returns the literal text, not the column. Wrap it, e.g. `"TRIM(MQP_C_POLICY_TYPE)"`, to use the column.

#### `derive_case_concat`
`args: {"condition": "<SQL bool>", "true_cols": [...], "false_cols": [...], "separator": "_"}` → blank-safe concatenation of `true_cols` or `false_cols`.

#### `derive_mapped_value`
`args: {"source_col": "<col>", "mapping": {"from": "to", ...}, "default_mode": "source"}`.
Matches `TRIM(CAST(source AS STRING))` exactly (**case-sensitive**) against the mapping keys. Unmatched values: the original value (`default_mode: "source"`) or NULL (`"null"`).

#### `derive_sql_expr`
`args: {"expr": "<Spark SQL expression>"}` → `F.expr(expr)`. Missing `expr` → warning, DataFrame unchanged. Use this for one-off logic (e.g. `CONCAT(LOWER(TRIM(MQP_C_OPPORTUNITY_ID)),'_1')`).

---

## 7. Source connectors

`ConnectorRegistry.get(source_type)` returns the **first registered** connector whose `supports()` is true.

| Connector | `source_type` | Name resolution | Registered by |
|---|---|---|---|
| `DeltaTableConnector` | `delta`, `""` | as given | `setup_defaults` |
| `ParquetVolumeConnector` | `parquet` | path (`/Volumes/...`) | `setup_defaults` |
| `MajescoConnector` | `majesco` | bare → `<default_schema>.<name>` (the schema passed to `setup_defaults`) | `setup_defaults(default_schema)` |
| `GoldConnector` | `gold`, `gold_delta` | bare → `gold_fq.<name>` (if constructed with `gold_fq`) | `setup_defaults` (without `gold_fq`) |
| `SilverConnector` | `silver`, `silver_delta` | bare → `silver_fq.<name>` | **must be registered manually** |

The executor already qualifies bare names before calling the connector (§3, step 3).

### `read(...)` parameters (all connectors)

`spark, source_ref, *, product, product_filter_col, is_common, watermark_col, load_type, start_date, end_date, watermark_ts, watermark_type, pqb_flag_col, pqb_flags, debug_filter_column, debug_filter_value, num_of_days_for_cmn_tables`

### Filters applied by `DeltaTableConnector` (and Majesco / Gold / Silver, which inherit it)

In order:

1. **Debug filter**: `debug_filter_column = debug_filter_value`. The value is coerced to the column type (string, bool, int, float, decimal, date `yyyyMMdd`/`yyyy-MM-dd`, timestamp). Both must be set; otherwise it is skipped with a warning.
2. **Product filter**: if `product` and `product_filter_col` are set and the column exists: `UPPER(col) IN (product)`. A missing column logs a warning and is skipped. `is_common` is **not** checked here.
3. **PQB filter**: `UPPER(pqb_flag_col) IN pqb_flags` when both are set and the column exists.
4. **Early return for Gold objects**: if the source name contains `HUB_` or `LNK_` → no watermark filter. `SAT_*` sources also return here, except `SAT_PREM` / `SAT_CMSN`, which get `LD_DT` within the last `num_of_days_for_cmn_tables` days (unless the value is empty or `NA`).
5. **Incremental watermark** (only when `load_type='incremental'` and `watermark_col` resolves; several columns → `COALESCE`):
   - `start_date` and `end_date`: `to_date(wm) BETWEEN start AND end`.
   - `start_date` only: `BETWEEN start AND current_date()`.
   - `end_date` only: `<= end`.
   - neither, but `watermark_ts`: `to_date(wm) > to_date(watermark_ts)`.
   - nothing: full source read.

`ParquetVolumeConnector` reads with `mergeSchema=true`, applies the product filter only when `is_common` is false (exact match, no `UPPER`), and the debug filter. No watermark.

The `DATE_DELETED` soft-delete filter is applied by the **executor** after the connector, for every source that has the column (§3, step 4).

---

## 8. DeltaWriter: how data is written

### 8.1 `DeltaWriter.write(...)`

```python
DeltaWriter.write(df, target_table, load_type="incremental", scd_type="scd1",
                  primary_keys=None, partition_by=None, physical_partition_by=None,
                  hashdiff_col="hashdiff", vacuum_retention_hours=168,
                  watermark_col=None, rank_order_cols=None, layer="GOLD") -> dict
```

Returns `{"rows_inserted", "rows_updated", "rows_affected", "new_watermark_ts"}`. Counts come from Delta `DESCRIBE HISTORY` metrics, and the watermark is `MAX(watermark_col)` on the committed table, as a **date** capped at today (America/Chicago). SCD2 incremental inserts are counted with an explicit `count()`.

| Situation | What happens |
|---|---|
| Table does not exist | **First load**: append with `mergeSchema`, partitioned by `partition_by` + physical partition columns. SCD1: `LD_DT` re-stamped to now. SCD2: rows are written as the executor prepared them (`LD_DT` from `build_df`; `LD_END_DT` / `SRC_EXPRN_DT` from the in-batch steps, so older in-batch versions arrive already closed). |
| `load_type='full'` and the table exists | **Full overwrite**: `MERGE INTO t USING (SELECT 1) ON TRUE WHEN MATCHED THEN DELETE` (removes **all rows in the table**), then append. |
| incremental, `scd1` | SCD1 MERGE (§8.2). |
| incremental, `scd2` | SCD2 expire + insert (§8.3). |
| Concurrent first-load race (table already exists) | Falls back to the MERGE path. |

Incremental loads require `primary_keys`.

### 8.2 SCD1 MERGE

- Merge key: `primary_keys` + the physical partition columns (Gold). For `LNK_*` tables with `partition_by`, `partition_by` is used instead of the PKs.
- Keys are matched null-safe (`<=>`).
- Match on existing row:
  - `hashdiff` column present → update when the hashdiff differs; all columns except `LD_DT` are updated.
  - No `hashdiff` → update when any non-technical, non-PK column differs.
- No match → insert all columns.
- `LD_DT` of new rows = now.

### 8.3 SCD2 (incremental)

1. With `rank_order_cols`, the source is reduced to the top-ranked row per merge key **for the MERGE only**.
2. **Change detection**: `hashdiff` if both sides have it; otherwise any non-technical column differs; otherwise "unchanged".
3. **Step 1a**: active target rows (`LD_END_DT IS NULL`) that changed get `LD_END_DT = now − 1 s`.
4. **Step 1b** (when the target has `SRC_EXPRN_DT` and the source has `SRC_EFF_DT`): open rows (`SRC_EXPRN_DT IS NULL`) that changed get `SRC_EXPRN_DT = source SRC_EFF_DT`.
5. **Step 2 (insert)**: source rows with no identical row in the target are appended with `LD_DT = now`. "Identical" = all columns equal, null-safe, excluding `LD_DT, LD_END_DT, BTCH_ID, BTCH_DT, SRC_EXPRN_DT, REF_ID, ERR_CD, ERR_FLG, REC_SRC_NM, PART_COL`, plus the physical partition columns. The comparison runs against the whole (partition-scoped) target, active and expired rows, so a returning old version is **not** re-inserted.
6. `REC_SRC_NM` and `ERR_CD` are cast to string and `ERR_FLG` to int before writing.

### 8.4 Partitioning (Gold layer only)

- **Physical partition columns**: `physical_partition_by` argument → `ctl_pipeline.physical_partition_by` (latest active GOLD row for the target) → default `REC_SRC_NM, PART_COL`.
- They must exist in the DataFrame, and an existing table must be partitioned on them; otherwise `ValueError`.
- They are added to MERGE keys and used to scope target reads to the partitions in the batch.
- The Silver layer skips all of this.

### 8.5 Schema alignment and safety

- Before writing, matching columns are cast to the target types, and `CHAR(n)`/`VARCHAR(n)` values are **truncated** to `n`.
- Delta optimistic-concurrency conflicts are retried up to **3** times, with back-off 120 s, 240 s and 480 s (+ jitter). Other errors are raised immediately.
- Existence checks use `SHOW TABLES`; metrics use `DESCRIBE HISTORY`. Safe on serverless / FGAC, with no `persist` or `cache`.

### 8.6 Maintenance helpers

- `DeltaWriter.optimize_zorder(spark, table, zorder_cols)` runs `OPTIMIZE ... ZORDER BY (...)`.
- `DeltaWriter.vacuum(spark, table, retention_hours=168)` runs `VACUUM ... RETAIN n HOURS`.

---

## 9. DQ runner and run log

### `run_dq(spark, settings, pipelines, batch_id, run_id=None)`

For each pipeline config (`target_table`, `pipeline_id`, `primary_keys`), one aggregation computes:

| Rule id | Check | Severity |
|---|---|---|
| `DQ0_TABLE_MISSING` | table readable | CRITICAL |
| `DQ1_ROWCOUNT` | at least 1 row | CRITICAL |
| `DQ2_NULL_<key>` | each PK column has no NULLs | CRITICAL |
| `DQ3_DUPKEY` | PK combination unique (`count - countDistinct`) | CRITICAL |

All results are appended to `<control_fq>.dq_results`. If any check failed, an exception `DQ GATE FAILED (...)` is raised **after** writing. The checks run on the whole table: for SCD2 tables, several versions per PK are normal and will fail `DQ3_DUPKEY`.

### `RunLogger(spark, settings, run_id).log(**kwargs)`

Appends one row to `<control_fq>.run_log` with: `run_id, batch_id, pipeline_id, product_code, target_table, load_type, rows_inserted, rows_updated, rows_affected, watermark_ts, new_watermark_ts, status, error_message, start_ts, end_ts`. The executor calls it automatically on success and on failure.

---

## 10. Logging and debug switches

Detailed logging only runs when the module logger is at **DEBUG** level. On top of that, these environment variables (`1/true/t/yes/y`) control what is logged:

| Variable | Default | Effect |
|---|---|---|
| `LOG_DATAFRAME_METRICS` | false | Row count after each stage (**triggers a Spark job per stage**). |
| `LOG_DATAFRAME_SAMPLE` | false | Show sample rows after each stage. |
| `LOG_DATAFRAME_SAMPLE_ROWS` | 10 | Rows per sample. |
| `LOG_QUERY_PLAN` | false | Query plans. |
| `LOG_SCHEMA` | true | Schemas. |
| `LOG_EXPRESSIONS` | true | Column-mapping and derived expressions. |
| `LOG_WINDOW_FUNCTIONS` | true | Window definitions (LEAD / rank). |
| `LOG_PRIMARY_KEYS` | true | Primary keys per pipeline. |
| `LOG_FILTER_RULES` | true | Every filter predicate applied (connectors, NULL-PK filter). |

Use `context.debug_filter_column` / `debug_filter_value` to run a pipeline for a single key (e.g. one `MQP_ENTITY_REFERENCE`).

---

## 11. Gotchas and observations

Things that behave differently from what a docstring, a comment or a reference SQL might suggest.

**Derive functions**

1. **`derive_concat` defaults to an empty separator.** The wrappers (`derive_qot_id`, `derive_plcy_id`, `derive_oppty_id`, `derive_coins_plcy_id`, `derive_undrly_plcy_id_*`) document "default `_`", but they inherit `""`. Always pass `"separator": "_"`.
2. **Missing columns do not fail.** Most derive functions turn an unknown column into NULL, so a misspelt column silently drops a key part. Check the output of new seeds.
3. **Timestamps in keys.** `derive_concat` (and its wrappers) formats only `TimestampType` as `yyyy-MM-ddTHH:mm:ss`. A `DateType` column gives `yyyy-MM-dd`, and a timestamp stored as a string is left as is. `derive_insd_obj_id_bo` formats both date and timestamp.
4. **`normalize: true` lowercases the `T`** in `derive_concat` and `derive_cntct_pnt_phys`. The ID wrappers force `normalize` off; only `derive_insd_obj_id_bo` restores the `T` after lowercasing.
5. **Blank parts are skipped** by all blank-safe builders (`a_c`). SQL written with `COALESCE(TRIM(x),'')` + `CONCAT_WS` keeps them (`a__c`). Expect differences for keys with empty parts (addresses, coverage keys, coinsurance keys).
6. **Case**: ID wrappers preserve case, `derive_cvrg_id` always lowercases, and `derive_lob_id` / `derive_column_copy` lowercase only with `normalize: true`.
7. **`derive_plcy_id` docstring** says "normalize always enabled"; the code disables it.
8. **`derive_cntct_type`** returns `Physical Address` (with a space), not `Physical_Address`, and fails if any of `MCN_E_MAIL`, `MCN_PHONE_1`, `MAD_LINE_1` is missing.
9. **`derive_case_value`** treats a bare column name as a literal string (§6.5).
10. **`derive_mapped_value`** matching is case-sensitive.
11. **`derive_recursive_lookup`** uses only the first reference row for the key, and its date-filter arg is named `filter-Eff-Exp` (hyphens).

**Rules and configuration**

12. **`ctl_column_map.transform_fn` / `transform_args` are ignored** by the executor. The header of `dv_derive_rule.py` mentions `build_derive_rules_from_map()`, but that function does not exist. Call derive functions from `ctl_rule` or `pre_rules_json`.
13. **Join keys must be created in `pre_rules_json`** on the source that needs them. `ctl_rule` runs after all joins.
14. **Column clash on joins**: right-side columns whose names already exist on the left are dropped. For example, `HUB.REC_SRC_NM` or `HUB.PART_COL` never reach the output; the left source's value is kept. To use a right-side value, copy it to a new name in that source's `pre_rules_json` (`derive_column_copy`).
15. **`hash_key` drops rows** whose hash input is blank (both `cols` and `mappings` forms).
16. **`filter_not_null`** also drops empty strings, and silently skips columns that don't exist.
17. **`DVRuleEngine.filter_expr` is never installed**: the base version is used (same behaviour).
18. **`dv_derivations.py` is legacy** and not imported. Its `derive_concat` coalesces NULLs to `''` and has no timestamp formatting, which differs from the `dv_derive_rule.py` version actually used.

**Loading and writing**

19. **`is_common` is ignored** by `DeltaTableConnector` (only the Parquet connector honours it). The product filter is `UPPER(col) IN (products)`, so pass product codes in upper case.
20. **Gold `HUB_*` / `LNK_*` / `SAT_*` sources are never watermark-filtered** (except `SAT_PREM` / `SAT_CMSN` by `LD_DT` days).
21. **`DATE_DELETED`**: any source with this column loses rows where it is set, before pre-rules and joins.
22. **FULL load deletes the whole target table** (not only the current product or partition), once per table per run key. The next pipelines for the same table in that run are switched to incremental.
23. **Non-SCD2 dedup is non-deterministic** when `watermark_col` isn't a column of the final DataFrame.
24. **SCD1 without a `hashdiff` column**: the MERGE registers `whenMatchedUpdateAll` before the `LD_DT`-preserving `whenMatchedUpdate`, with the same condition. Delta applies the first matching clause, so on this path **`LD_DT` is overwritten** on update, despite the code comment.
25. **`run_dq` `DQ3_DUPKEY`** will flag SCD2 tables with history (several versions per PK).
26. **`new_watermark_ts` is a date**, not a timestamp, capped at today in America/Chicago.

