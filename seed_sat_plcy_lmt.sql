-- =====================================================================
-- common_sat_plcy_lmt.sql
-- COMMON__GOLD__SAT_PLCY_LMT
-- =====================================================================

USE ${CATALOG}.${CONTROL};

-- ============================================================
-- CLEANUP
-- ============================================================

DELETE FROM ctl_pipeline     WHERE UPPER(TRIM(pipeline_id)) LIKE 'COMMON__GOLD__SAT_PLCY_LMT%';
DELETE FROM ctl_source       WHERE UPPER(TRIM(pipeline_id)) LIKE 'COMMON__GOLD__SAT_PLCY_LMT%';
DELETE FROM ctl_join         WHERE UPPER(TRIM(pipeline_id)) LIKE 'COMMON__GOLD__SAT_PLCY_LMT%';
DELETE FROM ctl_rule         WHERE UPPER(TRIM(pipeline_id)) LIKE 'COMMON__GOLD__SAT_PLCY_LMT%';
DELETE FROM ctl_column_map   WHERE UPPER(TRIM(pipeline_id)) LIKE 'COMMON__GOLD__SAT_PLCY_LMT%';

INSERT INTO ctl_pipeline (
    pipeline_id,
    product_code,
    domain,
    pipeline_name,
    target_table,
    base_source_alias,
    primary_keys,
    decimal_validation_columns,
    product_filter_col,
    partition_by,
    load_type_default,
    is_common,
    watermark_col,
    watermark_type,
    scd_type,
    pii_columns,
    is_active,
    mapping_version,
    updated_ts,
    layer,
    pqb_flag_col,
    scd2_lag_order_cols,
    scd2_rank_order_cols
)
VALUES (
    'COMMON__GOLD__SAT_PLCY_LMT',
    'COMMON',
    'POLICY',
    'POLICY__SAT_PLCY_LMT',
    'SAT_PLCY_LMT',
    'LMT',
    'QBE_HASH_PLCY_ID',
    NULL,
    NULL,
    'REC_SRC_NM,PART_COL',
    'INCREMENTAL',
    TRUE,
    'BTCH_DT',
    'TIMESTAMP',
    'SCD2',
    NULL,
    TRUE,
    'V1.1',
    CURRENT_TIMESTAMP,
    'GOLD',
    NULL,
    'SRC_EFF_DT,BTCH_DT,REF_ID',
    'SRC_EFF_DT DESC,BTCH_DT DESC,REF_ID DESC'
);

INSERT INTO ctl_source
(pipeline_id, seq, alias, source_ref, source_type, pre_rules_json)
VALUES
('COMMON__GOLD__SAT_PLCY_LMT', '1', 'LMT', 'PLCY_LMT', 'silver', '[]'),
('COMMON__GOLD__SAT_PLCY_LMT', '2', 'HUB', 'HUB_PLCY', 'gold', '[]'),
('COMMON__GOLD__SAT_PLCY_LMT', '3', 'DTL', 'PLCY_DTL', 'silver', '[{"rule_name":"derive","params_json":{"fn":"derive_plcy_id","target":"PLCY_ID","args":{"cols":["MQP_DISPLAY_POLICY_NUMBER","MQP_EFFECTIVE_DATE"],"separator":"_"}}}]');

--CTL JOIN

INSERT INTO ctl_join
(pipeline_id, join_seq, right_alias, join_type, left_col, right_col, pair_seq)
VALUES
('COMMON__GOLD__SAT_PLCY_LMT', 
1, 
'DTL', 
'inner', 
'MQP_ENTITY_REFERENCE', 
'MQP_ENTITY_REFERENCE', 
1),
('COMMON__GOLD__SAT_PLCY_LMT', 
1, 
'DTL', 
'inner', 
'MQP_DATE_MODIFIED', 
'MQP_DATE_MODIFIED', 
2),
('COMMON__GOLD__SAT_PLCY_LMT', 
2, 
'HUB', 
'inner', 
'PLCY_ID', 
'PLCY_ID', 
1);

-- CTL RULES

INSERT INTO ctl_rule (pipeline_id, seq, rule_name, params_json)
VALUES 
('COMMON__GOLD__SAT_PLCY_LMT', 0, 'record_source', '{"value":"109","target":"REC_SRC_NM"}'),
('COMMON__GOLD__SAT_PLCY_LMT', 1, 'filter_not_null', '{"cols":["PLCY_ID"]}');

-- CTL COLUMN MAP

INSERT INTO ctl_column_map (pipeline_id, seq, source_expr, target_column, role, col_type, transform_fn, transform_args)
VALUES 
('COMMON__GOLD__SAT_PLCY_LMT', 1, 'HUB.QBE_HASH_PLCY_ID', 'QBE_HASH_PLCY_ID', NULL, 'direct', NULL, NULL),
('COMMON__GOLD__SAT_PLCY_LMT', 2, 'LMT.MQP_ENTITY_REFERENCE', 'REF_ID', NULL, 'direct', NULL, NULL),
('COMMON__GOLD__SAT_PLCY_LMT', 3, 'LMT.LMT_TYPE', 'LMT_TP_CD', NULL, 'direct', NULL, NULL),
('COMMON__GOLD__SAT_PLCY_LMT', 4, 'LMT.LMT_AMT', 'LMT_AMT', NULL, 'direct', NULL, NULL),
('COMMON__GOLD__SAT_PLCY_LMT', 5, 'LMT.MQP_DATE_MODIFIED', 'SRC_EFF_DT', NULL, 'direct', NULL, NULL),
('COMMON__GOLD__SAT_PLCY_LMT', 6, '\'0\'', 'ERR_CD', NULL, 'direct', NULL, NULL),
('COMMON__GOLD__SAT_PLCY_LMT', 6, '\'0\'', 'ERR_FLG', NULL, 'direct', NULL, NULL),
('COMMON__GOLD__SAT_PLCY_LMT', 7, 'LOWER(MQP_PRODUCT_CODE)', 'PART_COL', NULL, 'derive', NULL, NULL),
('COMMON__GOLD__SAT_PLCY_LMT', 8, 'BTCH_ID', 'BTCH_ID', NULL, 'direct', NULL, NULL),
('COMMON__GOLD__SAT_PLCY_LMT', 9, 'BTCH_DT', 'BTCH_DT', NULL, 'direct', NULL, NULL);
