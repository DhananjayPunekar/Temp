-- =====================================================================
-- common_sat_plcy_ded.sql
-- COMMON__GOLD__SAT_PLCY_DED
-- =====================================================================

USE ${CATALOG}.${CONTROL};

-- ============================================================
-- CLEANUP
-- ============================================================

DELETE FROM ctl_pipeline     WHERE UPPER(TRIM(pipeline_id)) LIKE 'COMMON__GOLD__SAT_PLCY_DED%';
DELETE FROM ctl_source       WHERE UPPER(TRIM(pipeline_id)) LIKE 'COMMON__GOLD__SAT_PLCY_DED%';
DELETE FROM ctl_join         WHERE UPPER(TRIM(pipeline_id)) LIKE 'COMMON__GOLD__SAT_PLCY_DED%';
DELETE FROM ctl_rule         WHERE UPPER(TRIM(pipeline_id)) LIKE 'COMMON__GOLD__SAT_PLCY_DED%';
DELETE FROM ctl_column_map   WHERE UPPER(TRIM(pipeline_id)) LIKE 'COMMON__GOLD__SAT_PLCY_DED%';


INSERT INTO ctl_pipeline
(pipeline_id, product_code, domain, pipeline_name, target_table, base_source_alias, primary_keys, decimal_validation_columns, product_filter_col, partition_by, load_type_default, is_common, watermark_col, watermark_type, scd_type, pii_columns, is_active, mapping_version, updated_ts, layer, pqb_flag_col, scd2_lag_order_cols, scd2_rank_order_cols)
VALUES
('COMMON__GOLD__SAT_PLCY_DED', 'COMMON', 'POLICY', 'POLICY__SAT_PLCY_DED', 'SAT_PLCY_DED', 'PLD', 'QBE_HASH_PLCY_ID', NULL, NULL, 'REC_SRC_NM,PART_COL', 'incremental', 'True', 'BTCH_DT', 'timestamp', 'scd2', NULL, 'True', 'v1.0', CURRENT_TIMESTAMP, 'GOLD', NULL, 'SRC_EFF_DT,BTCH_DT,REF_ID' ,'SRC_EFF_DT DESC,BTCH_DT DESC,REF_ID DESC');

INSERT INTO ctl_source
(pipeline_id, seq, alias, source_ref, source_type, pre_rules_json)
VALUES
('COMMON__GOLD__SAT_PLCY_DED', '1', 'PLD', 'PLCY_DED', 'silver', '[]'),
('COMMON__GOLD__SAT_PLCY_DED', '2', 'HUB', 'HUB_PLCY', 'gold', '[]'),
('COMMON__GOLD__SAT_PLCY_DED', '3', 'POL', 'PLCY_DTL', 'silver', '[{"rule_name":"derive","params_json":{"fn":"derive_plcy_id","target":"PLCY_ID","args":{"cols":["MQP_DISPLAY_POLICY_NUMBER","MQP_EFFECTIVE_DATE"],"separator":"_"}}}]');

--CTL JOIN

INSERT INTO ctl_join
(pipeline_id, join_seq, right_alias, join_type, left_col, right_col, pair_seq)
VALUES
('COMMON__GOLD__SAT_PLCY_DED', 
1, 
'POL', 
'inner', 
'MQP_ENTITY_REFERENCE', 
'MQP_ENTITY_REFERENCE', 
1),
('COMMON__GOLD__SAT_PLCY_DED', 
1, 
'POL', 
'inner', 
'MQP_DATE_MODIFIED', 
'MQP_DATE_MODIFIED', 
2),
('COMMON__GOLD__SAT_PLCY_DED', 
2, 
'HUB', 
'inner', 
'PLCY_ID', 
'PLCY_ID', 
1);

-- CTL RULES

INSERT INTO ctl_rule (pipeline_id, seq, rule_name, params_json)
VALUES 
('COMMON__GOLD__SAT_PLCY_DED', 0, 'record_source', '{"value":"109","target":"REC_SRC_NM"}');


-- CTL COLUMN MAP

INSERT INTO ctl_column_map (pipeline_id, seq, source_expr, target_column, role, col_type, transform_fn, transform_args)
VALUES 
('COMMON__GOLD__SAT_PLCY_DED', 0, 'HUB.QBE_HASH_PLCY_ID', 'QBE_HASH_PLCY_ID', NULL, 'direct', NULL, NULL),
('COMMON__GOLD__SAT_PLCY_DED', 1, 'PLD.MQP_ENTITY_REFERENCE', 'REF_ID', NULL, 'direct', NULL, NULL),
('COMMON__GOLD__SAT_PLCY_DED', 2, 'PLD.DED_TYPE', 'DED_TP_CD', NULL, 'direct', NULL, NULL),
('COMMON__GOLD__SAT_PLCY_DED', 3, 'PLD.MQP_DATE_MODIFIED', 'SRC_EFF_DT', NULL, 'direct', NULL, NULL),
('COMMON__GOLD__SAT_PLCY_DED', 4, '\'0\'', 'ERR_CD', NULL, 'direct', NULL, NULL),
('COMMON__GOLD__SAT_PLCY_DED', 5, 'LOWER(MQP_PRODUCT_CODE)', 'PART_COL', NULL, 'direct', NULL, NULL),
('COMMON__GOLD__SAT_PLCY_DED', 6, 'BTCH_ID', 'BTCH_ID', NULL, 'direct', NULL, NULL),
('COMMON__GOLD__SAT_PLCY_DED', 7, 'BTCH_DT', 'BTCH_DT', NULL, 'direct', NULL, NULL),
('COMMON__GOLD__SAT_PLCY_DED', 8, '\'0\'', 'ERR_FLG', NULL, 'direct', NULL, NULL);
