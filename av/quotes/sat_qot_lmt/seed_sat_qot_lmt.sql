-- =====================================================================
-- seed_sat_qot_lmt.sql
-- AV__GOLD__SAT_QOT_LMT
-- Source spec: sat_qot_lmt.sql
-- =====================================================================

USE ${CATALOG}.${CONTROL};

-- ============================================================
-- CLEANUP
-- ============================================================

DELETE FROM ctl_pipeline     WHERE UPPER(TRIM(pipeline_id)) LIKE 'AV__GOLD__SAT_QOT_LMT%';
DELETE FROM ctl_source       WHERE UPPER(TRIM(pipeline_id)) LIKE 'AV__GOLD__SAT_QOT_LMT%';
DELETE FROM ctl_join         WHERE UPPER(TRIM(pipeline_id)) LIKE 'AV__GOLD__SAT_QOT_LMT%';
DELETE FROM ctl_rule         WHERE UPPER(TRIM(pipeline_id)) LIKE 'AV__GOLD__SAT_QOT_LMT%';
DELETE FROM ctl_column_map   WHERE UPPER(TRIM(pipeline_id)) LIKE 'AV__GOLD__SAT_QOT_LMT%';


-- CTL PIPELINE
-- product_filter_col = MQP_PRODUCT_CODE  -> restricts QOT_LMT to AV
--   NOTE: sat_qot_lmt.sql has no MQP_PRODUCT_CODE = 'AV' filter (sat_qot_ded.sql does).
--         Kept here for an AV-scoped pipeline, consistent with SAT_QOT_DED.
-- scd2 lag/rank cols                     -> DEDUPED CTE (ROW_NUMBER / LAG by QBE_HASH_QOT_ID)

INSERT INTO ctl_pipeline
(pipeline_id, product_code, domain, pipeline_name, target_table, base_source_alias, primary_keys, decimal_validation_columns, product_filter_col, partition_by, load_type_default, is_common, watermark_col, watermark_type, scd_type, pii_columns, is_active, mapping_version, updated_ts, layer, pqb_flag_col, scd2_lag_order_cols, scd2_rank_order_cols)
VALUES
('AV__GOLD__SAT_QOT_LMT', 'AV', 'QUOTE', 'QUOTE__SAT_QOT_LMT', 'SAT_QOT_LMT', 'QL', 'QBE_HASH_QOT_ID', NULL, 'MQP_PRODUCT_CODE', 'REC_SRC_NM,PART_COL', 'incremental', 'False', 'BTCH_DT', 'timestamp', 'scd2', NULL, 'True', 'v1.0', CURRENT_TIMESTAMP, 'GOLD', NULL, 'SRC_EFF_DT,BTCH_DT,REF_ID' ,'SRC_EFF_DT DESC,BTCH_DT DESC,REF_ID DESC');


-- CTL SOURCE
-- QL pre-rules:
--   QOT_ID     = derive_concat(MQP_ENTITY_REFERENCE, MQP_DATE_MODIFIED, '_')   (STD_QOT_ID in sat_qot_lmt.sql)
--                timestamp token -> yyyy-MM-ddTHH:mm:ss; normalize=false so the 'T' stays uppercase
--                (the plain CAST + LOWER in sat_qot_lmt.sql is not used)
--   REC_SRC_NM = '109'  -> used as 2nd join pair to enforce HQ.REC_SRC_NM = '109'
-- HUB: no pre-rule; HUB_QOT.QOT_ID already carries the yyyy-MM-ddTHH:mm:ss key (no LOWER, which would turn 'T' into 't')

INSERT INTO ctl_source
(pipeline_id, seq, alias, source_ref, source_type, pre_rules_json)
VALUES
('AV__GOLD__SAT_QOT_LMT', '1', 'QL', 'QOT_LMT', 'silver', '[{"rule_name":"derive","params_json":{"fn":"derive_concat","target":"QOT_ID","args":{"cols":["MQP_ENTITY_REFERENCE","MQP_DATE_MODIFIED"],"separator":"_","normalize":false}}},{"rule_name":"derive","params_json":{"fn":"default_value","target":"REC_SRC_NM","args":{"value":"109"}}}]'),
('AV__GOLD__SAT_QOT_LMT', '2', 'HUB', 'HUB_QOT', 'gold', '[]');


-- CTL JOIN

INSERT INTO ctl_join
(pipeline_id, join_seq, right_alias, join_type, left_col, right_col, pair_seq)
VALUES
('AV__GOLD__SAT_QOT_LMT', 
1, 
'HUB', 
'inner', 
'QOT_ID', 
'QOT_ID', 
1),
('AV__GOLD__SAT_QOT_LMT', 
1, 
'HUB', 
'inner', 
'REC_SRC_NM', 
'REC_SRC_NM', 
2);


-- CTL RULES

INSERT INTO ctl_rule (pipeline_id, seq, rule_name, params_json)
VALUES 
('AV__GOLD__SAT_QOT_LMT', 0, 'record_source', '{"value":"109","target":"REC_SRC_NM"}');


-- CTL COLUMN MAP
-- Not mapped (framework-managed): LD_DT, LD_END_DT, SRC_EXPRN_DT
-- Not mapped (NULL per spec -> defaulted to NULL by schema alignment):
--   LMT_DATA_TP_CD, LMT_BSIS_CD, LMT_APLY_TO_CD, LMT_CD

INSERT INTO ctl_column_map (pipeline_id, seq, source_expr, target_column, role, col_type, transform_fn, transform_args)
VALUES 
('AV__GOLD__SAT_QOT_LMT', 0, 'HUB.QBE_HASH_QOT_ID', 'QBE_HASH_QOT_ID', NULL, 'direct', NULL, NULL),
('AV__GOLD__SAT_QOT_LMT', 1, 'QL.LMT_TYPE', 'LMT_TP_CD', NULL, 'direct', NULL, NULL),
('AV__GOLD__SAT_QOT_LMT', 2, 'QL.MQP_ENTITY_REFERENCE', 'REF_ID', NULL, 'direct', NULL, NULL),
('AV__GOLD__SAT_QOT_LMT', 3, 'QL.LMT_AMT', 'LMT_AMT', NULL, 'direct', NULL, NULL),
('AV__GOLD__SAT_QOT_LMT', 4, 'QL.MQP_DATE_MODIFIED', 'SRC_EFF_DT', NULL, 'direct', NULL, NULL),
('AV__GOLD__SAT_QOT_LMT', 5, '\'0\'', 'ERR_CD', NULL, 'direct', NULL, NULL),
('AV__GOLD__SAT_QOT_LMT', 6, '\'0\'', 'ERR_FLG', NULL, 'direct', NULL, NULL),
('AV__GOLD__SAT_QOT_LMT', 7, 'LOWER(MQP_PRODUCT_CODE)', 'PART_COL', NULL, 'direct', NULL, NULL),
('AV__GOLD__SAT_QOT_LMT', 8, 'BTCH_ID', 'BTCH_ID', NULL, 'direct', NULL, NULL),
('AV__GOLD__SAT_QOT_LMT', 9, 'BTCH_DT', 'BTCH_DT', NULL, 'direct', NULL, NULL);
