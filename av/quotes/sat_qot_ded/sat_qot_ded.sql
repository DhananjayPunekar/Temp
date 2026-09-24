%sql
WITH QOT_DED_SRC AS
(
    SELECT

           QD.*,

           LOWER(
               CONCAT(
                   TRIM(CAST(QD.MQP_ENTITY_REFERENCE AS STRING)),
                   '_',
                   TRIM(CAST(QD.MQP_DATE_MODIFIED AS STRING))
               )
           ) AS QOT_ID

    FROM dsi_dev.silver_oct16.QOT_DED QD

    WHERE QD.MQP_PRODUCT_CODE = 'AV'
      AND QD.MQP_ENTITY_REFERENCE IS NOT NULL
      AND QD.MQP_DATE_MODIFIED IS NOT NULL
),

LINKED AS
(
    SELECT

           HQ.QBE_HASH_QOT_ID,

           QD.*

    FROM QOT_DED_SRC QD

    INNER JOIN dsi_dev.adv_db_majescoic.HUB_QOT HQ
        ON LOWER(TRIM(HQ.QOT_ID))
         = QD.QOT_ID
       AND HQ.REC_SRC_NM = '109'

    LEFT JOIN dsi_dev.adv_db_majescoic.SAT_QOT_DED SQD
        ON HQ.QBE_HASH_QOT_ID =
           SQD.QBE_HASH_QOT_ID
       AND SQD.REC_SRC_NM = '109'

    WHERE SQD.QBE_HASH_QOT_ID IS NULL
),

DEDUPED AS
(
    SELECT

           L.*,

           MAX(MQP_DATE_MODIFIED) OVER
           (
               PARTITION BY QBE_HASH_QOT_ID
           ) AS MAX_DATE_MODIFIED,

           ROW_NUMBER() OVER
           (
               PARTITION BY QBE_HASH_QOT_ID
               ORDER BY MQP_DATE_MODIFIED DESC
           ) AS RN,

           LAG(MQP_DATE_MODIFIED) OVER
           (
               PARTITION BY QBE_HASH_QOT_ID
               ORDER BY MQP_DATE_MODIFIED DESC
           ) AS PREV_MORE_RECENT_SRC_EFF_DT

    FROM LINKED L
),

FINAL AS
(
    SELECT

           D.QBE_HASH_QOT_ID,

           CURRENT_TIMESTAMP AS LD_DT,

           D.MQP_ENTITY_REFERENCE AS REF_ID,

           CASE
               WHEN D.RN = 1
               THEN NULL
               ELSE D.MAX_DATE_MODIFIED
           END AS LD_END_DT,

           '109' AS REC_SRC_NM,

           D.DED_TYPE AS DED_TP_CD,

           CAST(NULL AS STRING) AS DED_BSIS_CD,

           CAST(NULL AS STRING) AS DED_APLY_TO_CD,

           D.DED_AMT AS DED_AMT,

           CAST(NULL AS STRING) AS DED_DATA_TP_CD,

           CAST(NULL AS STRING) AS DED_CD,

           D.MQP_DATE_MODIFIED AS SRC_EFF_DT,

           CASE
               WHEN D.RN = 1
               THEN NULL
               ELSE D.PREV_MORE_RECENT_SRC_EFF_DT
           END AS SRC_EXPRN_DT,

           0 AS ERR_FLG,

           '0' AS ERR_CD

    FROM DEDUPED D
)

SELECT

       QBE_HASH_QOT_ID,
       LD_DT,
       DED_TP_CD,
       REF_ID,
       LD_END_DT,
       REC_SRC_NM,
       DED_BSIS_CD,
       DED_APLY_TO_CD,
       DED_AMT,
       DED_DATA_TP_CD,
       DED_CD,
       SRC_EFF_DT,
       SRC_EXPRN_DT,
       ERR_FLG,
       ERR_CD

FROM FINAL;
