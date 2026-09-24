%sql
WITH BASE AS
(
    SELECT

           Q.MQP_ENTITY_REFERENCE,
           Q.MQP_C_OPPORTUNITY_ID,
           Q.MQP_DATE_MODIFIED

    FROM dsi_dev.silver_oct16.QOT_DTL Q

    WHERE Q.MQP_PRODUCT_CODE = 'AV'
      AND Q.MQP_C_OPPORTUNITY_ID IS NOT NULL
      AND TRIM(
              CAST(Q.MQP_C_OPPORTUNITY_ID AS STRING)
          ) <> ''
),

LINKED AS
(
    SELECT

           HO.QBE_HASH_OPPTY_ID,

           B.*

    FROM BASE B

    INNER JOIN dsi_dev.adv_db_majescoic.HUB_OPPTY HO
        ON LOWER(TRIM(HO.OPPTY_ID))
         =
           CONCAT(
               LOWER(
                   TRIM(
                       CAST(
                           B.MQP_C_OPPORTUNITY_ID AS STRING
                       )
                   )
               ),
               '_1'
           )
       AND HO.REC_SRC_NM = '109'

    LEFT JOIN dsi_dev.adv_db_majescoic.SAT_OPPTY_DTL SOD
        ON HO.QBE_HASH_OPPTY_ID =
           SOD.QBE_HASH_OPPTY_ID
       AND SOD.REC_SRC_NM = '109'

    WHERE SOD.QBE_HASH_OPPTY_ID IS NULL
),

DEDUPED AS
(
    SELECT

           L.*,

           MAX(MQP_DATE_MODIFIED)
           OVER
           (
               PARTITION BY QBE_HASH_OPPTY_ID
           ) AS MAX_DATE_MODIFIED,

           ROW_NUMBER()
           OVER
           (
               PARTITION BY QBE_HASH_OPPTY_ID
               ORDER BY MQP_DATE_MODIFIED DESC
           ) AS RN,

           LAG(MQP_DATE_MODIFIED)
           OVER
           (
               PARTITION BY QBE_HASH_OPPTY_ID
               ORDER BY MQP_DATE_MODIFIED DESC
           ) AS PREV_MORE_RECENT_SRC_EFF_DT

    FROM LINKED L
),

FINAL AS
(
    SELECT

           D.QBE_HASH_OPPTY_ID,

           CURRENT_TIMESTAMP AS LD_DT,

           D.MQP_ENTITY_REFERENCE AS REF_ID,

           CASE
               WHEN D.RN = 1 THEN NULL
               ELSE D.MAX_DATE_MODIFIED
           END AS LD_END_DT,

           '109' AS REC_SRC_NM,

           CONCAT(
               LOWER(
                   TRIM(
                       CAST(
                           D.MQP_C_OPPORTUNITY_ID AS STRING
                       )
                   )
               ),
               '_1'
           ) AS OPPTY_ID,

           CAST(NULL AS STRING) AS CLSE_OUTCM_CD,
           CAST(NULL AS STRING) AS CLSE_RSN_DESCR,
           CAST(NULL AS STRING) AS QBE_CLSE_RSN_DESCR,
           CAST(NULL AS STRING) AS QBE_CLSE_OUTCM_DESCR,
           CAST(NULL AS STRING) AS QBE_OPPTY_STG_CD,
           CAST(NULL AS STRING) AS STG_NM,
           CAST(NULL AS STRING) AS BUS_UN_NM,
           CAST(NULL AS STRING) AS BUS_TP_DESCR,
           CAST(NULL AS TIMESTAMP) AS CLSE_DT,
           CAST(NULL AS STRING) AS LEAD_SRC,
           CAST(NULL AS DECIMAL(18,2)) AS PROJD_PPLN_AMT,
           CAST(NULL AS STRING) AS OPPTY_DESCR,
           CAST(NULL AS STRING) AS INTMDRY_CHNNL_NM,
           CAST(NULL AS STRING) AS CLRNC_SYS_ID,
           CAST(NULL AS DECIMAL(18,2)) AS CMSN_AMT,
           CAST(NULL AS DECIMAL(18,2)) AS CMSN_PCT,
           CAST(NULL AS STRING) AS CMSN_TP_DESCR,
           CAST(NULL AS STRING) AS CUR_CARR_NM,
           CAST(NULL AS STRING) AS ADDTNL_INSR_DTL,
           CAST(NULL AS TIMESTAMP) AS RCVD_DT,
           CAST(NULL AS STRING) AS HAZD_RTNG_NBR,
           CAST(NULL AS STRING) AS GO_TO_MKT_RSN_TXT,
           CAST(NULL AS STRING) AS LEAD_FLW_FLG,
           CAST(NULL AS STRING) AS LN_OF_BUS_DESCR,
           CAST(NULL AS STRING) AS LYR_FLG,
           CAST(NULL AS STRING) AS CHNNL_DESCR,
           CAST(NULL AS STRING) AS SUB_CTGRY,
           CAST(NULL AS TIMESTAMP) AS SUBMSN_DT,
           CAST(NULL AS TIMESTAMP) AS QOT_DT,
           CAST(NULL AS TIMESTAMP) AS PLCY_PRCSD_DT,
           CAST(NULL AS STRING) AS PLCY_NBR,
           CAST(NULL AS TIMESTAMP) AS PLCY_INCPTN_DT,
           CAST(NULL AS TIMESTAMP) AS PLCY_EXPRN_DT,

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

SELECT *
FROM FINAL;
