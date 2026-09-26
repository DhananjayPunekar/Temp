%sql
WITH SRC_PRTY AS
(
    SELECT DISTINCT
           CONCAT(
               LOWER(TRIM(BUSINESSNAME)),
               '_1'
           ) AS PRTY_ID,
           TRIM(PARTY_TYPE) AS PRTY_TP_CD
    FROM dsi_dev.silver_oct16.party
    WHERE BUSINESSNAME IS NOT NULL
      AND TRIM(BUSINESSNAME) <> ''
      AND PARTY_TYPE IS NOT NULL
      AND TRIM(PARTY_TYPE) <> ''
      AND MQP_PRODUCT_CODE = 'AV'
)

SELECT
       MD5(
           CONCAT_WS(
               '_',
               S.PRTY_ID,
               LOWER(S.PRTY_TP_CD),
               '109'
           )
       ) AS QBE_HASH_PRTY_ID,

       CURRENT_TIMESTAMP AS LD_DT,

       '109' AS REC_SRC_NM,

       S.PRTY_ID,

       S.PRTY_TP_CD,

       0 AS ERR_FLG,

       '0' AS ERR_CD

FROM SRC_PRTY S

LEFT JOIN dsi_dev.adv_db_majescoic.hub_prty H
       ON LOWER(H.PRTY_ID) = S.PRTY_ID
      AND LOWER(TRIM(H.PRTY_TP_CD)) = LOWER(TRIM(S.PRTY_TP_CD))
      AND H.REC_SRC_NM = '109'

WHERE H.QBE_HASH_PRTY_ID IS NULL;
