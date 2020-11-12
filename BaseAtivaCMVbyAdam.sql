SELECT 
  C.NM_SUB_MARCA_BI AS MARCA
  ,SUBSTR (A.SK_DATA,1,6) AS MES_REF
  ,CASE
      WHEN E.NM_REGIONAL_CMV = 'RSI -  Regional São Paulo – Interior' THEN TO_CHAR('RSI -  Regional São Paulo - Interior')
      WHEN E.NM_REGIONAL_CMV = 'RSC -  Regional São Paulo – Capital' THEN TO_CHAR('RSC -  Regional São Paulo - Capital')
      ELSE TO_CHAR(E.NM_REGIONAL_CMV) END as REGIONAL
  ,SUM(QT) AS TT_DOM
FROM INN.FS_BASE_CONECTADA A
LEFT JOIN INN.DS_MARCA B
   ON B.SK_MARCA = A.SK_MARCA
  AND B.FC_STATUS = 'A'
LEFT JOIN INN.DS_SUB_MARCA C
   ON C.SK_SUB_MARCA = A.SK_SUB_MARCA
  AND C.FC_STATUS = 'A'
LEFT JOIN INN.DS_REL_ESTRUT_ORGANIZACIONAL D
   ON A.SK_MUNICIPIO = D.SK_MUNICIPIO
   AND D.FC_STATUS = 'A'
LEFT JOIN INN.DS_REGIONAL_CMV E
   ON D.sk_regional_cmv = E.sk_regional_cmv
  AND E.FC_STATUS = 'A'
WHERE A.SK_DATA >= 20180101
and C.NM_SUB_MARCA_BI in ('NET', 'CTV', 'CFI')
GROUP BY 
C.NM_SUB_MARCA_BI,
SUBSTR (A.SK_DATA,1,6),
CASE
  WHEN E.NM_REGIONAL_CMV = 'RSI -  Regional São Paulo – Interior' THEN TO_CHAR('RSI -  Regional São Paulo - Interior')
  WHEN E.NM_REGIONAL_CMV = 'RSC -  Regional São Paulo – Capital' THEN TO_CHAR('RSC -  Regional São Paulo - Capital')
  ELSE TO_CHAR(E.NM_REGIONAL_CMV) END
order by 1

---======================================================================================================================
--KPI: "BASE ATIVA" CMV
--OBJETIVO: 
--FONTE: ADAM ME PASSOU A QUERY
--DATA: 14.02.2020

---======================================================================================================================

SELECT
      'CMV' AS MARCA
      ,SUBSTR (SK_DATA,1,6) AS MES_REF
      ,B.NM_AGRUPAMENTO_PLATAFORMA_BI AS PRODUTO
      ,CASE
        WHEN sk_regional = 4 THEN TO_CHAR('RSI -  Regional São Paulo - Interior')
        WHEN sk_regional = 5 THEN to_char('RSC -  Regional São Paulo - Capital')
        WHEN sk_regional = 3 THEN to_char('RCO -  Regional Centro Oeste')
        WHEN sk_regional = 6 THEN to_char('RNE -  Regional Nordeste')
        WHEN sk_regional = 1 THEN to_char('RRE -  Regional Rio de Janeiro e Espírito Santo')
        WHEN sk_regional = 2 THEN to_char('RRS -  Regional Rio Grande do Sul')
        WHEN sk_regional = 7 THEN to_char('RPS -  Regional Paraná e Santa Catarina')
        WHEN sk_regional = 10 THEN to_char('RNO -  Regional Norte')
        WHEN sk_regional = 8 THEN to_char('RBS -  Regional Bahia e Sergipe')
        WHEN sk_regional = 9 THEN to_char('RMG -  Regional Minas Gerais')
        ELSE TO_CHAR(sk_regional) END as REGIONAL
        ,SUM(QT_LINHA_ATIVA) AS TT_DOM
 --       SELECT UNIQUE SK_DATA
FROM INTMKT.FS_CONT_RAT_BAS_LINH_ATIVA_CMV@BASECLARO A

LEFT JOIN INTMKT.DS_ATENDIMENTO_PLATAFORMA@BASECLARO B
     ON A.SK_ATENDIMENTO_PLATAFORMA = B.SK_ATENDIMENTO_PLATAFORMA

WHERE SK_DATA >= &FIRT_DAY_YYYYMMDD
AND SK_FC_ASSINANTE_ZB = '1971852147'

GROUP BY
        SUBSTR (SK_DATA,1,6)
       ,B.NM_AGRUPAMENTO_PLATAFORMA_BI
      ,CASE
        WHEN sk_regional = 4 THEN TO_CHAR('RSI -  Regional São Paulo - Interior')
        WHEN sk_regional = 5 THEN to_char('RSC -  Regional São Paulo - Capital')
        WHEN sk_regional = 3 THEN to_char('RCO -  Regional Centro Oeste')
        WHEN sk_regional = 6 THEN to_char('RNE -  Regional Nordeste')
        WHEN sk_regional = 1 THEN to_char('RRE -  Regional Rio de Janeiro e Espírito Santo')
        WHEN sk_regional = 2 THEN to_char('RRS -  Regional Rio Grande do Sul')
        WHEN sk_regional = 7 THEN to_char('RPS -  Regional Paraná e Santa Catarina')
        WHEN sk_regional = 10 THEN to_char('RNO -  Regional Norte')
        WHEN sk_regional = 8 THEN to_char('RBS -  Regional Bahia e Sergipe')
        WHEN sk_regional = 9 THEN to_char('RMG -  Regional Minas Gerais')
          ELSE TO_CHAR(sk_regional) END
order by 2,3,4

---====================================================================================================================
SELECT * FROM ALL_ALL_TABLES WHERE TABLE_NAME LIKE '%SK_MOTIVO_URA%'
SELECT * FROM ALL_ALL_TABLES WHERE TABLE_NAME LIKE '%MOTIVO%' AND OWNER LIKE 'INN'


SELECT * FROM INN.DS_MOTIVO_URA;

SELECT * FROM INN.DS_SUBMOTIVO_URA;

SELECT * FROM BI_DIM_MOTIVO_ATENDIMENTO@BASECLARO M ---75020 REGISTROS CONSULTADO EM 05.12.2019

SELECT UNIQUE M.DW_MOTIVO_ATENDIMENTO FROM BI_DIM_MOTIVO_ATENDIMENTO@BASECLARO M GROUP BY M.DW_MOTIVO_ATENDIMENTO  --- 42 ROWS CONSULTADO EM 05.12.2019

SELECT * FROM INN.DW_MOTIVO_EXPURGO_OUV
