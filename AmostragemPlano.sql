--========================================================================================================================
--                              PEDIDO DA FRANCINE: ACESSOS DOS CLIENTES RELATIVOS AO PS8
--  BRIEFING:
-- HIST�RICO DOS ULTIMOS 6 MESES 
-- DE 200 CLIENTES RELATIVO AOS
-- PRODUTOS PR� PAGO/CONTROLE/P�S
-- DDD41                            
-- 11980185386 NTC_FLEX TENTOU ACESSAR MINHA CLARO
--========================================================================================================================
--SELECT COUNT (DISTINCT NUM_NTC || TO_CHAR(DT_INI_ATENDIMENTO,'RRRRMMDD HH24MISS'))  AS QTD_ACESSOS -- 1.546.851
--SELECT *  FROM INN.EXT_ADMINSITE
--SELECT *  FROM INN.EXT_ADMINSITE WHERE TRIM(SUBSTR(NM_DESC_PEDIDO,INSTR(NM_DESC_PEDIDO,'|origem:')+8)) IN ('NETAPP','MINHANET','MEUTECNICO','BOT','NETAPPNOVO','MIND')
--SELECT * FROM BI_DIM_MOTIVO_ATENDIMENTO
---SELECT * FROM ALL_ALL_TABLES WHERE OWNER LIKE 'DWH' AND TABLE_NAME LIKE '%HIS%ASS%'

WITH 
BASE1 AS
(
SELECT /*PARALELL (32)*/         
         SUBSTR(TO_CHAR(A.DAT_INICIO_ATENDIMENTO, 'RRRRMMDD'),1,6) AS SAFRA
         ,A.NUM_NTC
         ,SUBSTR(A.NUM_NTC,1,2) AS DDD
         ,HIS.COD_AREA
         ,MIN(TO_DATE(TO_CHAR(A.DAT_INICIO_ATENDIMENTO, 'RRRRMMDD')||' '||HOR_INICIO_ATENDIMENTO,'RRRRMMDD HH24MISS')) DT_PRI_ATEND
         ,HIS.COD_PLATAFORMA_ATU

FROM DWH.BI_FP_ASSINANTE_ATEND_FECHADO A
INNER JOIN BI_DIM_METODO_CONTATO MT             ON A.DW_METODO_CONTATO = MT.DW_METODO_CONTATO
LEFT JOIN BI_DIM_MOTIVO_ATENDIMENTO MA          ON A.DW_MOTIVO_ATENDIMENTO = MA.DW_MOTIVO_ATENDIMENTO
LEFT JOIN BI_HIS_ASSINANTE HIS                  ON A.NUM_NTC = HIS.NUM_NTC 
                                                AND SUBSTR(TO_CHAR(A.DAT_INICIO_ATENDIMENTO,'RRRRMMDD'),1,6) = SUBSTR(TO_CHAR(HIS.DAT_MOVIMENTO,'RRRRMMDD'),1,6) 


     WHERE  A.DW_METODO_CONTATO IN (81, 411, 451,701) --EXPURGO 681 FLEX, 552 Minha Claro Empresa
     AND UPPER(A.DSC_OBSERVACAO_ATENDIMENTO) LIKE '%MINHACLAROWEB%AUTENTICA%USU%' ---SOMENTE ACESSO                                              
     AND SUBSTR(A.NUM_NTC,1,2) IN ('41','11') ---- DDD QUE A FRANCINE SOLICITOU
     AND A.DAT_INICIO_ATENDIMENTO BETWEEN TO_DATE(&DATA_INICIAL, 'RRRRMMDD') AND  TO_DATE(&DATA_FINAL, 'RRRRMMDD') 
     AND HIS.DAT_MOVIMENTO BETWEEN TO_DATE(&DATA_INICIAL, 'RRRRMMDD') AND  TO_DATE(&DATA_FINAL, 'RRRRMMDD')
     
 GROUP BY SUBSTR(TO_CHAR(A.DAT_INICIO_ATENDIMENTO, 'RRRRMMDD'),1,6) 
         ,A.NUM_NTC
         ,SUBSTR(A.NUM_NTC,1,2)
         ,HIS.COD_AREA 
         ,TO_DATE(TO_CHAR(A.DAT_INICIO_ATENDIMENTO, 'RRRRMMDD')||' '||HOR_INICIO_ATENDIMENTO,'RRRRMMDD HH24MISS')
         ,HIS.COD_PLATAFORMA_ATU
  
 ),
BASE2A AS (SELECT * FROM BASE1 WHERE ROWNUM < 201 AND COD_PLATAFORMA_ATU = 'PREPG'),
BASE2B AS (SELECT * FROM BASE1 WHERE ROWNUM < 201 AND COD_PLATAFORMA_ATU = 'AUTOC'),
BASE2C AS (SELECT * FROM BASE1 WHERE ROWNUM < 201 AND COD_PLATAFORMA_ATU 
       IN ('POSBC','POSBL','POSCM','POSCP','POSDT','POSFX','POSPG','POSRI')),

BASE3 AS (SELECT * FROM BASE2A
UNION ALL
SELECT * FROM BASE2B),

BASE4 AS (
SELECT * FROM BASE2C
UNION ALL
SELECT * FROM BASE3
)
SELECT * FROM BASE4

--========================================================================================================================
--                              PEDIDO DA FRANCINE: ACESSOS DOS CLIENTES RELATIVOS AO PS8
--  BRIEFING:
-- CLIENTES COM PLANO FAMILIA
-- DE 200 CLIENTES RELATIVO AOS
-- PRODUTOS PR� PAGO/CONTROLE/P�S
-- DDD41                            
-- 11980185386 NTC_FLEX TENTOU ACESSAR MINHA CLARO
--========================================================================================================================
---DW_NUM_CLIENTE_ATU = 291969964 --- POSSUI 6
WITH 
BASE0 AS 
(
SELECT DISTINCT NUM_CLIENTE_ATU ,NUM_NTC ,COUNT(NUM_NTC) QT_NTC 
FROM BI_HIS_ASSINANTE
 WHERE DAT_MOVIMENTO BETWEEN TO_DATE(&DATA_INICIAL, 'RRRRMMDD') AND  TO_DATE(&DATA_FINAL, 'RRRRMMDD') 
AND COD_SUB_STS_ATU = 'A'
AND COD_PLATAFORMA_ATU NOT IN 'PREPG'
---AND NUM_CLIENTE_ATU = 114173056
--AND NUM_NTC IN ('11980299093', '11976502040')
GROUP BY NUM_CLIENTE_ATU ,NUM_NTC
),
BASE1 AS
(
SELECT /*PARALELL (32)*/         
         SUBSTR(TO_CHAR(A.DAT_INICIO_ATENDIMENTO, 'RRRRMMDD'),1,6) AS SAFRA
         ,B.NUM_CLIENTE_ATU
         ,A.NUM_NTC
         ,SUBSTR(A.NUM_NTC,1,2) AS DDD
         ,HIS.COD_AREA
         ,MIN(TO_DATE(TO_CHAR(A.DAT_INICIO_ATENDIMENTO, 'RRRRMMDD')||' '||HOR_INICIO_ATENDIMENTO,'RRRRMMDD HH24MISS')) DT_PRI_ATEND
         ,HIS.COD_PLATAFORMA_ATU
         ,B.QT_NTC AS QT_LINHAS

FROM DWH.BI_FP_ASSINANTE_ATEND_FECHADO A
INNER JOIN BI_DIM_METODO_CONTATO MT             ON A.DW_METODO_CONTATO = MT.DW_METODO_CONTATO
INNER JOIN BASE0 B                              ON A.NUM_NTC = B.NUM_NTC
LEFT JOIN BI_DIM_MOTIVO_ATENDIMENTO MA          ON A.DW_MOTIVO_ATENDIMENTO = MA.DW_MOTIVO_ATENDIMENTO
LEFT JOIN BI_HIS_ASSINANTE HIS                  ON A.NUM_NTC = HIS.NUM_NTC 
                                                AND SUBSTR(TO_CHAR(A.DAT_INICIO_ATENDIMENTO,'RRRRMMDD'),1,6) = SUBSTR(TO_CHAR(HIS.DAT_MOVIMENTO,'RRRRMMDD'),1,6) 


     WHERE  A.DW_METODO_CONTATO IN (81, 411, 451,701) --EXPURGO 681 FLEX, 552 Minha Claro Empresa
     AND UPPER(A.DSC_OBSERVACAO_ATENDIMENTO) LIKE '%MINHACLAROWEB%AUTENTICA%USU%' ---SOMENTE ACESSO                                              
     AND SUBSTR(A.NUM_NTC,1,2) IN ('41','11') ---- DDD QUE A FRANCINE SOLICITOU
     AND A.DAT_INICIO_ATENDIMENTO BETWEEN TO_DATE(&DATA_INICIAL, 'RRRRMMDD') AND  TO_DATE(&DATA_FINAL, 'RRRRMMDD') 
     AND HIS.DAT_MOVIMENTO BETWEEN TO_DATE(&DATA_INICIAL, 'RRRRMMDD') AND  TO_DATE(&DATA_FINAL, 'RRRRMMDD')
     AND HIS.COD_SUB_STS_ATU = 'A' ---- SOMENTE CLIENTE ATIVOS
     AND HIS.COD_PLATAFORMA_ATU NOT IN 'PREPG'
     AND B.QT_NTC > 1 ------ SOMENTE CLIENTES QUE POSSUEM MAIS DE UM TELEFONE
    --- AND B.NUM_CLIENTE_ATU = 114173056 TEST
 GROUP BY SUBSTR(TO_CHAR(A.DAT_INICIO_ATENDIMENTO, 'RRRRMMDD'),1,6) 
         ,B.NUM_CLIENTE_ATU
         ,A.NUM_NTC
         ,SUBSTR(A.NUM_NTC,1,2)
         ,HIS.COD_AREA 
         ,TO_DATE(TO_CHAR(A.DAT_INICIO_ATENDIMENTO, 'RRRRMMDD')||' '||HOR_INICIO_ATENDIMENTO,'RRRRMMDD HH24MISS')
         ,HIS.COD_PLATAFORMA_ATU
         ,B.QT_NTC
)
SELECT DISTINCT SAFRA ,NUM_NTC ,COD_AREA ,MIN(DT_PRI_ATEND) AS DT_PRI_ATEND ,COD_PLATAFORMA_ATU ,SUM(QT_LINHAS) AS QT_LINHAS
FROM BASE1 WHERE ROWNUM < 201 AND COD_AREA = '11'
GROUP BY SAFRA ,NUM_NTC ,COD_AREA ,COD_PLATAFORMA_ATU
UNION ALL
SELECT DISTINCT SAFRA ,NUM_NTC ,COD_AREA ,MIN(DT_PRI_ATEND) AS DT_PRI_ATEND ,COD_PLATAFORMA_ATU ,SUM(QT_LINHAS) AS QT_LINHAS
FROM BASE1 WHERE ROWNUM < 201 AND COD_AREA = '41' 
GROUP BY SAFRA ,NUM_NTC ,COD_AREA ,COD_PLATAFORMA_ATU

---AND NUM_NTC IN ('11980299093', '11976502040')

--DW
