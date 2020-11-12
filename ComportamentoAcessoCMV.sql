/*------------------------------------------------------------------------------------
        SQUAD AUTO ATENDIMENTO
-- TEMA :  BASE CADASTRADA
-- OBJETIVO : LEVANTAMENTO DA BASE CADASTRADA DOS USUÁRIOS DA CMV
-- RESPONSÁVEL: CARLOS TOME
-- DATA : 04/12/2019

------------------------------------------------------------------------------------*/

/*--------------------------------------------------------------------------------------------------------
PRIMEIRA ETAPA :  CRIACAO TABELA TEMPORÁRIA CONTENDO SOMENTE A FOTOGRAFIA DO MÊS DE ANÁLISE (FOTOGRAFIA)
---------------------------------------------------------------------------------------------------------*/
;
DROP TABLE TMP_SQD_AA_BASE_CAD_CMV_01;
CREATE TABLE TMP_SQD_AA_BASE_CAD_CMV_01 COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS 

SELECT /*+ PARALLEL (32)*/
         DISTINCT A.NUM_NTC,
         LAST_DAY(TO_DATE('&DT_REFERENCIA', 'DD/MM/RRRR')) AS DT_REFERENCIA,
         LAST_DAY(TO_DATE(TO_CHAR(A.DAT_INICIO_ATENDIMENTO, 'RRRRMMDD'),'RRRRMMDD')) DT_ACESSO,
         TO_CHAR(TO_DATE(TO_CHAR(A.DAT_INICIO_ATENDIMENTO, 'RRRRMMDD'),'RRRRMMDD'),'YYYY/MM') DT_MES_ACESSO,
         1                                                 AS M_0        
 
 FROM DWH.BI_FP_ASSINANTE_ATEND_FECHADO A

     WHERE  A.DW_METODO_CONTATO IN (81, 411, 451,681,701,552)
     AND TO_DATE(A.DAT_INICIO_ATENDIMENTO,'DD/MM/RRRR') BETWEEN LAST_DAY(ADD_MONTHS(TO_DATE('&DT_REFERENCIA', 'DD/MM/RRRR'),-1))+1
                                                        AND  
                                                        LAST_DAY(TO_DATE('&DT_REFERENCIA', 'DD/MM/RRRR'))
                                                        ;                                                        
     -- teste ntc_wagner
--     AND NUM_NTC = '11980299093'
     -- TESTE RENAN
   --   AND 
   --    A.NUM_NTC = '11965978509' 
   --    AND 
   --    A.NUM_NTC = '11920002117'         
        ;
        
        
-- SOMENTE VALIDAÇÕES

SELECT COUNT(*) AS QTD
       ,DT_MES_ACESSO AS MES
FROM TMP_SQD_AA_BASE_CAD_CMV_01
      GROUP BY 
       DT_MES_ACESSO
      ;

SELECT * FROM TMP_SQD_AA_BASE_CAD_CMV_01  WHERE NUM_NTC = '11980299093';




;

------------------------------------------------------------------------------------------------------------------
/*--------------------------------------------------------------------------------------------------------
SEGUNDA ETAPA :  CRIACAO TABELA TEMPORÁRIA CONTENDO OS ACESSOS RETROATIVOS DE M-1 ATÉ M-5
---------------------------------------------------------------------------------------------------------*/

DROP TABLE TMP_SQD_AA_BASE_CAD_CMV_02;
CREATE TABLE TMP_SQD_AA_BASE_CAD_CMV_02 COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS 

SELECT /*+ PARALLEL (32)*/
         DISTINCT A.NUM_NTC,
         LAST_DAY(TO_DATE('&DT_REFERENCIA', 'DD/MM/RRRR')) AS DT_REFERENCIA,
         LAST_DAY(TO_DATE(TO_CHAR(A.DAT_INICIO_ATENDIMENTO, 'RRRRMMDD'),'RRRRMMDD')) DT_ACESSO,
         TO_CHAR(TO_DATE(TO_CHAR(A.DAT_INICIO_ATENDIMENTO, 'RRRRMMDD'),'RRRRMMDD'),'YYYY/MM') DT_MES_ACESSO
      
 
 FROM DWH.BI_FP_ASSINANTE_ATEND_FECHADO A

     WHERE  A.DW_METODO_CONTATO IN (81, 411, 451,681,701,552)
     AND TO_DATE(A.DAT_INICIO_ATENDIMENTO,'DD/MM/RRRR') BETWEEN LAST_DAY(ADD_MONTHS(TO_DATE('&DT_REFERENCIA', 'DD/MM/RRRR'),-6))+1
                                                        AND  ADD_MONTHS(LAST_DAY(TO_DATE('&DT_REFERENCIA', 'DD/MM/RRRR')),-1)

                                                        ;
-- TRAZER O ADD_MONTHS - 1 A PARTIR DA DATA DE REFERENCIA DA FOTOGRAFIA


-- SOMENTE VALIDAÇÕES
SELECT -- MIN(DT_MES_ACESSO) 
*
FROM TMP_SQD_AA_BASE_CAD_CMV_02  
WHERE NUM_NTC = '11980299093'
-- WHERE NUM_NTC = '11920015565' -- SOMENTE M-1
;

SELECT *
FROM TMP_SQD_AA_BASE_CAD_CMV_02  
WHERE NUM_NTC = '21994282681';
---------------------------------------------------------------------------------------------------------------------
/*--------------------------------------------------------------------------------------------------------
TERCEIRA ETAPA :  CRIACAO TABELA TEMPORÁRIA CRUZANDOS OS DADOS DA TABELA RETROATIVA COM O MÊS DE ANÁLISE
TRANSPONDO TAMBÉM AS COLUNAS
---------------------------------------------------------------------------------------------------------*/

DROP TABLE TMP_SQD_AA_BASE_CAD_CMV_03a;
CREATE TABLE TMP_SQD_AA_BASE_CAD_CMV_03a COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS 

SELECT 
         A.NUM_NTC
         ,A.DT_REFERENCIA
         ,A.DT_ACESSO
         ,A.DT_MES_ACESSO
        -- ,NVL(B.M_0,0) AS M_0
         ,0 AS M_0
         ,CASE WHEN A.DT_ACESSO = ADD_MONTHS(A.DT_REFERENCIA,-1)  THEN '1' ELSE '0' END AS M_1 
         ,CASE WHEN A.DT_ACESSO = ADD_MONTHS(A.DT_REFERENCIA,-2)  THEN '1' ELSE '0' END AS M_2 
         ,CASE WHEN A.DT_ACESSO = ADD_MONTHS(A.DT_REFERENCIA,-3)  THEN '1' ELSE '0' END AS M_3 
         ,CASE WHEN A.DT_ACESSO = ADD_MONTHS(A.DT_REFERENCIA,-4)  THEN '1' ELSE '0' END AS M_4 
         ,CASE WHEN A.DT_ACESSO = ADD_MONTHS(A.DT_REFERENCIA,-5)  THEN '1' ELSE '0' END AS M_5 


FROM TMP_SQD_AA_BASE_CAD_CMV_02 A 
LEFT JOIN TMP_SQD_AA_BASE_CAD_CMV_01 B ON A.NUM_NTC = B.NUM_NTC
-- WHERE A.NUM_NTC = '11980299093'--'11920015565'
;


---------------- SOMENTE VALIDAÇÕES------------------------------------------------------
SELECT -- MIN (DT_ACESSO) AS TESTE
       -- ADD_MONTHS(DT_REFERENCIA,-4) TESTE2
       MIN (DT_REFERENCIA) AS DATA
FROM TMP_SQD_AA_BASE_CAD_CMV_02;

SELECT *FROM TMP_SQD_AA_BASE_CAD_CMV_03a WHERE M_6 = 1;
;
/*--------------------------------------------------------------------------------------------------------
QUARTA ETAPA :  CRIACAO TABELA TEMPORÁRIA SUMARIZANDO OS DADOS E TRAZENDO O MAX DA DATA DO ACESSO
---------------------------------------------------------------------------------------------------------*/
DROP TABLE TMP_SQD_AA_BASE_CAD_CMV_03B;
CREATE TABLE TMP_SQD_AA_BASE_CAD_CMV_03B COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS 

SELECT NUM_NTC
       ,DT_REFERENCIA
       ,MAX(DT_ACESSO) AS DT_ACESSO
       ,SUM(M_0)       AS M_0    
       ,SUM(M_1)       AS M_1
       ,SUM(M_2)       AS M_2
       ,SUM(M_3)       AS M_3
       ,SUM(M_4)       AS M_4
       ,SUM(M_5)       AS M_5
 

FROM TMP_SQD_AA_BASE_CAD_CMV_03a

GROUP BY 
       NUM_NTC
       ,DT_REFERENCIA
       ;
       
       
-- SOMENTE VALIDAÇÕES
SELECT * FROM TMP_SQD_AA_BASE_CAD_CMV_03B 
WHERE NUM_NTC = '11980299093' -- NTC_WAGNER
-- WHERE NUM_NTC ='11920015565'
;

/*--------------------------------------------------------------------------------------------------------
QUINTA ETAPA :  CRIACAO TABELA TEMPORÁRIA COM A UNIAO DOS DADOS REFERENTE A M_0 E HISTORICO
---------------------------------------------------------------------------------------------------------*/
DROP TABLE TMP_SQD_AA_BASE_CAD_CMV_04a;
CREATE TABLE TMP_SQD_AA_BASE_CAD_CMV_04a COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS 

SELECT 
       NUM_NTC
       ,DT_REFERENCIA
       ,DT_ACESSO
       ,M_0
       ,M_1
       ,M_2
       ,M_3
       ,M_4
       ,M_5
 

FROM TMP_SQD_AA_BASE_CAD_CMV_03B 
-- WHERE NUM_NTC = '11980299093' -- NTC_WAGNER
-- WHERE NUM_NTC ='11920015565'

UNION ALL

SELECT 
       NUM_NTC
      ,DT_REFERENCIA
      ,DT_ACESSO
      ,M_0
      ,0 AS M_1
      ,0 AS M_2
      ,0 AS M_3
      ,0 AS M_4
      ,0 AS M_5
                  
FROM TMP_SQD_AA_BASE_CAD_CMV_01 
-- WHERE NUM_NTC = '11980299093' -- NTC_WAGNER 
-- WHERE NUM_NTC ='11920015565'
;

-- SOMENTE VALIDAÇÕES
SELECT * FROM TMP_SQD_AA_BASE_CAD_CMV_04a
 WHERE NUM_NTC = '11980299093' -- NTC_WAGNER 
;

/*--------------------------------------------------------------------------------------------------------
SEXTA ETAPA :  CRIACAO TABELA TEMPORÁRIA COM AS AGREGAÇÕES
---------------------------------------------------------------------------------------------------------*/
DROP TABLE TMP_SQD_AA_BASE_CAD_CMV_04;
CREATE TABLE TMP_SQD_AA_BASE_CAD_CMV_04 COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS 

SELECT 
      NUM_NTC
      ,DT_REFERENCIA
      ,MAX(DT_ACESSO) AS DT_ACESSO
      ,SUM(M_0)       AS M_0
      ,SUM(M_1)       AS M_1
      ,SUM(M_2)       AS M_2
      ,SUM(M_3)       AS M_3
      ,SUM(M_4)       AS M_4
      ,SUM(M_5)       AS M_5
      ,SUM(M_0+M_1+M_2+M_3+M_4+M_5) AS FLG_NOVO
 

FROM TMP_SQD_AA_BASE_CAD_CMV_04a
GROUP BY  NUM_NTC
      ,DT_REFERENCIA

;

-- SOMENTE VALIDAÇÕES
SELECT * FROM TMP_SQD_AA_BASE_CAD_CMV_04 WHERE FLG_NOVO >1 AND M_0 = 1
-- WHERE NUM_NTC = '11980299093' -- NTC_WAGNER
;
-- SELECT * FROM TMP_SQD_AA_BASE_CAD_CMV_04 WHERE M_4 = 1;
----------------------------------------------------------------------------------------
/*--------------------------------------------------------------------------------------------------------
SETIMA ETAPA :  CRIACAO TABELA TEMPORÁRIA COM A CLASSIFICAÇÃO DOS NTCS
---------------------------------------------------------------------------------------------------------*/
DROP TABLE TMP_SQD_AA_BASE_CAD_CMV_05;
CREATE TABLE TMP_SQD_AA_BASE_CAD_CMV_05 COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS 

SELECT 

       NUM_NTC
       ,DT_REFERENCIA
       ,DT_ACESSO
       ,M_0
       ,M_1
       ,M_2
       ,M_3
       ,M_4
       ,M_5
       ,FLG_NOVO
       ,CASE 
             WHEN M_0 = 1 AND FLG_NOVO = 1 THEN 'NOVO'
             WHEN M_0 = 1 AND FLG_NOVO > 1  THEN 'ACESSOU - ' || TO_CHAR(FLG_NOVO) 

       /* REGRA ABANDONADOR*/ 
             WHEN M_0 = 0 AND M_1 = 1 THEN      'ABANDONADOR -1' 
             WHEN M_0 = 0 AND M_1 = 0 AND M_2 = 1  THEN      'ABANDONADOR -2'
             WHEN M_0 = 0 AND M_1 = 0 AND M_2 = 0 AND M_3 = 1  THEN      'ABANDONADOR -3'
             WHEN M_0 = 0 AND M_1 = 0 AND M_2 = 0 AND M_3 = 0 AND M_4 = 1 THEN      'ABANDONADOR -4'
             WHEN M_0 = 0 AND M_1 = 0 AND M_2 = 0 AND M_3 = 0 AND M_4 = 0 AND M_5 = 1 THEN      'ABANDONADOR -5'
              
       --      WHEN M_4 = 0 AND M_3 = 0 AND M_2 = 0 AND M_1 = 1 AND M_0 = 1 THEN      'RECORRENTE -1'
       --      WHEN M_4 = 0 AND M_3 = 0 AND M_2 = 1 AND M_1 = 1 AND M_0 = 1 THEN      'RECORRENTE -2'
       --      WHEN M_4 = 0 AND M_3 = 1 AND M_2 = 1 AND M_1 = 1 AND M_0 = 1 THEN      'RECORRENTE -3'
       --      WHEN M_4 = 1 AND M_3 = 1 AND M_2 = 1 AND M_1 = 1 AND M_0 = 1 THEN      'RECORRENTE -4'
           --  WHEN M_5 = 1 AND M_4 = 1 AND M_3 = 1 AND M_2 = 1 AND M_1 = 1 AND M_0 = 1 THEN      'RECORRENTE -5'
             ELSE 'OCASIONAL'         
        END AS STS_ACESSO

FROM TMP_SQD_AA_BASE_CAD_CMV_04 ;
-- WHERE NUM_NTC = '11980299093' -- NTC_WAGNER
-- WHERE NUM_NTC = '11920001512' -- SOMENTE UM ACESSO
-- WHERE NUM_NTC = '11920015565' -- m -1 
;


-- SOMENTE VALIDAÇÕES

SELECT /*+parallel (32)*/ * FROM TMP_SQD_AA_BASE_CAD_CMV_05 WHERE STS_ACESSO IN ('ABANDONADOR -4');
SELECT /*+parallel (32)*/ * FROM TMP_SQD_AA_BASE_CAD_CMV_05 WHERE STS_ACESSO = 'ACESSOU - 3'-- NOT IN ('NOVO','OCASIONAL')
-- WHERE NUM_NTC = '11980299093' -- NTC_WAGNER
-- WHERE NUM_NTC ='11920001512' -- SOMENTE UM ACESSO
-- WHERE NUM_NTC = '11920015565' -- m -1 
-- WHERE NUM_NTC = '11920015565'
;

SELECT * FROM TMP_SQD_AA_BASE_CAD_CMV_05 WHERE STS_ACESSO = 'ABANDONADOR -4' ;

---------------------------------------------------------------------------------------------------
-- CRIAÇÃO / INSERT TABELA FINAL DTL --------------------------------------------------------------
-- DROP TABLE BUS_SQD_AA_BASE_CAD_CMV_DTL;
-- CREATE TABLE BUS_SQD_AA_BASE_CAD_CMV_DTL COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS 
-- TRUNCATE TABLE BUS_SQD_AA_BASE_CAD_CMV_DTL DROP STORAGE;
INSERT /*+APPEND */ INTO BUS_SQD_AA_BASE_CAD_CMV_DTL
SELECT 
   DT_REFERENCIA
   ,'CMV'        AS DSC_MARCA
   ,NUM_NTC
   ,DT_ACESSO
   ,M_0
   ,M_1
   ,M_2
   ,M_3
   ,M_4
   ,M_5   
   ,FLG_NOVO
   ,STS_ACESSO
   
FROM TMP_SQD_AA_BASE_CAD_CMV_05

;

GRANT SELECT ON BUS_SQD_AA_BASE_CAD_CMV_DTL TO PUBLIC;


SELECT * FROM BUS_SQD_AA_BASE_CAD_CMV_DTL WHERE NUM_NTC = '11980299093';

DROP TABLE TMP_SQD_AA_BASE_CAD_CMV_01;
DROP TABLE TMP_SQD_AA_BASE_CAD_CMV_02;
DROP TABLE TMP_SQD_AA_BASE_CAD_CMV_03a;
DROP TABLE TMP_SQD_AA_BASE_CAD_CMV_03b;
DROP TABLE TMP_SQD_AA_BASE_CAD_CMV_04a;
DROP TABLE TMP_SQD_AA_BASE_CAD_CMV_04;
DROP TABLE TMP_SQD_AA_BASE_CAD_CMV_05;

---------------------------------------------------------------------------------------------------
-- CRIAÇÃO / INSERT TABELA FINAL AGG --------------------------------------------------------------
-- DROP TABLE BUS_SQD_AA_BASE_CAD_CMV_AGG
CREATE TABLE BUS_SQD_AA_BASE_CAD_CMV_AGG COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS 

-- INSERT /*+APPEND */ INTO BUS_SQD_AA_BASE_CAD_CMV_AGG
;
SELECT /*+parallel (32)*/ 
       DT_REFERENCIA
      ,DSC_MARCA
      ,STS_ACESSO 
      ,COUNT (DISTINCT NUM_NTC) AS QTD_ACESSOS
      
FROM BUS_SQD_AA_BASE_CAD_CMV_DTL 
group by STS_ACESSO
      ,DSC_MARCA
      ,DT_REFERENCIA
;




SELECT * FROM BUS_SQD_AA_BASE_CAD_CMV_AGG;

GRANT SELECT ON BUS_SQD_AA_BASE_CAD_CMV_AGG TO PUBLIC;




---------------------------------------------------------------------------------------------
-- somente testes --------------------------------------------
SELECT 
NUM_NTC
,DT_REFERENCIA
-- DT_ACESSO
-- DT_MES_ACESSO
,M_0
-- NUM_NTC_2
-- DT_REFERENCIA_2
,MAX(DT_ACESSO) AS DT_ACESSO
,DT_MES_ACESSO AS DT_MES_ACESSO
,SUM(M_1) AS M_1
,SUM(M_2) AS M_2
,SUM(M_3) AS M_3
,SUM(M_4) AS M_4
,SUM(M_5) AS M_5 

FROM TMP_SQD_AA_BASE_CAD_CMV_03
GROUP BY 
NUM_NTC
,DT_REFERENCIA
,M_0
,DT_MES_ACESSO
;




---------------------------------------------------------------------------------------------------------------------

SELECT NUM_NTC
       ,DT_ACESSO
       ,DT_REFERENCIA
       ,CASE WHEN DT_ACESSO = DT_REFERENCIA  THEN '1' ELSE '0' END AS M_0 
       ,CASE WHEN DT_ACESSO = ADD_MONTHS(DT_REFERENCIA,-1)  THEN '1' ELSE '0' END AS M_1 
       ,CASE WHEN DT_ACESSO = ADD_MONTHS(DT_REFERENCIA,-2)  THEN '1' ELSE '0' END AS M_2 
       ,CASE WHEN DT_ACESSO = ADD_MONTHS(DT_REFERENCIA,-3)  THEN '1' ELSE '0' END AS M_3 
       ,CASE WHEN DT_ACESSO = ADD_MONTHS(DT_REFERENCIA,-4)  THEN '1' ELSE '0' END AS M_4 
       ,CASE WHEN DT_ACESSO = ADD_MONTHS(DT_REFERENCIA,-5)  THEN '1' ELSE '0' END AS M_5 
 FROM TMP_SQD_AA_BASE_CAD_CMV_01
-- FROM TMP_SQD_AA_BASE_CAD_CMV_02 
-- WHERE NUM_NTC = '11980299093'
-- WHERE NUM_NTC ='11965978509'
-- WHERE NUM_NTC = '11920001512' -- SOMENTE UM ACESSO
-- WHERE NUM_NTC = '11920002117'
-- WHERE NUM_NTC = '11920015565' -- m -1 
;


------------------------------------------------------------------------------------------------------------------
SELECT 
         NUM_NTC
         ,DT_REFERENCIA
         ,DT_ACESSO
         ,DT_MES_ACESSO
         ,ADD_MONTHS(DT_REFERENCIA,-1) AS M_1
        ,CASE 
              WHEN DT_ACESSO = ADD_MONTHS(DT_REFERENCIA,-5) THEN 'ABANDONADOR -5'
              WHEN DT_ACESSO = ADD_MONTHS(DT_REFERENCIA,-4) THEN 'ABANDONADOR -4'                         
              WHEN DT_ACESSO = ADD_MONTHS(DT_REFERENCIA,-3) THEN 'ABANDONADOR -3' 
              WHEN DT_ACESSO = ADD_MONTHS(DT_REFERENCIA,-2) THEN 'ABANDONADOR -2'   
              WHEN DT_ACESSO = ADD_MONTHS(DT_REFERENCIA,-1) THEN 'ABANDONADOR -1'                     
              WHEN DT_ACESSO BETWEEN  ADD_MONTHS(DT_REFERENCIA,-1) AND DT_REFERENCIA THEN 'RECORRENTE' 
             
        ELSE 'N/A' 
        END FILTRO

FROM TMP_SQD_AA_BASE_CAD_CMV_01 A
-- WHERE a.NUM_NTC = '11980299093' -- ntc wagner
-- WHERE NUM_NTC = '11965978509' -- ntc renan
 WHERE NUM_NTC = '11920015565' -- acessou m-1
-- WHERE NUM_NTC = '11920002117'
-- WHERE NUM_NTC = '11920001512'-- SOMENTE UM ACESSO


;


SELECT *

FROM TMP_SQD_AA_BASE_CAD_CMV_01 
WHERE NUM_NTC = '11920001512' -- SOMENTE UM ACESSO
-- WHERE NUM_NTC = '11980299093'
-- WHERE NUM_NTC = '11920015565'

;


----------------

SELECT COUNT (*) AS QTD 
       ,NUM_NTC
FROM TMP_SQD_AA_BASE_CAD_CMV_01 
WHERE DT_REFERENCIA = DT_ACESSO
GROUP BY NUM_NTC
HAVING COUNT (*) = 1
;

SELECT MIN(DT_ACESSO) AS DT_INI
       ,MAX(DT_ACESSO) AS DT_FIM
       ,NUM_NTC
FROM TMP_SQD_AA_BASE_CAD_CMV_01 
-- WHERE DT_REFERENCIA = DT_ACESSO    
GROUP BY NUM_NTC;   
       



-----------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------
SELECT /*+ PARALLEL (32)*/
         A.NUM_NTC                         as NM_LOGIN,
        MIN(TO_DATE(TO_CHAR(A.DAT_INICIO_ATENDIMENTO, 'RRRRMMDD')||' '||A.HOR_INICIO_ATENDIMENTO,'RRRRMMDD HH24MISS')) DT_PRIMEIRO_ACSS,
        MAX(TO_DATE(TO_CHAR(A.DAT_INICIO_ATENDIMENTO, 'RRRRMMDD')||' '||A.HOR_INICIO_ATENDIMENTO,'RRRRMMDD HH24MISS')) DT_ULTIMO_ACSS
       
     FROM DWH.BI_FP_ASSINANTE_ATEND_FECHADO A
     LEFT JOIN DWH.BI_DIM_METODO_CONTATO B ON A.DW_METODO_CONTATO = B.DW_METODO_CONTATO

       WHERE  A.DW_METODO_CONTATO IN (81, 411, 451,681,701,552)
        AND TO_DATE(A.DAT_INICIO_ATENDIMENTO,'DD/MM/RRRR') BETWEEN TO_DATE('&DATA_INICIAL', 'DD/MM/RRRR') AND  TO_DATE('&DATA_FINAL', 'DD/MM/RRRR')
            AND 
            A.NUM_NTC = '11920001512'
          --  A.NUM_NTC = '11980299093'
         
       GROUP BY
        NUM_NTC

;

-- SELECT * FROM ALL_TAB_COLS WHERE COLUMN_NAME = 'DW_METODO_CONTATO' AND OWNER = 'DWH';
-- SELECT * FROM DWH.BI_DIM_METODO_CONTATO;
