SELECT * FROM ALL_ALL_TABLES WHERE OWNER LIKE '%U92277452%'

SELECT * FROM U92277452.TMP_SQD_AA_APP_ACC
/*------------------------------------------------------------------------------------
        SQUAD AUTO ATENDIMENTO
-- TEMA :  BASE CADASTRADA
-- OBJETIVO : LEVANTAMENTO DA BASE CADASTRADA DOS USUÁRIOS DA CMV
-- RESPONSÁVEL: CARLOS TOME
-- DATA : 04/12/2019

-- TABELAS: U92047747.BUS_SQD_AA_BASE_CAD_CMV_AGG (RETIDO DIGITAL)
			U92047747.BUS_SQD_AA_BASE_CAD_CMV_DTL (RETIDO DIGITAL)
			U92277452.BUS_SQD_AA_BASE_CAD_CMV_AGG (RETIDO DIGITAL) ---dropadas em 08/05/2020
--          U92277452.BUS_SQD_AA_BASE_CAD_CMV_DTL (RETIDO DIGITAL) ---dropadas em 08/05/2020
            
            
------------------------------------------------------------------------------------*/
SELECT UNIQUE DT_REFERENCIA FROM U92047747.BUS_SQD_AA_BASE_CAD_CMV_AGG; 
SELECT UNIQUE DT_REFERENCIA FROM U92047747.BUS_SQD_AA_BASE_CAD_CMV_DTL; 

DROP TABLE TMP_SQD_AA_BASE_CAD_CMV_01;
DROP TABLE TMP_SQD_AA_BASE_CAD_CMV_02;
DROP TABLE TMP_SQD_AA_BASE_CAD_CMV_03a;
DROP TABLE TMP_SQD_AA_BASE_CAD_CMV_03B;
DROP TABLE TMP_SQD_AA_BASE_CAD_CMV_04a;
DROP TABLE TMP_SQD_AA_BASE_CAD_CMV_04;
DROP TABLE TMP_SQD_AA_BASE_CAD_CMV_05;
DROP TABLE BUS_SQD_AA_BASE_CAD_CMV_DTL;

/*--------------------------------------------------------------------------------------------------------
PRIMEIRA ETAPA :  CRIACAO TABELA TEMPORÁRIA CONTENDO SOMENTE A FOTOGRAFIA DO MÊS DE ANÁLISE (FOTOGRAFIA)
---------------------------------------------------------------------------------------------------------*/
--ETAPA 1 ACESSOS DO MÊS 
DROP TABLE TMP_SQD_AA_BASE_CAD_CMV_01;
CREATE TABLE TMP_SQD_AA_BASE_CAD_CMV_01 COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS 

SELECT /*+ PARALLEL (32)*/
         DISTINCT A.NUM_NTC,
         LAST_DAY(TO_DATE('&&DT_REFERENCIA', 'DD/MM/RRRR')) AS DT_REFERENCIA,
         LAST_DAY(TO_DATE(TO_CHAR(A.DAT_INICIO_ATENDIMENTO, 'RRRRMMDD'),'RRRRMMDD')) DT_ACESSO,
         TO_CHAR(TO_DATE(TO_CHAR(A.DAT_INICIO_ATENDIMENTO, 'RRRRMMDD'),'RRRRMMDD'),'YYYY/MM') DT_MES_ACESSO,
         1                                                 AS M_0        
 
 FROM DWH.BI_FP_ASSINANTE_ATEND_FECHADO A

     WHERE  A.DW_METODO_CONTATO IN (81, 411, 451,681,701,552,791)---791 NOVO APP INSERIDO A PARTIR DE ABRIL
     AND TO_DATE(A.DAT_INICIO_ATENDIMENTO,'DD/MM/RRRR') BETWEEN LAST_DAY(ADD_MONTHS(TO_DATE('&&DT_REFERENCIA', 'DD/MM/RRRR'),-1))+1
                                                        AND  
                                                        LAST_DAY(TO_DATE('&&DT_REFERENCIA', 'DD/MM/RRRR'))
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

--ETAPA 2 ACESSOS RETROATIVOS
DROP TABLE TMP_SQD_AA_BASE_CAD_CMV_02;
CREATE TABLE TMP_SQD_AA_BASE_CAD_CMV_02 COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS 

SELECT /*+ PARALLEL (32)*/
         DISTINCT A.NUM_NTC,
         LAST_DAY(TO_DATE('&&DT_REFERENCIA', 'DD/MM/RRRR')) AS DT_REFERENCIA,
         LAST_DAY(TO_DATE(TO_CHAR(A.DAT_INICIO_ATENDIMENTO, 'RRRRMMDD'),'RRRRMMDD')) DT_ACESSO,
         TO_CHAR(TO_DATE(TO_CHAR(A.DAT_INICIO_ATENDIMENTO, 'RRRRMMDD'),'RRRRMMDD'),'YYYY/MM') DT_MES_ACESSO
      
 
 FROM DWH.BI_FP_ASSINANTE_ATEND_FECHADO A

     WHERE  A.DW_METODO_CONTATO IN (81, 411, 451,681,701,552,791)
     AND TO_DATE(A.DAT_INICIO_ATENDIMENTO,'DD/MM/RRRR') BETWEEN LAST_DAY(ADD_MONTHS(TO_DATE('&&DT_REFERENCIA', 'DD/MM/RRRR'),-6))+1
                                                        AND  ADD_MONTHS(LAST_DAY(TO_DATE('&&DT_REFERENCIA', 'DD/MM/RRRR')),-1)


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

--ETAPA 3 03A 
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

SELECT *FROM TMP_SQD_AA_BASE_CAD_CMV_03a WHERE M_4 = 1;
;
/*--------------------------------------------------------------------------------------------------------
QUARTA ETAPA :  CRIACAO TABELA TEMPORÁRIA SUMARIZANDO OS DADOS E TRAZENDO O MAX DA DATA DO ACESSO
---------------------------------------------------------------------------------------------------------*/

--ETAPA 4 - TABELA SUMARIZADA
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
--ETAPA 5 - UNION ALL SUMARIZADAS
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
--ETAPA 6 AGG
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

-- ETAPA 7 CLASSIFICAÇÃO DOS NTCS
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


/*VALIDAÇÃO DO STATUS*/
SELECT  /*+parallel (32)*/ ------------------------------------------------------------------------ETAPA 5 CLASSIFICAÇÃO
    HIS.*
    ,CASE WHEN FL_LIGA = 1 AND FL_ACESSO = 1  THEN 'LIGA E ACESSA'
          WHEN FL_LIGA = 0 AND FL_ACESSO = 1  THEN 'SOMENTE ACESSA'
          WHEN FL_LIGA = 1 AND FL_ACESSO = 0  THEN 'SOMENTE LIGA'
          WHEN FL_LIGA = 0 AND FL_ACESSO = 0  THEN 'SEM INTERAÇÃO'
            ELSE 'XXXX' END AS STS_ATENDIMENTO
FROM TMP_SQDAA_COMP_ATM_CMV_P4B HIS
WHERE CASE WHEN FL_LIGA = 1 AND FL_ACESSO = 1  THEN 'LIGA E ACESSA'
          WHEN FL_LIGA = 0 AND FL_ACESSO = 1  THEN 'SOMENTE ACESSA'
          WHEN FL_LIGA = 1 AND FL_ACESSO = 0  THEN 'SOMENTE LIGA'
          WHEN FL_LIGA = 0 AND FL_ACESSO = 0  THEN 'SEM INTERAÇÃO'
            ELSE 'XXXX' END = 'XXXX'
;


/*--===============================================================================================================
                        CRIAÇÃO / INSERT TABELA FINAL DTL
TABELAS:                        
--    BUS_SQD_AA_BASE_CAD_CMV_DTL
---U92277452.BUS_SQD_AA_BASE_CAD_CMV_AGG (RETIDO DIGITAL)
---U92277452.BUS_SQD_AA_BASE_CAD_CMV_DTL (RETIDO DIGITAL)

--TIME PROCESSAMENTO 3743,759 SECONDS
--===============================================================================================================*/
--ETAPA 8 - FINAL DTL
DROP TABLE BUS_SQD_AA_BASE_CAD_CMV_DTL;
CREATE TABLE BUS_SQD_AA_BASE_CAD_CMV_DTL COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS 
--INSERT /*+APPEND */ INTO BUS_SQD_AA_BASE_CAD_CMV_DTL
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
   
FROM TMP_SQD_AA_BASE_CAD_CMV_05;
--COMMIT;
--UNION ALL

--CREATE TABLE BUS_SQD_AA_BASE_CAD_CMV_DTL COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS 
--SELECT * FROM BUS_SQD_AA_BASE_CAD_CMV_DTL
;

---U92277452.BUS_SQD_AA_BASE_CAD_CMV_DTL (RETIDO DIGITAL)
--GRANT SELECT ON BUS_SQD_AA_BASE_CAD_CMV_DTL TO PUBLIC;

--SELECT * FROM BUS_SQD_AA_BASE_CAD_CMV_DTL WHERE NUM_NTC = '11980299093';

/*--===============================================================================================================

      CRIAÇÃO / INSERT TABELA FINAL AGG
TABELAS: ---U92277452.BUS_SQD_AA_BASE_CAD_CMV_AGG (RETIDO DIGITAL)
         ---U92047747.BUS_SQD_AA_BASE_CAD_CMV_AGG (RETIDO DIGITAL) 
DROP TABLE BUS_SQD_AA_BASE_CAD_CMV_AGG
CREATE TABLE BUS_SQD_AA_BASE_CAD_CMV_AGG COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS 
--===============================================================================================================*/
--ETAPA 9 AGG COMPORTAMENTO DE ACESSO
--DELETE BUS_SQD_AA_BASE_CAD_CMV_AGG WHERE DT_REFERENCIA = '31/03/2020';
--SELECT * FROM BUS_SQD_AA_BASE_CAD_CMV_AGG WHERE DT_REFERENCIA = '31/03/2020';
INSERT /*+APPEND */ INTO BUS_SQD_AA_BASE_CAD_CMV_AGG
--CREATE TABLE BUS_SQD_AA_BASE_CAD_CMV_AGG_V1 COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS
SELECT /*+parallel (32)*/ 
       DT_REFERENCIA
      ,DSC_MARCA
      ,STS_ACESSO 
      ,COUNT (DISTINCT NUM_NTC) AS QT_USU_UNICOS
      
FROM BUS_SQD_AA_BASE_CAD_CMV_DTL 
WHERE DT_REFERENCIA = TO_DATE('&&DT_REFERENCIA', 'DD/MM/RRRR')
group by STS_ACESSO
      ,DSC_MARCA
      ,DT_REFERENCIA
;
COMMIT;
--UNION ALL
--SELECT * FROM BUS_SQD_AA_BASE_CAD_CMV_AGG;

--GRANT SELECT ON BUS_SQD_AA_BASE_CAD_CMV_AGG TO PUBLIC;



/*--==========================================================================================================================
                                                          
                                                          RESUMOS E TESTES
                                                          
--==========================================================================================================================*/
SELECT * FROM BUS_SQD_AA_BASE_CAD_CMV_AGG ORDER BY dt_referencia;

SELECT /*+PARALLEL (32)*/
       TO_CHAR(DT_REFERENCIA,'YYYYMM')             SAFRA
       ,STS_ACESSO
       ,COUNT(DISTINCT(NUM_NTC))                   QT_CLIENTE_UNICOS
       ,COUNT(*)                                   QT_ACESSOS
 
FROM BUS_SQD_AA_BASE_CAD_CMV_DTL A
       --U92277452.BUS_SQD_AA_BASE_CAD_CMV_DTL A
GROUP BY TO_CHAR(DT_REFERENCIA,'YYYYMM')           
       ,STS_ACESSO
  --     ,CASE WHEN STS_ACESSO LIKE '%ABANDONADOR%' THEN ACESSO_INATIVO ELSE ACESSO_ATIVO
ORDER BY TO_CHAR(DT_REFERENCIA,'YYYYMM')           
       ,STS_ACESSO;
----=================================================================================================
                                    -- ANALISE CLIENTES NOVOS
                                    --select /*+PARALLEL (30)*/ * from OPS_ALTERYX.BI_FP_BASE_ASSINANTE_CMV
----=================================================================================================
WITH
HIS_ASSINANTE AS ------------------------------------------------------------------------------ CLIENTE NOVO
(
SELECT /*+PARALLEL (32)*/ * FROM DWH.BI_HIS_ASSINANTE  
WHERE DAT_MOVIMENTO BETWEEN TO_DATE(&&DT_INI, 'RRRRMMDD') AND TO_DATE(&&DT_FIM, 'RRRRMMDD')
),

STS_NOVO AS 
(

SELECT DISTINCT  /*+PARALLEL (32)*/
        AC.DT_REFERENCIA
        ,AC.DSC_MARCA
        ,AC.NUM_NTC
        ,AC.DT_ACESSO
        ,AC.STS_ACESSO
         ,CASE WHEN AC.NUM_NTC IS NULL THEN 'NAO_LOCALIZADO'
               WHEN TO_CHAR(MAX(HIS.DAT_ATIVACAO),'YYYYMM') IS NULL THEN 'NAO_LOCALIZADO'
               WHEN TO_CHAR(MAX(HIS.DAT_ATIVACAO),'YYYYMM') =  TO_CHAR(AC.DT_REFERENCIA,'YYYYMM') THEN 'CLIENTE_NOVO'
               WHEN TO_CHAR(MAX(HIS.DAT_ATIVACAO),'YYYYMM') <  TO_CHAR(AC.DT_REFERENCIA,'YYYYMM') THEN 'CLIENTE_BASE'
               ELSE 'CLIENTE_OUTROS' 
                    END AS STS_NOVO
         

FROM BUS_SQD_AA_BASE_CAD_CMV_DTL AC
                    ---U92277452.BUS_SQD_AA_BASE_CAD_CMV_DTL AC
LEFT JOIN HIS_ASSINANTE HIS 
      ON AC.NUM_NTC = HIS.NUM_NTC
WHERE TO_CHAR(AC.DT_REFERENCIA,'YYYYMM') LIKE &&ANOMES_ATU
            AND AC.STS_ACESSO = 'NOVO'
GROUP BY AC.DT_REFERENCIA
        ,AC.DSC_MARCA
        ,AC.NUM_NTC
        ,AC.DT_ACESSO
        ,AC.STS_ACESSO)

SELECT 
  DT_REFERENCIA
  ,STS_NOVO
  ,COUNT(DISTINCT(NUM_NTC)) AS QT_NUM_NTC
FROM STS_NOVO 
GROUP BY DT_REFERENCIA
  ,STS_NOVO
;
 --- 1.352.645 JAN.2020
 --- 1.346.810
 --- 1.352.645

--------------------------------------------------------------------------------CONSULTA CLIENTES ATIVOS  NO inn
SELECT
      'CMV' AS MARCA
      ,SUBSTR (SK_DATA,1,6) AS MES_REF
      ,B.NM_AGRUPAMENTO_PLATAFORMA_BI AS PRODUTO
   
            ,SUM(QT_LINHA_ATIVA) AS TT_DOM
FROM INTMKT.FS_CONT_RAT_BAS_LINH_ATIVA_CMV A
LEFT JOIN INTMKT.DS_ATENDIMENTO_PLATAFORMA B ON A.SK_ATENDIMENTO_PLATAFORMA = B.SK_ATENDIMENTO_PLATAFORMA
WHERE SK_DATA >= 20200101
---AND SK_FC_ASSINANTE_ZB = '1971852147'
GROUP BY
      SUBSTR (SK_DATA,1,6)
      ,B.NM_AGRUPAMENTO_PLATAFORMA_BI

order by 1;

--=================================================================================================
                                    -- ANALISE ABANDONADORES
---=================================================================================================
 -- ANALISE ABANDONADORES
WITH
STS_ABANDONO AS 
(

SELECT  /*+PARALLEL (32)*/
        AC.DT_REFERENCIA
        ,AC.DSC_MARCA
        ,AC.NUM_NTC
        ,AC.DT_ACESSO
        ,AC.STS_ACESSO
        ,MAX(ST.COD_SUB_STS_ATU) AS COD_SUB_STS_ATU 

FROM U92047747.BUS_SQD_AA_BASE_CAD_CMV_DTL AC
LEFT JOIN DWH.BI_HIS_ASSINANTE HIS 
      ON AC.NUM_NTC = HIS.NUM_NTC
LEFT JOIN DWH.BI_DIM_STATUS ST
      ON ST.STS_DW = HIS.STS_DW
WHERE TO_CHAR(DT_REFERENCIA,'YYYYMM') LIKE &&ANOMES_ATU
            AND AC.STS_ACESSO LIKE '%ABAND%'
            AND DAT_MOVIMENTO BETWEEN TO_DATE(&&DT_INI, 'RRRRMMDD') AND TO_DATE(&&DT_FIM, 'RRRRMMDD')
 GROUP BY AC.DT_REFERENCIA
        ,AC.DSC_MARCA
        ,AC.NUM_NTC
        ,AC.DT_ACESSO
        ,AC.STS_ACESSO)

SELECT 
  DT_REFERENCIA
  ,STS_ACESSO
  ,COD_SUB_STS_ATU
  ,COUNT(DISTINCT(NUM_NTC)) QT_CLIENTE
  ,COUNT(*) QT_CLIENTE
FROM STS_ABANDONO
GROUP BY DT_REFERENCIA
  ,STS_ACESSO
  ,COD_SUB_STS_ATU
ORDER BY STS_ACESSO ,COD_SUB_STS_ATU
;




/*---------------------------------------VERSÃO ANTIGA------------------------------------------------------------------------------*/
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
       ,CASE /* REGRA ABANDONADOR*/
             WHEN M_0 = 1 AND FLG_NOVO = 1 THEN 'NOVO'
             WHEN M_0 = 1 AND M_1 = 1 THEN      'RECORRENTE'
             WHEN M_0 = 0 AND M_1 = 1 THEN      'ABANDONADOR -1' 
             WHEN M_0 = 0 AND M_1 = 0 AND M_2 = 1  THEN      'ABANDONADOR -2'
             WHEN M_0 = 0 AND M_1 = 0 AND M_2 = 0 AND M_3 = 1  THEN      'ABANDONADOR -3'
             WHEN M_0 = 0 AND M_1 = 0 AND M_2 = 0 AND M_3 = 0 AND M_4 = 1 THEN      'ABANDONADOR -4'
             WHEN M_0 = 0 AND M_1 = 0 AND M_2 = 0 AND M_3 = 0 AND M_4 = 0 AND M_5 = 1 THEN      'ABANDONADOR -5'
             /* REGRA RECORRENTE*/  
             WHEN M_4 = 0 AND M_3 = 0 AND M_2 = 0 AND M_1 = 1 AND M_0 = 1 THEN      'RECORRENTE -1'
             WHEN M_4 = 0 AND M_3 = 0 AND M_2 = 1 AND M_1 = 1 AND M_0 = 1 THEN      'RECORRENTE -2'
             WHEN M_4 = 0 AND M_3 = 1 AND M_2 = 1 AND M_1 = 1 AND M_0 = 1 THEN      'RECORRENTE -3'
             WHEN M_4 = 1 AND M_3 = 1 AND M_2 = 1 AND M_1 = 1 AND M_0 = 1 THEN      'RECORRENTE -4'
             WHEN M_5 = 1 AND M_4 = 1 AND M_3 = 1 AND M_2 = 1 AND M_1 = 1 AND M_0 = 1 THEN      'RECORRENTE -5'
             ELSE 'OCASIONAL'         
        END AS STS_ACESSO

FROM TMP_SQD_AA_BASE_CAD_CMV_04 ;
-- WHERE NUM_NTC = '11980299093' -- NTC_WAGNER
-- WHERE NUM_NTC = '11920001512' -- SOMENTE UM ACESSO
-- WHERE NUM_NTC = '11920015565' -- m -1 
;


-- SOMENTE VALIDAÇÕES

SELECT /*+parallel (32)*/ * FROM TMP_SQD_AA_BASE_CAD_CMV_05 
 WHERE NUM_NTC = '11980299093' -- NTC_WAGNER
-- WHERE NUM_NTC ='11920001512' -- SOMENTE UM ACESSO
-- WHERE NUM_NTC = '11920015565' -- m -1 
-- WHERE NUM_NTC = '11920015565'
;

SELECT * FROM TMP_SQD_AA_BASE_CAD_CMV_05 WHERE STS_ACESSO = 'ABANDONADOR -4' ;

/*--===============================================================================================================
                        CRIAÇÃO / INSERT TABELA FINAL DTL
TABELAS:                        
--    BUS_SQD_AA_BASE_CAD_CMV_DTL
---U92277452.BUS_SQD_AA_BASE_CAD_CMV_AGG (RETIDO DIGITAL)
---U92277452.BUS_SQD_AA_BASE_CAD_CMV_DTL (RETIDO DIGITAL)

--TIME PROCESSAMENTO 3743,759 SECONDS
--===============================================================================================================*/
--FINAL DTL
--DROP TABLE BUS_SQD_AA_BASE_CAD_CMV_DTL;
CREATE TABLE BUS_SQD_AA_BASE_CAD_CMV_DTL COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS 
--INSERT /*+APPEND */ INTO BUS_SQD_AA_BASE_CAD_CMV_DTL
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
   
FROM TMP_SQD_AA_BASE_CAD_CMV_05;

--UNION ALL

--CREATE TABLE BUS_SQD_AA_BASE_CAD_CMV_DTL COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS 
SELECT * FROM BUS_SQD_AA_BASE_CAD_CMV_DTL
;

---U92277452.BUS_SQD_AA_BASE_CAD_CMV_DTL (RETIDO DIGITAL)
GRANT SELECT ON BUS_SQD_AA_BASE_CAD_CMV_DTL TO PUBLIC;

SELECT * FROM BUS_SQD_AA_BASE_CAD_CMV_DTL WHERE NUM_NTC = '11980299093';

/*--===============================================================================================================

      CRIAÇÃO / INSERT TABELA FINAL AGG
TABELAS: ---U92277452.BUS_SQD_AA_BASE_CAD_CMV_AGG (RETIDO DIGITAL)
         ---U92047747.BUS_SQD_AA_BASE_CAD_CMV_AGG (RETIDO DIGITAL) 
DROP TABLE BUS_SQD_AA_BASE_CAD_CMV_AGG
CREATE TABLE BUS_SQD_AA_BASE_CAD_CMV_AGG COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS 
--===============================================================================================================*/
--AGG COMPORTAMENTO DE ACESSO
--DELETE BUS_SQD_AA_BASE_CAD_CMV_AGG WHERE DT_REFERENCIA = '31/03/2020';
--SELECT * FROM BUS_SQD_AA_BASE_CAD_CMV_AGG WHERE DT_REFERENCIA = '31/03/2020';

--INSERT /*+APPEND */ INTO BUS_SQD_AA_BASE_CAD_CMV_AGG
CREATE TABLE BUS_SQD_AA_BASE_CAD_CMV_AGG_V1 COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS
SELECT /*+parallel (32)*/ 
       DT_REFERENCIA
      ,DSC_MARCA
      ,STS_ACESSO 
      ,COUNT (DISTINCT NUM_NTC) AS QT_USU_UNICOS
      
FROM BUS_SQD_AA_BASE_CAD_CMV_DTL 
group by STS_ACESSO
      ,DSC_MARCA
      ,DT_REFERENCIA
UNION ALL
SELECT * FROM BUS_SQD_AA_BASE_CAD_CMV_AGG;

DROP TABLE BUS_SQD_AA_BASE_CAD_CMV_AGG_V1;


CREATE TABLE BUS_SQD_AA_BASE_CAD_CMV_AGG COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS
SELECT * FROM BUS_SQD_AA_BASE_CAD_CMV_AGG_V1;

SELECT * FROM BUS_SQD_AA_BASE_CAD_CMV_AGG;
GRANT SELECT ON BUS_SQD_AA_BASE_CAD_CMV_AGG TO PUBLIC;




      


/*MES_ATU AS

(SELECT 
 *
FROM U92277452.BUS_SQD_AA_BASE_CAD_CMV_DTL
WHERE TO_CHAR(DT_REFERENCIA,'YYYYMM') LIKE &ANOMES_ATU
      AND STS_ACESSO = 'NOVO'),

MES_ANT AS

(SELECT 
 *
FROM U92277452.BUS_SQD_AA_BASE_CAD_CMV_DTL
WHERE TO_CHAR(DT_REFERENCIA,'YYYYMM') LIKE &ANOMES_ANT)

SELECT 
      TO_CHAR(A.DT_REFERENCIA,'YYYYMM')                 SAFRA
      ,SUM(CASE WHEN A.NUM_NTC IS NULL THEN 1 ELSE 0 END) AS NOVO_BASE
      ,SUM(CASE WHEN A.


FROM MES_ATU A
LEFT JOIN MES_ANT B ON
     A.NUM_NTC = B.NUM_NTC
WHERE STS_ACESSO = 'NOVO'*/

----=================================================================================================
                                    -- ANALISE ABANDONADORES
----=================================================================================================
WITH
STS_ABANDONO AS 
(
SELECT  /*+PARALLEL (32)*/
        AC.DT_REFERENCIA
        ,AC.DSC_MARCA
        ,AC.NUM_NTC
        ,AC.DT_ACESSO
        ,AC.STS_ACESSO
        ,1 AS QT
        ,MAX(ST.COD_SUB_STS_ATU) AS COD_SUB_STS_ATU 

FROM BUS_SQD_AA_BASE_CAD_CMV_DTL AC
--        U92277452.BUS_SQD_AA_BASE_CAD_CMV_DTL AC
LEFT JOIN DWH.BI_HIS_ASSINANTE HIS 
      ON AC.NUM_NTC = HIS.NUM_NTC
LEFT JOIN DWH.BI_DIM_STATUS ST
      ON ST.STS_DW = HIS.STS_DW
WHERE TO_CHAR(DT_REFERENCIA,'YYYYMM') LIKE &ANOMES_ATU
            AND AC.STS_ACESSO LIKE '%ABAND%'
            AND DAT_MOVIMENTO BETWEEN TO_DATE(&&DT_INI, 'RRRRMMDD') AND TO_DATE(&&DT_FIM, 'RRRRMMDD')
 GROUP BY AC.DT_REFERENCIA
        ,AC.DSC_MARCA
        ,AC.NUM_NTC
        ,AC.DT_ACESSO
        ,AC.STS_ACESSO)

SELECT 
  DT_REFERENCIA
  ,STS_ACESSO
  ,COD_SUB_STS_ATU
  ,SUM(QT) AS QT_CLIENTE
FROM STS_ABANDONO
GROUP BY DT_REFERENCIA
  ,STS_ACESSO
  ,COD_SUB_STS_ATU
ORDER BY STS_ACESSO ,COD_SUB_STS_ATU
;

----=================================================================================================
                                    -- ANALISE ABANDONADORES
                                    --TESTE ZB1 E ZB2 
----=================================================================================================
WITH
STS_ABANDONO AS 
(

SELECT  /*+PARALLEL (32)*/
        AC.DT_REFERENCIA
        ,AC.DSC_MARCA
        ,AC.NUM_NTC
        ,AC.DT_ACESSO
        ,AC.STS_ACESSO
        ,1 AS QT
        ,max(ST.COD_PLATAFORMA_ANT) AS COD_PLATAFORMA_ANT
        ,max(ST.COD_PLATAFORMA_ATU) AS COD_PLATAFORMA_ATU
        ,MAX(ST.COD_SUB_STS_ATU) AS COD_SUB_STS_ATU 

FROM U92277452.BUS_SQD_AA_BASE_CAD_CMV_DTL AC
LEFT JOIN DWH.BI_HIS_ASSINANTE HIS 
      ON AC.NUM_NTC = HIS.NUM_NTC
LEFT JOIN DWH.BI_DIM_STATUS ST
      ON ST.STS_DW = HIS.STS_DW
WHERE TO_CHAR(DT_REFERENCIA,'YYYYMM') LIKE &&ANOMES_ATU
            AND AC.STS_ACESSO LIKE '%ABAND%'
            AND DAT_MOVIMENTO BETWEEN TO_DATE(&&DT_INI, 'RRRRMMDD') AND TO_DATE(&&DT_FIM, 'RRRRMMDD')
 GROUP BY AC.DT_REFERENCIA
        ,AC.DSC_MARCA
        ,AC.NUM_NTC
        ,AC.DT_ACESSO
        ,AC.STS_ACESSO)

SELECT 
  DT_REFERENCIA
  ,STS_ACESSO
  ,NUM_NTC
  ,COD_SUB_STS_ATU
  ,COD_PLATAFORMA_ANT
  ,COD_PLATAFORMA_ATU
  ,COD_SUB_STS_ATU
 --- ,SUM(QT) AS QT_CLIENTE
FROM STS_ABANDONO
WHERE COD_SUB_STS_ATU IN ('ZB1','ZB2') AND COD_PLATAFORMA_ANT <> COD_PLATAFORMA_ATU AND 
GROUP BY DT_REFERENCIA
  ,STS_ACESSO
  ,NUM_NTC
  ,COD_SUB_STS_ATU
  ,COD_PLATAFORMA_ANT
  ,COD_PLATAFORMA_ATU
  ,COD_SUB_STS_ATU
---ORDER BY STS_ACESSO ,COD_SUB_STS_ATU
;


/*SELECT \*+PARALLEL (32)*\
 COD_SUB_STS_ATU, COUNT(*)
SELECT *
  FROM DWH.BI_HIS_ASSINANTE
  WHERE TO_CHAR(DAT_MOVIMENTO,'YYYYMM') LIKE &ANOMES_ATU 
 GROUP BY COD_SUB_STS_ATU;
SELECT * FROM DWH.BI_FP_CORE_STATUS_PONTO_ASS --COD_SUB_STS COD_STS_ANT
SELECT * FROM DWH.HIS_CORE_STATUS_PONTO_ASS;



SELECT * FROM ALL_ALL_TABLES WHERE \*OWNER LIKE 'DWH' AND*\ TABLE_NAME LIKE '%HIS_CORE_STATUS_PONTO_ASS%'

SELECT * FROM DWH.BI_HIS_CORE_STATUS_PONTO_ASS; --
SELECT * FROM DWH.BI_DIM_STS_REL_ACAO_CLI_CORP;---n/a
SELECT * FROM DWH.DM_HIS_ASSINANTE; --- NÃO EXISTE
SELECT * FROM DWH.DM_FP_BASE_ASSINANTE; -- NÃO EXISTE NO DWH
SELECT * FROM DWH.BI_FP_ASS_CONTA_CMV;--- NÃO EXISTE
SELECT * FROM DWH.BI_DIM_STATUS; -- DSC_STATUS STS_DW   */ 


CREATE TABLE TMP_SQD_AA_STS_NOVO AS
SELECT DISTINCT  /*+PARALLEL (32)*/
        AC.DT_REFERENCIA
        ,AC.DSC_MARCA
        ,AC.NUM_NTC
        ,AC.DT_ACESSO
        ,AC.STS_ACESSO
      /*   ,CASE WHEN AC.NUM_NTC IS NULL THEN 'NAO_LOCALIZADO'
               WHEN TO_CHAR(MAX(HIS.DAT_ATIVACAO),'YYYYMM') IS NULL THEN 'NAO_LOCALIZADO'
               WHEN TO_CHAR(MAX(HIS.DAT_ATIVACAO),'YYYYMM') =  TO_CHAR(AC.DT_REFERENCIA,'YYYYMM') THEN 'CLIENTE_NOVO'
               WHEN TO_CHAR(MAX(HIS.DAT_ATIVACAO),'YYYYMM') <  TO_CHAR(AC.DT_REFERENCIA,'YYYYMM') THEN 'CLIENTE_BASE'
               ELSE 'CLIENTE_OUTROS' 
                    END AS STS_NOVO*/
         

FROM U92277452.BUS_SQD_AA_BASE_CAD_CMV_DTL AC
/*LEFT JOIN HIS_ASSINANTE HIS 
      ON AC.NUM_NTC = HIS.NUM_NTC*/
WHERE TO_CHAR(AC.DT_REFERENCIA,'YYYYMM') LIKE &&ANOMES_ATU
            
GROUP BY AC.DT_REFERENCIA
        ,AC.DSC_MARCA
        ,AC.NUM_NTC
        ,AC.DT_ACESSO
        ,AC.STS_ACESSO;
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
        AND TO_DATE(A.DAT_INICIO_ATENDIMENTO,'DD/MM/RRRR') BETWEEN TO_DATE('&&DATA_INICIAL', 'DD/MM/RRRR') AND  TO_DATE('&&DATA_FINAL', 'DD/MM/RRRR')
            AND 
            A.NUM_NTC = '11920001512'
          --  A.NUM_NTC = '11980299093'
         
       GROUP BY
        NUM_NTC

;

-- SELECT * FROM ALL_TAB_COLS WHERE COLUMN_NAME = 'DW_METODO_CONTATO' AND OWNER = 'DWH';
-- SELECT * FROM DWH.BI_DIM_METODO_CONTATO;



----===================================================================================== tabelas teste e agg
SELECT * FROM U92277452.BUS_SQD_AA_BASE_CAD_CMV_DTL@BASECLARO
SELECT DISTINCT 
       SUBSTR(TO_DATE(DT_REFERENCIA, 'RRRRMMDD'),1,6) SAFRA 
       ,STS_ACESSO
       ,SUM(M0_


SELECT * FROM U92277452.BUS_SQD_AA_BASE_CAD_CMV_AGG@BASECLARO;


SELECT /*+PARALLEL (32)*/ * FROM U92277452.BUS_SQD_AA_BASE_CAD_CMV_DTL WHERE NUM_NTC =17992092513; -- OCASIONAL
SELECT /*+PARALLEL (32)*/ * FROM U92277452.BUS_SQD_AA_BASE_CAD_CMV_DTL WHERE NUM_NTC =17992133545; -- OCASIONAL -- REVER
SELECT /*+PARALLEL (32)*/ * FROM U92277452.BUS_SQD_AA_BASE_CAD_CMV_DTL WHERE NUM_NTC =17992144589; -- OCASIONAL
