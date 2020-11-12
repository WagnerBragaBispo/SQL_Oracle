/*------------------------------------------------------------------------------------
        SQUAD AUTO ATENDIMENTO
-- TEMA :  BASE CADASTRADA
-- OBJETIVO : LEVANTAMENTO DA BASE CADASTRADA DOS USUÁRIOS DA CMV DO NOVO APP
-- RESPONSÁVEL: WAGNER BRAGA BISPO
-- DATA : 10/08/2020

-- TABELAS: 
            
------------------------------------------------------------------------------------*/

DROP TABLE TMP_SQDAA_COMP_AC_NOVOAPP_01;
DROP TABLE TMP_SQDAA_COMP_AC_NOVOAPP_02;
DROP TABLE TMP_SQDAA_COMP_AC_NOVOAPP_03a;
DROP TABLE TMP_SQDAA_COMP_AC_NOVOAPP_03B;
DROP TABLE TMP_SQDAA_COMP_AC_NOVOAPP_04a;
DROP TABLE TMP_SQDAA_COMP_AC_NOVOAPP_04;
DROP TABLE TMP_SQDAA_COMP_AC_NOVOAPP_05;
DROP TABLE BUS_SQDAA_COMP_AC_NOVOAPP_DTL;



/*--------------------------------------------------------------------------------------------------------
PRIMEIRA ETAPA :  CRIACAO TABELA TEMPORÁRIA CONTENDO SOMENTE A FOTOGRAFIA DO MÊS DE ANÁLISE (FOTOGRAFIA)
---------------------------------------------------------------------------------------------------------*/
--ETAPA 1 ACESSOS DO MÊS 
DROP TABLE TMP_SQDAA_COMP_AC_NOVOAPP_01;
CREATE TABLE TMP_SQDAA_COMP_AC_NOVOAPP_01 COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS 

SELECT /*+ PARALLEL (32)*/
         DISTINCT A.NUM_NTC,
         LAST_DAY(TO_DATE('&&DT_REFERENCIA', 'DD/MM/RRRR')) AS DT_REFERENCIA,
         LAST_DAY(TO_DATE(TO_CHAR(A.DAT_INICIO_ATENDIMENTO, 'RRRRMMDD'),'RRRRMMDD')) DT_ACESSO,
         TO_CHAR(TO_DATE(TO_CHAR(A.DAT_INICIO_ATENDIMENTO, 'RRRRMMDD'),'RRRRMMDD'),'YYYY/MM') DT_MES_ACESSO,
         1                                                 AS M_0        
 
 FROM DWH.BI_FP_ASSINANTE_ATEND_FECHADO A

     WHERE  A.DW_METODO_CONTATO IN (791)---791 NOVO APP INSERIDO A PARTIR DE ABRIL
	 AND A.DSC_OBSERVACAO_ATENDIMENTO LIKE '%Autenticação de usuário%' --%AUTENTICA
     AND TO_DATE(A.DAT_INICIO_ATENDIMENTO,'DD/MM/RRRR') BETWEEN LAST_DAY(ADD_MONTHS(TO_DATE('&&DT_REFERENCIA', 'DD/MM/RRRR'),-1))+1
                                                        AND  
                                                        LAST_DAY(TO_DATE('&&DT_REFERENCIA', 'DD/MM/RRRR'))
                                                        ;                                                        
        
        

------------------------------------------------------------------------------------------------------------------
/*--------------------------------------------------------------------------------------------------------
SEGUNDA ETAPA :  CRIACAO TABELA TEMPORÁRIA CONTENDO OS ACESSOS RETROATIVOS DE M-1 ATÉ M-5
---------------------------------------------------------------------------------------------------------*/

--ETAPA 2 ACESSOS RETROATIVOS
DROP TABLE TMP_SQDAA_COMP_AC_NOVOAPP_02;
CREATE TABLE TMP_SQDAA_COMP_AC_NOVOAPP_02 COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS 

SELECT /*+ PARALLEL (32)*/
         DISTINCT A.NUM_NTC,
         LAST_DAY(TO_DATE('&DT_REFERENCIA', 'DD/MM/RRRR')) AS DT_REFERENCIA,
         LAST_DAY(TO_DATE(TO_CHAR(A.DAT_INICIO_ATENDIMENTO, 'RRRRMMDD'),'RRRRMMDD')) DT_ACESSO,
         TO_CHAR(TO_DATE(TO_CHAR(A.DAT_INICIO_ATENDIMENTO, 'RRRRMMDD'),'RRRRMMDD'),'YYYY/MM') DT_MES_ACESSO
      
 
 FROM DWH.BI_FP_ASSINANTE_ATEND_FECHADO A

     WHERE  A.DW_METODO_CONTATO IN (791)---791 NOVO APP INSERIDO A PARTIR DE ABRIL
	 AND A.DSC_OBSERVACAO_ATENDIMENTO LIKE '%Autenticação de usuário%' --%AUTENTICA
     AND TO_DATE(A.DAT_INICIO_ATENDIMENTO,'DD/MM/RRRR') BETWEEN LAST_DAY(ADD_MONTHS(TO_DATE('&&DT_REFERENCIA', 'DD/MM/RRRR'),-6))+1
                                                        AND  ADD_MONTHS(LAST_DAY(TO_DATE('&&DT_REFERENCIA', 'DD/MM/RRRR')),-1)
                                                        ;

---------------------------------------------------------------------------------------------------------------------
/*--------------------------------------------------------------------------------------------------------
      TERCEIRA ETAPA :  CRIACAO TABELA TEMPORÁRIA CRUZANDOS OS DADOS DA TABELA RETROATIVA COM O MÊS DE ANÁLISE
      TRANSPONDO TAMBÉM AS COLUNAS
---------------------------------------------------------------------------------------------------------*/

--ETAPA 3 03A 
DROP TABLE TMP_SQDAA_COMP_AC_NOVOAPP_03a;
CREATE TABLE TMP_SQDAA_COMP_AC_NOVOAPP_03a COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS 

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


FROM TMP_SQDAA_COMP_AC_NOVOAPP_02 A 
LEFT JOIN TMP_SQDAA_COMP_AC_NOVOAPP_01 B ON A.NUM_NTC = B.NUM_NTC
;

/*--------------------------------------------------------------------------------------------------------
QUARTA ETAPA :  CRIACAO TABELA TEMPORÁRIA SUMARIZANDO OS DADOS E TRAZENDO O MAX DA DATA DO ACESSO
---------------------------------------------------------------------------------------------------------*/

--ETAPA 4 - TABELA SUMARIZADA
DROP TABLE TMP_SQDAA_COMP_AC_NOVOAPP_03B;
CREATE TABLE TMP_SQDAA_COMP_AC_NOVOAPP_03B COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS 

SELECT NUM_NTC
       ,DT_REFERENCIA
       ,MAX(DT_ACESSO) AS DT_ACESSO
       ,SUM(M_0)       AS M_0    
       ,SUM(M_1)       AS M_1
       ,SUM(M_2)       AS M_2
       ,SUM(M_3)       AS M_3
       ,SUM(M_4)       AS M_4
       ,SUM(M_5)       AS M_5
 

FROM TMP_SQDAA_COMP_AC_NOVOAPP_03a

GROUP BY 
       NUM_NTC
       ,DT_REFERENCIA
;
/*--------------------------------------------------------------------------------------------------------
QUINTA ETAPA :  CRIACAO TABELA TEMPORÁRIA COM A UNIAO DOS DADOS REFERENTE A M_0 E HISTORICO
---------------------------------------------------------------------------------------------------------*/
--ETAPA 5 - UNION ALL SUMARIZADAS
DROP TABLE TMP_SQDAA_COMP_AC_NOVOAPP_04a;
CREATE TABLE TMP_SQDAA_COMP_AC_NOVOAPP_04a COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS 

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
 

FROM TMP_SQDAA_COMP_AC_NOVOAPP_03B 
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
                  
FROM TMP_SQDAA_COMP_AC_NOVOAPP_01 
;

/*--------------------------------------------------------------------------------------------------------
SEXTA ETAPA :  CRIACAO TABELA TEMPORÁRIA COM AS AGREGAÇÕES
---------------------------------------------------------------------------------------------------------*/
--ETAPA 6 AGG
DROP TABLE TMP_SQDAA_COMP_AC_NOVOAPP_04;
CREATE TABLE TMP_SQDAA_COMP_AC_NOVOAPP_04 COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS 

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
 

FROM TMP_SQDAA_COMP_AC_NOVOAPP_04a
GROUP BY  NUM_NTC
      ,DT_REFERENCIA

;
----------------------------------------------------------------------------------------
/*--------------------------------------------------------------------------------------------------------
SETIMA ETAPA :  CRIACAO TABELA TEMPORÁRIA COM A CLASSIFICAÇÃO DOS NTCS
---------------------------------------------------------------------------------------------------------*/

-- ETAPA 7 CLASSIFICAÇÃO DOS NTCS
DROP TABLE TMP_SQDAA_COMP_AC_NOVOAPP_05;
CREATE TABLE TMP_SQDAA_COMP_AC_NOVOAPP_05 COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS 

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
             ELSE 'OCASIONAL'         
        END AS STS_ACESSO

FROM TMP_SQDAA_COMP_AC_NOVOAPP_04 ;


/*--===============================================================================================================
                        CRIAÇÃO / INSERT TABELA FINAL DTL
TABELAS:                        

---U92047747.BUS_SQDAA_COMP_AC_NOVOAPP_AGG 
---U92047747.BUS_SQDAA_COMP_AC_NOVOAPP_DTL 

--TIME PROCESSAMENTO 3743,759 SECONDS
--===============================================================================================================*/
--ETAPA 8 - FINAL DTL
DROP TABLE BUS_SQDAA_COMP_AC_NOVOAPP_DTL;
CREATE TABLE BUS_SQDAA_COMP_AC_NOVOAPP_DTL COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS 
---INSERT /*+APPEND */ INTO BUS_SQDAA_COMP_AC_NOVOAPP_DTL
SELECT 
   DT_REFERENCIA
   ,'CMV_NOVO_APP'        AS DSC_MARCA
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
   
FROM TMP_SQDAA_COMP_AC_NOVOAPP_05;

/*--===============================================================================================================

      CRIAÇÃO / INSERT TABELA FINAL AGG
TABELAS: 
		U92047747.BUS_SQDAA_COMP_AC_NOVOAPP_DTL
		U92047747.BUS_SQDAA_COMP_AC_NOVOAPP_AGG

--===============================================================================================================*/
--ETAPA 9 AGG COMPORTAMENTO DE ACESSO
--DELETE BUS_SQD_AA_BASE_CAD_CMV_AGG WHERE DT_REFERENCIA = '31/03/2020';
--SELECT * FROM BUS_SQD_AA_BASE_CAD_CMV_AGG WHERE DT_REFERENCIA = '31/03/2020';
--CREATE TABLE BUS_SQDAA_COMP_AC_NOVOAPP_AGG COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS

INSERT /*+APPEND */ INTO BUS_SQDAA_COMP_AC_NOVOAPP_AGG
SELECT /*+parallel (32)*/ 
       DT_REFERENCIA
      ,DSC_MARCA
      ,STS_ACESSO 
      ,COUNT (DISTINCT NUM_NTC) AS QT_USU_UNICOS
	  ,SYSDATE AS DAT_CRIACAO
      
FROM BUS_SQDAA_COMP_AC_NOVOAPP_DTL 
--WHERE DT_REFERENCIA = '30/04/2020'
group by STS_ACESSO
      ,DSC_MARCA
      ,DT_REFERENCIA
;
COMMIT;



/*--==========================================================================================================================
                                                          
                                                          RESUMOS E TESTES
                                                          
--==========================================================================================================================*/
SELECT * FROM BUS_SQDAA_COMP_AC_NOVOAPP_AGG ORDER BY dt_referencia;

----=================================================================================================
                                    -- ANALISE CLIENTES NOVOS
                                    --select /*+PARALLEL (30)*/ * from OPS_ALTERYX.BI_FP_BASE_ASSINANTE_CMV
----=================================================================================================
WITH
HIS_ASSINANTE AS ------------------------------------------------------------------------------ CLIENTE NOVO
(
SELECT /*+PARALLEL (32)*/ * FROM DWH.BI_HIS_ASSINANTE  
WHERE DAT_MOVIMENTO BETWEEN TO_DATE(&&DT_INI_YYYYMMDD, 'RRRRMMDD') AND TO_DATE(&&DT_FIM_YYYYMMDD, 'RRRRMMDD')
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
         

FROM BUS_SQDAA_COMP_AC_NOVOAPP_DTL AC
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

FROM U92047747.BUS_SQDAA_COMP_AC_NOVOAPP_DTL AC
LEFT JOIN DWH.BI_HIS_ASSINANTE HIS 
      ON AC.NUM_NTC = HIS.NUM_NTC
LEFT JOIN DWH.BI_DIM_STATUS ST
      ON ST.STS_DW = HIS.STS_DW
WHERE TO_CHAR(DT_REFERENCIA,'YYYYMM') LIKE &&ANOMES_ATU
            AND AC.STS_ACESSO LIKE '%ABAND%'
            AND DAT_MOVIMENTO BETWEEN TO_DATE(&&DT_INI_YYYYMMDD, 'RRRRMMDD') AND TO_DATE(&&DT_FIM_YYYYMMDD, 'RRRRMMDD')
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
  
FROM STS_ABANDONO
GROUP BY DT_REFERENCIA
  ,STS_ACESSO
  ,COD_SUB_STS_ATU
ORDER BY STS_ACESSO ,COD_SUB_STS_ATU
;



