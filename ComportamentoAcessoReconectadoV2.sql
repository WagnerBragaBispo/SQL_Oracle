/*--===============================================================================================================================
            CRIAÇÃO DAS TABELAS - WAGNER 
	OJETIVO: Comportamento de acessos + reconectados - MCM

	TABELAS: U92047747.BI_FT_SQDAA_RET_DIG
			 U92047747.BI_AGG_SQDAA_RET_DIG	
			 
			 
			 Critérios: 
					Status Conectado são os clientes que mantiveram pelo menos 1 acesso em cada mês da safra, ou seja, mês Atual (M0) e mês Anterior (M-1);

					Status Desconectado são os clientes com pelo menos 1 acesso no mês Anterior (M-1), mas não tiveram acesso no mês Atual (M0);

					Status Reconectado são os clientes com pelo menos 1 acesso no mês Atual (M0), mas não tiveram acesso no mês Anterior (M-1).

--===============================================================================================================================*/

---AND MC.NUM_NTC = '19994633200' ---ACESSO EM M0 29/06/20 
---AND MC.NUM_NTC = '89994080623' ---ACESSO EM M-1 01/05/20 E 29/06/20
---NUM_NTC = '11999068012' --DESCONECTADO -1; 

DROP TABLE TMP_SQDAA_MCM_REC_01A;
DROP TABLE TMP_SQDAA_MCM_REC_01B;
DROP TABLE TMP_SQDAA_MCM_REC_01C;
DROP TABLE TMP_SQDAA_MCM_REC_01D;
DROP TABLE TMP_SQDAA_MCM_REC_02A;
DROP TABLE TMP_SQDAA_MCM_REC_02B;
DROP TABLE TMP_SQDAA_MCM_REC_02C;
DROP TABLE TMP_SQDAA_MCM_REC_02D;
DROP TABLE TMP_SQDAA_MCM_REC_03A;
DROP TABLE TMP_SQDAA_MCM_REC_03B;
DROP TABLE TMP_SQDAA_MCM_REC_04;
DROP TABLE TMP_SQDAA_MCM_REC_05;
DROP TABLE FT_SQDAA_MCM_ACESSO_MOV;


/*--------------------------------------------------------------------------------------------------------
PRIMEIRA ETAPA :  CRIACAO TABELA TEMPORÁRIA CONTENDO SOMENTE A FOTOGRAFIA DO MÊS DE ANÁLISE (FOTOGRAFIA)
---------------------------------------------------------------------------------------------------------*/

-- ETAPA 1A ACESSO_M0 (ANTIGO APP + SITE)
DROP TABLE TMP_SQDAA_MCM_REC_01A;
CREATE TABLE TMP_SQDAA_MCM_REC_01A COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS
SELECT /*+ PARALLEL (32)*/
         DISTINCT MC.NUM_NTC
         ,LAST_DAY(TO_DATE('&&DT_REFERENCIA', 'DD/MM/RRRR')) AS DT_REFERENCIA
         ,LAST_DAY(TO_DATE(TO_CHAR(MC.DAT_INICIO_ATENDIMENTO, 'RRRRMMDD'),'RRRRMMDD')) AS DT_ACESSO
         ,TO_CHAR(TO_DATE(TO_CHAR(MC.DAT_INICIO_ATENDIMENTO, 'RRRRMMDD'),'RRRRMMDD'),'YYYY/MM') AS DT_MES_ACESSO
         ,1                                                 AS M_0        
 
     FROM DWH.BI_FP_ASSINANTE_ATEND_FECHADO MC
	 WHERE  MC.DW_METODO_CONTATO IN (81, 411, 451,701)-------EXPURDO 681 App Flex 552 Minha Claro Empresa  
			AND MC.DSC_OBSERVACAO_ATENDIMENTO LIKE '%Autenticação de usuário%' --%AUTENTICA
			AND MC.DAT_INICIO_ATENDIMENTO BETWEEN TO_DATE(&&DT_INI_YYYYMMDD, 'RRRRMMDD') AND  TO_DATE(&&DT_FIM_YYYYMMDD, 'RRRRMMDD');


-- ETAPA 1B ACESSO_M0 (NOVO APP)
DROP TABLE TMP_SQDAA_MCM_REC_01B;
CREATE TABLE TMP_SQDAA_MCM_REC_01B COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS
SELECT /*+ PARALLEL (32)*/
         DISTINCT MC.NUM_NTC
         ,LAST_DAY(TO_DATE('&&DT_REFERENCIA', 'DD/MM/RRRR')) AS DT_REFERENCIA
         ,LAST_DAY(TO_DATE(TO_CHAR(MC.DAT_INICIO_ATENDIMENTO, 'RRRRMMDD'),'RRRRMMDD')) AS DT_ACESSO
         ,TO_CHAR(TO_DATE(TO_CHAR(MC.DAT_INICIO_ATENDIMENTO, 'RRRRMMDD'),'RRRRMMDD'),'YYYY/MM') AS DT_MES_ACESSO
         ,1                                                 AS M_0        
FROM DWH.BI_FP_ASSINANTE_ATEND_FECHADO MC 
WHERE MC.DW_METODO_CONTATO IN (791)     -------NOVO APP MINHA CLARO MÓVEL  
			AND MC.DSC_OBSERVACAO_ATENDIMENTO LIKE '%Autenticação de usuário%' --%AUTENTICA
			AND MC.DAT_INICIO_ATENDIMENTO BETWEEN TO_DATE(&&DT_INI_YYYYMMDD, 'RRRRMMDD') AND  TO_DATE(&&DT_FIM_YYYYMMDD, 'RRRRMMDD');
 
-- ETAPA 1C UNIFICAR OS DOIS ACESSOS
DROP TABLE TMP_SQDAA_MCM_REC_01C;
CREATE TABLE TMP_SQDAA_MCM_REC_01C COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS
SELECT * FROM TMP_SQDAA_MCM_REC_01A
UNION ALL
SELECT * FROM TMP_SQDAA_MCM_REC_01B;

-- ETAPA 1D REMOVER OS DUPLICADOS
DROP TABLE TMP_SQDAA_MCM_REC_01D;
CREATE TABLE TMP_SQDAA_MCM_REC_01D COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS
SELECT /*+ PARALLEL (32)*/ DISTINCT 
          NUM_NTC
          ,DT_REFERENCIA
          ,MIN(DT_ACESSO) AS DT_ACESSO
          ,DT_MES_ACESSO
          ,M_0   
FROM TMP_SQDAA_MCM_REC_01C
GROUP BY NUM_NTC
          ,DT_REFERENCIA
          ,DT_MES_ACESSO
          ,M_0;

/*--------------------------------------------------------------------------------------------------------
SEGUNDA ETAPA :  CRIACAO TABELA TEMPORÁRIA CONTENDO OS ACESSOS RETROATIVOS DE M-1
---------------------------------------------------------------------------------------------------------*/
-- ETAPA 2A ACESSO_M-1 (ANTIGO APP + SITE)
DROP TABLE TMP_SQDAA_MCM_REC_02A;
CREATE TABLE TMP_SQDAA_MCM_REC_02A COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS
SELECT /*+ PARALLEL (32)*/
         DISTINCT MC.NUM_NTC
         ,LAST_DAY(TO_DATE('&&DT_REFERENCIA', 'DD/MM/RRRR')) AS DT_REFERENCIA
         ,LAST_DAY(TO_DATE(TO_CHAR(MC.DAT_INICIO_ATENDIMENTO, 'RRRRMMDD'),'RRRRMMDD')) DT_ACESSO
         ,TO_CHAR(TO_DATE(TO_CHAR(MC.DAT_INICIO_ATENDIMENTO, 'RRRRMMDD'),'RRRRMMDD'),'YYYY/MM') DT_MES_ACESSO
         ,1                                                 AS M_0        
     FROM DWH.BI_FP_ASSINANTE_ATEND_FECHADO MC
	 WHERE  MC.DW_METODO_CONTATO IN (81, 411, 451,701)-------EXPURDO 681 App Flex 552 Minha Claro Empresa  
		 AND UPPER(MC.DSC_OBSERVACAO_ATENDIMENTO) LIKE '%AUTENTICAÇÃO DE USUÁRIO%'
		 AND TO_DATE(MC.DAT_INICIO_ATENDIMENTO,'DD/MM/RRRR') BETWEEN LAST_DAY(ADD_MONTHS(TO_DATE('&&DT_REFERENCIA', 'DD/MM/RRRR'),-2))+1
																AND  ADD_MONTHS(LAST_DAY(TO_DATE('&&DT_REFERENCIA', 'DD/MM/RRRR')),-1)
														;
-- ETAPA 2B ACESSO_M-1 (NOVO APP)
DROP TABLE TMP_SQDAA_MCM_REC_02B;
CREATE TABLE TMP_SQDAA_MCM_REC_02B COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS
SELECT /*+ PARALLEL (32)*/
         DISTINCT MC.NUM_NTC
         ,LAST_DAY(TO_DATE('&&DT_REFERENCIA', 'DD/MM/RRRR')) AS DT_REFERENCIA
         ,LAST_DAY(TO_DATE(TO_CHAR(MC.DAT_INICIO_ATENDIMENTO, 'RRRRMMDD'),'RRRRMMDD')) DT_ACESSO
         ,TO_CHAR(TO_DATE(TO_CHAR(MC.DAT_INICIO_ATENDIMENTO, 'RRRRMMDD'),'RRRRMMDD'),'YYYY/MM') DT_MES_ACESSO
         ,1                                                 AS M_0        
     FROM DWH.BI_FP_ASSINANTE_ATEND_FECHADO MC
	 WHERE  MC.DW_METODO_CONTATO IN (791)-------EXPURDO 681 App Flex 552 Minha Claro Empresa  
	 AND UPPER(MC.DSC_OBSERVACAO_ATENDIMENTO) LIKE '%AUTENTICAÇÃO DE USUÁRIO%'
     AND TO_DATE(MC.DAT_INICIO_ATENDIMENTO,'DD/MM/RRRR') BETWEEN LAST_DAY(ADD_MONTHS(TO_DATE('&&DT_REFERENCIA', 'DD/MM/RRRR'),-2))+1
															AND  ADD_MONTHS(LAST_DAY(TO_DATE('&&DT_REFERENCIA', 'DD/MM/RRRR')),-1)
														;
-- ETAPA 2C UNIFICAR OS DOIS ACESSOS
DROP TABLE TMP_SQDAA_MCM_REC_02C;
CREATE TABLE TMP_SQDAA_MCM_REC_02C COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS
SELECT * FROM TMP_SQDAA_MCM_REC_02A
UNION ALL
SELECT * FROM TMP_SQDAA_MCM_REC_02B;

-- ETAPA 2D REMOVER OS DUPLICADOS
DROP TABLE TMP_SQDAA_MCM_REC_02D;
CREATE TABLE TMP_SQDAA_MCM_REC_02D COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS
SELECT /*+ PARALLEL (32)*/ DISTINCT 
          NUM_NTC
          ,DT_REFERENCIA
          ,MIN(DT_ACESSO) AS DT_ACESSO
          ,DT_MES_ACESSO
          ,M_0   
FROM TMP_SQDAA_MCM_REC_02C
GROUP BY NUM_NTC
          ,DT_REFERENCIA
          ,DT_MES_ACESSO
          ,M_0;



/*--------------------------------------------------------------------------------------------------------
TERCEIRA ETAPA :  CRIACAO TABELA TEMPORÁRIA CRUZANDOS OS DADOS DA TABELA RETROATIVA COM O MÊS DE ANÁLISE
      TRANSPONDO TAMBÉM AS COLUNAS
---------------------------------------------------------------------------------------------------------*/
-- ETAPA 3 
DROP TABLE TMP_SQDAA_MCM_REC_03A;
CREATE TABLE TMP_SQDAA_MCM_REC_03A COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS
SELECT 
         A.NUM_NTC
         ,A.DT_REFERENCIA
         ,A.DT_ACESSO
         ,A.DT_MES_ACESSO
        -- ,NVL(B.M_0,0) AS M_0
         ,0 AS M_0
         ,CASE WHEN A.DT_ACESSO = ADD_MONTHS(A.DT_REFERENCIA,-1)  THEN '1' ELSE '0' END AS M_1 
         
	 FROM TMP_SQDAA_MCM_REC_02D A 
LEFT JOIN TMP_SQDAA_MCM_REC_01D B ON A.NUM_NTC = B.NUM_NTC
;

/*--------------------------------------------------------------------------------------------------------
QUARTA ETAPA :  CRIACAO TABELA TEMPORÁRIA SUMARIZANDO OS DADOS E TRAZENDO O MAX DA DATA DO ACESSO
---------------------------------------------------------------------------------------------------------*/

--ETAPA 4 - TABELA SUMARIZADA
DROP TABLE TMP_SQDAA_MCM_REC_03B;
CREATE TABLE TMP_SQDAA_MCM_REC_03B COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS 
SELECT NUM_NTC
       ,DT_REFERENCIA
       ,MAX(DT_ACESSO) AS DT_ACESSO
       ,SUM(M_0)       AS M_0    
       ,SUM(M_1)       AS M_1      
FROM TMP_SQDAA_MCM_REC_03A
GROUP BY 
       NUM_NTC
       ,DT_REFERENCIA
;

/*--------------------------------------------------------------------------------------------------------
QUINTA ETAPA :  CRIACAO TABELA TEMPORÁRIA COM A UNIAO DOS DADOS REFERENTE A M_0 E HISTORICO
---------------------------------------------------------------------------------------------------------*/
--ETAPA 5 - UNION ALL SUMARIZADAS
DROP TABLE TMP_SQDAA_MCM_REC_04;
CREATE TABLE TMP_SQDAA_MCM_REC_04 COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS 
SELECT 
       NUM_NTC
       ,DT_REFERENCIA
       ,DT_ACESSO
       ,M_0
       ,M_1     
FROM TMP_SQDAA_MCM_REC_03B 

UNION ALL

SELECT 
       NUM_NTC
      ,DT_REFERENCIA
      ,DT_ACESSO
      ,M_0
      ,0 AS M_1     
FROM TMP_SQDAA_MCM_REC_01D 
;

/*--------------------------------------------------------------------------------------------------------
SEXTA ETAPA :  CRIACAO TABELA TEMPORÁRIA COM AS AGREGAÇÕES
---------------------------------------------------------------------------------------------------------*/
--ETAPA 6 AGG
DROP TABLE TMP_SQDAA_MCM_REC_05;
CREATE TABLE TMP_SQDAA_MCM_REC_05 COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS 
SELECT 
      NUM_NTC
      ,DT_REFERENCIA
      ,MAX(DT_ACESSO) AS DT_ACESSO
      ,SUM(M_0)       AS M_0
      ,SUM(M_1)       AS M_1    
      ,SUM(M_0+M_1) AS CONECTADO

FROM TMP_SQDAA_MCM_REC_04
GROUP BY  NUM_NTC
      ,DT_REFERENCIA
;
----------------------------------------------------------------------------------------
/*--------------------------------------------------------------------------------------------------------
SETIMA ETAPA :  CRIACAO TABELA TEMPORÁRIA COM A CLASSIFICAÇÃO DOS NTCS
---------------------------------------------------------------------------------------------------------*/
-- ETAPA 7 CLASSIFICAÇÃO DOS NTCS
DROP TABLE FT_SQDAA_MCM_ACESSO_MOV;
--INSERT /*+APPEND */ INTO FT_SQDAA_MCM_ACESSO_MOV COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS 
CREATE TABLE FT_SQDAA_MCM_ACESSO_MOV COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS 
SELECT 
       NUM_NTC
       ,DT_REFERENCIA
       ,DT_ACESSO
       ,M_0
       ,M_1
       ,CONECTADO
       ,CASE 
             WHEN M_0 = 1 AND CONECTADO = 1 THEN 'RECONECTADO'
             WHEN M_0 = 1 AND CONECTADO > 1  THEN 'CONECTATADO - ' || TO_CHAR(CONECTADO) 

       /* REGRA ABANDONADOR*/ 
             WHEN M_0 = 0 AND M_1 = 1 THEN      'DESCONECTADO'             
             ELSE 'OCASIONAL'         
        END AS STS_ACESSO
FROM TMP_SQDAA_MCM_REC_05 
;

--- Resumo
SELECT DT_REFERENCIA 
       ,sts_acesso 
       ,count(distinct(num_ntc)) AS QT_USU_UNICOS 
FROM FT_SQDAA_MCM_ACESSO_MOV
GROUP BY DT_REFERENCIA ,sts_acesso;

/*---===================================================================================================================================
			ANALISE ROLLOUT NOVO APP
			Bright*AA001
		

---===================================================================================================================================*/

--Clientes que acessaram no mês
SELECT DISTINCT DT_REFERENCIA ,COUNT(DISTINCT(NUM_NTC)) AS QT_USU_UNICOS ,'M0' AS SAFRA ,'Antigo App + Site' AS CANAL ,'Clientes que acessaram no mês' AS STATUS FROM TMP_SQDAA_MCM_REC_01A GROUP BY DT_REFERENCIA
--31/07/20 4.576.742 (ANTIGO APP + SITE)
--31/08/20 3.582.877 (ANTIGO APP + SITE)
--30/09/20 4.261.202 (ANTIGO APP + SITE)

UNION ALL

SELECT DISTINCT DT_REFERENCIA ,COUNT(DISTINCT(NUM_NTC)) AS QT_USU_UNICOS ,'M0' AS SAFRA ,'Novo App' AS CANAL ,'Clientes que acessaram no mês' AS STATUS  FROM TMP_SQDAA_MCM_REC_01B GROUP BY DT_REFERENCIA
--31/07/20 268.289 (NOVO APP)
--31/08/20 1.045.564 (NOVO APP) 

UNION ALL

SELECT DISTINCT DT_REFERENCIA ,COUNT(DISTINCT(NUM_NTC)) AS QT_USU_UNICOS ,'M0' AS SAFRA ,'Novo App + Antigo App + Site' AS CANAL ,'Clientes que acessaram no mês (único independente dos canais)' AS STATUS FROM TMP_SQDAA_MCM_REC_01D GROUP BY DT_REFERENCIA
--31/07/20 4.668.436 (NOVO APP + ANTIGO APP + SITE) REMOVER OS DUPLICADOS
--31/08/20 4.229.469 (NOVO APP + ANTIGO APP + SITE) REMOVER OS DUPLICADOS


UNION ALL

--Clientes que acessaram mês anterior
SELECT DISTINCT DT_REFERENCIA ,COUNT(DISTINCT(NUM_NTC)) AS QT_USU_UNICOS ,'M1' AS SAFRA ,'Antigo App + Site' AS CANAL ,'Clientes que acessaram no mês' AS STATUS FROM TMP_SQDAA_MCM_REC_02A GROUP BY DT_REFERENCIA
--31/07/20 4.507.802 (ANTIGO APP + SITE)
--31/08/20 4.576.742 (ANTIGO APP + SITE)

UNION ALL

SELECT DISTINCT DT_REFERENCIA ,COUNT(DISTINCT(NUM_NTC)) AS QT_USU_UNICOS ,'M1' AS SAFRA ,'Novo App' AS CANAL ,'Clientes que acessaram no mês' AS STATUS  FROM TMP_SQDAA_MCM_REC_02B GROUP BY DT_REFERENCIA
--31/07/20 50.677 (NOVO APP)
--31/08/20 268.289 (NOVO APP)

UNION ALL

SELECT DISTINCT DT_REFERENCIA ,COUNT(DISTINCT(NUM_NTC)) AS QT_USU_UNICOS ,'M1' AS SAFRA ,'Novo App + Antigo App + Site' AS CANAL ,'Clientes que acessaram no mês (único independente dos canais)' AS STATUS FROM TMP_SQDAA_MCM_REC_02D GROUP BY DT_REFERENCIA;
--31/07/20 4.531.937 (NOVO APP + ANTIGO APP + SITE) REMOVER OS DUPLICADOS
--31/08/20 4.668.436 (NOVO APP + ANTIGO APP + SITE) REMOVER OS DUPLICADOS



/*--==========================================================================================================================================================
  
  ETAPA1: ANALISE ROLLOUT NOVO APP
  
---==========================================================================================================================================================*/
----> ACESSARAM OS DOIS CANAIS
SELECT MV.DT_REFERENCIA  ,count(distinct(MV.num_ntc)) AS QT_USU_UNICOS ,'Os dois' AS DSC_ANALISE
FROM TMP_SQDAA_MCM_REC_01B MV ---> BASE NOVO APP
WHERE MV.num_ntc IN (SELECT /*+ PARALLEL (32)*/ DISTINCT A.NUM_NTC FROM TMP_SQDAA_MCM_REC_01A A GROUP BY a.num_ntc) ---> BASE ANTIO APP & SITE
GROUP BY DT_REFERENCIA
--31/07/20 176595 
--31/08/20 398972

UNION ALL

----> ACESSARAM APENAS O NOVO APP
SELECT MV.DT_REFERENCIA ,count(distinct(MV.num_ntc)) AS QT_USU_UNICOS ,'Somente o Novo App' AS DSC_ANALISE
FROM TMP_SQDAA_MCM_REC_01B MV ---> BASE NOVO APP
WHERE MV.num_ntc NOT IN (SELECT /*+ PARALLEL (32)*/ DISTINCT A.NUM_NTC FROM TMP_SQDAA_MCM_REC_01A A GROUP BY a.num_ntc) ---> BASE ANTIO APP & SITE
GROUP BY DT_REFERENCIA
--31/07/20 91694  
--31/08/20 646592

UNION ALL

--> clientes QUE ACESSARAM MES ATUAL E ANTERIOR
SELECT MV.DT_REFERENCIA ,count(distinct(MV.num_ntc)) AS QT_USU_UNICOS ,'Acessaram o mês anterior também' AS DSC_ANALISE
FROM TMP_SQDAA_MCM_REC_01B MV ---> BASE NOVO APP
WHERE MV.num_ntc IN (SELECT /*+ PARALLEL (32)*/ DISTINCT A.NUM_NTC FROM TMP_SQDAA_MCM_REC_02B A GROUP BY a.num_ntc) ---> BASE ANTIO APP & SITE
GROUP BY DT_REFERENCIA
--31/07/20 19107
--31/08/20 168.586

UNION ALL

--> novo no canal
SELECT MV.DT_REFERENCIA ,count(distinct(MV.num_ntc)) AS QT_USU_UNICOS ,'Novos na plataforma' AS DSC_ANALISE
FROM TMP_SQDAA_MCM_REC_01B MV ---> BASE NOVO APP
WHERE MV.num_ntc NOT IN (SELECT /*+ PARALLEL (32)*/ DISTINCT A.NUM_NTC FROM TMP_SQDAA_MCM_REC_02B A GROUP BY a.num_ntc) ---> BASE ANTIO APP & SITE
GROUP BY DT_REFERENCIA;
--31/07/20 249182
--31/08/20 876.978
/*
  ETAPA2: CLIENTES QUE ACESSOU OS OUTROS CANAIS
*/

