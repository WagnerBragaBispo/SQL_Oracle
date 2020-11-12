/*
--====================================================================================================================================================================================

	74444 | Estruturar indicador de comportamento de usuários únicos por marca (resumo executivo) - DTH - (FASE 1)
	
	O que precisa ser feito: construir a visão de usuários únicos por marca que só ligam, só acessam, ligam e acessam e não interagem no mês para DTH

	+ detalhes: considerar os canais digitais para essa medição  de acessos (site / app/ whatsapp )
para o indicador só acessa e liga e acessa, precisaremos abrir o acesso por canal, para que saibamos qual o canal que o cliente que só acessa usa e qual o canal que o cliente que liga e acessa busca. 

	O card pode ser concluído quando (específico): tivermos esse indicador construído e validado com as áreas interessadas (DAC, Digital, Técnica e FIN). 


CONTRATO TEST NO ACESSO 021/168951997 (CONECTADO)  e 021/180907227
	site loga com e-mail: renanx@me.com
	no app com cpf: 22484340818
--====================================================================================================================================================================================
*/

DROP TABLE TMP_SQDAA_COMP_ATM_DTH_P1;
CREATE TABLE TMP_SQDAA_COMP_ATM_DTH_P1 COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS
SELECT  /*+parallel (32)*/ ----------------------------------------------------------------------------------------------ETAPA 1 BASE ATIVA
	DISTINCT
		ADD_MONTHS(LAST_DAY(DAT_MOVIMENTO),-1)+1 AS DAT_MOVIMENTO
		,COD_OPERADORA
		,NUM_CONTRATO
		,1 AS FLG_BASE_ATIVA
FROM OPS_ALTERYX.BI_FP_CONTRATO_PRODUTO_CTV@BASECLARO
WHERE 0 = 0
	AND DAT_MOVIMENTO in to_date('01/09/2020','DD/MM/YYYY')
	--AND DAT_MOVIMENTO in TRUNC(TO_DATE(LAST_DAY(ADD_MONTHS(TO_DATE('&&DAT_REF_YYYYMMDD','YYYYMMDD'),-1))+1,'DD/MM/YYYY'))
GROUP BY 
		DAT_MOVIMENTO
		,COD_OPERADORA
		,NUM_CONTRATO
;
/*----------------------------------------------------------------------------
					VALIDAÇÃO BASE ATIVA
----------------------------------------------------------------------------*/
SELECT COUNT(NUM_CONTRATO) QT_DOM1 ,COUNT(DISTINCT(NUM_CONTRATO)) QT_DOM2 FROM TMP_SQDAA_COMP_ATM_DTH_P1;
--01/08/2020 1.002.915
--01/09/2020 962.573	962573

/*--====================================================================================================================================================================================				
--====================================================================================================================================================================================*/

DROP TABLE TMP_SQDAA_COMP_ATM_DTH_P2;
CREATE TABLE TMP_SQDAA_COMP_ATM_DTH_P2 COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS
SELECT   /*+parallel (32)*/ ----------------------------------------------------------------------------------------------ETAPA 2 BASE DE LIGACOES
	DISTINCT
	TO_DATE(
      ADD_MONTHS(
              LAST_DAY(TO_DATE(&&DT_MOVIMENTO, 'YYYYMMDD')),-1)+1,'DD/MM/YYYY') AS DAT_MOVIMENTO
		,LIG.CD_CIDADE_CONTRATO
		,LIG.NR_CONTRATO AS NUM_CONTRATO
FROM INN.ANALITICA_CR_BI_LIG_CTV LIG
WHERE 0 = 0
      AND LIG.SK_DATA BETWEEN 
		TO_NUMBER(
                  TO_CHAR(
                      (ADD_MONTHS(
                            LAST_DAY(
                                TO_DATE(&&DT_MOVIMENTO, 'YYYYMMDD')),-1)+1),'YYYYMMDD')) AND 
		TO_NUMBER(
            TO_CHAR(
                LAST_DAY(
                    TO_DATE(&&DT_MOVIMENTO, 'YYYYMMDD')),'YYYYMMDD')) 
      AND LIG.NR_CONTRATO IS NOT NULL 
          
GROUP BY 
    TO_DATE(
      ADD_MONTHS(
              LAST_DAY(TO_DATE(&&DT_MOVIMENTO, 'YYYYMMDD')),-1)+1,'DD/MM/YYYY')
		,LIG.CD_CIDADE_CONTRATO
		,LIG.NR_CONTRATO	
;
/*----------------------------------------------------------------------------
					VALIDAÇÃO 
----------------------------------------------------------------------------*/
SELECT COUNT(NUM_CONTRATO) QT_DOM1 ,COUNT(DISTINCT(CD_CIDADE_CONTRATO||NUM_CONTRATO)) QT_DOM2 FROM TMP_SQDAA_COMP_ATM_DTH_P2;
--01/08 
-- 01/09/2020 411.427 411427

/*--====================================================================================================================================================================================				
--====================================================================================================================================================================================*/
DROP TABLE TMP_SQDAA_COMP_ATM_DTH_P3A;
CREATE TABLE TMP_SQDAA_COMP_ATM_DTH_P3A COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS
SELECT   /*+parallel (32)*/ ----------------------------------------------------------------------------------------------ETAPA 3A BASE DE ACESSOS IDM
  DISTINCT
  TO_DATE(
      ADD_MONTHS(
              LAST_DAY(TO_DATE(TRUNC(&&DT_MOVIMENTO), 'YYYYMMDD')),-1)+1,'DD/MM/YYYY') AS DAT_MOVIMENTO
    ,IDM.USER_ID AS LOGIN_AUTENTICA
    ,COUNT(DISTINCT(idm.user_id||IDM.SK_DATA||REPLACE(SUBSTR(TO_CHAR(IDM.EVENT_TIMESTAMP,'dd/mm/yyyy hh24:mi:ss'),12,8),':',''))) AS QT_ACESSO
    ,SUM(CASE WHEN UPPER(IDM.CLIENT_ID) = 'NETAPP'                        THEN 1 ELSE 0 END) AS QT_ACSS_APP
    ,SUM(CASE WHEN UPPER(IDM.CLIENT_ID) = 'CLAROTV_WCP_PROD'              THEN 1 ELSE 0 END) AS QT_ACSS_SITE
    ,SUM(CASE WHEN UPPER(IDM.CLIENT_ID) IN ('NETAPP','CLAROTV_WCP_PROD') THEN 1 ELSE 0 END) AS QT_ACSS_SITE_APP
FROM INN.DW_IDM_LOG IDM  
WHERE 0 = 0
    AND TRUNC(IDM.EVENT_TIMESTAMP) BETWEEN 
        add_months(LAST_DAY(TO_DATE(&&DT_MOVIMENTO, 'YYYYMMDD')),-1)+1
            AND 
              LAST_DAY(TO_DATE(&&DT_MOVIMENTO, 'YYYYMMDD')) 
    AND IDM.EVENT_NAME = 'TOKEN_SUCCESS'
    AND UPPER(IDM.CLIENT_ID) IN ('NETAPP','CLAROTV_WCP_PROD')
    AND IDM.USER_ID IN (SELECT ds_email AS LOGIN_AUTENTICA FROM INN.DW_ASSINANTE_CTV WHERE ds_email IS NOT NULL
                        UNION ALL
                        SELECT TO_CHAR(nr_cpf_cnpj_assinante) AS LOGIN_AUTENTICA FROM INN.DW_ASSINANTE_CTV WHERE nr_cpf_cnpj_assinante IS NOT NULL
                        UNION ALL
                        SELECT REPLACE(REGEXP_SUBSTR (ds_email,'^.*@'),'@','') AS LOGIN_AUTENTICA FROM INN.DW_ASSINANTE_CTV WHERE ds_email IS NOT NULL
						UNION ALL
						SELECT TO_CHAR(nr_contrato) AS LOGIN_AUTENTICA FROM INN.DW_ASSINANTE_CTV  WHERE nr_contrato IS NOT NULL)
GROUP BY 
   
    TO_DATE(
      ADD_MONTHS(
              LAST_DAY(TO_DATE(TRUNC(&&DT_MOVIMENTO), 'YYYYMMDD')),-1)+1,'DD/MM/YYYY')
    ,IDM.USER_ID 
;

/*----------------------------------------------------------------------------
					VALIDAÇÃO 
----------------------------------------------------------------------------*/
SELECT SUM(QT_ACESSO) AS QT FROM TMP_SQDAA_COMP_ATM_DTH_P3A;-- ORDER BY login_autentica;
--356.058
SELECT * FROM TMP_SQDAA_COMP_ATM_DTH_P3A WHERE  login_autentica IN ('renanx@me.com','renanx@gmail.com', '22484340818');

DROP TABLE TMP_SQDAA_COMP_ATM_DTH_P3B;
CREATE TABLE TMP_SQDAA_COMP_ATM_DTH_P3B COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS
SELECT   /*+parallel (32)*/ ----------------------------------------------------------------------------------------------ETAPA 3B ACESSO SITE
  DISTINCT
		TO_DATE(
      ADD_MONTHS(
              LAST_DAY(TO_DATE(TRUNC(&&DT_MOVIMENTO), 'YYYYMMDD')),-1)+1,'DD/MM/YYYY') AS DAT_MOVIMENTO
		,CD.NR_CONTRATO AS NUM_CONTRATO
    ,CD.NR_CPF_CNPJ_ASSINANTE AS NUM_CPF_CNPJ
		,1  AS FL_ACESSO
    ,'SITE MCR CTV' AS CANAL_ATD
    ,'CPF_CNPJ' AS TP_AUTENTICACAO
FROM INN.DW_ASSINANTE_CTV CD
WHERE 0 = 0 
     AND TO_CHAR(CD.nr_cpf_cnpj_assinante) IN (SELECT LOGIN_AUTENTICA FROM TMP_SQDAA_COMP_ATM_DTH_P3A WHERE QT_ACSS_SITE > 0)
 --    OR TO_CHAR(CD.DS_EMAIL) IN (SELECT LOGIN_AUTENTICA FROM TMP_SQDAA_COMP_ATM_DTH_P3A WHERE QT_ACSS_SITE > 0)
 --    OR TO_CHAR(CD.NR_CONTRATO) IN (SELECT LOGIN_AUTENTICA FROM TMP_SQDAA_COMP_ATM_DTH_P3A WHERE QT_ACSS_SITE > 0)
 --    OR REPLACE(REGEXP_SUBSTR (CD.ds_email,'^.*@'),'@','') IN (SELECT LOGIN_AUTENTICA FROM TMP_SQDAA_COMP_ATM_DTH_P3A WHERE QT_ACSS_SITE > 0)
GROUP BY 
	TO_DATE(
      ADD_MONTHS(
              LAST_DAY(TO_DATE(TRUNC(&&DT_MOVIMENTO), 'YYYYMMDD')),-1)+1,'DD/MM/YYYY')
	,CD.NR_CONTRATO
  ,CD.NR_CPF_CNPJ_ASSINANTE

UNION ALL

SELECT   /*+parallel (32)*/ ------------------------------------------------------------------------ETAPA 3B ACESSO SITE
  DISTINCT
		TO_DATE(
      ADD_MONTHS(
              LAST_DAY(TO_DATE(TRUNC(&&DT_MOVIMENTO), 'YYYYMMDD')),-1)+1,'DD/MM/YYYY') AS DAT_MOVIMENTO
		,CD.NR_CONTRATO AS NUM_CONTRATO
    ,CD.NR_CPF_CNPJ_ASSINANTE AS NUM_CPF_CNPJ
		,1  AS FL_ACESSO
    ,'SITE MCR CTV' AS CANAL_ATD
    ,'EMAIL' AS TP_AUTENTICACAO
FROM INN.DW_ASSINANTE_CTV CD
WHERE 0 = 0 
--     AND TO_CHAR(CD.nr_cpf_cnpj_assinante) IN (SELECT LOGIN_AUTENTICA FROM TMP_SQDAA_COMP_ATM_DTH_P3A WHERE QT_ACSS_SITE > 0)
     AND  TO_CHAR(CD.DS_EMAIL) IN (SELECT LOGIN_AUTENTICA FROM TMP_SQDAA_COMP_ATM_DTH_P3A WHERE QT_ACSS_SITE > 0)
 --    OR TO_CHAR(CD.NR_CONTRATO) IN (SELECT LOGIN_AUTENTICA FROM TMP_SQDAA_COMP_ATM_DTH_P3A WHERE QT_ACSS_SITE > 0)
 --    OR REPLACE(REGEXP_SUBSTR (CD.ds_email,'^.*@'),'@','') IN (SELECT LOGIN_AUTENTICA FROM TMP_SQDAA_COMP_ATM_DTH_P3A WHERE QT_ACSS_SITE > 0)
GROUP BY 
	TO_DATE(
      ADD_MONTHS(
              LAST_DAY(TO_DATE(TRUNC(&&DT_MOVIMENTO), 'YYYYMMDD')),-1)+1,'DD/MM/YYYY')
	,CD.NR_CONTRATO
  ,CD.NR_CPF_CNPJ_ASSINANTE


UNION ALL

SELECT   /*+parallel (32)*/ ------------------------------------------------------------------------ETAPA 3B ACESSO SITE
  DISTINCT
		TO_DATE(
      ADD_MONTHS(
              LAST_DAY(TO_DATE(TRUNC(&&DT_MOVIMENTO), 'YYYYMMDD')),-1)+1,'DD/MM/YYYY') AS DAT_MOVIMENTO
		,CD.NR_CONTRATO AS NUM_CONTRATO
    ,CD.NR_CPF_CNPJ_ASSINANTE AS NUM_CPF_CNPJ
		,1  AS FL_ACESSO
    ,'SITE MCR CTV' AS CANAL_ATD
    ,'CONTRATO' AS TP_AUTENTICACAO
FROM INN.DW_ASSINANTE_CTV CD
WHERE 0 = 0 
--     AND TO_CHAR(CD.nr_cpf_cnpj_assinante) IN (SELECT LOGIN_AUTENTICA FROM TMP_SQDAA_COMP_ATM_DTH_P3A WHERE QT_ACSS_SITE > 0)
--     AND  TO_CHAR(CD.DS_EMAIL) IN (SELECT LOGIN_AUTENTICA FROM TMP_SQDAA_COMP_ATM_DTH_P3A WHERE QT_ACSS_SITE > 0)
     AND TO_CHAR(CD.NR_CONTRATO) IN (SELECT LOGIN_AUTENTICA FROM TMP_SQDAA_COMP_ATM_DTH_P3A WHERE QT_ACSS_SITE > 0)
 --    OR REPLACE(REGEXP_SUBSTR (CD.ds_email,'^.*@'),'@','') IN (SELECT LOGIN_AUTENTICA FROM TMP_SQDAA_COMP_ATM_DTH_P3A WHERE QT_ACSS_SITE > 0)
GROUP BY 
	TO_DATE(
      ADD_MONTHS(
              LAST_DAY(TO_DATE(TRUNC(&&DT_MOVIMENTO), 'YYYYMMDD')),-1)+1,'DD/MM/YYYY')
	,CD.NR_CONTRATO
  ,CD.NR_CPF_CNPJ_ASSINANTE

UNION ALL  
  
SELECT   /*+parallel (32)*/ ------------------------------------------------------------------------ETAPA 3B ACESSO SITE
  DISTINCT
		TO_DATE(
      ADD_MONTHS(
              LAST_DAY(TO_DATE(TRUNC(&&DT_MOVIMENTO), 'YYYYMMDD')),-1)+1,'DD/MM/YYYY') AS DAT_MOVIMENTO
		,CD.NR_CONTRATO AS NUM_CONTRATO
    ,CD.NR_CPF_CNPJ_ASSINANTE AS NUM_CPF_CNPJ
		,1  AS FL_ACESSO
    ,'SITE MCR CTV' AS CANAL_ATD
    ,'USERNAME' AS TP_AUTENTICACAO
FROM INN.DW_ASSINANTE_CTV CD
WHERE 0 = 0 
--     AND TO_CHAR(CD.nr_cpf_cnpj_assinante) IN (SELECT LOGIN_AUTENTICA FROM TMP_SQDAA_COMP_ATM_DTH_P3A WHERE QT_ACSS_SITE > 0)
--     AND  TO_CHAR(CD.DS_EMAIL) IN (SELECT LOGIN_AUTENTICA FROM TMP_SQDAA_COMP_ATM_DTH_P3A WHERE QT_ACSS_SITE > 0)
--     AND TO_CHAR(CD.NR_CONTRATO) IN (SELECT LOGIN_AUTENTICA FROM TMP_SQDAA_COMP_ATM_DTH_P3A WHERE QT_ACSS_SITE > 0)
      AND REPLACE(REGEXP_SUBSTR (CD.ds_email,'^.*@'),'@','') IN (SELECT LOGIN_AUTENTICA FROM TMP_SQDAA_COMP_ATM_DTH_P3A WHERE QT_ACSS_SITE > 0)
GROUP BY 
	TO_DATE(
      ADD_MONTHS(
              LAST_DAY(TO_DATE(TRUNC(&&DT_MOVIMENTO), 'YYYYMMDD')),-1)+1,'DD/MM/YYYY')
	,CD.NR_CONTRATO
  ,CD.NR_CPF_CNPJ_ASSINANTE

;
/*----------------------------------------------------------------------------
					resumo 
----------------------------------------------------------------------------*/
SELECT CANAL_ATD ,TP_AUTENTICACAO ,COUNT(DISTINCT(NUM_CONTRATO)) QT_CTR_UNICO  FROM TMP_SQDAA_COMP_ATM_DTH_P3B GROUP BY CANAL_ATD ,TP_AUTENTICACAO ;



DROP TABLE TMP_SQDAA_COMP_ATM_DTH_P3C;
CREATE TABLE TMP_SQDAA_COMP_ATM_DTH_P3C COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS
SELECT   /*+parallel (32)*/ ----------------------------------------------------------------------------------------------ETAPA 3C ACESSO APP
   DISTINCT
		TO_DATE(
      ADD_MONTHS(
              LAST_DAY(TO_DATE(TRUNC(&&DT_MOVIMENTO), 'YYYYMMDD')),-1)+1,'DD/MM/YYYY') AS DAT_MOVIMENTO
		,CD.NR_CONTRATO AS NUM_CONTRATO
    ,CD.NR_CPF_CNPJ_ASSINANTE AS NUM_CPF_CNPJ
		,1  AS FL_ACESSO
    ,'APP MCR CTV' AS CANAL_ATD
    ,'CPF_CNPJ' AS TP_AUTENTICACAO
FROM INN.DW_ASSINANTE_CTV CD
WHERE 0 = 0 
     AND TO_CHAR(CD.nr_cpf_cnpj_assinante) IN (SELECT LOGIN_AUTENTICA FROM TMP_SQDAA_COMP_ATM_DTH_P3A WHERE QT_ACSS_APP > 0)
 --    OR TO_CHAR(CD.DS_EMAIL) IN (SELECT LOGIN_AUTENTICA FROM TMP_SQDAA_COMP_ATM_DTH_P3A WHERE QT_ACSS_APP > 0)
 --    OR TO_CHAR(CD.NR_CONTRATO) IN (SELECT LOGIN_AUTENTICA FROM TMP_SQDAA_COMP_ATM_DTH_P3A WHERE QT_ACSS_APP > 0)
 --    OR REPLACE(REGEXP_SUBSTR (CD.ds_email,'^.*@'),'@','') IN (SELECT LOGIN_AUTENTICA FROM TMP_SQDAA_COMP_ATM_DTH_P3A WHERE QT_ACSS_APP > 0)
GROUP BY 
	TO_DATE(
      ADD_MONTHS(
              LAST_DAY(TO_DATE(TRUNC(&&DT_MOVIMENTO), 'YYYYMMDD')),-1)+1,'DD/MM/YYYY')
	,CD.NR_CONTRATO
  ,CD.NR_CPF_CNPJ_ASSINANTE

UNION ALL

SELECT   /*+parallel (32)*/ ------------------------------------------------------------------------ETAPA 3C ACESSO APP
  DISTINCT
		TO_DATE(
      ADD_MONTHS(
              LAST_DAY(TO_DATE(TRUNC(&&DT_MOVIMENTO), 'YYYYMMDD')),-1)+1,'DD/MM/YYYY') AS DAT_MOVIMENTO
		,CD.NR_CONTRATO AS NUM_CONTRATO
    ,CD.NR_CPF_CNPJ_ASSINANTE AS NUM_CPF_CNPJ
		,1  AS FL_ACESSO
    ,'APP MCR CTV' AS CANAL_ATD
    ,'EMAIL' AS TP_AUTENTICACAO
FROM INN.DW_ASSINANTE_CTV CD
WHERE 0 = 0 
--     AND TO_CHAR(CD.nr_cpf_cnpj_assinante) IN (SELECT LOGIN_AUTENTICA FROM TMP_SQDAA_COMP_ATM_DTH_P3A WHERE QT_ACSS_APP > 0)
     AND  TO_CHAR(CD.DS_EMAIL) IN (SELECT LOGIN_AUTENTICA FROM TMP_SQDAA_COMP_ATM_DTH_P3A WHERE QT_ACSS_APP > 0)
 --    OR TO_CHAR(CD.NR_CONTRATO) IN (SELECT LOGIN_AUTENTICA FROM TMP_SQDAA_COMP_ATM_DTH_P3A WHERE QT_ACSS_APP > 0)
 --    OR REPLACE(REGEXP_SUBSTR (CD.ds_email,'^.*@'),'@','') IN (SELECT LOGIN_AUTENTICA FROM TMP_SQDAA_COMP_ATM_DTH_P3A WHERE QT_ACSS_APP > 0)
GROUP BY 
	TO_DATE(
      ADD_MONTHS(
              LAST_DAY(TO_DATE(TRUNC(&&DT_MOVIMENTO), 'YYYYMMDD')),-1)+1,'DD/MM/YYYY')
	,CD.NR_CONTRATO
  ,CD.NR_CPF_CNPJ_ASSINANTE


UNION ALL

SELECT   /*+parallel (32)*/ ------------------------------------------------------------------------ETAPA 3C ACESSO APP
  DISTINCT
		TO_DATE(
      ADD_MONTHS(
              LAST_DAY(TO_DATE(TRUNC(&&DT_MOVIMENTO), 'YYYYMMDD')),-1)+1,'DD/MM/YYYY') AS DAT_MOVIMENTO
		,CD.NR_CONTRATO AS NUM_CONTRATO
    ,CD.NR_CPF_CNPJ_ASSINANTE AS NUM_CPF_CNPJ
		,1  AS FL_ACESSO
    ,'APP MCR CTV' AS CANAL_ATD
    ,'CONTRATO' AS TP_AUTENTICACAO
FROM INN.DW_ASSINANTE_CTV CD
WHERE 0 = 0 
--     AND TO_CHAR(CD.nr_cpf_cnpj_assinante) IN (SELECT LOGIN_AUTENTICA FROM TMP_SQDAA_COMP_ATM_DTH_P3A WHERE QT_ACSS_APP > 0)
--     AND  TO_CHAR(CD.DS_EMAIL) IN (SELECT LOGIN_AUTENTICA FROM TMP_SQDAA_COMP_ATM_DTH_P3A WHERE QT_ACSS_APP > 0)
     AND TO_CHAR(CD.NR_CONTRATO) IN (SELECT LOGIN_AUTENTICA FROM TMP_SQDAA_COMP_ATM_DTH_P3A WHERE QT_ACSS_APP > 0)
 --    OR REPLACE(REGEXP_SUBSTR (CD.ds_email,'^.*@'),'@','') IN (SELECT LOGIN_AUTENTICA FROM TMP_SQDAA_COMP_ATM_DTH_P3A WHERE QT_ACSS_APP > 0)
GROUP BY 
	TO_DATE(
      ADD_MONTHS(
              LAST_DAY(TO_DATE(TRUNC(&&DT_MOVIMENTO), 'YYYYMMDD')),-1)+1,'DD/MM/YYYY')
	,CD.NR_CONTRATO
  ,CD.NR_CPF_CNPJ_ASSINANTE

UNION ALL  
  
SELECT   /*+parallel (32)*/ ------------------------------------------------------------------------ETAPA 3C ACESSO APP
  DISTINCT
		TO_DATE(
      ADD_MONTHS(
              LAST_DAY(TO_DATE(TRUNC(&&DT_MOVIMENTO), 'YYYYMMDD')),-1)+1,'DD/MM/YYYY') AS DAT_MOVIMENTO
		,CD.NR_CONTRATO AS NUM_CONTRATO
    ,CD.NR_CPF_CNPJ_ASSINANTE AS NUM_CPF_CNPJ
		,1  AS FL_ACESSO
    ,'APP MCR CTV' AS CANAL_ATD
    ,'USERNAME' AS TP_AUTENTICACAO
FROM INN.DW_ASSINANTE_CTV CD
WHERE 0 = 0 
--     AND TO_CHAR(CD.nr_cpf_cnpj_assinante) IN (SELECT LOGIN_AUTENTICA FROM TMP_SQDAA_COMP_ATM_DTH_P3A WHERE QT_ACSS_APP > 0)
--     AND  TO_CHAR(CD.DS_EMAIL) IN (SELECT LOGIN_AUTENTICA FROM TMP_SQDAA_COMP_ATM_DTH_P3A WHERE QT_ACSS_APP > 0)
--     AND TO_CHAR(CD.NR_CONTRATO) IN (SELECT LOGIN_AUTENTICA FROM TMP_SQDAA_COMP_ATM_DTH_P3A WHERE QT_ACSS_APP > 0)
      AND REPLACE(REGEXP_SUBSTR (CD.ds_email,'^.*@'),'@','') IN (SELECT LOGIN_AUTENTICA FROM TMP_SQDAA_COMP_ATM_DTH_P3A WHERE QT_ACSS_APP > 0)
GROUP BY 
	TO_DATE(
      ADD_MONTHS(
              LAST_DAY(TO_DATE(TRUNC(&&DT_MOVIMENTO), 'YYYYMMDD')),-1)+1,'DD/MM/YYYY')
	,CD.NR_CONTRATO
	,CD.NR_CPF_CNPJ_ASSINANTE
;
/*----------------------------------------------------------------------------
					resumo 
----------------------------------------------------------------------------*/
SELECT CANAL_ATD ,TP_AUTENTICACAO ,COUNT(DISTINCT(NUM_CONTRATO)) QT_CTR_UNICO  FROM TMP_SQDAA_COMP_ATM_DTH_P3C GROUP BY CANAL_ATD ,TP_AUTENTICACAO ;



/*--====================================================================================================================================================================================				
--====================================================================================================================================================================================*/
DROP TABLE TMP_SQDAA_COMP_ATM_DTH_P4A;
CREATE TABLE TMP_SQDAA_COMP_ATM_DTH_P4A COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS -------ETAPA 4A ACESSOS QUE NÃO ESTAVAM NA BASE ATIVA
SELECT /*+parallel (32)*/ DISTINCT LIG.DAT_MOVIMENTO ,21 AS COD_OPERADORA ,LIG.NUM_CONTRATO ,0 AS FLG_BASE_ATIVA 
      FROM OPS_ALTERYX.BI_FP_CONTRATO_CTV@BASECLARO DOM RIGHT JOIN TMP_SQDAA_COMP_ATM_DTH_P2 LIG ON (DOM.NUM_CONTRATO = LIG.NUM_CONTRATO) 
		WHERE 0 = 0
			AND DOM.DAT_MOVIMENTO = TO_CHAR(TO_DATE(
				ADD_MONTHS(
					LAST_DAY(TO_DATE(TRUNC(&&DT_MOVIMENTO), 'YYYYMMDD')),-1)+1,'DD/MM/YYYY'))
			AND DOM.NUM_CONTRATO IS NULL
UNION ALL
SELECT /*+parallel (32)*/ DISTINCT SIT.DAT_MOVIMENTO ,21 AS COD_OPERADORA ,SIT.NUM_CONTRATO ,0 AS FLG_BASE_ATIVA
      FROM OPS_ALTERYX.BI_FP_CONTRATO_CTV@BASECLARO DOM RIGHT JOIN TMP_SQDAA_COMP_ATM_DTH_P3B SIT ON (DOM.NUM_CONTRATO = SIT.NUM_CONTRATO) 
		WHERE 0 = 0
			AND DOM.DAT_MOVIMENTO = TO_CHAR(TO_DATE(
				ADD_MONTHS(
					LAST_DAY(TO_DATE(TRUNC(&&DT_MOVIMENTO), 'YYYYMMDD')),-1)+1,'DD/MM/YYYY'))
			AND DOM.NUM_CONTRATO IS NULL
UNION ALL
SELECT /*+parallel (32)*/ DISTINCT APP.DAT_MOVIMENTO ,21 AS COD_OPERADORA ,APP.NUM_CONTRATO ,0 AS FLG_BASE_ATIVA
      FROM OPS_ALTERYX.BI_FP_CONTRATO_CTV@BASECLARO DOM RIGHT JOIN TMP_SQDAA_COMP_ATM_DTH_P3C APP ON (DOM.NUM_CONTRATO = APP.NUM_CONTRATO) 
		WHERE 0 = 0
			AND DOM.DAT_MOVIMENTO = TO_CHAR(TO_DATE(
				ADD_MONTHS(
					LAST_DAY(TO_DATE(TRUNC(&&DT_MOVIMENTO), 'YYYYMMDD')),-1)+1,'DD/MM/YYYY'))
			AND DOM.NUM_CONTRATO IS NULL
;
/*----------------------------------------------------------------------------
					resumo 
----------------------------------------------------------------------------*/
SELECT COUNT(NUM_CONTRATO) QT_DOM1 ,COUNT(DISTINCT(NUM_CONTRATO)) QT_DOM2 FROM TMP_SQDAA_COMP_ATM_DTH_P4A;

INSERT /*+APPEND */ INTO TMP_SQDAA_COMP_ATM_DTH_P1 --------------------------------------------------ETAPA 4A INCLUIR OS ACESSOS E LIGAÇÕES QUE NÃO ESTAVAM NA BASE ATIVA
SELECT /*+PARALLEL (32)*/
	DISTINCT
       TO_DATE(
      ADD_MONTHS(
              LAST_DAY(TO_DATE(&&DT_MOVIMENTO, 'YYYYMMDD')),-1)+1,'DD/MM/YYYY') AS DAT_MOVIMENTO
      ,DOM.COD_OPERADORA
      ,DOM.NUM_CONTRATO
      ,0 AS FLG_BASE_ATIVA
FROM TMP_SQDAA_COMP_ATM_DTH_P4A DOM
	GROUP BY 
    TO_DATE(
      ADD_MONTHS(
              LAST_DAY(TO_DATE(&&DT_MOVIMENTO, 'YYYYMMDD')),-1)+1,'DD/MM/YYYY')
      ,DOM.COD_OPERADORA
      ,DOM.NUM_CONTRATO
      
;
COMMIT;

/*--====================================================================================================================================================================================				
--====================================================================================================================================================================================*/
DROP TABLE TMP_SQDAA_COMP_ATM_DTH_P4B;
CREATE TABLE TMP_SQDAA_COMP_ATM_DTH_P4B COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS
SELECT  /*+parallel (32)*/ ------------------------------------------------------------------------ETAPA 4B CROSS BASE ATIVA & ACESSO & LIGA
    DOM.DAT_MOVIMENTO
	,DOM.COD_OPERADORA
	,DOM.NUM_CONTRATO
    ,DOM.FLG_BASE_ATIVA
    ,CASE WHEN LIG.NUM_CONTRATO IS NULL THEN 0 ELSE 1 END AS FL_LIGA
    ,CASE WHEN SIT.NUM_CONTRATO IS NULL THEN 0 ELSE 1 END AS FL_SITE
    ,CASE WHEN APP.NUM_CONTRATO IS NULL THEN 0 ELSE 1 END AS FL_APP    
    ,MAX(CASE WHEN DOM.NUM_CONTRATO = SIT.NUM_CONTRATO THEN 1
              WHEN DOM.NUM_CONTRATO = APP.NUM_CONTRATO THEN 1              
                ELSE 0 END) AS FL_ACESSO
FROM TMP_SQDAA_COMP_ATM_DTH_P1        DOM
LEFT JOIN TMP_SQDAA_COMP_ATM_DTH_P2   LIG           ON (DOM.NUM_CONTRATO = LIG.NUM_CONTRATO)
LEFT JOIN TMP_SQDAA_COMP_ATM_DTH_P3B  SIT           ON (DOM.NUM_CONTRATO = SIT.NUM_CONTRATO)
LEFT JOIN TMP_SQDAA_COMP_ATM_DTH_P3C  APP           ON (DOM.NUM_CONTRATO = APP.NUM_CONTRATO)

GROUP BY 
	DOM.DAT_MOVIMENTO
	,DOM.COD_OPERADORA
	,DOM.NUM_CONTRATO
    ,DOM.FLG_BASE_ATIVA
    ,CASE WHEN LIG.NUM_CONTRATO IS NULL THEN 0 ELSE 1 END
    ,CASE WHEN SIT.NUM_CONTRATO IS NULL THEN 0 ELSE 1 END
    ,CASE WHEN APP.NUM_CONTRATO IS NULL THEN 0 ELSE 1 END
    ,(CASE WHEN DOM.NUM_CONTRATO = SIT.NUM_CONTRATO THEN 1
           WHEN DOM.NUM_CONTRATO = APP.NUM_CONTRATO THEN 1              
                ELSE 0 END)
;


SELECT DISTINCT
  DAT_MOVIMENTO
  ,COUNT(NUM_CONTRATO) QT_DOM1
  ,COUNT(DISTINCT(NUM_CONTRATO)) QT_DOM2
  ,SUM(FL_LIGA) QT_LIGA
  ,SUM(FL_SITE) QT_FL_SITE
  ,SUM(FL_APP) QT_FL_APP
  ,SUM(FL_ACESSO) QT_ACESSO
  FROM TMP_SQDAA_COMP_ATM_DTH_P4B
WHERE FLG_BASE_ATIVA <> 0 --- RETIRANDO OS INATIVOS
GROUP BY dat_movimento
;


DROP TABLE TMP_SQDAA_COMP_ATM_DTH_P5;
CREATE TABLE TMP_SQDAA_COMP_ATM_DTH_P5 COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS
SELECT  /*+parallel (32)*/ ------------------------------------------------------------------------ETAPA 5 CLASSIFICAÇÃO
    DOM.DAT_MOVIMENTO
	,DOM.COD_OPERADORA
	,DOM.NUM_CONTRATO
    ,DOM.FLG_BASE_ATIVA
    ,DOM.FL_LIGA
    ,DOM.FL_SITE
    ,DOM.FL_APP
    ,DOM.FL_ACESSO
    ,CASE WHEN DOM.FL_LIGA = 1 AND DOM.FL_ACESSO = 1  THEN 'LIGA E ACESSA'
          WHEN DOM.FL_LIGA = 0 AND DOM.FL_ACESSO = 1  THEN 'SOMENTE ACESSA'
          WHEN DOM.FL_LIGA = 1 AND DOM.FL_ACESSO = 0  THEN 'SOMENTE LIGA'
          WHEN DOM.FL_LIGA = 0 AND DOM.FL_ACESSO = 0  THEN 'SEM INTERAÇÃO'
            ELSE 'XXXX' END AS STS_ATENDIMENTO
   --  ,COUNT(*)
FROM TMP_SQDAA_COMP_ATM_DTH_P4B DOM
GROUP BY 
    DOM.DAT_MOVIMENTO
	,DOM.COD_OPERADORA
	,DOM.NUM_CONTRATO
    ,DOM.FLG_BASE_ATIVA
    ,DOM.FL_LIGA
    ,DOM.FL_SITE
    ,DOM.FL_APP
    ,DOM.FL_ACESSO
            ;
--GRANT SELECT ON TMP_SQDAA_COMP_ATM_HFC_P5 TO PUBLIC;

INSERT /*+APPEND */ INTO AGG_COMP_ATEND_DTH ------------------------------------------------------------------------ETAPA 5 TABELA AGREGADA
--CREATE TABLE AGG_COMP_ATEND_DTH COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS
SELECT DISTINCT
	  DOM.DAT_MOVIMENTO
	  ,DOM.FLG_BASE_ATIVA
	  ,DOM.STS_ATENDIMENTO
	  ,COUNT(NUM_CONTRATO)        	AS QT_ATIVO
	  ,SUM(FL_LIGA)             	AS QT_LIGA
	  ,SUM(FL_SITE)             	AS QT_FL_SITE
	  ,SUM(FL_APP)              	AS QT_FL_APP
	  ,SUM(FL_ACESSO)           	AS QT_ACESSO
	  ,SYSDATE                  	AS DAT_CRIACAO
FROM TMP_SQDAA_COMP_ATM_DTH_P5 DOM
GROUP BY 
	DOM.DAT_MOVIMENTO
	,DOM.FLG_BASE_ATIVA
	,DOM.STS_ATENDIMENTO
;
COMMIT;






/*
-- TESTES

*/
SELECT * FROM TMP_SQDAA_COMP_ATM_DTH_P4B WHERE FL_SITE = 1 AND fl_app = 1;

SELECT * FROM TMP_SQDAA_COMP_ATM_DTH_P3B WHERE NUM_CONTRATO = 76642300
UNION ALL
SELECT * FROM TMP_SQDAA_COMP_ATM_DTH_P3C WHERE NUM_CONTRATO = 76642300;

SELECT * FROM OPS_ALTERYX.BI_FP_CONTRATO_CTV@BASECLARO WHERE NUM_CONTRATO = 76642300 AND DAT_MOVIMENTO = '01/09/2020';

SELECT * FROM INN.DW_ASSINANTE_CTV WHERE NR_CONTRATO = 76642300;

SELECT * FROM INN.DW_IDM_LOG IDM  
WHERE IDM.USER_ID IN ('76642300', '26911738873', 'adautocruz@hotmail.com.br', 'adautocruz') 
      AND TRUNC(IDM.EVENT_TIMESTAMP) BETWEEN 
        add_months(LAST_DAY(TO_DATE(&&DT_MOVIMENTO, 'YYYYMMDD')),-1)+1
            AND 
              LAST_DAY(TO_DATE(&&DT_MOVIMENTO, 'YYYYMMDD'))
              
              ORDER BY IDM.EVENT_TIMESTAMP
              ;










;
/*----------------------------------------------------------------------------
					VALIDAÇÃO 
----------------------------------------------------------------------------*/



















DROP TABLE TMP_SQDAA_COMP_ATM_DTH_P3A;
CREATE TABLE TMP_SQDAA_COMP_ATM_DTH_P3A COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS
SELECT   /*+parallel (32)*/ ------------------------------------------------------------------------ETAPA 3A BASE DE ACESSOS IDM
  DISTINCT
  TO_DATE(
      ADD_MONTHS(
              LAST_DAY(TO_DATE(TRUNC(&&DT_MOVIMENTO), 'YYYYMMDD')),-1)+1,'DD/MM/YYYY') AS DAT_MOVIMENTO
    ,IDM.USER_ID AS LOGIN_AUTENTICA
    ,COUNT(DISTINCT(idm.user_id||IDM.SK_DATA||REPLACE(SUBSTR(TO_CHAR(IDM.EVENT_TIMESTAMP,'dd/mm/yyyy hh24:mi:ss'),12,8),':',''))) AS QT_ACESSO
    ,SUM(CASE WHEN UPPER(IDM.CLIENT_ID) = 'NETAPP'                        THEN 1 ELSE 0 END) AS QT_ACSS_APP
    ,SUM(CASE WHEN UPPER(IDM.CLIENT_ID) = 'CLAROTV_WCP_PROD'              THEN 1 ELSE 0 END) AS QT_ACSS_SITE
    ,SUM(CASE WHEN UPPER(IDM.CLIENT_ID) IN ('NETAPP','CLAROTV_WCP_PROD')  THEN 1 ELSE 0 END) AS QT_ACSS_SITE_APP
FROM INN.DW_IDM_LOG IDM  
WHERE 0 = 0
    AND TRUNC(IDM.EVENT_TIMESTAMP) BETWEEN 
        add_months(LAST_DAY(TO_DATE(&&DT_MOVIMENTO, 'YYYYMMDD')),-1)+1
            AND 
              LAST_DAY(TO_DATE(&&DT_MOVIMENTO, 'YYYYMMDD')) 
    AND IDM.EVENT_NAME = 'TOKEN_SUCCESS'
    AND UPPER(IDM.CLIENT_ID) IN ('NETAPP','CLAROTV_WCP_PROD')
    
GROUP BY 
   
    TO_DATE(
      ADD_MONTHS(
              LAST_DAY(TO_DATE(TRUNC(&&DT_MOVIMENTO), 'YYYYMMDD')),-1)+1,'DD/MM/YYYY')
    ,IDM.USER_ID 
;





DROP TABLE TMP_SQDAA_COMP_ATM_DTH_P3A;
CREATE TABLE TMP_SQDAA_COMP_ATM_DTH_P3A COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS
SELECT   /*+parallel (32)*/ ------------------------------------------------------------------------ETAPA 3 BASE ACESSO IDM
  DISTINCT
  TO_DATE(
      ADD_MONTHS(
              LAST_DAY(TO_DATE(TRUNC(IDM.EVENT_TIMESTAMP), 'YYYYMMDD')),-1)+1,'DD/MM/YYYY') AS DAT_MOVIMENTO
  ,CD.NR_CONTRATO AS NUM_CONTRATO
  ,CD.NR_CPF_CNPJ_ASSINANTE
  ,CASE WHEN UPPER(IDM.CLIENT_ID) = 'NETAPP' THEN 1 ELSE 0 END                        AS FL_ACSS_APP
  ,CASE WHEN UPPER(IDM.CLIENT_ID) = 'CLAROTV_WCP_PROD' THEN 1 ELSE 0 END              AS FL_ACSS_SITE
  ,CASE WHEN UPPER(IDM.CLIENT_ID) IN ('NETAPP','CLAROTV_WCP_PROD') THEN 1 ELSE 0 END  AS FL_ACESSO

FROM INN.DW_IDM_LOG IDM
FULL OUTER JOIN INN.DW_ASSINANTE_CTV CD 
      ON idm.user_id LIKE CD.ds_email 
      OR idm.user_id LIKE CD.nr_contrato  
      OR idm.user_id LIKE CD.nr_cpf_cnpj_assinante
WHERE 0 = 0
  AND TRUNC(IDM.EVENT_TIMESTAMP) BETWEEN 
    add_months(LAST_DAY(TO_DATE(&&DT_MOVIMENTO, 'YYYYMMDD')),-1)+1
            AND 
              LAST_DAY(TO_DATE(&&DT_MOVIMENTO, 'YYYYMMDD')) 
GROUP BY 
    TO_DATE(
      ADD_MONTHS(
              LAST_DAY(TO_DATE(TRUNC(IDM.EVENT_TIMESTAMP), 'YYYYMMDD')),-1)+1,'DD/MM/YYYY')
    ,CD.NR_CONTRATO
    ,CD.NR_CPF_CNPJ_ASSINANTE
    ,CASE WHEN UPPER(IDM.CLIENT_ID) = 'NETAPP' THEN 1 ELSE 0 END 
    ,CASE WHEN UPPER(IDM.CLIENT_ID) = 'CLAROTV_WCP_PROD' THEN 1 ELSE 0 END 
    ,CASE WHEN UPPER(IDM.CLIENT_ID) IN ('NETAPP','CLAROTV_WCP_PROD') THEN 1 ELSE 0 END 
;

DROP TABLE TMP_SQDAA_COMP_ATM_DTH_P3A;
CREATE TABLE TMP_SQDAA_COMP_ATM_DTH_P3A COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS
SELECT   /*+parallel (32)*/ ------------------------------------------------------------------------ETAPA 3 BASE ACESSO IDM
	DISTINCT 
	TO_DATE(
      ADD_MONTHS(
              LAST_DAY(TO_DATE(TRUNC(IDM.EVENT_TIMESTAMP), 'YYYYMMDD')),-1)+1,'DD/MM/YYYY') AS DAT_MOVIMENTO
    --IDM.SK_DATA
    --,IDM.EVENT_TIMESTAMP
    --,IDM.USER_ID
    --,IDM.CLIENT_ID
    ,a.nr_contrato
	
    --,A.ds_email
    --,a.nr_cpf_cnpj_assinante
FROM INN.DW_IDM_LOG IDM
FULL OUTER JOIN INN.DW_ASSINANTE_CTV A 
      ON idm.user_id LIKE a.ds_email 
      OR idm.user_id LIKE a.nr_contrato  
         OR idm.user_id LIKE a.nr_cpf_cnpj_assinante
WHERE 
  0 = 0
  AND IDM.SK_DATA BETWEEN TO_NUMBER(&&DATA_INICIAL) AND TO_NUMBER(&&DATA_FINAL)
-- AND IDM.USER_ID = '22484340818'
  AND IDM.EVENT_NAME = 'TOKEN_SUCCESS'
  --AND A.NR_CPF_CNPJ_ASSINANTE = 22484340818
  AND UPPER(TRIM(IDM.USER_ID)) IN ('RENANX@ME.COM', 'RENANX@GMAIL.COM','22484340818')
ORDER BY IDM.EVENT_TIMESTAMP
;


SELECT 
    IDM.SK_DATA
    ,IDM.EVENT_TIMESTAMP
    ,IDM.USER_ID
    ,IDM.CLIENT_ID
    ,a.nr_contrato
    ,A.ds_email
    ,a.nr_cpf_cnpj_assinante
FROM INN.DW_IDM_LOG IDM
FULL OUTER JOIN INN.DW_ASSINANTE_CTV A 
      ON idm.user_id LIKE a.ds_email 
      OR idm.user_id LIKE a.nr_contrato  
         OR idm.user_id LIKE a.nr_cpf_cnpj_assinante
WHERE 
  0 = 0
  AND IDM.SK_DATA BETWEEN TO_NUMBER(&&DATA_INICIAL) AND TO_NUMBER(&&DATA_FINAL)
-- AND IDM.USER_ID = '22484340818'
  AND IDM.EVENT_NAME = 'TOKEN_SUCCESS'
  --AND A.NR_CPF_CNPJ_ASSINANTE = 22484340818
  AND IDM.USER_ID IN ('renanx@me.com', 'renanx@gmail.com','22484340818')
ORDER BY IDM.EVENT_TIMESTAMP
;

--USER_ID IN ('renanx@me.com', 'renanx@gmail.com','22484340818')  -- DS_EMAIL
-- NR_CONTRATO IN (166453086, 168951997, 50579050, 154722952) --- CONTRATO
-- CLIENT_ID IN ('MINHA_NET_WCP', 'NetApp', 'ClaroTv_WCP_PROD', 'MinhaClaroDIG', 'NowOnlineClaro',  'Toolbox'

/*
		TABELAS DA CLARO TV

FT_BASE_PONTO_ASSINATURA_NET
FT_BASE_PONTO_ASSINATURA_CFI2 VARCHAR2
FT_BASE_PONTO_DOMICILIO_CFI VARCHAR2
FT_SAIDA_PONTO_DOMICILIO_CTV NUMBER
FT_BASE_PONTO_DOMICILIO_CTV NUMBER
DW_OCORRENCIA_CTV NUMBER
FT_BASE_PONTO_ASSINATURA_CTV NUMBER
DW_ASSINANTE_CTV
FT_FATURA_DIGITAL VARCHAR2
FT_BASE_PONTO_DOMICILIO_HH 
*/


Descrição:
Prezados por gentiliza, favor realizar o desbloqueio/reset do meu usuário T6137207 com acesso ao INN.

Segue TNS que utilizo:

INN.WORLD =
INN = (DESCRIPTION = (ADDRESS = (PROTOCOL = TCP)(HOST = ecc01lpa-scan1.br1.ocm.s7086037.oraclecloudatcustomer.com)(PORT = 1521))(CONNECT_DATA = (SERVER = DEDICATED)(SERVICE_NAME = INN) (FAILOVER_MODE = (TYPE = select)(METHOD = basic))) )



Dep: SQUAD AUTOATENDIMENTO
Servidor: ecc01lpa-scan1.br1.ocm.s7086037.oraclecloudatcustomer.com
Instancia: INN
Palicação: INN


--==========================================================================================================================================
W@6n45*009

chamado do Tableau 10112339 


ipconfig /flushdns

hostname NRSPDT1083612

https://acessootp.claro.com.br/


Wa6n45*001
########## banco BI da Claro VERSÃO 27/01/2020
P00DW1,P00DW1.WORLD =
  (DESCRIPTION =(FAILOVER=ON)
    (LOAD_BALANCE = yes)
    (ADDRESS_LIST =
      (ADDRESS = (PROTOCOL = TCP)(HOST =10.54.151.168)(PORT = 1521))
      (ADDRESS = (PROTOCOL = TCP)(HOST =10.54.151.169)(PORT = 1521))
      (ADDRESS = (PROTOCOL = TCP)(HOST =10.54.151.170)(PORT = 1521))
      (ADDRESS = (PROTOCOL = TCP)(HOST =10.54.151.171)(PORT = 1521))
      (ADDRESS = (PROTOCOL = TCP)(HOST =10.54.151.172)(PORT = 1521))
      (ADDRESS = (PROTOCOL = TCP)(HOST =10.54.151.173)(PORT = 1521))
      (ADDRESS = (PROTOCOL = TCP)(HOST =10.54.151.174)(PORT = 1521))
      (ADDRESS = (PROTOCOL = TCP)(HOST =10.54.151.175)(PORT = 1521))
    )
    (CONNECT_DATA =
      (SERVICE_NAME = P00DW1)
    )
  )

# INN
# ############################### Claro*Wa07
INN.WORLD =
INN = (DESCRIPTION = (ADDRESS = (PROTOCOL = TCP)(HOST = ecc01lpa-scan1.br1.ocm.s7086037.oraclecloudatcustomer.com)(PORT = 1521))(CONNECT_DATA = (SERVER = DEDICATED)(SERVICE_NAME = INN) (FAILOVER_MODE = (TYPE = select)(METHOD = basic))) )

==========================================================================================================
Prezados por gentiliza, favor realizar o desbloqueio/reset do meu usuário T6137207 com acesso ao INN.

Segue TNS que utilizo:

INN.WORLD =
INN = (DESCRIPTION = (ADDRESS = (PROTOCOL = TCP)(HOST = ecc01lpa-scan1.br1.ocm.s7086037.oraclecloudatcustomer.com)(PORT = 1521))(CONNECT_DATA = (SERVER = DEDICATED)(SERVICE_NAME = INN) (FAILOVER_MODE = (TYPE = select)(METHOD = basic))) )



Prezados,

Acredito que o bloqueio da minha senha está atrelado a troca da senha de rede e ao acessar o Oracle SQL Developer o mesmo solicitou uma migração e após realizar este procedimento não apareceu mais as minhas conexões nos bancos de dados. Realizei 3 tentativas e resultou no bloqueio.

Vejam o log de migração:
<?xml version="1.0" encoding="windows-1252" standalone="no"?>
<log>
</log>
--*************************************************************************************************************************************************************************************


CREATE TABLE TMP_SQDAA_COMP_ATM_DTH_P3A COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS
SELECT   /*+parallel (32)*/ ----------------------------------------------------------------------------------------------ETAPA 3A BASE DE ACESSOS IDM
  DISTINCT
  TO_DATE(
      ADD_MONTHS(
              LAST_DAY(TO_DATE(TRUNC(&&DT_MOVIMENTO), 'YYYYMMDD')),-1)+1,'DD/MM/YYYY') AS DAT_MOVIMENTO
    ,IDM.USER_ID AS LOGIN_AUTENTICA
    ,COUNT(DISTINCT(idm.user_id||IDM.SK_DATA||REPLACE(SUBSTR(TO_CHAR(IDM.EVENT_TIMESTAMP,'dd/mm/yyyy hh24:mi:ss'),12,8),':',''))) AS QT_ACESSO
    ,SUM(CASE WHEN UPPER(IDM.CLIENT_ID) = 'NETAPP'                        THEN 1 ELSE 0 END) AS QT_ACSS_APP
    ,SUM(CASE WHEN UPPER(IDM.CLIENT_ID) = 'CLAROTV_WCP_PROD'              THEN 1 ELSE 0 END) AS QT_ACSS_SITE
    ,SUM(CASE WHEN UPPER(IDM.CLIENT_ID) IN ('NETAPP','CLAROTV_WCP_PROD') THEN 1 ELSE 0 END) AS QT_ACSS_SITE_APP
FROM INN.DW_IDM_LOG IDM  
WHERE 0 = 0
    AND TRUNC(IDM.EVENT_TIMESTAMP) BETWEEN 
        add_months(LAST_DAY(TO_DATE(&&DT_MOVIMENTO, 'YYYYMMDD')),-1)+1
            AND 
              LAST_DAY(TO_DATE(&&DT_MOVIMENTO, 'YYYYMMDD')) 
    AND IDM.EVENT_NAME = 'TOKEN_SUCCESS'
    AND UPPER(IDM.CLIENT_ID) IN ('NETAPP','CLAROTV_WCP_PROD')
    AND IDM.USER_ID IN (SELECT ds_email AS LOGIN_AUTENTICA FROM INN.DW_ASSINANTE_CTV WHERE ds_email IS NOT NULL
                        UNION ALL
                        SELECT TO_CHAR(nr_cpf_cnpj_assinante) AS LOGIN_AUTENTICA FROM INN.DW_ASSINANTE_CTV WHERE nr_cpf_cnpj_assinante IS NOT NULL
                        UNION ALL
                        SELECT REPLACE(REGEXP_SUBSTR (ds_email,'^.*@'),'@','') AS LOGIN_AUTENTICA FROM INN.DW_ASSINANTE_CTV WHERE ds_email IS NOT NULL
						UNION ALL
						SELECT TO_CHAR(nr_contrato) AS LOGIN_AUTENTICA FROM INN.DW_ASSINANTE_CTV  WHERE nr_contrato IS NOT NULL)
GROUP BY 
   
    TO_DATE(
      ADD_MONTHS(
              LAST_DAY(TO_DATE(TRUNC(&&DT_MOVIMENTO), 'YYYYMMDD')),-1)+1,'DD/MM/YYYY')
    ,IDM.USER_ID 
;
