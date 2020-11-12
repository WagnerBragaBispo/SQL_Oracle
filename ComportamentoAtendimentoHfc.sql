/*---==============================================================================================================================================================

		70296 - Atualização do indicador de atendimento de clientes (liga e acesso, somente acesso, somente liga)
		O que precisa ser feito:  Recriar o indicador para Residencial e móvel dos clientes, criando uma base ativa considerando as seguintes divisões:

		Clientes que ligam e acessam;
		Clientes que acessam;
		Clientes que só liga;
		Clientes que não interagiram no mês;
		
		instrução da query:
			Passo 1A: Trazer a base ativa de clientes da Móvel, sendo a mesma utilizada no Contact Rate;
			Passo 1B: Trazer a base ativa de clientes da NET, sendo a mesma utilizada no Contact Rate;
			Passo 2: Móvel empilhar as bases de acesso dos canais (USSD + MCM + NovoAPP + WhatsApp + ChatBot);
			
			OPS_ALTERYX.BOOK_EVENTOS --- indicado para saber quais ocorrências foram aberta no caso NET
			OPS_ALTERYX.WRK_BOOK_ASSINANTE
			OPS_ALTERYX.BOOK_EVENTOS_EQUIPAMENTO
			OPS_ALTERYX.BOOK_EVENTOS_V2
			OPS_ALTERYX.BOOK_SELF_SERVICE
			OPS_ALTERYX.CX_NPS_MOVEL_BOOK
			
			Raniere data Stu
			
			RANIERI SANTOS SILVA
			Não incomodar
			COORD INDICADORES NEGOCIOS
			10MZBR6B20 GER DE GESTAO DE INFORMACOES
			NAO INFORMADO
			RANIERI.SILVA@claro.com.br
			
			TESTE: 003 350349900 CONTRATO
			CID OPERADORA TI 25666 COD_OPERADORA 13
			
			Aplicativo
			Aplicativo - Meu TÃ©cnico
			Desktop
			Mobile
			Site e Mobile Browser
			
---==============================================================================================================================================================*/

DROP TABLE TMP_SQDAA_COMP_ATM_HFC_P1; -------------------------------------------------------------------------ETAPA 1 BASE ATIVA
DROP TABLE TMP_SQDAA_COMP_ATM_HFC_P2; -------------------------------------------------------------------------ETAPA 2 BASE DE LIGACOES
DROP TABLE TMP_SQDAA_COMP_ATM_HFC_P3A; ------------------------------------------------------------------------ETAPA 3A FATURA FÁCIL
DROP TABLE TMP_SQDAA_COMP_ATM_HFC_P3B; ------------------------------------------------------------------------ETAPA 3B IN ('MINHANET')
DROP TABLE TMP_SQDAA_COMP_ATM_HFC_P3C; ------------------------------------------------------------------------ETAPA 3C AREA LOGADA IN ('NETAPP','NETAPPNOVO')
DROP TABLE TMP_SQDAA_COMP_ATM_HFC_P4A; ------------------------------------------------------------------------ETAPA 4A ACESSOS QUE NÃO ESTAVAM NA BASE ATIVA
DROP TABLE TMP_SQDAA_COMP_ATM_HFC_P4B; ------------------------------------------------------------------------ETAPA 4B INCLUIR OS ACESSOS E LIGAÇÕES QUE NÃO ESTAVAM NA BASE ATIVA
DROP TABLE TMP_SQDAA_COMP_ATM_HFC_P5; -------------------------------------------------------------------------ETAPA 5 CLASSIFICAÇÃO


DROP TABLE TMP_SQDAA_COMP_ATM_HFC_P1;
CREATE TABLE TMP_SQDAA_COMP_ATM_HFC_P1 COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS
SELECT  /*+parallel (32)*/ ------------------------------------------------------------------------ETAPA 1 BASE ATIVA
	DISTINCT
		ADD_MONTHS(LAST_DAY(DAT_MOVIMENTO),-1)+1 AS DAT_MOVIMENTO
		,COD_OPERADORA
		,NUM_CONTRATO
		,regexp_replace(TO_CHAR(COD_OPERADORA,'000')||TO_CHAR(NUM_CONTRATO,'000000000'),'[[:space:]]*','') AS CODIGO_NET
		,1 AS FLG_BASE_ATIVA
FROM OPS_ALTERYX.BI_FP_CONTRATO_NET@BASECLARO
WHERE 0 = 0
	AND DAT_MOVIMENTO in to_date('01/08/2020','DD/MM/YYYY')
	--AND DAT_MOVIMENTO in TRUNC(TO_DATE(LAST_DAY(ADD_MONTHS(TO_DATE('&&DAT_REF_YYYYMMDD','YYYYMMDD'),-1))+1,'DD/MM/YYYY'))
GROUP BY 
		DAT_MOVIMENTO
		,COD_OPERADORA
		,NUM_CONTRATO
;
/*VALIDAÇÃO BASE ATIVA*/
-- 01/08/2020 - 10.322.241 TOTAL DE DOMICILIOS
SELECT COUNT(CODIGO_NET) QT_DOM1 ,COUNT(DISTINCT(CODIGO_NET)) QT_DOM2 FROM TMP_SQDAA_COMP_ATM_HFC_P1;


DROP TABLE TMP_SQDAA_COMP_ATM_HFC_P2;
CREATE TABLE TMP_SQDAA_COMP_ATM_HFC_P2 COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS
SELECT DISTINCT  /*+parallel (32)*/ ------------------------------------------------------------------------ETAPA 2 BASE DE LIGACOES
	TO_DATE(
      ADD_MONTHS(
              LAST_DAY(TO_DATE(&&DT_MOVIMENTO, 'YYYYMMDD')),-1)+1,'DD/MM/YYYY') AS DAT_MOVIMENTO
		,CASE WHEN LIG.CD_CIDADE_CONTRATO = 76066 THEN 696 ELSE LIG.CD_OPERADORA END AS COD_OPERADORA
		,LIG.CD_CIDADE_CONTRATO
		,LIG.NR_CONTRATO AS NUM_CONTRATO
		,CASE WHEN LIG.CD_CIDADE_CONTRATO = 76066 THEN regexp_replace('696'||TO_CHAR(LIG.NR_CONTRATO,'000000000'),'[[:space:]]*','')
			  ELSE regexp_replace(TO_CHAR(LIG.CD_OPERADORA ,'000')||TO_CHAR(LIG.NR_CONTRATO,'000000000'),'[[:space:]]*','') END 
				AS CODIGO_NET

FROM INN.ANALITICA_CR_BI_LIG_NET LIG

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
      AND LIG.CD_CIDADE_CONTRATO NOT IN (-1, 2121) ---2121 CLARO TV
      
GROUP BY 
    TO_DATE(
      ADD_MONTHS(
              LAST_DAY(TO_DATE(&&DT_MOVIMENTO, 'YYYYMMDD')),-1)+1,'DD/MM/YYYY')
		,CASE WHEN LIG.CD_CIDADE_CONTRATO = 76066 THEN 696 ELSE LIG.CD_OPERADORA END
		,LIG.CD_CIDADE_CONTRATO
		,LIG.NR_CONTRATO
		,CASE WHEN LIG.CD_CIDADE_CONTRATO = 76066 THEN regexp_replace('696'||TO_CHAR(LIG.NR_CONTRATO,'000000000'),'[[:space:]]*','')
			  ELSE regexp_replace(TO_CHAR(LIG.CD_OPERADORA ,'000')||TO_CHAR(LIG.NR_CONTRATO,'000000000'),'[[:space:]]*','') END
;
/*----------------------------------------------------------------------------
		VALIDAÇÃO 
------------------------------------------------------------------------------*/
SELECT COUNT(CODIGO_NET) QT_DOM1 ,COUNT(DISTINCT(CODIGO_NET)) QT_DOM2 FROM TMP_SQDAA_COMP_ATM_HFC_P2;
 --01/08/2020 8405563                3140315 
--01/08/2020 3101198	3101197




DROP TABLE TMP_SQDAA_COMP_ATM_HFC_P3A;
CREATE TABLE TMP_SQDAA_COMP_ATM_HFC_P3A COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS
SELECT DISTINCT /*+parallel (32)*/ ------------------------------------------------------------------------ETAPA 3A FATURA FÁCIL
		
		add_months(LAST_DAY(TO_DATE(&&DT_MOVIMENTO, 'YYYYMMDD')),-1)+1 AS DAT_MOVIMENTO
		,TO_NUMBER(TO_CHAR(SUBSTR(A.CODIGO_NET,1,3),'000')) 					AS COD_OPERADORA
		,TO_NUMBER(TO_CHAR(SUBSTR(A.CODIGO_NET,4,9),'000000000')) 				AS NUM_CONTRATO
		,A.CODIGO_NET
		,'SITE MCR FATURA FACIL' AS CANAL_ATD     
FROM INN.EXT_ADMINSITE A
     
WHERE 
	0 = 0
		AND TRUNC(A.DT_HR_SOLICITACAO) BETWEEN 
          add_months(LAST_DAY(TO_DATE(&&DT_MOVIMENTO, 'YYYYMMDD')),-1)+1
            AND 
              LAST_DAY(TO_DATE(&&DT_MOVIMENTO, 'YYYYMMDD')) 
		AND UPPER(A.NM_TIPO_PEDIDO) LIKE '%FATURA F?CIL%' --->>> SOMENTE ACESSO
		AND A.NM_DESC_PEDIDO LIKE 'LOG de Aplicacao' -- MARCACAO DA AREA NÃO LOGADA DO FATURA FÁCIL;
GROUP BY   
		add_months(LAST_DAY(TO_DATE(&&DT_MOVIMENTO, 'YYYYMMDD')),-1)+1
		,TO_NUMBER(TO_CHAR(SUBSTR(A.CODIGO_NET,1,3),'000')) 
		,TO_NUMBER(TO_CHAR(SUBSTR(A.CODIGO_NET,4,9),'000000000'))
		,A.CODIGO_NET
ORDER BY A.CODIGO_NET
;
/*----------------------------------------------------------------------------
		VALIDAÇÃO 
------------------------------------------------------------------------------*/
SELECT COUNT(CODIGO_NET) QT1 ,COUNT(DISTINCT(CODIGO_NET)) AS QT2 FROM TMP_SQDAA_COMP_ATM_HFC_P3A;
-- 01/08/2020 125.284 125.284  


DROP TABLE TMP_SQDAA_COMP_ATM_HFC_P3B;
CREATE TABLE TMP_SQDAA_COMP_ATM_HFC_P3B COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS
SELECT DISTINCT /*+parallel (32)*/ ------------------------------------------------------------------------ETAPA 3B IN ('MINHANET')

		add_months(LAST_DAY(TO_DATE(&&DT_MOVIMENTO, 'YYYYMMDD')),-1)+1 AS DAT_MOVIMENTO
		,TO_NUMBER(TO_CHAR(SUBSTR(A.CODIGO_NET,1,3),'000')) 					AS COD_OPERADORA
		,TO_NUMBER(TO_CHAR(SUBSTR(A.CODIGO_NET,4,9),'000000000')) 				AS NUM_CONTRATO
		,A.CODIGO_NET
		,'SITE MCR' AS CANAL_ATD     
FROM INN.EXT_ADMINSITE A
     
WHERE 
		0 = 0
		AND TRUNC(A.DT_HR_SOLICITACAO) BETWEEN 
          add_months(LAST_DAY(TO_DATE(&&DT_MOVIMENTO, 'YYYYMMDD')),-1)+1
            AND 
              LAST_DAY(TO_DATE(&&DT_MOVIMENTO, 'YYYYMMDD')) 
		AND NM_TIPO_PEDIDO = 'Consulta VT DashBoard' --->>> SOMENTE ACESSO
		AND TRIM(SUBSTR(NM_DESC_PEDIDO,INSTR(A.NM_DESC_PEDIDO,'|origem:')+8)) IN ('MINHANET') 
GROUP BY   
	add_months(LAST_DAY(TO_DATE(&&DT_MOVIMENTO, 'YYYYMMDD')),-1)+1
	,TO_NUMBER(TO_CHAR(SUBSTR(A.CODIGO_NET,1,3),'000')) 
	,TO_NUMBER(TO_CHAR(SUBSTR(A.CODIGO_NET,4,9),'000000000'))
	,A.CODIGO_NET
ORDER BY A.CODIGO_NET
;

/*----------------------------------------------------------------------------
		VALIDAÇÃO 
------------------------------------------------------------------------------*/
SELECT COUNT(CODIGO_NET) QT1 ,COUNT(DISTINCT(CODIGO_NET)) AS QT2 FROM TMP_SQDAA_COMP_ATM_HFC_P3B;
--01/08/2020 1.060.545                1060545 



DROP TABLE TMP_SQDAA_COMP_ATM_HFC_P3C;
CREATE TABLE TMP_SQDAA_COMP_ATM_HFC_P3C COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS
SELECT DISTINCT /*+parallel (32)*/ ------------------------------------------------------------------------ETAPA 3C AREA LOGADA IN ('NETAPP','NETAPPNOVO')

		add_months(LAST_DAY(TO_DATE(&&DT_MOVIMENTO, 'YYYYMMDD')),-1)+1 AS DAT_MOVIMENTO
		,TO_NUMBER(TO_CHAR(SUBSTR(A.CODIGO_NET,1,3),'000')) 					AS COD_OPERADORA
		,TO_NUMBER(TO_CHAR(SUBSTR(A.CODIGO_NET,4,9),'000000000')) 				AS NUM_CONTRATO
		,A.CODIGO_NET
		,'APP' AS CANAL_ATD     
FROM INN.EXT_ADMINSITE A
     
WHERE 
		0 = 0
		AND TRUNC(A.DT_HR_SOLICITACAO) BETWEEN 
          add_months(LAST_DAY(TO_DATE(&&DT_MOVIMENTO, 'YYYYMMDD')),-1)+1
            AND 
              LAST_DAY(TO_DATE(&&DT_MOVIMENTO, 'YYYYMMDD')) 
		AND NM_TIPO_PEDIDO = 'Consulta VT DashBoard' --->>> SOMENTE ACESSO
		AND TRIM(SUBSTR(NM_DESC_PEDIDO,INSTR(A.NM_DESC_PEDIDO,'|origem:')+8)) IN ('NETAPP','NETAPPNOVO') --'BOT','MIND';
GROUP BY   
	add_months(LAST_DAY(TO_DATE(&&DT_MOVIMENTO, 'YYYYMMDD')),-1)+1
	,TO_NUMBER(TO_CHAR(SUBSTR(A.CODIGO_NET,1,3),'000')) 
	,TO_NUMBER(TO_CHAR(SUBSTR(A.CODIGO_NET,4,9),'000000000'))
	,A.CODIGO_NET
ORDER BY A.CODIGO_NET
;
/*----------------------------------------------------------------------------
		VALIDAÇÃO 
------------------------------------------------------------------------------*/
SELECT COUNT(CODIGO_NET) QT1 ,COUNT(DISTINCT(CODIGO_NET)) AS QT2 FROM TMP_SQDAA_COMP_ATM_HFC_P3C;
-- 01/08/2020 2.709  2709



DROP TABLE TMP_SQDAA_COMP_ATM_HFC_P4A;
CREATE TABLE TMP_SQDAA_COMP_ATM_HFC_P4A COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS -------ETAPA 4A ACESSOS QUE NÃO ESTAVAM NA BASE ATIVA
SELECT /*+parallel (32)*/ DISTINCT LIG.DAT_MOVIMENTO ,LIG.COD_OPERADORA ,LIG.NUM_CONTRATO ,LIG.CODIGO_NET 
      FROM TMP_SQDAA_COMP_ATM_HFC_P1 DOM RIGHT JOIN TMP_SQDAA_COMP_ATM_HFC_P2 LIG ON (DOM.CODIGO_NET = LIG.CODIGO_NET) WHERE DOM.CODIGO_NET IS NULL
UNION ALL
SELECT /*+parallel (32)*/ DISTINCT FAC.DAT_MOVIMENTO ,FAC.COD_OPERADORA ,FAC.NUM_CONTRATO ,FAC.CODIGO_NET 
      FROM TMP_SQDAA_COMP_ATM_HFC_P1 DOM RIGHT JOIN TMP_SQDAA_COMP_ATM_HFC_P3A FAC ON (DOM.CODIGO_NET = FAC.CODIGO_NET) WHERE DOM.CODIGO_NET IS NULL

UNION ALL
SELECT /*+parallel (32)*/ DISTINCT SIT.DAT_MOVIMENTO ,SIT.COD_OPERADORA ,SIT.NUM_CONTRATO ,SIT.CODIGO_NET 
      FROM TMP_SQDAA_COMP_ATM_HFC_P1 DOM RIGHT JOIN TMP_SQDAA_COMP_ATM_HFC_P3B SIT ON (DOM.CODIGO_NET = SIT.CODIGO_NET) WHERE DOM.CODIGO_NET IS NULL
UNION ALL
SELECT /*+parallel (32)*/ DISTINCT APP.DAT_MOVIMENTO ,APP.COD_OPERADORA ,APP.NUM_CONTRATO ,APP.CODIGO_NET 
      FROM TMP_SQDAA_COMP_ATM_HFC_P1 DOM RIGHT JOIN TMP_SQDAA_COMP_ATM_HFC_P3C APP ON (DOM.CODIGO_NET = APP.CODIGO_NET) WHERE DOM.CODIGO_NET IS NULL
;
/*----------------------------------------------------------------------------
		VALIDAÇÃO 
------------------------------------------------------------------------------*/
SELECT COUNT(CODIGO_NET) QT_DOM1 ,COUNT(DISTINCT(CODIGO_NET)) QT_DOM2 FROM TMP_SQDAA_COMP_ATM_HFC_P4A;
-- 01/08/2020 12	12


INSERT /*+APPEND */ INTO TMP_SQDAA_COMP_ATM_HFC_P1 --------------------------------------------------ETAPA 4A INCLUIR OS ACESSOS E LIGAÇÕES QUE NÃO ESTAVAM NA BASE ATIVA
SELECT /*+PARALLEL (32)*/
	DISTINCT
       TO_DATE(
      ADD_MONTHS(
              LAST_DAY(TO_DATE(&&DT_MOVIMENTO, 'YYYYMMDD')),-1)+1,'DD/MM/YYYY') AS DAT_MOVIMENTO
      ,DOM.COD_OPERADORA
      ,DOM.NUM_CONTRATO
      ,dom.codigo_net
      ,0 AS FLG_BASE_ATIVA
FROM TMP_SQDAA_COMP_ATM_HFC_P4A DOM
	GROUP BY 
    TO_DATE(
      ADD_MONTHS(
              LAST_DAY(TO_DATE(&&DT_MOVIMENTO, 'YYYYMMDD')),-1)+1,'DD/MM/YYYY')
      ,DOM.COD_OPERADORA
      ,DOM.NUM_CONTRATO
      ,dom.codigo_net
;
COMMIT;


DROP TABLE TMP_SQDAA_COMP_ATM_HFC_P4B;
CREATE TABLE TMP_SQDAA_COMP_ATM_HFC_P4B COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS
SELECT  /*+parallel (32)*/ ------------------------------------------------------------------------ETAPA 4B CROSS BASE ATIVA & ACESSO & LIGA
    DOM.DAT_MOVIMENTO
		,DOM.COD_OPERADORA
		,DOM.NUM_CONTRATO
    ,DOM.CODIGO_NET
    ,DOM.FLG_BASE_ATIVA
    ,CASE WHEN LIG.CODIGO_NET IS NULL THEN 0 ELSE 1 END AS FL_LIGA
    ,CASE WHEN FAC.CODIGO_NET IS NULL THEN 0 ELSE 1 END AS FL_SITE_FATFACIL
    ,CASE WHEN SIT.CODIGO_NET IS NULL THEN 0 ELSE 1 END AS FL_SITE
    ,CASE WHEN APP.CODIGO_NET IS NULL THEN 0 ELSE 1 END AS FL_APP    
    ,MAX(CASE WHEN DOM.CODIGO_NET = FAC.CODIGO_NET THEN 1
              WHEN DOM.CODIGO_NET = SIT.CODIGO_NET THEN 1
              WHEN DOM.CODIGO_NET = APP.CODIGO_NET THEN 1              
                ELSE 0 END) AS FL_ACESSO
FROM TMP_SQDAA_COMP_ATM_HFC_P1        DOM
LEFT JOIN TMP_SQDAA_COMP_ATM_HFC_P2   LIG           ON (DOM.CODIGO_NET = LIG.CODIGO_NET)
LEFT JOIN TMP_SQDAA_COMP_ATM_HFC_P3A  FAC           ON (DOM.CODIGO_NET = FAC.CODIGO_NET)
LEFT JOIN TMP_SQDAA_COMP_ATM_HFC_P3B  SIT           ON (DOM.CODIGO_NET = SIT.CODIGO_NET)
LEFT JOIN TMP_SQDAA_COMP_ATM_HFC_P3C  APP           ON (DOM.CODIGO_NET = APP.CODIGO_NET)

GROUP BY 
  DOM.DAT_MOVIMENTO
		,DOM.COD_OPERADORA
		,DOM.NUM_CONTRATO
    ,DOM.CODIGO_NET
    ,DOM.FLG_BASE_ATIVA
    ,CASE WHEN LIG.CODIGO_NET IS NULL THEN 0 ELSE 1 END
    ,CASE WHEN FAC.CODIGO_NET IS NULL THEN 0 ELSE 1 END
    ,CASE WHEN SIT.CODIGO_NET IS NULL THEN 0 ELSE 1 END
    ,CASE WHEN APP.CODIGO_NET IS NULL THEN 0 ELSE 1 END
    ,(CASE WHEN DOM.CODIGO_NET = FAC.CODIGO_NET THEN 1
              WHEN DOM.CODIGO_NET = SIT.CODIGO_NET THEN 1
              WHEN DOM.CODIGO_NET = APP.CODIGO_NET THEN 1              
                ELSE 0 END)
;



SELECT 
  COUNT(CODIGO_NET) QT_DOM1
  ,COUNT(DISTINCT(CODIGO_NET)) QT_DOM2
  ,SUM(FL_LIGA) QT_LIGA
  ,SUM(FL_SITE_FATFACIL) QT_SITE_FATFACIL
  ,SUM(FL_SITE) QT_FL_SITE
  ,SUM(FL_APP) QT_FL_APP
  ,SUM(FL_ACESSO) QT_ACESSO
  FROM TMP_SQDAA_COMP_ATM_HFC_P4B
WHERE FLG_BASE_ATIVA <> 0 --- RETIRANDO OS INATIVOS
;


DROP TABLE TMP_SQDAA_COMP_ATM_HFC_P5;
CREATE TABLE TMP_SQDAA_COMP_ATM_HFC_P5 COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS
SELECT  /*+parallel (32)*/ ------------------------------------------------------------------------ETAPA 5 CLASSIFICAÇÃO
    DOM.DAT_MOVIMENTO
		,DOM.COD_OPERADORA
		,DOM.NUM_CONTRATO
    ,DOM.CODIGO_NET
    ,DOM.FLG_BASE_ATIVA
    ,DOM.FL_LIGA
    ,DOM.FL_SITE_FATFACIL
    ,DOM.FL_SITE
    ,DOM.FL_APP
    ,DOM.FL_ACESSO
    ,CASE WHEN DOM.FL_LIGA = 1 AND DOM.FL_ACESSO = 1  THEN 'LIGA E ACESSA'
          WHEN DOM.FL_LIGA = 0 AND DOM.FL_ACESSO = 1  THEN 'SOMENTE ACESSA'
          WHEN DOM.FL_LIGA = 1 AND DOM.FL_ACESSO = 0  THEN 'SOMENTE LIGA'
          WHEN DOM.FL_LIGA = 0 AND DOM.FL_ACESSO = 0  THEN 'SEM INTERAÇÃO'
            ELSE 'XXXX' END AS STS_ATENDIMENTO
   --  ,COUNT(*)
FROM TMP_SQDAA_COMP_ATM_HFC_P4B DOM
GROUP BY 
    DOM.DAT_MOVIMENTO
		,DOM.COD_OPERADORA
		,DOM.NUM_CONTRATO
    ,DOM.CODIGO_NET
    ,DOM.FLG_BASE_ATIVA
    ,DOM.FL_LIGA
    ,DOM.FL_SITE_FATFACIL
    ,DOM.FL_SITE
    ,DOM.FL_APP
    ,DOM.FL_ACESSO
            ;
--GRANT SELECT ON TMP_SQDAA_COMP_ATM_HFC_P5 TO PUBLIC;

INSERT /*+APPEND */ INTO AGG_COMP_ATEND_HFC ------------------------------------------------------------------------ETAPA 5 TABELA AGREGADA
--CREATE TABLE AGG_COMP_ATEND_HFC COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS
SELECT DISTINCT
  DOM.DAT_MOVIMENTO
	,DOM.FLG_BASE_ATIVA
  ,DOM.STS_ATENDIMENTO
  ,COUNT(CODIGO_NET)        AS QT_ATIVO
  ,SUM(FL_LIGA)             AS QT_LIGA
  ,SUM(FL_SITE_FATFACIL)    AS QT_FL_SITE_FATFACIL
  ,SUM(FL_SITE)             AS QT_FL_SITE
  ,SUM(FL_APP)              AS QT_FL_APP
  ,SUM(FL_ACESSO)           AS QT_ACESSO
  ,SYSDATE                  AS DAT_CRIACAO
  FROM TMP_SQDAA_COMP_ATM_HFC_P5 DOM

GROUP BY 
   DOM.DAT_MOVIMENTO
	,DOM.FLG_BASE_ATIVA
  ,DOM.STS_ATENDIMENTO
;
COMMIT;









/* VALIDA CR -------------------------
SELECT 
    NM_MOTIVO_URA
    ,SUM(QT_RAT_MOT_FINAL) QT_RAT_MOT_FINAL
  
FROM INN.ANALITICA_CR_BI_LIG_NET  
WHERE 0 = 0
      AND SK_DATA BETWEEN 20200801 AND 20200831
      AND NM_EXPURGO_URA = 'Ligações Válidas'
      --AND NR_CONTRATO IS NOT NULL 
      --AND CD_CIDADE_CONTRATO <> -1 
      AND FC_DIRECIONADO_RETIDO = 'Sim'
GROUP BY NM_MOTIVO_URA
;
*/


/*---****************************************************************************************************************************************
		VALIDAÇÃO DA BASE ATIVA NET
	
---*****************************************************************************************************************************************/

SELECT 
  dat_movimento
  ,COUNT(DISTINCT(TO_CHAR(COD_OPERADORA,'000')||TO_CHAR(NUM_CONTRATO,'000000000'))) QT_DOM
FROM OPS_ALTERYX.BI_FP_CONTRATO_NET
WHERE DAT_MOVIMENTO in to_date('01/08/2020','DD/MM/YYYY')
GROUP BY DAT_MOVIMENTO;
--- 01/08/20 10.322.241


SELECT 
  dat_movimento
  ,COUNT(DISTINCT(TO_CHAR(COD_OPERADORA,'000')||TO_CHAR(NUM_CONTRATO,'000000000'))) QT_DOM
FROM OPS_ALTERYX.BI_FP_CONTRATO_NET
WHERE DAT_MOVIMENTO > TO_DATE('01/12/2019','DD/MM/YYYY')
GROUP BY DAT_MOVIMENTO;
/*
01/08/20	10322241
01/05/20	10264939
01/02/20	10156003
01/06/20	10283075
01/04/20	10237150
01/01/20	10035888
01/07/20	10307101
01/03/20	10201831
*/



/*---****************************************************************************************************************************************
		VALIDAÇÃO CONTACT RATE
	
---*****************************************************************************************************************************************/

SELECT 
				SUBSTR(TO_CHAR(SK_DATA),1,6) AS SAFRA
				,NR_OCORRENCIA
				,NM_SUB_MARCA
				,CD_CIDADE_CONTRATO
				,DH_LIGACAO
				,NR_CONTRATO
				,NM_MOTIVO_URA_REMARCADO
				,NM_SUBMOTIVO_URA
				,NR_TELEFONE_CHAMADA
				,SK_DATA
				,NM_MARCA
			FROM INN.ANALITICA_CR_BI_LIG_NET  WHERE NR_CONTRATO IS NOT NULL AND 
												   CD_CIDADE_CONTRATO <> -1 AND 
												   SK_DATA BETWEEN 20200601 AND 20200831


/*---****************************************************************************************************************************************
		VALIDAÇÃO dos acessos
	
---*****************************************************************************************************************************************/

SELECT 
TO_DATE(A.DT_HR_SOLICITACAO, 'DD/MM/RRRR') as DT_EVENTO,
A.NM_CIDADE,
A.NM_TIPO_PEDIDO,
COUNT (*) AS QTDE_TRANSACOES
FROM INN.EXT_ADMINSITE A
WHERE TRUNC(A.DT_HR_SOLICITACAO) BETWEEN '01/08/2020' AND '22/09/2020'
AND NM_TIPO_PEDIDO IN ('Data de vencimento',
                       'Cancelamento de Fatura Digital',
                       'Debito Conta Corrente',
                       'Endere?o de Cobran?a',
                       'Promessa de Pagamento',
                       'Solicita??o de Fatura Digital',
                       'Visita Tecnica - Cancela',
                       'Visita Tecnica - Confirma',
                       'Visita Tecnica - Reagenda',
                       'Altera??o de Nome Rede Wifi',
                       'Altera??o de Senha Rede Wifi',
                       'Altera??o de Nome e Senha Rede Wifi',
                       'Cadastro - NET Combo',
                       'Dados pessoais',
                       'Cadastro - NET Combo - Conta Filho',
                       'Altera??o de Senha - Conta Filho',
                       'Cadastro - Mobile Site/APP')
GROUP BY
TO_DATE(A.DT_HR_SOLICITACAO, 'DD/MM/RRRR'),
A.NM_CIDADE,
A.NM_TIPO_PEDIDO;


