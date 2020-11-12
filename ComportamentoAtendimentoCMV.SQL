/*--==========================================================================================================================================================================================


	DESCRIÇÃO DO CARD INICIADO EM 14.09.2020:
	
		CARD: 74441 | Estruturar indicador de comportamento de usuário únicos por marca (resumo executivo) - Móvel - (FASE 1)

		O que precisa ser feito: construir a visão de usuários únicos por marca que só ligam, só acessam, ligam e acessam e não interagem no mês para cada marca 

			+ detalhes: considerar os canais digitais para essa medição  de acessos (site / app/ whatsapp / ussd)
			para o indicador só acessa e liga e acessa, precisaremos abrir o acesso por canal, para que saibamos qual o canal que o cliente que só acessa usa e qual o canal que o cliente que liga e acessa busca. 

			O card pode ser concluído quando (específico): tivermos esse indicador construído e validado com as áreas interessadas (DAC, Digital, Técnica e FIN). 


		Principais bases:
			Ativa 
			Contact Rate
			Acessos

CMV - BASE ACESSO
				CLIENTES QUE ACESSAM: 
			OK		NOVO APP --> PS8	1.045.564 (APURADO 1.045.564)
			OK		SITE E APP ANTIGO + PJ --> PS8	3.582.877 (APURADO 3.582.877)
			OK		USSD --> MINT	4.662.575 (Retido digital 3.391.002) (APURADO 3.253.178)
			OK		WHATSAPP --> Phe usa o Blip	1.293.175	(APURADO 1.865.834)
					Fatura Fácil --> SEM ORIGEM 174.564
					CHATBOT	--> Google Analytics	4.158
					MINHA CLARO PJ	--> Google Analytics	24.896
					MINHA CLARO FLEX	--> Google Analytics	82.556

	TABELAS:
		
		SELECT * FROM AGG_COMP_ATEND; --> TABELA AGREGADA 
		SELECT * FROM AGG_COMP_ATEND WHERE dat_criacao LIKE TO_DATE('23/09/2020','DD/MM/YYYY');
		GRANT SELECT ON SQDAA_FT_COMP_ATM_CMV TO PUBLIC; --> TABELA CRIADO COM O MÊS DE AGSOTO PARA WILL
		
		TMP_SQDAA_COMP_ATM_CMV_P1 	--> ETAPA 1 BASE ATIVA
		TMP_SQDAA_COMP_ATM_CMV_P2 	--> ETAPA 2 LIGAÇÕES
		TMP_SQDAA_COMP_ATM_CMV_P3A 	--> ETAPA 3A ACESSO NOVO APP
		TMP_SQDAA_COMP_ATM_CMV_P3B 	--> ETAPA 3B ACESSO SITE APP ANTIGO
		TMP_SQDAA_COMP_ATM_CMV_P3C	--> ETAPA 3C ACESSO MINHA CLARO PJ
		TMP_SQDAA_COMP_ATM_CMV_P3D	--> ETAPA 3D ACESSO USSD
		TMP_SQDAA_COMP_ATM_CMV_P3E	--> ETAPA 3E ACESSO WHATSAPP
		
		
		DMMKT.DM_FP_BASE_ASSINANTE
		
		SELECT FAM.DW_NUM_NTC,
FAM.NUM_NTC
FROM DMMKT.DM_FP_BASE_ASSINANTE FAM, DWH.BI_DIM_TIPO_MOVIMENTO TM
WHERE 0 = 0
AND FAM.COD_TIPO_MOV_DW_AJUSTADO > 0
AND TM.COD_TIPO_MOV_DW = FAM.COD_TIPO_MOV_DW_AJUSTADO
AND TM.IND_BASE_ATIVA = 'S'
AND FAM.DAT_EXPIRACAO_DW IS NULL
AND FAM.DAT_MOVIMENTO IN
LAST_DAY(TRUNC(TO_DATE('01/08/2020', 'DD/MM/YYYY'), 'MM'))
AND FAM.COD_PLATAFORMA_ATU||FAM.cod_sub_sts_atu <> 'PREPGZB1'
AND FAM.COD_PLATAFORMA_ATU <> 'SGIOT'
GROUP BY FAM.DW_NUM_NTC,
FAM.NUM_NTC
		
--==========================================================================================================================================================================================*/

DROP TABLE TMP_SQDAA_COMP_ATM_CMV_P1;
DROP TABLE TMP_SQDAA_COMP_ATM_CMV_P2;
DROP TABLE TMP_SQDAA_COMP_ATM_CMV_P3A;
DROP TABLE TMP_SQDAA_COMP_ATM_CMV_P3B;
DROP TABLE TMP_SQDAA_COMP_ATM_CMV_P3C;
DROP TABLE TMP_SQDAA_COMP_ATM_CMV_P3D;
DROP TABLE TMP_SQDAA_COMP_ATM_CMV_P3E;
DROP TABLE TMP_SQDAA_COMP_ATM_CMV_P4A;
DROP TABLE TMP_SQDAA_COMP_ATM_CMV_P4B;
DROP TABLE TMP_SQDAA_COMP_ATM_CMV_P5;


--DROP TABLE TMP_SQDAA_COMP_ATM_CMV_P1;
--CREATE TABLE TMP_SQDAA_COMP_ATM_CMV_P1 COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS
--SELECT  /*+parallel (32)*/ ------------------------------------------------------------------------ETAPA 1 BASE ATIVA
/*	DISTINCT
		HIS.DAT_MOVIMENTO
		,CAST(HIS.NUM_NTC AS VARCHAR2(16)) AS NUM_NTC
		,HIS.DW_NUM_NTC
from dwh.bi_his_assinante his
left outer join dwh.bi_dim_tipo_movimento tm                                                on  (tm.cod_tipo_mov_dw = his.cod_tipo_mov_dw)
where   
 1= 1
 and his.dat_expiracao_dw is null
 and his.dat_movimento in (to_date(&&DT_FIMMES_YYYYMMDD,'YYYYMMDD'))
 and tm.ind_base_ativa = 'S'
 --AND HIS.COD_STS_ATU IN ('A','S') 
  AND HIS.COD_SUB_STS_ATU IN ('A')
 GROUP BY 	HIS.DAT_MOVIMENTO
			,HIS.NUM_NTC
			,HIS.DW_NUM_NTC
;*/

DROP TABLE TMP_SQDAA_COMP_ATM_CMV_P1;
CREATE TABLE TMP_SQDAA_COMP_ATM_CMV_P1 COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS
SELECT
  /*+parallel (32)*/
  ------------------------------------------------------------------------ETAPA 1 BASE ATIVA
   LAST_DAY(TO_DATE(TO_DATE('&&DT_FIMMES_YYYYMMDD','YYYYMMDD'),'DD/MM/YYYY')) AS DAT_MOVIMENTO
  ,FAM.NUM_NTC
  ,FAM.DW_NUM_NTC
FROM DMMKT.DM_FP_BASE_ASSINANTE FAM,
  DWH.BI_DIM_TIPO_MOVIMENTO TM
WHERE 0                          = 0
AND FAM.COD_TIPO_MOV_DW_AJUSTADO > 0
AND TM.COD_TIPO_MOV_DW           = FAM.COD_TIPO_MOV_DW_AJUSTADO
AND TM.IND_BASE_ATIVA            = 'S'
AND FAM.DAT_EXPIRACAO_DW        IS NULL
AND FAM.DAT_MOVIMENTO           LIKE
  --LAST_DAY(TRUNC(TO_DATE('01/06/2020', 'DD/MM/YYYY'), 'MM'))
  LAST_DAY(TO_DATE(TO_DATE('&&DT_FIMMES_YYYYMMDD','YYYYMMDD'),'DD/MM/YYYY'))
AND FAM.COD_PLATAFORMA_ATU
  ||FAM.cod_sub_sts_atu    <> 'PREPGZB1'
AND FAM.COD_PLATAFORMA_ATU <> 'SGIOT'
GROUP BY
  LAST_DAY(TO_DATE(TO_DATE('&&DT_FIMMES_YYYYMMDD','YYYYMMDD'),'DD/MM/YYYY'))
  ,FAM.NUM_NTC
  ,FAM.DW_NUM_NTC;
  
GRANT SELECT ON TMP_SQDAA_COMP_ATM_CMV_P1 TO PUBLIC;
/*VALIDAÇÃO
SELECT 
  DAT_MOVIMENTO
  ,COUNT(NUM_NTC) QT1 
  ,COUNT(DISTINCT(NUM_NTC)) QT2 
FROM TMP_SQDAA_COMP_ATM_CMV_P1
GROUP BY dat_movimento;
*/


DROP TABLE TMP_SQDAA_COMP_ATM_CMV_P2;
CREATE TABLE TMP_SQDAA_COMP_ATM_CMV_P2 COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS
SELECT  /*+parallel (32)*/ ------------------------------------------------------------------------ETAPA 2 LIGAÇÕES
	DISTINCT
             FT.CD_NUMERO_TELEFONE                           AS NUM_NTC
FROM INTMKT.FT_ATENDIMENTO_URA FT -- OK
WHERE FT.SK_DATA BETWEEN &DATA_INICIAL AND &DATA_FINAL
GROUP BY 
			FT.CD_NUMERO_TELEFONE 
;

DROP TABLE TMP_SQDAA_COMP_ATM_CMV_P3A;
CREATE TABLE TMP_SQDAA_COMP_ATM_CMV_P3A COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS
SELECT /*+parallel (32)*/ ------------------------------------------------------------------------ETAPA 3A ACESSO NOVO APP
        
          A.NUM_NTC
               
 FROM DWH.BI_FP_ASSINANTE_ATEND_FECHADO A
     WHERE
        A.DW_METODO_CONTATO IN (791)
        AND A.DSC_OBSERVACAO_ATENDIMENTO LIKE '%Autenticação de usuário%' --%AUTENTICA
        AND TO_DATE(A.DAT_INICIO_ATENDIMENTO,'DD/MM/RRRR') BETWEEN LAST_DAY(ADD_MONTHS(TO_DATE('&&DT_FIMMES_YYYYMMDD','RRRRMMDD'),-1))+1
                                                        AND  
                                                        LAST_DAY(TO_DATE('&&DT_FIMMES_YYYYMMDD','RRRRMMDD'))
GROUP BY A.NUM_NTC
         ;

DROP TABLE TMP_SQDAA_COMP_ATM_CMV_P3B;
CREATE TABLE TMP_SQDAA_COMP_ATM_CMV_P3B COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS
SELECT /*+parallel (32)*/ ------------------------------------------------------------------------ETAPA 3B ACESSO SITE APP ANTIGO
       DISTINCT 
          A.NUM_NTC
                
 FROM DWH.BI_FP_ASSINANTE_ATEND_FECHADO A
     WHERE
        A.DW_METODO_CONTATO IN (701)
        AND A.DSC_OBSERVACAO_ATENDIMENTO LIKE '%Autenticação de usuário%' --%AUTENTICA
        AND TO_DATE(A.DAT_INICIO_ATENDIMENTO,'DD/MM/RRRR') BETWEEN LAST_DAY(ADD_MONTHS(TO_DATE('&&DT_FIMMES_YYYYMMDD','RRRRMMDD'),-1))+1
                                                        AND  
                                                        LAST_DAY(TO_DATE('&&DT_FIMMES_YYYYMMDD','RRRRMMDD'))
GROUP BY A.NUM_NTC
         ;

DROP TABLE TMP_SQDAA_COMP_ATM_CMV_P3C;
CREATE TABLE TMP_SQDAA_COMP_ATM_CMV_P3C COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS
SELECT /*+parallel (32)*/ ------------------------------------------------------------------------ETAPA 3C ACESSO MINHA CLARO PJ
       DISTINCT 
          A.NUM_NTC              
 FROM DWH.BI_FP_ASSINANTE_ATEND_FECHADO A
     WHERE
        A.DW_METODO_CONTATO IN (411)
        AND A.DSC_OBSERVACAO_ATENDIMENTO LIKE '%Autenticação de usuário%' --%AUTENTICA
        AND TO_DATE(A.DAT_INICIO_ATENDIMENTO,'DD/MM/RRRR') BETWEEN LAST_DAY(ADD_MONTHS(TO_DATE('&&DT_FIMMES_YYYYMMDD','RRRRMMDD'),-1))+1
                                                        AND  
                                                        LAST_DAY(TO_DATE('&&DT_FIMMES_YYYYMMDD','RRRRMMDD'))
GROUP BY A.NUM_NTC
;

DROP TABLE TMP_SQDAA_COMP_ATM_CMV_P3D;
CREATE TABLE TMP_SQDAA_COMP_ATM_CMV_P3D COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS
SELECT /*+parallel (32)*/ ------------------------------------------------------------------------ETAPA 3D ACESSO USSD
        UNIQUE   
         A.NUM_NTC --ok 
FROM DWH.BI_FP_ATENDIMENTO_MINT A     
WHERE A.COD_SHORT_CODE = 1052 --- SHORT CODE USSD
        AND TO_DATE(A.DAT_INICIO_ATENDIMENTO,'DD/MM/RRRR') BETWEEN LAST_DAY(ADD_MONTHS(TO_DATE('&&DT_FIMMES_YYYYMMDD','RRRRMMDD'),-1))+1
                                                                AND  
                                                                LAST_DAY(TO_DATE('&&DT_FIMMES_YYYYMMDD','RRRRMMDD'))                                                      
        --AND A.COD_PLATAFORMA NOT IN ('-2','-3') -->SOMENTE CLIENTES CLARO
        AND A.DW_NUM_CANAL_MINT = 2 --> CANAL USSD VER NA TABELA VER TABELA "DWH.BI_DIM_CANAL_MINT"
        --AND A.NUM_PROTOCOLO IS NOT NULL --> SOMENTE ACESSOS QUE POSSUEM PROTOCOLOS
;

DROP TABLE TMP_SQDAA_COMP_ATM_CMV_P3E;
CREATE TABLE TMP_SQDAA_COMP_ATM_CMV_P3E COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS
SELECT /*+parallel (32)*/ ------------------------------------------------------------------------ETAPA 3E ACESSO WHATSAPP
        UNIQUE 
           A.NUM_NTC --ok    
FROM DWH.BI_FP_ATENDIMENTO_MINT A
WHERE A.COD_SHORT_CODE = 9652 --- SHORT CODE WPP
	AND TO_DATE(A.DAT_INICIO_ATENDIMENTO,'DD/MM/RRRR') BETWEEN LAST_DAY(ADD_MONTHS(TO_DATE('&&DT_FIMMES_YYYYMMDD','RRRRMMDD'),-1))+1
                                                        AND  
                                                        LAST_DAY(TO_DATE('&&DT_FIMMES_YYYYMMDD','RRRRMMDD'))
;

DROP TABLE TMP_SQDAA_COMP_ATM_CMV_P4A;
CREATE TABLE TMP_SQDAA_COMP_ATM_CMV_P4A COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS -------ETAPA 4A ACESSOS QUE NÃO ESTAVAM NA BASE ATIVA
SELECT /*+parallel (32)*/ LIG.NUM_NTC FROM TMP_SQDAA_COMP_ATM_CMV_P1 HIS RIGHT JOIN TMP_SQDAA_COMP_ATM_CMV_P2 LIG ON (HIS.NUM_NTC = LIG.NUM_NTC) WHERE HIS.NUM_NTC IS NULL
UNION ALL
SELECT /*+parallel (32)*/ APP.NUM_NTC FROM TMP_SQDAA_COMP_ATM_CMV_P1 HIS RIGHT JOIN TMP_SQDAA_COMP_ATM_CMV_P3A APP ON (HIS.NUM_NTC = APP.NUM_NTC) WHERE HIS.NUM_NTC IS NULL
UNION ALL
SELECT /*+parallel (32)*/ SIT.NUM_NTC FROM TMP_SQDAA_COMP_ATM_CMV_P1 HIS RIGHT JOIN TMP_SQDAA_COMP_ATM_CMV_P3B SIT ON (HIS.NUM_NTC = SIT.NUM_NTC) WHERE HIS.NUM_NTC IS NULL
UNION ALL
SELECT /*+parallel (32)*/ MPJ.NUM_NTC FROM TMP_SQDAA_COMP_ATM_CMV_P1 HIS RIGHT JOIN TMP_SQDAA_COMP_ATM_CMV_P3C MPJ ON (HIS.NUM_NTC = MPJ.NUM_NTC) WHERE HIS.NUM_NTC IS NULL
UNION ALL
SELECT /*+parallel (32)*/ USS.NUM_NTC FROM TMP_SQDAA_COMP_ATM_CMV_P1 HIS RIGHT JOIN TMP_SQDAA_COMP_ATM_CMV_P3D USS ON (HIS.NUM_NTC = USS.NUM_NTC) WHERE HIS.NUM_NTC IS NULL
UNION ALL
SELECT /*+parallel (32)*/ WPP.NUM_NTC FROM TMP_SQDAA_COMP_ATM_CMV_P1 HIS RIGHT JOIN TMP_SQDAA_COMP_ATM_CMV_P3E WPP ON (HIS.NUM_NTC = WPP.NUM_NTC) WHERE HIS.NUM_NTC IS NULL
;

/*VALIDAÇÕES */
--SELECT COUNT(NUM_NTC) QT1 ,COUNT(DISTINCT(NUM_NTC)) QT2 FROM TMP_SQDAA_COMP_ATM_CMV_P4A;
-- 31/08/2020 2270943	2.103.077;
--SELECT NUM_NTC , COUNT(*) FROM TMP_SQDAA_COMP_ATM_CMV_P4A GROUP BY NUM_NTC HAVING COUNT(*) >1;
-- CASOS DUPLICADOS EM 31/08/2020 11920006804 11930045920

INSERT /*+APPEND */ INTO TMP_SQDAA_COMP_ATM_CMV_P1 --------------------------------------------------ETAPA 4A INCLUIR OS ACESSOS E LIGAÇÕES QUE NÃO ESTAVAM NA BASE ATIVA
SELECT /*+PARALLEL (32)*/
	DISTINCT
       LAST_DAY(TO_DATE(TO_DATE('&&DT_FIMMES_YYYYMMDD','YYYYMMDD'),'DD/MM/YYYY')) AS DAT_MOVIMENTO
      ,HIS.NUM_NTC
      ,-3 AS DW_NUM_NTC
FROM TMP_SQDAA_COMP_ATM_CMV_P4A HIS
	GROUP BY HIS.NUM_NTC;
COMMIT;

/*VALIDAÇÕES */
---SELECT * FROM TMP_SQDAA_COMP_ATM_CMV_P1 WHERE DW_NUM_NTC = -3;

DROP TABLE TMP_SQDAA_COMP_ATM_CMV_P4B;
CREATE TABLE TMP_SQDAA_COMP_ATM_CMV_P4B COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS
SELECT  /*+parallel (32)*/ ------------------------------------------------------------------------ETAPA 4B CROSS BASE ATIVA & ACESSO & LIGA
    HIS.DAT_MOVIMENTO
		,HIS.NUM_NTC
		,HIS.DW_NUM_NTC
    ,CASE WHEN LIG.NUM_NTC IS NULL THEN 0 ELSE 1 END AS FL_LIGA
    ,CASE WHEN APP.NUM_NTC IS NULL THEN 0 ELSE 1 END AS FL_NOVOAPP
    ,CASE WHEN SIT.NUM_NTC IS NULL THEN 0 ELSE 1 END AS FL_SITEAPP
    ,CASE WHEN MPJ.NUM_NTC IS NULL THEN 0 ELSE 1 END AS FL_MINHAPJ
    ,CASE WHEN USS.NUM_NTC IS NULL THEN 0 ELSE 1 END AS FL_USSD
    ,CASE WHEN WPP.NUM_NTC IS NULL THEN 0 ELSE 1 END AS FL_WHATSAPP
    ,MAX(CASE WHEN HIS.NUM_NTC = APP.NUM_NTC THEN 1
              WHEN HIS.NUM_NTC = SIT.NUM_NTC THEN 1
              WHEN HIS.NUM_NTC = MPJ.NUM_NTC THEN 1
              WHEN HIS.NUM_NTC = USS.NUM_NTC THEN 1
              WHEN HIS.NUM_NTC = WPP.NUM_NTC THEN 1
                ELSE 0 END) AS FL_ACESSO
FROM TMP_SQDAA_COMP_ATM_CMV_P1        HIS
LEFT JOIN TMP_SQDAA_COMP_ATM_CMV_P2   LIG           ON (HIS.NUM_NTC = LIG.NUM_NTC)
LEFT JOIN TMP_SQDAA_COMP_ATM_CMV_P3A  APP           ON (HIS.NUM_NTC = APP.NUM_NTC)
LEFT JOIN TMP_SQDAA_COMP_ATM_CMV_P3B  SIT           ON (HIS.NUM_NTC = SIT.NUM_NTC)
LEFT JOIN TMP_SQDAA_COMP_ATM_CMV_P3C  MPJ           ON (HIS.NUM_NTC = MPJ.NUM_NTC)
LEFT JOIN TMP_SQDAA_COMP_ATM_CMV_P3D  USS           ON (HIS.NUM_NTC = USS.NUM_NTC)
LEFT JOIN TMP_SQDAA_COMP_ATM_CMV_P3E  WPP           ON (HIS.NUM_NTC = WPP.NUM_NTC)

GROUP BY 
  HIS.DAT_MOVIMENTO
		,HIS.NUM_NTC
		,HIS.DW_NUM_NTC
    ,CASE WHEN LIG.NUM_NTC IS NULL THEN 0 ELSE 1 END
    ,CASE WHEN APP.NUM_NTC IS NULL THEN 0 ELSE 1 END
    ,CASE WHEN SIT.NUM_NTC IS NULL THEN 0 ELSE 1 END
    ,CASE WHEN MPJ.NUM_NTC IS NULL THEN 0 ELSE 1 END
    ,CASE WHEN USS.NUM_NTC IS NULL THEN 0 ELSE 1 END
    ,CASE WHEN WPP.NUM_NTC IS NULL THEN 0 ELSE 1 END
    ,(CASE WHEN HIS.NUM_NTC = APP.NUM_NTC THEN 1
              WHEN HIS.NUM_NTC = SIT.NUM_NTC THEN 1
              WHEN HIS.NUM_NTC = MPJ.NUM_NTC THEN 1
              WHEN HIS.NUM_NTC = USS.NUM_NTC THEN 1
              WHEN HIS.NUM_NTC = WPP.NUM_NTC THEN 1
                ELSE 0 END)
;

/*VALIDAÇÕES */
---11920014423 USSD WHTS
---11920014423 USSD WHTS
SELECT 
  COUNT(NUM_NTC) QT_ATIVO
  ,SUM(FL_LIGA) QT_LIGA
  ,SUM(FL_NOVOAPP) QT_NOVOAPP
  ,SUM(FL_SITEAPP) QT_SITEAPP
  ,SUM(FL_MINHAPJ) QT_MINHAPJ
  ,SUM(FL_USSD) QT_USSD
  ,SUM(FL_WHATSAPP) QT_WHATSAPP
  ,SUM(FL_ACESSO) QT_ACESSO
  FROM TMP_SQDAA_COMP_ATM_CMV_P4B
WHERE dw_num_ntc <> -3 --- RETIRANDO OS INATIVOS
;
-- 31/08/2020 18991022147 -- FL_LIGA + FL_SITEAPP

DROP TABLE TMP_SQDAA_COMP_ATM_CMV_P5;
CREATE TABLE TMP_SQDAA_COMP_ATM_CMV_P5 COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS
SELECT  /*+parallel (32)*/ ------------------------------------------------------------------------ETAPA 5 CLASSIFICAÇÃO
    DAT_MOVIMENTO
		,NUM_NTC
		,DW_NUM_NTC
    ,FL_LIGA
    ,FL_NOVOAPP
    ,FL_SITEAPP
    ,FL_MINHAPJ
    ,FL_USSD
    ,FL_WHATSAPP
    ,FL_ACESSO
    ,CASE WHEN FL_LIGA = 1 AND FL_ACESSO = 1  THEN 'LIGA E ACESSA'
          WHEN FL_LIGA = 0 AND FL_ACESSO = 1  THEN 'SOMENTE ACESSA'
          WHEN FL_LIGA = 1 AND FL_ACESSO = 0  THEN 'SOMENTE LIGA'
          WHEN FL_LIGA = 0 AND FL_ACESSO = 0  THEN 'SEM INTERAÇÃO'
            ELSE 'XXXX' END AS STS_ATENDIMENTO
   --  ,COUNT(*)
FROM TMP_SQDAA_COMP_ATM_CMV_P4B HIS
GROUP BY 
    DAT_MOVIMENTO
		,NUM_NTC
		,DW_NUM_NTC
    ,FL_LIGA
    ,FL_NOVOAPP
    ,FL_SITEAPP
    ,FL_MINHAPJ
    ,FL_USSD
    ,FL_WHATSAPP
    ,FL_ACESSO
--HAVING COUNT(*) > 1
            ;
GRANT SELECT ON TMP_SQDAA_COMP_ATM_CMV_P5 TO PUBLIC;

INSERT /*+APPEND */ INTO AGG_COMP_ATEND ------------------------------------------------------------------------ETAPA 5 TABELA AGREGADA
--CREATE TABLE AGG_COMP_ATEND COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS
SELECT 
  DAT_MOVIMENTO
  ,'LINHAS ATIVAS'    AS STS_LINHA
  ,STS_ATENDIMENTO  
  ,COUNT(NUM_NTC)     AS QT_ATIVO
  ,SUM(FL_LIGA)       AS QT_LIGA
  ,SUM(FL_NOVOAPP)    AS QT_NOVOAPP
  ,SUM(FL_SITEAPP)    AS QT_SITEAPP
  ,SUM(FL_MINHAPJ)    AS QT_MINHAPJ
  ,SUM(FL_USSD)       AS QT_USSD
  ,SUM(FL_WHATSAPP)   AS QT_WHATSAPP
  ,SUM(FL_ACESSO)     AS QT_ACESSO
  ,SYSDATE            AS DAT_CRIACAO
  FROM TMP_SQDAA_COMP_ATM_CMV_P5
WHERE dw_num_ntc <> -3 --- RETIRANDO OS INATIVOS
GROUP BY DAT_MOVIMENTO
  ,STS_ATENDIMENTO
UNION ALL
SELECT 
  DAT_MOVIMENTO
  ,'LINHAS INATIVAS'  AS STS_LINHA
  ,STS_ATENDIMENTO  
  ,COUNT(NUM_NTC)     AS QT_ATIVO
  ,SUM(FL_LIGA)       AS QT_LIGA
  ,SUM(FL_NOVOAPP)    AS QT_NOVOAPP
  ,SUM(FL_SITEAPP)    AS QT_SITEAPP
  ,SUM(FL_MINHAPJ)    AS QT_MINHAPJ
  ,SUM(FL_USSD)       AS QT_USSD
  ,SUM(FL_WHATSAPP)   AS QT_WHATSAPP
  ,SUM(FL_ACESSO)     AS QT_ACESSO
  ,SYSDATE            AS DAT_CRIACAO
  FROM TMP_SQDAA_COMP_ATM_CMV_P5
WHERE dw_num_ntc = -3 --- OS INATIVOS
GROUP BY DAT_MOVIMENTO
  ,STS_ATENDIMENTO
;
COMMIT;


SELECT * FROM AGG_COMP_ATEND WHERE dat_criacao LIKE TO_DATE('28/09/2020','DD/MM/YYYY');
SELECT * FROM AGG_COMP_ATEND;



DROP TABLE SQDAA_FT_COMP_ATM_CMV;
CREATE TABLE SQDAA_FT_COMP_ATM_CMV COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS
SELECT * FROM TMP_SQDAA_COMP_ATM_CMV_P5;
GRANT SELECT ON SQDAA_FT_COMP_ATM_CMV TO PUBLIC;











/*--*********************************************************************************************************************************************************************
			CMV - BASE ATIVA ---> VALIDAÇÃO
--**********************************************************************************************************************************************************************/
SET DEFINE ON;

WITH
BASE_ATIVA AS
(
SELECT DISTINCT  
   HIS.DAT_MOVIMENTO
  ,HIS.NUM_NTC
  ,HIS.DW_NUM_NTC
from dwh.bi_his_assinante his
left outer join dwh.bi_dim_tipo_movimento tm                                                on  (tm.cod_tipo_mov_dw = his.cod_tipo_mov_dw)
---LEFT JOIN INTMKT.DS_ATENDIMENTO_PLATAFORMA DS                                               ON (HIS.cod_plataforma_atu = DS.CD_PLATAFORMA)
where   
 1= 1
 and his.dat_expiracao_dw is null
 and his.dat_movimento in (to_date(&&DT_FIMMES_YYYYMMDD,'YYYYMMDD'))
 and tm.ind_base_ativa = 'S'
 --AND HIS.COD_STS_ATU IN ('A','S') 
  AND HIS.COD_SUB_STS_ATU IN ('A')
 GROUP BY HIS.DAT_MOVIMENTO
  ,HIS.NUM_NTC
  ,HIS.DW_NUM_NTC
)
SELECT 	
  HIS.DAT_MOVIMENTO 
 ,COUNT(HIS.NUM_NTC) QT_LINHA
FROM BASE_ATIVA HIS
GROUP BY HIS.DAT_MOVIMENTO;
-- 31/08/20	46.215.118 --> BASE ATIVA DO PORTAL 47.428.361 


/*--*********************************************************************************************************************************************************************
			CMV - BASE ACESSO
				CLIENTES QUE ACESSAM: 
			OK		NOVO APP --> PS8	1.045.564 (APURADO 1.045.564)
			OK		SITE E APP ANTIGO --> PS8	3.582.877 (APURADO 3.582.877)
			OK		USSD --> MINT	4.662.575 (Retido digital 3.391.002) (APURADO 3.253.178)
			OK		WHATSAPP --> Phe usa o Blip	1.293.175	(APURADO 1.865.834)
					Fatura Fácil --> SEM ORIGEM 174.564
					CHATBOT	--> Google Analytics	4.158
					MINHA CLARO PJ	--> Google Analytics	24.896
					MINHA CLARO FLEX	--> Google Analytics	82.556


--**********************************************************************************************************************************************************************/
WITH
BASE_ACESSO_NOVOAPP AS
(
SELECT /*+ PARALLEL (32)*/
       DISTINCT 
          A.NUM_NTC
         ,LAST_DAY(A.DAT_INICIO_ATENDIMENTO) AS DAT_MOVIMENTO
         ,SYSDATE                       AS DAT_CRIACAO
 FROM DWH.BI_FP_ASSINANTE_ATEND_FECHADO A
     WHERE
        A.DW_METODO_CONTATO IN (791)
        AND A.DSC_OBSERVACAO_ATENDIMENTO LIKE '%Autenticação de usuário%' --%AUTENTICA
        AND TO_DATE(A.DAT_INICIO_ATENDIMENTO,'DD/MM/RRRR') BETWEEN LAST_DAY(ADD_MONTHS(TO_DATE('&&DT_FIMMES_YYYYMMDD','RRRRMMDD'),-1))+1
                                                        AND  
                                                        LAST_DAY(TO_DATE('&&DT_FIMMES_YYYYMMDD','RRRRMMDD'))
 )
SELECT 
  AC.DAT_MOVIMENTO 
 ,COUNT(AC.NUM_NTC) QT_LINHA
 ,COUNT(DISTINCT(AC.NUM_NTC)) QT_LINHA_DIST
FROM BASE_ACESSO_NOVOAPP AC
GROUP BY AC.DAT_MOVIMENTO;
--1.045.564   31/08/2020

WITH
BASE_ACESSO_SITEAPP AS
(
SELECT /*+ PARALLEL (32)*/
       DISTINCT 
          A.NUM_NTC
         ,LAST_DAY(A.DAT_INICIO_ATENDIMENTO) AS DAT_MOVIMENTO
         ,SYSDATE                       AS DAT_CRIACAO
 FROM DWH.BI_FP_ASSINANTE_ATEND_FECHADO A

     WHERE
        A.DW_METODO_CONTATO IN (81, 411, 451,681,701,552)
        AND UPPER(A.DSC_OBSERVACAO_ATENDIMENTO) LIKE '%MINHACLAROWEB%AUTENTICA%USU%' --%AUTENTICA
        AND TO_DATE(A.DAT_INICIO_ATENDIMENTO,'DD/MM/RRRR') BETWEEN LAST_DAY(ADD_MONTHS(TO_DATE('&&DT_FIMMES_YYYYMMDD','RRRRMMDD'),-1))+1
                                                        AND  
                                                        LAST_DAY(TO_DATE('&&DT_FIMMES_YYYYMMDD','RRRRMMDD'))
 )
SELECT 
  AC.DAT_MOVIMENTO 
 ,COUNT(AC.NUM_NTC) QT_LINHA
FROM BASE_ACESSO_SITEAPP AC
GROUP BY AC.DAT_MOVIMENTO;
---3.582.877   31/08/2020


----UNIFICANDO NOVO APP E ANTIGO
--A.DW_METODO_CONTATO IN (81, 411, 451,681,701,552,791)
        --AND A.DSC_OBSERVACAO_ATENDIMENTO LIKE '%Autenticação de usuário%' --%AUTENTICA
        --4.962.458
                      
---NOVA PROPOSTA PHELIPE
---        A.DW_METODO_CONTATO IN (81, 411, 451,681,701,552)
---        AND UPPER(A.DSC_OBSERVACAO_ATENDIMENTO) LIKE '%MINHACLARO%AUTENTICAÇÃO DE USUÁRIO%' --%AUTENTICA
---        --4.383.126

WITH 
BASE_ACESSO_WPP AS 
(
SELECT DISTINCT /*+parallel (32)*/ ------------------------------------------------------------------------ETAPA 1 ACESSO WPP
        LAST_DAY(A.DAT_INICIO_ATENDIMENTO) AS DAT_MOVIMENTO
        ,SUBSTR(TO_CHAR(A.DAT_INICIO_ATENDIMENTO, 'YYYYMMDD'),1,6) AS SAFRA --ok
        ,A.NUM_NTC --ok    
		,SYSDATE                       AS DAT_CRIACAO
FROM DWH.BI_FP_ATENDIMENTO_MINT A
LEFT JOIN DWH.BI_DIM_CANAL_MINT B ON A.DW_NUM_CANAL_MINT = B.DW_NUM_CANAL_MINT
WHERE A.COD_SHORT_CODE = 9652 --- SHORT CODE WPP
--AND A.DAT_INICIO_ATENDIMENTO BETWEEN TO_DATE(&DATA_INICIAL, 'RRRRMMDD') AND  TO_DATE(&DATA_FINAL, 'RRRRMMDD')
	AND TO_DATE(A.DAT_INICIO_ATENDIMENTO,'DD/MM/RRRR') BETWEEN LAST_DAY(ADD_MONTHS(TO_DATE('&&DT_FIMMES_YYYYMMDD','RRRRMMDD'),-1))+1
                                                        AND  
                                                        LAST_DAY(TO_DATE('&&DT_FIMMES_YYYYMMDD','RRRRMMDD'))
GROUP BY  LAST_DAY(A.DAT_INICIO_ATENDIMENTO)
        ,SUBSTR(TO_CHAR(A.DAT_INICIO_ATENDIMENTO, 'YYYYMMDD'),1,6)
        ,A.NUM_NTC
)
SELECT 
  AC.SAFRA
 ,COUNT(AC.NUM_NTC) QT_LINHA
FROM BASE_ACESSO_WPP AC
GROUP BY AC.SAFRA
;
--31/08/20	1.865.834




WITH
BASE_ACESSO_USSD AS (
SELECT /*+parallel (32)*/ UNIQUE   
          ---LAST_DAY(TRUNC(A.DAT_INICIO_ATENDIMENTO)) AS DAT_MOVIMENTO
        A.NUM_NTC --ok 
FROM DWH.BI_FP_ATENDIMENTO_MINT A     
WHERE A.COD_SHORT_CODE = 1052 --- SHORT CODE USSD
        AND TO_DATE(A.DAT_INICIO_ATENDIMENTO,'DD/MM/RRRR') BETWEEN LAST_DAY(ADD_MONTHS(TO_DATE('&&DT_FIMMES_YYYYMMDD','RRRRMMDD'),-1))+1
                                                                AND  
                                                                LAST_DAY(TO_DATE('&&DT_FIMMES_YYYYMMDD','RRRRMMDD'))                                                      
        AND A.COD_PLATAFORMA NOT IN ('-2','-3') -->SOMENTE CLIENTES CLARO
        AND A.DW_NUM_CANAL_MINT = 2 --> CANAL USSD VER NA TABELA VER TABELA "DWH.BI_DIM_CANAL_MINT"
        AND A.NUM_PROTOCOLO IS NOT NULL --> SOMENTE ACESSOS QUE POSSUEM PROTOCOLOS
) SELECT COUNT(NUM_NTC) QT_LINHA1 ,COUNT(DISTINCT(NUM_NTC)) QT_LINHA2 FROM BASE_ACESSO_USSD;



---OLD
SET DEFINE ON;
WITH
BASE_ACESSO_USSD AS (

SELECT  /*+parallel (32)*/ 
---        LAST_DAY(TRUNC(A.DAT_INICIO_ATENDIMENTO)) AS DAT_MOVIMENTO
        TO_DATE('&&DT_FIMMES_YYYYMMDD','DD/MM/RRRR') AS DAT_MOVIMENTO
        ,SUBSTR(TO_CHAR(A.DAT_INICIO_ATENDIMENTO, 'YYYYMMDD'),1,6) AS SAFRA --ok
        ,A.NUM_NTC --ok
        ,SYSDATE                       AS DAT_CRIACAO
        ,count(*) QT
 
FROM DWH.BI_FP_ATENDIMENTO_MINT A     
WHERE A.COD_SHORT_CODE = 1052 --- SHORT CODE USSD
        AND TO_DATE(A.DAT_INICIO_ATENDIMENTO,'DD/MM/RRRR') BETWEEN LAST_DAY(ADD_MONTHS(TO_DATE('&&DT_FIMMES_YYYYMMDD','RRRRMMDD'),-1))+1
                                                                AND  
                                                                LAST_DAY(TO_DATE('&&DT_FIMMES_YYYYMMDD','RRRRMMDD'))                                                      
        AND A.COD_PLATAFORMA NOT IN ('-2','-3') -->SOMENTE CLIENTES CLARO
        AND A.DW_NUM_CANAL_MINT = 2 --> CANAL USSD VER NA TABELA VER TABELA "DWH.BI_DIM_CANAL_MINT"
        AND A.NUM_PROTOCOLO IS NOT NULL --> SOMENTE ACESSOS QUE POSSUEM PROTOCOLOS

GROUP BY  LAST_DAY(TRUNC(A.DAT_INICIO_ATENDIMENTO))
        ,SUBSTR(TO_CHAR(A.DAT_INICIO_ATENDIMENTO, 'YYYYMMDD'),1,6)
        ,A.NUM_NTC --ok   
)

SELECT 
  AC.SAFRA
  ,SUM(AC.QT)
  ,COUNT(DISTINCT(AC.NUM_NTC))
FROM BASE_ACESSO_USSD AC
GROUP BY AC.SAFRA;


/*--*********************************************************************************************************************************************************************
			CMV - CONTACT RATE (VALIDAÇÃO)
			
--**********************************************************************************************************************************************************************/

WITH 
BASE_CR AS (
SELECT DISTINCT /*+parallel (32)*/ ------------------------------------------------------------------------ETAPA 2 LIGAÇÕES
       LAST_DAY(TO_DATE(FT.DT_INICIO_LIGACAO,'YYYY/MM/DD')) AS DAT_MOVIMENTO       
       ,FT.CD_NUMERO_TELEFONE                           AS NUM_NTC
FROM INTMKT.FT_ATENDIMENTO_URA FT -- OK
WHERE FT.SK_DATA BETWEEN &DATA_INICIAL AND &DATA_FINAL
GROUP BY 
		LAST_DAY(TO_DATE(FT.DT_INICIO_LIGACAO,'YYYY/MM/DD'))
       ,FT.CD_NUMERO_TELEFONE 
) SELECT 
  COUNT(NUM_NTC) AS QT_LINHAS1
  ,COUNT(DISTINCT(NUM_NTC)) QT_LINHAS2
FROM BASE_CR
;


WITH 
BASE_CR AS (
SELECT /*+ PARALLEL (32)*/ 
       LAST_DAY(TO_DATE(FT.DT_INICIO_LIGACAO,'YYYY/MM/DD')) AS DAT_REFERENCIA       
       ,FT.DT_INICIO_LIGACAO SK_DATA       
       ,'CMV' AS NM_MARCA
       ,FT.SK_ATENDIMENTO_URA AS COD_CHAMADA -- CALL_ID
       ,TO_CHAR(FT.CD_NUMERO_TELEFONE ||' '||FT.DT_INICIO_LIGACAO ||' '||FT.NR_HORA_INICIO_LIGACAO) AS SK_LIGA
       ,TO_DATE(TO_DATE(FT.DT_INICIO_LIGACAO,'YYYY/MM/DD') ||' ' ||FT.NR_HORA_INICIO_LIGACAO,'DD/MM/RRRR HH24:MI:SS') AS DT_INI_LIGACAO
       ,TO_DATE(TO_DATE(FT.DT_FIM_LIGACAO,'YYYY/MM/DD') ||' ' ||FT.NR_HORA_FIM_LIGACAO,'DD/MM/RRRR HH24:MI:SS') AS DT_FIM_LIGACAO
       ,FT.CD_NUMERO_TELEFONE                           AS NUM_NTC
       ,CASE
         WHEN FT.FN_LIGACAO_IDENTIFICADA = 1 THEN
          'SIM'
         ELSE
          'NAO'
       END                                AS FLG_LIG_IDENTIFICADA
       ,STS.COD_SUB_STS_ATU DSC_STS_CLIENTE
       ,CASE
         WHEN FT.FN_CORPORATIVO = 1 THEN
          'SIM'
         ELSE
          'NAO'
       END                               AS FLG_CORPORATIVO
       ,CASE
         WHEN VA.NM_VISAO_ANALISE_BI = 'CONTACT RATE DIRECIONADO AO HUMANO' THEN
          'DIRECIONADO HUMANO'
         ELSE
          'RETIDO URA'
       END                           AS DSC_DIRECIONADO_RETIDO
       ,ATM.NM_TIPO_MOTIVO_BI         AS DSC_MOTIVO_URA
       ,STM.NM_SUB_TIPO_MOTIVO_BI     AS DSC_SUB_MOTIVO_URA
       ,ATL.NM_TIPO_LIGACAO_BI        AS DSC_EXPURGO_URA
       ,PP.DSC_PLANO_PRECO_BI         AS DSC_PLANO_CLIENTE
       ,SYSDATE                       AS DAT_CRIACAO

  FROM INTMKT.FT_ATENDIMENTO_URA FT -- OK
  LEFT JOIN DWH.BI_DIM_STATUS STS -- OK
    ON STS.STS_DW = FT.SK_STATUS
  LEFT JOIN INTMKT.DS_ATENDIMENTO_TIPO_MOTIVO ATM -- OK
    ON ATM.SK_ATENDIMENTO_TIPO_MOTIVO = FT.SK_ATENDIMENTO_TIPO_MOTIVO

  LEFT JOIN INTMKT.DS_ATENDIMENTO_TIPO_LIGACAO ATL -- OK
    ON ATL.SK_ATENDIMENTO_TIPO_LIGACAO = FT.SK_ATENDIMENTO_TIPO_LIGACAO
  LEFT JOIN DWH.BI_DIM_PLANO_PRECO PP -- OK
    ON PP.DW_PLANO = FT.SK_PLANO
  LEFT JOIN INTMKT.DS_ATENDIMENTO_SUB_TIPO_MOTIVO STM -- OK
   ON STM.SK_ATENDIMENTO_SUB_TIPO_MOTIVO =
       FT.SK_ATENDIMENTO_SUB_TIPO_MOTIVO
  LEFT JOIN INTMKT.DS_VISAO_ANALISE VA -- OK
    ON VA.SK_VISAO_ANALISE = FT.SK_VISAO_ANALISE
  LEFT JOIN intmkt.ds_atendimento_tipo_ligacao ATL 
ON        atl.sk_atendimento_tipo_ligacao = ft.sk_atendimento_tipo_ligacao 

WHERE FT.SK_DATA BETWEEN &DATA_INICIAL AND &DATA_FINAL
--AND       atl.nm_tipo_ligacao_bi = 'Ligações Válidas' -- FILTRO OK (SÃO CLIENTES QUE NÃO TIVERAM ABANDONO)
--AND va.nm_visao_analise_bi = 'Contact Rate Direcionado ao Humano' -- FILTRO OK

)
SELECT 
	
  COUNT(DISTINCT(NUM_NTC)) QT_LINHAS
FROM BASE_CR
;
--COUNT(COD_CHAMADA) QT_CHAMADAS
-- 31/08/2020 12.167.866 CR TOTAL 5.334.317 LINHAS
-- 31/08/2020 5.066.268 CR ATH 3.067.342 LINHAS
---AND       atl.nm_tipo_ligacao_bi = 'Ligações Válidas' -- FILTRO OK (SÃO CLIENTES QUE NÃO TIVERAM ABANDONO)
---AND va.nm_visao_analise_bi = 'Contact Rate Direcionado ao Humano' -- FILTRO OK



















SELECT
      'CMV' AS MARCA
      ,SUBSTR (SK_DATA,1,6) AS MES_REF
      ,B.NM_AGRUPAMENTO_PLATAFORMA_BI AS PRODUTO
   
            ,SUM(QT_LINHA_ATIVA) AS TT_DOM
FROM INTMKT.FS_CONT_RAT_BAS_LINH_ATIVA_CMV@BASECLARO A
LEFT JOIN INTMKT.DS_ATENDIMENTO_PLATAFORMA@BASECLARO B ON A.SK_ATENDIMENTO_PLATAFORMA = B.SK_ATENDIMENTO_PLATAFORMA
WHERE SK_DATA >= 20200101
---AND SK_FC_ASSINANTE_ZB = '1971852147'
GROUP BY
      SUBSTR (SK_DATA,1,6)
      ,B.NM_AGRUPAMENTO_PLATAFORMA_BI

order by 1;




SELECT  
            
            DS.NM_AGRUPAMENTO_PLATAFORMA_BI AS PRODUTO
            ,COUNT(DISTINCT(BA.DW_NUM_NTC)) QT_DW_NTC
            ,COUNT(DISTINCT(BA.NUM_NTC)) QT_NTC
FROM OPS_ALTERYX.BI_FP_BASE_ASSINANTE_CMV BA
LEFT JOIN INTMKT.DS_ATENDIMENTO_PLATAFORMA DS ON BA.COD_PLATAFORMA = DS.CD_PLATAFORMA
WHERE TO_CHAR(DAT_MOVIMENTO,'YYYYMMDD') = '20200801'
    AND BA.COD_STATUS IN ('A','S')
    AND BA.cod_sub_status IN ('A')
GROUP BY DS.NM_AGRUPAMENTO_PLATAFORMA_BI;



SELECT 
  DS.NM_AGRUPAMENTO_PLATAFORMA_BI AS PRODUTO
  ,COUNT(DISTINCT(his.num_ntc)) AS QT_LINHA
from dwh.bi_his_assinante his
left outer join dwh.bi_dim_tipo_movimento tm                                                on  (tm.cod_tipo_mov_dw = his.cod_tipo_mov_dw)
LEFT JOIN INTMKT.DS_ATENDIMENTO_PLATAFORMA DS                                               ON (HIS.cod_plataforma_atu = DS.CD_PLATAFORMA)
where
 1= 1
 and his.dat_expiracao_dw is null
-- and HIS.cod_plataforma_atu in ('POSCW', 'AUTOC', 'POSBL', 'POSPG', 'PREPG', 'CTLFC')
 and his.dat_movimento in (to_date(20200831,'YYYYMMDD'))
 and tm.ind_base_ativa = 'S'

GROUP BY DS.NM_AGRUPAMENTO_PLATAFORMA_BI
;

/*
	DESCRIÇÃO DO COD_STS_ATU
	TABELA ORIGEM DM_HIS_ASSINANTE
	
Codigo do status atual. E o codigo/ sigla do status atual que representa o status da BEN (Billing Entity Number) no sistema Mobile (Sistema do Pos-Pago) de um NTC (Numero de Telefone Celular) de um cliente/ assinante da Claro Movel. Ex: A = Ativo; B = Portabilidade OUT; C = Cancelado; D = Suspensao Cobranca (Assinante estava Suspenso); E = Suspensao Final Cobranca (Assinante estava susp); F = Suspensao Final; G = Portabilidade OUT Suspensao; H = Portabilidade OUT FROM_OD_SUSPENED; I = Pre-ativo Portabilidade; J = Portabilidade FROM_OD_SUS_FROM_SUSPEND; K = Portabilidade OUT FROM_OD_FINAL_SUSPENDED; L = Suspensao Final Cobranca (Ass. estava susp final); M = Portabilidade Supenso Final que foi portado; N = Status Nao Classificado Mobile; O = Portabilidade OUT FROM_OD_FINAL_SUS_FROM_SUSPEND; P = Pre-ativo; Q = Portab. OUT FROM_OD_FINAL_SUS_FROM_FINAL_SUSPEND; R = Reservado; S = Suspenso; U = Suspensao Cobranca (Assinante estava Ativo); V = Suspensao Final Cobranca (Assinante estava ativo).
*/

SELECT * FROM DWH.DM_HIS_ASSINANTE@BASECLARO;

SELECT * FROM DWH.BI_DIM_ASSINANTE@BASECLARO WHERE NUM_NTC = '11980299093'; --DW_NUM_NTC 441235318

FROM DWH.BI_DIM_ASSINANTE@BASECLARO
WHERE NUM_NTC = '11980299093'; 
--NUM_ASS_UNIF_DW = 400939059
--NUM_NTC = '11980299093'

SELECT  
  count(DISTINCT(NUM_NTC)) QT_NTC
  ,COUNT(DISTINCT(DW_NUM_NTC)) QT_DW_NTC
    
FROM DWH.BI_DIM_ASSINANTE@BASECLARO
WHERE NUM_NTC = '11980299093';
---135.552.591	444.931.367 14.09.2020 15:56
WHERE DAT_EXPIRACAO_DW IS NOT NULL;
--98.240.552	323.055.748




SELECT * FROM INTMKT.DS_ATENDIMENTO_PLATAFORMA@BASECLARO;

DESC INTMKT.DS_ATENDIMENTO_PLATAFORMA@BASECLARO;

SELECT * FROM DWH.BI_DIM_ASSINANTE@BASECLARO
WHERE NUM_NTC = '11980299093'; --NUM_ASS_UNIF_DW 400939059

WHERE NUM_ASS_UNIF_DW = 400939059; --NUM_ASS_UNIF_DW = 400939059 NUM_NTC = '11980299093'
WHERE NUM_NTC = '11980299093'; --NUM_ASS_UNIF_DW 400939059

DW_NUM_NTC = 441154279
NUM_NTC = 11979979214
NUM_CLIENTE = 114173056 --- ESTE DEFINI CLIENTE UNICO COM MAIS DE UMA LINHA
DW_NUM_CLIENTE = 1019582546


--- TO_CHAR(DAT_EXPIRACAO_DW,'YYYYMMDD') > '20200801'
;

WHERE TO_CHAR(AC.DT_REFERENCIA,'YYYYMM') LIKE &ANOMES_ATU


select

 his.dat_movimento,

 his.dw_num_ntc,

 his.num_ntc,

 his.dat_ativacao,

 his.dat_ult_nova_ativacao,

 his.cod_plataforma_atu,

 td.ind_titularidade,

 tp.dsc_tipo_cliente,

 his.cod_tipo_cliente,

 his.cod_sub_tipo_cliente,

 cli.dsc_nome_cliente,

 cli.num_cpf,

 cli.dat_nasc,

 cli.DSC_ENDERECO,

 cli.DSC_COMPLEMENTO,

 cli.dsc_bairro,

 cli.dsc_cidade,

 cli.SGL_ESTADO,

 cli.NUM_CEP,

 bb.cod_ibge,

 vd.dw_ponto_venda_comta,

 cta.cod_ponto_venda_comta,

 cta.cod_amdocs,

 cta.COD_TIPO_AGENTE_VENDA,

 cta.dsc_nome_ponto_venda_comta,

 cta.dsc_grupo_ponto_venda_comta,


 vd.dw_canal_venda,

 cn.cod_canal_venda,

 cn.dsc_canal_venda,

 cn.dsc_subtipo_bi,


 his.cod_plano_tarif_atu,

 pl.dsc_plano_preco


 from dwh.bi_his_assinante@baseclaro his

 left outer join  dwh.bi_dim_cliente@baseclaro cli                                                     on (his.dw_num_cliente_atu = cli.dw_num_cliente)

 left outer join (select * from dwh.bi_dim_core_ibge@baseclaro where ind_registro_ativo = 'S') bb      on (cli.dsc_cidade = bb.dsc_municipio and cli.sgl_estado = bb.dsc_sgl_estado)

 left outer join dwh.bi_dim_tipo_movimento@baseclaro tm                                                on  (tm.cod_tipo_mov_dw = his.cod_tipo_mov_dw)

 left outer join dwh.bi_dim_tipo_cliente@baseclaro tp                                                  on his.DW_TIPO_CLIENTE = tp.DW_TIPO_CLIENTE

 left outer join DWH.BI_FP_VENDA_ATIVACAO@baseclaro vd                                                 on his.dw_num_ntc = vd.DW_NUM_NTC and vd.dat_expiracao_dw is null

 left outer join dwh.bi_dim_ponto_venda_comta@baseclaro cta                                            on vd.dw_ponto_venda_comta = cta.dw_ponto_venda_comta

 left outer join dwh.bi_dim_canal_venda@baseclaro  cn                                                  on (cn.DW_CANAL_VENDA = vd.dw_canal_venda )

 left outer join dwh.bi_dim_plano_preco@baseclaro  pl                                                  on (pl.dw_plano = his.dw_plano_tarif_atu)

 left outer join dwh.bi_fp_cliente_titularidade@baseclaro  td                                          on his.dw_num_ntc = td.dw_num_ntc

            and td.dat_movimento = trunc (add_months(trunc(his.dat_movimento),0),'MM')

 where

 1= 1

 and his.dat_expiracao_dw is null

 and cod_plataforma_atu in ('POSCW', 'AUTOC', 'POSBL', 'POSPG', 'PREPG', 'CTLFC')

 and his.dat_movimento in (to_date(20200131,'YYYYMMDD'))

 and tm.ind_base_ativa = 'S'
