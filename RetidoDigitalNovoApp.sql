
/*--===============================================================================================================================
            CRIAÇÃO DAS TABELAS - TOMÉ E WAGNER 
	OJETIVO: CRIAÇÃO DO RETIDO DIGITAL MINHA CLARO MÓVEL

	TABELAS: U92047747.BI_FT_SQDAA_RET_DIG_N_APP
			 U92047747.BI_AGG_SQDAA_RET_DIG_N_APP	
			 
--===============================================================================================================================*/

DROP TABLE TMP_SQDAA_RET_DIG_N_APP_01A;
DROP TABLE TMP_SQDAA_RET_DIG_N_APP_01B;
DROP TABLE TMP_SQDAA_RET_DIG_N_APP_03A;
DROP TABLE TMP_SQDAA_RET_DIG_N_APP_03B;
DROP TABLE TMP_SQDAA_RET_DIG_N_APP_03C; 
DROP TABLE TMP_SQDAA_RET_DIG_N_APP_03D;
DROP TABLE TMP_SQDAA_RET_DIG_N_APP_03E;
DROP TABLE TMP_SQDAA_RET_DIG_N_APP_03F;
DROP TABLE BI_FT_SQDAA_RET_DIG_N_APP;

-- PASSO ORIGEM  DWH.BI_FP_ASSINANTE_ATEND_FECHADO

-- ETAPA 1 ACESSO NOVO APP
DROP TABLE TMP_SQDAA_RET_DIG_N_APP_01A;
CREATE TABLE TMP_SQDAA_RET_DIG_N_APP_01A COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS

SELECT  /*+ PARALLEL (32)*/
       LAST_DAY(DAT_INICIO_ATENDIMENTO) AS DAT_REFERENCIA 
       ,TO_NUMBER(TO_CHAR(MC.DAT_INICIO_ATENDIMENTO, 'RRRRMMDD')) SK_DATA
       ,TO_DATE(TO_CHAR(MC.DAT_INICIO_ATENDIMENTO, 'RRRRMMDD')||' '||MC.HOR_INICIO_ATENDIMENTO,'RRRRMMDD HH24MISS') AS DAT_INI_ATENDIMENTO
       ,TO_DATE(TO_CHAR(MC.DAT_FIM_ATENDIMENTO, 'RRRRMMDD')||' '||MC.HOR_FIM_ATENDIMENTO,'RRRRMMDD HH24MISS') AS DAT_FIM_ATENDIMENTO
       ,MC.NUM_NTC
       ,MC.DW_METODO_CONTATO
       ,DM.DSC_METODO_CONTATO
       ,M.COD_MOTIVO_ATEND_NIVEL_1
       ,M.DSC_MOTIVO_ATEND_NIVEL_1
       ,M.COD_MOTIVO_ATEND_NIVEL_2
       ,M.DSC_MOTIVO_ATEND_NIVEL_2
       ,M.COD_MOTIVO_ATEND_NIVEL_3
       ,M.DSC_MOTIVO_ATEND_NIVEL_3
       ,M.COD_MOTIVO_ATEND_NIVEL_4
       ,M.DSC_MOTIVO_ATEND_NIVEL_4
       ,M.COD_MOTIVO_ATEND_NIVEL_5
       ,M.DSC_MOTIVO_ATEND_NIVEL_5
       ,SYSDATE AS DAT_CRIACAO
       

FROM DWH.BI_FP_ASSINANTE_ATEND_FECHADO MC 
LEFT JOIN BI_DIM_MOTIVO_ATENDIMENTO M
     ON MC.DW_MOTIVO_ATENDIMENTO = M.DW_MOTIVO_ATENDIMENTO
LEFT JOIN BI_DIM_METODO_CONTATO DM
     ON MC.DW_METODO_CONTATO = DM.DW_METODO_CONTATO
WHERE MC.DW_METODO_CONTATO IN (791)     -------NOVO APP MINHA CLARO MÓVEL 
  
      AND MC.DSC_OBSERVACAO_ATENDIMENTO LIKE '%Autenticação de usuário%' --%AUTENTICA
      AND MC.DAT_INICIO_ATENDIMENTO BETWEEN TO_DATE(&&DATA_INICIAL, 'RRRRMMDD') AND  TO_DATE(&&DATA_FINAL, 'RRRRMMDD')
;
-- PASSO ORIGEM  INTMKT.FT_ATENDIMENTO_URA
--ETAPA 2 CR NOVO APP
DROP TABLE TMP_SQDAA_RET_DIG_N_APP_01B;
CREATE TABLE TMP_SQDAA_RET_DIG_N_APP_01B COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS

       SELECT /*+ PARALLEL (32)*/
       LAST_DAY(TO_DATE(FT.DT_INICIO_LIGACAO,'YYYY/MM/DD')) AS DAT_REFERENCIA,       
       FT.DT_INICIO_LIGACAO SK_DATA,       
       'CMV' AS NM_MARCA,
       FT.SK_ATENDIMENTO_URA AS COD_CHAMADA, -- CALL_ID
       TO_DATE(TO_DATE(FT.DT_INICIO_LIGACAO,'YYYY/MM/DD') ||' ' ||FT.NR_HORA_INICIO_LIGACAO,'DD/MM/RRRR HH24:MI:SS') AS DAT_INI_LIGACAO,
       TO_DATE(TO_DATE(FT.DT_FIM_LIGACAO,'YYYY/MM/DD') ||' ' ||FT.NR_HORA_FIM_LIGACAO,'DD/MM/RRRR HH24:MI:SS') AS DAT_FIM_LIGACAO,
       FT.CD_NUMERO_TELEFONE                           AS NUM_NTC,
       CASE
         WHEN FT.FN_LIGACAO_IDENTIFICADA = 1 THEN
          'SIM'
         ELSE
          'NAO'
       END                                AS FLG_LIG_IDENTIFICADA,
       STS.COD_SUB_STS_ATU DSC_STS_CLIENTE,
       CASE
         WHEN FT.FN_CORPORATIVO = 1 THEN
          'SIM'
         ELSE
          'NAO'
       END                               AS FLG_CORPORATIVO,
       CASE
         WHEN VA.NM_VISAO_ANALISE_BI = 'CONTACT RATE DIRECIONADO AO HUMANO' THEN
          'DIRECIONADO HUMANO'
         ELSE
          'RETIDO URA'
       END                           AS DSC_DIRECIONADO_RETIDO,
       ATM.NM_TIPO_MOTIVO_BI         AS DSC_MOTIVO_URA,
       STM.NM_SUB_TIPO_MOTIVO_BI     AS DSC_SUB_MOTIVO_URA,
       ATL.NM_TIPO_LIGACAO_BI        AS DSC_EXPURGO_URA,
       PP.DSC_PLANO_PRECO_BI         AS DSC_PLANO_CLIENTE,
       SYSDATE                       AS DAT_CRIACAO

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

WHERE FT.SK_DATA BETWEEN &&DATA_INICIAL AND &&DATA_FINAL
;
--- SAFRA D 0
--ETAPA 03A D0 NOVO APP
DROP TABLE TMP_SQDAA_RET_DIG_N_APP_03A;
CREATE TABLE TMP_SQDAA_RET_DIG_N_APP_03A COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS

SELECT /*+parallel(32) */ 
        DISTINCT
        AC.DAT_REFERENCIA
       ,AC.SK_DATA
       ,AC.NUM_NTC
       ,AC.DAT_INI_ATENDIMENTO
       ,AC.DAT_FIM_ATENDIMENTO
       ,CR.DAT_INI_LIGACAO
       ,CR.DAT_FIM_LIGACAO
       ,CASE WHEN TRUNC(AC.DAT_INI_ATENDIMENTO) = TRUNC(CR.DAT_INI_LIGACAO) 
             THEN 1
             ELSE 0
             END FLG_LIG_ACESSA 
      ,AC.COD_MOTIVO_ATEND_NIVEL_1      
      ,AC.DSC_MOTIVO_ATEND_NIVEL_1        
      ,AC.COD_MOTIVO_ATEND_NIVEL_2
      ,AC.DSC_MOTIVO_ATEND_NIVEL_2
      ,AC.COD_MOTIVO_ATEND_NIVEL_3
      ,AC.DSC_MOTIVO_ATEND_NIVEL_3
      ,AC.COD_MOTIVO_ATEND_NIVEL_4
      ,AC.DSC_MOTIVO_ATEND_NIVEL_4
      ,AC.COD_MOTIVO_ATEND_NIVEL_5
      ,AC.DSC_MOTIVO_ATEND_NIVEL_5
      ,CR.DSC_MOTIVO_URA
      ,CASE WHEN TRUNC(AC.DAT_INI_ATENDIMENTO) = TRUNC(CR.DAT_INI_LIGACAO) 
            THEN 'ACESSOU E LIGOU'
            ELSE 'RETIDO DIGITAL'
        END AS DSC_VISAO_ANALISE
      ,'D 0'   AS DSC_SAFRA  
      ,AC.DW_METODO_CONTATO
      ,SYSDATE AS DAT_CRIACAO
FROM TMP_SQDAA_RET_DIG_N_APP_01A AC -- app
LEFT JOIN TMP_SQDAA_RET_DIG_N_APP_01B CR ON AC.NUM_NTC=CR.NUM_NTC -- cr
      AND TRUNC(AC.DAT_INI_ATENDIMENTO) = TRUNC(CR.DAT_INI_LIGACAO) 
            
ORDER BY  AC.NUM_NTC
         ,AC.DAT_INI_ATENDIMENTO      
         ,CR.DAT_INI_LIGACAO
     ;
	 
--- SAFRA D 1
--ETAPA 03B D+1 DO NOVO APP
DROP TABLE TMP_SQDAA_RET_DIG_N_APP_03B;
CREATE TABLE TMP_SQDAA_RET_DIG_N_APP_03B COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS

SELECT /*+parallel(32) */ 
        DISTINCT
        AC.DAT_REFERENCIA
       ,AC.SK_DATA
       ,AC.NUM_NTC
       ,AC.DAT_INI_ATENDIMENTO
       ,AC.DAT_FIM_ATENDIMENTO
       ,CR.DAT_INI_LIGACAO
       ,CR.DAT_FIM_LIGACAO
       ,CASE WHEN TRUNC(CR.DAT_INI_LIGACAO) BETWEEN TRUNC(AC.DAT_INI_ATENDIMENTO) AND TRUNC(AC.DAT_INI_ATENDIMENTO)+1 
             THEN 1
             ELSE 0
             END FLG_LIG_ACESSA 
      ,AC.COD_MOTIVO_ATEND_NIVEL_1      
      ,AC.DSC_MOTIVO_ATEND_NIVEL_1        
      ,AC.COD_MOTIVO_ATEND_NIVEL_2
      ,AC.DSC_MOTIVO_ATEND_NIVEL_2
      ,AC.COD_MOTIVO_ATEND_NIVEL_3
      ,AC.DSC_MOTIVO_ATEND_NIVEL_3
      ,AC.COD_MOTIVO_ATEND_NIVEL_4
      ,AC.DSC_MOTIVO_ATEND_NIVEL_4
      ,AC.COD_MOTIVO_ATEND_NIVEL_5
      ,AC.DSC_MOTIVO_ATEND_NIVEL_5
      ,CR.DSC_MOTIVO_URA
      ,CASE WHEN TRUNC(CR.DAT_INI_LIGACAO) BETWEEN TRUNC(AC.DAT_INI_ATENDIMENTO) AND TRUNC(AC.DAT_INI_ATENDIMENTO)+1
            THEN 'ACESSOU E LIGOU'
            ELSE 'RETIDO DIGITAL'
        END AS DSC_VISAO_ANALISE
      ,'D 1'   AS DSC_SAFRA 
      ,AC.DW_METODO_CONTATO 
      ,SYSDATE AS DAT_CRIACAO
FROM TMP_SQDAA_RET_DIG_N_APP_01A AC -- app
LEFT JOIN TMP_SQDAA_RET_DIG_N_APP_01B CR ON AC.NUM_NTC=CR.NUM_NTC -- cr
      AND TRUNC(CR.DAT_INI_LIGACAO) BETWEEN TRUNC(AC.DAT_INI_ATENDIMENTO) AND TRUNC(AC.DAT_INI_ATENDIMENTO)+1
            
ORDER BY  AC.NUM_NTC
         ,AC.DAT_INI_ATENDIMENTO      
         ,CR.DAT_INI_LIGACAO     
;

--- SAFRA D 2
--ETAPA 03C D+2
DROP TABLE TMP_SQDAA_RET_DIG_N_APP_03C;     
CREATE TABLE TMP_SQDAA_RET_DIG_N_APP_03C COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS

SELECT /*+parallel(32) */ 
        DISTINCT
        AC.DAT_REFERENCIA
       ,AC.SK_DATA
       ,AC.NUM_NTC
       ,AC.DAT_INI_ATENDIMENTO
       ,AC.DAT_FIM_ATENDIMENTO
       ,CR.DAT_INI_LIGACAO
       ,CR.DAT_FIM_LIGACAO
       ,CASE WHEN TRUNC(CR.DAT_INI_LIGACAO) BETWEEN TRUNC(AC.DAT_INI_ATENDIMENTO) AND TRUNC(AC.DAT_INI_ATENDIMENTO)+2 
             THEN 1
             ELSE 0
             END FLG_LIG_ACESSA 
      ,AC.COD_MOTIVO_ATEND_NIVEL_1      
      ,AC.DSC_MOTIVO_ATEND_NIVEL_1        
      ,AC.COD_MOTIVO_ATEND_NIVEL_2
      ,AC.DSC_MOTIVO_ATEND_NIVEL_2
      ,AC.COD_MOTIVO_ATEND_NIVEL_3
      ,AC.DSC_MOTIVO_ATEND_NIVEL_3
      ,AC.COD_MOTIVO_ATEND_NIVEL_4
      ,AC.DSC_MOTIVO_ATEND_NIVEL_4
      ,AC.COD_MOTIVO_ATEND_NIVEL_5
      ,AC.DSC_MOTIVO_ATEND_NIVEL_5
      ,CR.DSC_MOTIVO_URA
      ,CASE WHEN TRUNC(CR.DAT_INI_LIGACAO) BETWEEN TRUNC(AC.DAT_INI_ATENDIMENTO) AND TRUNC(AC.DAT_INI_ATENDIMENTO)+2
            THEN 'ACESSOU E LIGOU'
            ELSE 'RETIDO DIGITAL'
        END AS DSC_VISAO_ANALISE
      ,'D 2'   AS DSC_SAFRA  
      ,AC.DW_METODO_CONTATO
      ,SYSDATE AS DAT_CRIACAO
FROM TMP_SQDAA_RET_DIG_N_APP_01A AC -- app
LEFT JOIN TMP_SQDAA_RET_DIG_N_APP_01B CR ON AC.NUM_NTC=CR.NUM_NTC -- cr
      AND TRUNC(CR.DAT_INI_LIGACAO) BETWEEN TRUNC(AC.DAT_INI_ATENDIMENTO) AND TRUNC(AC.DAT_INI_ATENDIMENTO)+2
            
ORDER BY  AC.NUM_NTC
         ,AC.DAT_INI_ATENDIMENTO      
         ,CR.DAT_INI_LIGACAO
;


--   SAFRA D 3
--ETAPA 03D D72H
DROP TABLE TMP_SQDAA_RET_DIG_N_APP_03D;
CREATE TABLE TMP_SQDAA_RET_DIG_N_APP_03D COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS

SELECT /*+parallel(32) */ 
        DISTINCT
        AC.DAT_REFERENCIA
       ,AC.SK_DATA
       ,AC.NUM_NTC
       ,AC.DAT_INI_ATENDIMENTO
       ,AC.DAT_FIM_ATENDIMENTO
       ,CR.DAT_INI_LIGACAO
       ,CR.DAT_FIM_LIGACAO
       ,CASE WHEN TRUNC(CR.DAT_INI_LIGACAO) BETWEEN TRUNC(AC.DAT_INI_ATENDIMENTO) AND TRUNC(AC.DAT_INI_ATENDIMENTO)+3 
             THEN 1
             ELSE 0
             END FLG_LIG_ACESSA 
      ,AC.COD_MOTIVO_ATEND_NIVEL_1      
      ,AC.DSC_MOTIVO_ATEND_NIVEL_1        
      ,AC.COD_MOTIVO_ATEND_NIVEL_2
      ,AC.DSC_MOTIVO_ATEND_NIVEL_2
      ,AC.COD_MOTIVO_ATEND_NIVEL_3
      ,AC.DSC_MOTIVO_ATEND_NIVEL_3
      ,AC.COD_MOTIVO_ATEND_NIVEL_4
      ,AC.DSC_MOTIVO_ATEND_NIVEL_4
      ,AC.COD_MOTIVO_ATEND_NIVEL_5
      ,AC.DSC_MOTIVO_ATEND_NIVEL_5
      ,CR.DSC_MOTIVO_URA
      ,CASE WHEN TRUNC(CR.DAT_INI_LIGACAO) BETWEEN TRUNC(AC.DAT_INI_ATENDIMENTO) AND TRUNC(AC.DAT_INI_ATENDIMENTO)+3
            THEN 'ACESSOU E LIGOU'
            ELSE 'RETIDO DIGITAL'
        END AS DSC_VISAO_ANALISE
      ,'D 3'   AS DSC_SAFRA  
     ,AC.DW_METODO_CONTATO
      ,SYSDATE AS DAT_CRIACAO
FROM TMP_SQDAA_RET_DIG_N_APP_01A AC -- app
LEFT JOIN TMP_SQDAA_RET_DIG_N_APP_01B CR ON AC.NUM_NTC=CR.NUM_NTC -- cr
      AND TRUNC(CR.DAT_INI_LIGACAO) BETWEEN TRUNC(AC.DAT_INI_ATENDIMENTO) AND TRUNC(AC.DAT_INI_ATENDIMENTO)+3
            
ORDER BY  AC.NUM_NTC
         ,AC.DAT_INI_ATENDIMENTO      
         ,CR.DAT_INI_LIGACAO
;

-- SAFRA D 7
--ETAPA 03E D7D
DROP TABLE TMP_SQDAA_RET_DIG_N_APP_03E;
CREATE TABLE TMP_SQDAA_RET_DIG_N_APP_03E COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS

SELECT /*+parallel(32) */ 
        DISTINCT
        AC.DAT_REFERENCIA
       ,AC.SK_DATA
       ,AC.NUM_NTC
       ,AC.DAT_INI_ATENDIMENTO
       ,AC.DAT_FIM_ATENDIMENTO
       ,CR.DAT_INI_LIGACAO
       ,CR.DAT_FIM_LIGACAO
       ,CASE WHEN TRUNC(CR.DAT_INI_LIGACAO) BETWEEN TRUNC(AC.DAT_INI_ATENDIMENTO) AND TRUNC(AC.DAT_INI_ATENDIMENTO)+7 
             THEN 1
             ELSE 0
             END FLG_LIG_ACESSA 
      ,AC.COD_MOTIVO_ATEND_NIVEL_1      
      ,AC.DSC_MOTIVO_ATEND_NIVEL_1        
      ,AC.COD_MOTIVO_ATEND_NIVEL_2
      ,AC.DSC_MOTIVO_ATEND_NIVEL_2
      ,AC.COD_MOTIVO_ATEND_NIVEL_3
      ,AC.DSC_MOTIVO_ATEND_NIVEL_3
      ,AC.COD_MOTIVO_ATEND_NIVEL_4
      ,AC.DSC_MOTIVO_ATEND_NIVEL_4
      ,AC.COD_MOTIVO_ATEND_NIVEL_5
      ,AC.DSC_MOTIVO_ATEND_NIVEL_5
      ,CR.DSC_MOTIVO_URA
      ,CASE WHEN TRUNC(CR.DAT_INI_LIGACAO) BETWEEN TRUNC(AC.DAT_INI_ATENDIMENTO) AND TRUNC(AC.DAT_INI_ATENDIMENTO)+7
            THEN 'ACESSOU E LIGOU'
            ELSE 'RETIDO DIGITAL'
        END AS DSC_VISAO_ANALISE
      ,'D 7'   AS DSC_SAFRA  
      ,AC.DW_METODO_CONTATO
      ,SYSDATE AS DAT_CRIACAO
FROM TMP_SQDAA_RET_DIG_N_APP_01A AC -- app
LEFT JOIN TMP_SQDAA_RET_DIG_N_APP_01B CR ON AC.NUM_NTC=CR.NUM_NTC -- cr
      AND TRUNC(CR.DAT_INI_LIGACAO) BETWEEN TRUNC(AC.DAT_INI_ATENDIMENTO) AND TRUNC(AC.DAT_INI_ATENDIMENTO)+7
            
ORDER BY  AC.NUM_NTC
         ,AC.DAT_INI_ATENDIMENTO      
         ,CR.DAT_INI_LIGACAO
;
-- SAFRA D MES
--ETAPA 03F DM0
DROP TABLE TMP_SQDAA_RET_DIG_N_APP_03F;
CREATE TABLE TMP_SQDAA_RET_DIG_N_APP_03F COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS

SELECT /*+parallel(32) */ 
        DISTINCT
        AC.DAT_REFERENCIA
       ,AC.SK_DATA
       ,AC.NUM_NTC
       ,AC.DAT_INI_ATENDIMENTO
       ,AC.DAT_FIM_ATENDIMENTO
       ,CR.DAT_INI_LIGACAO
       ,CR.DAT_FIM_LIGACAO
       ,CASE WHEN TRUNC(CR.DAT_INI_LIGACAO) BETWEEN TRUNC(AC.DAT_INI_ATENDIMENTO) AND TRUNC(LAST_DAY(AC.DAT_INI_ATENDIMENTO)) 
             THEN 1
             ELSE 0
             END FLG_LIG_ACESSA 
      ,AC.COD_MOTIVO_ATEND_NIVEL_1      
      ,AC.DSC_MOTIVO_ATEND_NIVEL_1        
      ,AC.COD_MOTIVO_ATEND_NIVEL_2
      ,AC.DSC_MOTIVO_ATEND_NIVEL_2
      ,AC.COD_MOTIVO_ATEND_NIVEL_3
      ,AC.DSC_MOTIVO_ATEND_NIVEL_3
      ,AC.COD_MOTIVO_ATEND_NIVEL_4
      ,AC.DSC_MOTIVO_ATEND_NIVEL_4
      ,AC.COD_MOTIVO_ATEND_NIVEL_5
      ,AC.DSC_MOTIVO_ATEND_NIVEL_5
      ,CR.DSC_MOTIVO_URA
      ,CASE WHEN TRUNC(CR.DAT_INI_LIGACAO) BETWEEN TRUNC(AC.DAT_INI_ATENDIMENTO) AND TRUNC(LAST_DAY(AC.DAT_INI_ATENDIMENTO))
            THEN 'ACESSOU E LIGOU'
            ELSE 'RETIDO DIGITAL'
        END AS DSC_VISAO_ANALISE
      ,'D MES'   AS DSC_SAFRA  
     ,AC.DW_METODO_CONTATO
     ,SYSDATE AS DAT_CRIACAO
FROM TMP_SQDAA_RET_DIG_N_APP_01A AC -- app
LEFT JOIN TMP_SQDAA_RET_DIG_N_APP_01B CR ON AC.NUM_NTC=CR.NUM_NTC -- cr
      AND TRUNC(CR.DAT_INI_LIGACAO) BETWEEN TRUNC(AC.DAT_INI_ATENDIMENTO) AND TRUNC(LAST_DAY(AC.DAT_INI_ATENDIMENTO))
            
ORDER BY  AC.NUM_NTC
         ,AC.DAT_INI_ATENDIMENTO      
         ,CR.DAT_INI_LIGACAO
;


-- SELECT * FROM BI_FT_SQDAA_RET_DIG_N_APP;
--BI_FT
DROP TABLE BI_FT_SQDAA_RET_DIG_N_APP;
CREATE TABLE BI_FT_SQDAA_RET_DIG_N_APP COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS

SELECT * FROM TMP_SQDAA_RET_DIG_N_APP_03A  
UNION ALL
SELECT * FROM TMP_SQDAA_RET_DIG_N_APP_03B
UNION ALL
SELECT * FROM TMP_SQDAA_RET_DIG_N_APP_03C
UNION ALL
SELECT * FROM TMP_SQDAA_RET_DIG_N_APP_03D
UNION ALL
SELECT * FROM TMP_SQDAA_RET_DIG_N_APP_03E
UNION ALL
SELECT * FROM TMP_SQDAA_RET_DIG_N_APP_03F   
;


--- AGREGADA
INSERT INTO BI_AGG_SQDAA_RET_DIG_N_APP
--CREATE TABLE BI_AGG_SQDAA_RET_DIG_N_APP COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS
SELECT /*+PARALLEL (32)*/ 
         DAT_REFERENCIA
        ,DW_METODO_CONTATO 
        ,DSC_SAFRA
        ,DSC_VISAO_ANALISE
        ,DSC_MOTIVO_ATEND_NIVEL_1
        ,DSC_MOTIVO_ATEND_NIVEL_2
        ,DSC_MOTIVO_ATEND_NIVEL_3
        ,DSC_MOTIVO_ATEND_NIVEL_4
        ,DSC_MOTIVO_ATEND_NIVEL_5
        ,NVL(DSC_MOTIVO_URA,'-3') AS  DSC_MOTIVO_URA      
        ,COUNT(DISTINCT NUM_NTC) AS QTD_USU_UNICOS
        ,COUNT (DISTINCT NUM_NTC || TO_CHAR(DAT_INI_ATENDIMENTO,'RRRRMMDD HH24MISS'))  AS QTD_ACESSO
FROM BI_FT_SQDAA_RET_DIG_N_APP
---WHERE DSC_SAFRA = 'D 0'
GROUP BY 
         DAT_REFERENCIA
        ,DW_METODO_CONTATO 
        ,DSC_SAFRA
        ,DSC_VISAO_ANALISE
        ,DSC_MOTIVO_ATEND_NIVEL_1
        ,DSC_MOTIVO_ATEND_NIVEL_2
        ,DSC_MOTIVO_ATEND_NIVEL_3
        ,DSC_MOTIVO_ATEND_NIVEL_4
        ,DSC_MOTIVO_ATEND_NIVEL_5
        ,NVL(DSC_MOTIVO_URA,'-3');  
 COMMIT;


/*--============================================================================================================================================================
         RESUMOS
--============================================================================================================================================================*/
SELECT DISTINCT --------------------------------------------------------------------------------------------->>> RESUMO TABELA
       R.DAT_REFERENCIA
       ,R.DSC_SAFRA
       ,R.DSC_VISAO_ANALISE
       ,SUM(R.QTD_USU_UNICOS) AS QTD_USU_UNICOS
       ,SUM(R.QTD_ACESSO) AS QTD_ACESSO
FROM BI_AGG_SQDAA_RET_DIG_N_APP R
GROUP BY   R.DAT_REFERENCIA
       ,R.DSC_SAFRA
       ,R.DSC_VISAO_ANALISE
ORDER BY R.DAT_REFERENCIA
       ,R.DSC_SAFRA
       ,R.DSC_VISAO_ANALISE ASC; 
       
SELECT  /*+ PARALLEL (32)*/ --------------------------------------------------------------------------------------------->>> USUÁRIOS UNICOS TOTAL
       DAT_REFERENCIA 
       ,COUNT(DISTINCT(NUM_NTC)) AS QT_USUARIOS_UNICOS
       ,COUNT (DISTINCT NUM_NTC || TO_CHAR(DAT_INI_ATENDIMENTO,'RRRRMMDD HH24MISS')) AS QT_ACESSOS
          
FROM TMP_SQDAA_RET_DIG_N_APP_01A  
GROUP BY DAT_REFERENCIA
      ;
	  

/*--============================================================================================================================================================
         MOTIVOS DO RETIDO NO MESMO DIA 
		 
		 
--============================================================================================================================================================*/
--TRANSAÇÕES DIGITAIS -------------------------------PARTE 1

WITH 
MCM_MOT_RET_P1 AS (
SELECT /*+parallel (32)*/ 
    SUBSTR(TO_CHAR(MC.DAT_INICIO_ATENDIMENTO, 'RRRRMMDD'),1,6) AS SAFRA
    ,MC.DAT_INICIO_ATENDIMENTO
    ,MC.NUM_NTC
    ,TO_CHAR(MC.NUM_NTC ||' '||TO_CHAR(MC.DAT_INICIO_ATENDIMENTO, 'RRRRMMDD')) AS SK_TRANSACAO
    ,MC.DW_METODO_CONTATO
    ,DM.DSC_METODO_CONTATO
    ,M.DSC_MOTIVO_ATEND_NIVEL_2 
    ,CASE WHEN M.COD_MOTIVO_ATEND_NIVEL_1 NOT IN (45) THEN 'FORA DO DASHBOARD'
          WHEN M.COD_MOTIVO_ATEND_NIVEL_2 NOT IN (8189,8190,33155,8188) THEN 'FORA DO DASHBOARD'
          WHEN M.COD_MOTIVO_ATEND_NIVEL_3 NOT IN (68164,68167,68176,68171) THEN 'FORA DO DASHBOARD' ELSE 'DASHBOARD' END DSC_ANALISE_NAVEGACAO
    ,CASE WHEN M.COD_MOTIVO_ATEND_NIVEL_1 NOT IN (45) THEN 1
          WHEN M.COD_MOTIVO_ATEND_NIVEL_2 NOT IN (8189,8190,33155,8188) THEN 1
          WHEN M.COD_MOTIVO_ATEND_NIVEL_3 NOT IN (68164,68167,68176,68171) THEN 1 ELSE 0 END FLG_ANALISE_NAVEGACAO
    
   ,TO_CHAR(TO_CHAR(M.DSC_MOTIVO_ATEND_NIVEL_1) ||'-'||
                           TO_CHAR(M.DSC_MOTIVO_ATEND_NIVEL_2) ||'-'||
                           TO_CHAR(M.DSC_MOTIVO_ATEND_NIVEL_3)||'-'||
                (CASE WHEN M.DSC_MOTIVO_ATEND_NIVEL_4 = '-3' THEN '-' 
                      ELSE M.DSC_MOTIVO_ATEND_NIVEL_4 END)||'-'||
                (CASE WHEN M.DSC_MOTIVO_ATEND_NIVEL_5 = '-3' THEN '-' 
                      ELSE M.DSC_MOTIVO_ATEND_NIVEL_5 END)) AS CONCATENADO
   ,1 AS QT_TRANSACAO
      FROM DWH.BI_FP_ASSINANTE_ATEND_FECHADO MC
 LEFT JOIN DWH.BI_DIM_MOTIVO_ATENDIMENTO M          ON MC.DW_MOTIVO_ATENDIMENTO = M.DW_MOTIVO_ATENDIMENTO
 LEFT JOIN DWH.BI_DIM_METODO_CONTATO DM             ON MC.DW_METODO_CONTATO = DM.DW_METODO_CONTATO

WHERE MC.DW_METODO_CONTATO IN (791) 
      AND MC.DAT_INICIO_ATENDIMENTO BETWEEN TO_DATE(20200601, 'RRRRMMDD') AND  TO_DATE(20200630, 'RRRRMMDD')  
)

SELECT /*+parallel (32)*/ DISTINCT
    MOT.SAFRA
    ,MOT.DW_METODO_CONTATO
    ,MOT.DSC_METODO_CONTATO
    ,MOT.DSC_MOTIVO_ATEND_NIVEL_2
    ,MOT.DSC_ANALISE_NAVEGACAO
    ,MOT.CONCATENADO
    ,SUM(MOT.FLG_ANALISE_NAVEGACAO) AS QT_NAV_FORA_DASHBOARD
    ,SUM(MOT.QT_TRANSACAO) AS QT_TRANSACAO
    ,COUNT(DISTINCT(MOT.SK_TRANSACAO)) AS QT_TRANS_USU_DIA
FROM MCM_MOT_RET_P1 MOT
GROUP BY MOT.SAFRA
    ,MOT.DW_METODO_CONTATO
    ,MOT.DSC_METODO_CONTATO
    ,MOT.DSC_MOTIVO_ATEND_NIVEL_2
    ,MOT.DSC_ANALISE_NAVEGACAO
    ,MOT.CONCATENADO;
	
--TRANSAÇÕES DIGITAIS RETIDAS -------------------------------PARTE 2
WITH 
MCM_MOT_RET_P1 AS (
SELECT /*+parallel (32)*/ 
    SUBSTR(TO_CHAR(MC.DAT_INICIO_ATENDIMENTO, 'RRRRMMDD'),1,6) AS SAFRA
    ,MC.DAT_INICIO_ATENDIMENTO
    ,MC.NUM_NTC
    ,TO_CHAR(MC.NUM_NTC ||' '||TO_CHAR(MC.DAT_INICIO_ATENDIMENTO, 'RRRRMMDD')) AS SK_TRANSACAO
    ,MC.DW_METODO_CONTATO
    ,DM.DSC_METODO_CONTATO
    ,M.DSC_MOTIVO_ATEND_NIVEL_2 
    ,CASE WHEN M.COD_MOTIVO_ATEND_NIVEL_1 NOT IN (45) THEN 'FORA DO DASHBOARD'
          WHEN M.COD_MOTIVO_ATEND_NIVEL_2 NOT IN (8189,8190,33155,8188) THEN 'FORA DO DASHBOARD'
          WHEN M.COD_MOTIVO_ATEND_NIVEL_3 NOT IN (68164,68167,68176,68171) THEN 'FORA DO DASHBOARD' ELSE 'DASHBOARD' END DSC_ANALISE_NAVEGACAO
    ,CASE WHEN M.COD_MOTIVO_ATEND_NIVEL_1 NOT IN (45) THEN 1
          WHEN M.COD_MOTIVO_ATEND_NIVEL_2 NOT IN (8189,8190,33155,8188) THEN 1
          WHEN M.COD_MOTIVO_ATEND_NIVEL_3 NOT IN (68164,68167,68176,68171) THEN 1 ELSE 0 END FLG_ANALISE_NAVEGACAO
    
   ,TO_CHAR(TO_CHAR(M.DSC_MOTIVO_ATEND_NIVEL_1) ||'-'||
                           TO_CHAR(M.DSC_MOTIVO_ATEND_NIVEL_2) ||'-'||
                           TO_CHAR(M.DSC_MOTIVO_ATEND_NIVEL_3)||'-'||
                (CASE WHEN M.DSC_MOTIVO_ATEND_NIVEL_4 = '-3' THEN '-' 
                      ELSE M.DSC_MOTIVO_ATEND_NIVEL_4 END)||'-'||
                (CASE WHEN M.DSC_MOTIVO_ATEND_NIVEL_5 = '-3' THEN '-' 
                      ELSE M.DSC_MOTIVO_ATEND_NIVEL_5 END)) AS CONCATENADO
   ,1 AS QT_TRANSACAO
      FROM DWH.BI_FP_ASSINANTE_ATEND_FECHADO MC
 LEFT JOIN DWH.BI_DIM_MOTIVO_ATENDIMENTO M          ON MC.DW_MOTIVO_ATENDIMENTO = M.DW_MOTIVO_ATENDIMENTO
 LEFT JOIN DWH.BI_DIM_METODO_CONTATO DM             ON MC.DW_METODO_CONTATO = DM.DW_METODO_CONTATO

WHERE MC.DW_METODO_CONTATO IN (791) 
      AND MC.DAT_INICIO_ATENDIMENTO BETWEEN TO_DATE(20200601, 'RRRRMMDD') AND  TO_DATE(20200630, 'RRRRMMDD')  
),

MCM_MOT_RET_P2 AS (
SELECT /*+parallel (32)*/ DISTINCT
  MOT.*

FROM MCM_MOT_RET_P1 MOT
INNER JOIN U92047747.BI_FT_SQDAA_RET_DIG_N_APP RET ON MOT.SK_TRANSACAO = TO_CHAR(RET.NUM_NTC ||' '||TO_CHAR(RET.DAT_INI_ATENDIMENTO, 'RRRRMMDD'))
WHERE 
 RET.DAT_INI_ATENDIMENTO BETWEEN TO_DATE(20200601, 'RRRRMMDD') AND  TO_DATE(20200630, 'RRRRMMDD')
 AND RET.DSC_VISAO_ANALISE = 'RETIDO DIGITAL'
 AND RET.DSC_SAFRA = 'D 0'
)
SELECT /*+parallel (32)*/ DISTINCT
    MOT.SAFRA
    ,MOT.DW_METODO_CONTATO
    ,MOT.DSC_METODO_CONTATO
    ,MOT.DSC_MOTIVO_ATEND_NIVEL_2
    ,MOT.DSC_ANALISE_NAVEGACAO
    ,MOT.CONCATENADO
    ,SUM(MOT.FLG_ANALISE_NAVEGACAO) AS QT_NAV_FORA_DASHBOARD
    ,SUM(MOT.QT_TRANSACAO) AS QT_TRANSACAO
    ,COUNT(DISTINCT(MOT.SK_TRANSACAO)) AS QT_TRANS_USU_DIA
FROM MCM_MOT_RET_P2 MOT
GROUP BY MOT.SAFRA
    ,MOT.DW_METODO_CONTATO
    ,MOT.DSC_METODO_CONTATO
    ,MOT.DSC_MOTIVO_ATEND_NIVEL_2
    ,MOT.DSC_ANALISE_NAVEGACAO
    ,MOT.CONCATENADO;

/*--============================================================================================================================================================
         Rollout par ano novo app
		 
		 
--============================================================================================================================================================*/

/*DSC_ROLLOUT      QT_UU                  
---------------- ---------------------- 
ACESSA MCM       26542                  
SOMENTE NOVO APP 24135                  
47,69% dos clientes acessaram somente o novo app em junho/2020*/


--SELECT * FROM BI_FT_SQDAA_RET_DIG_N_APP;
WITH 
ROLLOUT_P1 AS (
SELECT  /*+ PARALLEL (32)*/
       LAST_DAY(DAT_INICIO_ATENDIMENTO) AS DAT_REFERENCIA 
       ,SUBSTR(TO_CHAR(MC.DAT_INICIO_ATENDIMENTO, 'RRRRMMDD'),1,6) AS SAFRA --ok
       ,TO_NUMBER(TO_CHAR(MC.DAT_INICIO_ATENDIMENTO, 'RRRRMMDD')) SK_DATA
       ,TO_DATE(TO_CHAR(MC.DAT_INICIO_ATENDIMENTO, 'RRRRMMDD')||' '||MC.HOR_INICIO_ATENDIMENTO,'RRRRMMDD HH24MISS') AS DAT_INI_ATENDIMENTO
       ,TO_DATE(TO_CHAR(MC.DAT_FIM_ATENDIMENTO, 'RRRRMMDD')||' '||MC.HOR_FIM_ATENDIMENTO,'RRRRMMDD HH24MISS') AS DAT_FIM_ATENDIMENTO
       ,MC.NUM_NTC
       ,MC.DW_METODO_CONTATO
       ,DM.DSC_METODO_CONTATO

FROM DWH.BI_FP_ASSINANTE_ATEND_FECHADO MC 
INNER JOIN BI_FT_SQDAA_RET_DIG_N_APP NV   ON NV.NUM_NTC = MC.NUM_NTC
LEFT JOIN BI_DIM_METODO_CONTATO DM        ON MC.DW_METODO_CONTATO = DM.DW_METODO_CONTATO
WHERE MC.DW_METODO_CONTATO IN (81,411,451,701,709)     -------EXPURDO 681 App Flex 552 Minha Claro Empresa    
      AND UPPER(MC.DSC_OBSERVACAO_ATENDIMENTO) LIKE '%MINHACLAROWEB%AUTENTICA%USU%'
      AND MC.DAT_INICIO_ATENDIMENTO BETWEEN TO_DATE(20200601, 'RRRRMMDD') AND  TO_DATE(20200630, 'RRRRMMDD')
),
ROLLOUT_P2 AS (
SELECT /*+ PARALLEL (32)*/ DISTINCT
   CASE WHEN REP.NUM_NTC IS NULL THEN 'SOMENTE NOVO APP' ELSE 'ACESSA MCM' END AS DSC_ROLLOUT
   ,COUNT(DISTINCT(NV.NUM_NTC)) AS QT_UU
  
FROM BI_FT_SQDAA_RET_DIG_N_APP NV
LEFT JOIN ROLLOUT_P1 REP ON NV.NUM_NTC = REP.NUM_NTC
GROUP BY CASE WHEN REP.NUM_NTC IS NULL THEN 'SOMENTE NOVO APP' ELSE 'ACESSA MCM' END
)
SELECT * FROM ROLLOUT_P2;
	