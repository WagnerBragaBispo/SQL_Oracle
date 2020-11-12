
--===========================================================================================================================
--                        CONSULTA NO ATENDIMENTO FECHAMDO DE SET.19 
--                                         VS
--                                 MOTIVOS CLASSIFICADOS 
--===========================================================================================================================

WITH ATEND_FECHADO AS (

SELECT /*PARALLEL (32) */
        T.NUM_NTC
        ,T.DAT_INICIO_ATENDIMENTO
        ,T.DW_METODO_CONTATO
        ,T.DW_MOTIVO_ATENDIMENTO
        ,regexp_replace(TO_CHAR(M.COD_MOTIVO_ATEND_NIVEL_1,'00000')||
                        TO_CHAR(M.COD_MOTIVO_ATEND_NIVEL_2,'00000')|| 
                        TO_CHAR(M.COD_MOTIVO_ATEND_NIVEL_3,'00000'),'[[:space:]]*','')||
               (CASE WHEN M.COD_MOTIVO_ATEND_NIVEL_4 = -3 THEN '00000' 
                     ELSE (regexp_replace(TO_CHAR(M.COD_MOTIVO_ATEND_NIVEL_4,'00000'),'[[:space:]]*',''))END)||
                (CASE WHEN M.COD_MOTIVO_ATEND_NIVEL_5 = -3 THEN '00000' 
                      ELSE (regexp_replace(TO_CHAR(M.COD_MOTIVO_ATEND_NIVEL_5,'00000'),'[[:space:]]*',''))END) 
                           AS ID_MOTIVOS_TUPLA
---SELECT *                  
FROM BI_FP_ASSINANTE_ATEND_FECHADO@BASECLARO T
LEFT JOIN BI_DIM_MOTIVO_ATENDIMENTO@BASECLARO M
     ON T.DW_MOTIVO_ATENDIMENTO = M.DW_MOTIVO_ATENDIMENTO
LEFT JOIN BI_DIM_METODO_CONTATO@BASECLARO MC
     ON T.DW_METODO_CONTATO = MC.DW_METODO_CONTATO
WHERE T.DAT_INICIO_ATENDIMENTO BETWEEN TO_DATE(&DATA_INICIAL, 'RRRRMMDD') AND  TO_DATE(&DATA_FINAL, 'RRRRMMDD')
      AND MC.DW_METODO_CONTATO IN (81, 411, 451,701) --EXPURGO 681 FLEX, 552 Minha Claro Empresa
--and UPPER(t.DSC_OBSERVACAO_ATENDIMENTO) LIKE '%MINHACLAROWEB%AUTENTICA%USU%'        
---AND T.NUM_NTC = '67991820001'
GROUP BY 
         T.NUM_NTC
        ,T.DAT_INICIO_ATENDIMENTO
        ,T.DW_METODO_CONTATO
        ,T.DW_MOTIVO_ATENDIMENTO
    ,regexp_replace(TO_CHAR(M.COD_MOTIVO_ATEND_NIVEL_1,'00000')||
                        TO_CHAR(M.COD_MOTIVO_ATEND_NIVEL_2,'00000')|| 
                        TO_CHAR(M.COD_MOTIVO_ATEND_NIVEL_3,'00000'),'[[:space:]]*','')||
               (CASE WHEN M.COD_MOTIVO_ATEND_NIVEL_4 = -3 THEN '00000' 
                     ELSE (regexp_replace(TO_CHAR(M.COD_MOTIVO_ATEND_NIVEL_4,'00000'),'[[:space:]]*',''))END)||
                (CASE WHEN M.COD_MOTIVO_ATEND_NIVEL_5 = -3 THEN '00000' 
                      ELSE (regexp_replace(TO_CHAR(M.COD_MOTIVO_ATEND_NIVEL_5,'00000'),'[[:space:]]*',''))END) 
                           
),

ATEND_FECHADO2 AS (
SELECT  
      T.NUM_NTC
        ,T.DAT_INICIO_ATENDIMENTO
        ,T.DW_METODO_CONTATO
        ,T.DW_MOTIVO_ATENDIMENTO
       ,DMO.ID_FUNCIONALIDADE
       ,DMO.DSC_FUNCIONALIDADE
       ,DMO.DSC_CATEGORIA
       ,DMO.FC_APP_SITE
       ,DMO.FC_MOTIVO_AUTOM               
FROM ATEND_FECHADO T
LEFT JOIN U92277452.TMP_SQDAA_DIM_MOT_CMV@BASECLARO DMO
     ON T.ID_MOTIVOS_TUPLA = DMO.ID_MOTIVOS_TUPLA

   
          
GROUP BY T.NUM_NTC
        ,T.DAT_INICIO_ATENDIMENTO
        ,T.DW_METODO_CONTATO
        ,T.DW_MOTIVO_ATENDIMENTO
       ,DMO.ID_FUNCIONALIDADE 
       ,DMO.DSC_FUNCIONALIDADE
       ,DMO.DSC_CATEGORIA
       ,DMO.FC_APP_SITE
       ,DMO.FC_MOTIVO_AUTOM      
)

SELECT DISTINCT
       DAT_INICIO_ATENDIMENTO
       ,DSC_CATEGORIA
       ,ID_FUNCIONALIDADE
       ,DSC_FUNCIONALIDADE
      -- ,FC_APP_SITE
      -- ,FC_MOTIVO_AUTOM
      ,COUNT(NUM_NTC) QT_NTC
FROM ATEND_FECHADO2 
GROUP BY DAT_INICIO_ATENDIMENTO
       ,DSC_CATEGORIA
       ,ID_FUNCIONALIDADE
       ,DSC_FUNCIONALIDADE
      -- ,FC_APP_SITE
      -- ,FC_MOTIVO_AUTOM
              
  ;


--===========================================================================================================================
--CONSULTA OS CLIENTES QUE UTILIZARAM O CANAL DO WHATSAPP-------------

--===========================================================================================================================

TMP_SQD_AA_WTSAPP_01 AS
(
SELECT /*+parallel (32)*/
        A.DAT_REFERENCIA,
        --TO_CHAR(A.DAT_REFERENCIA, 'YYYYMMDD') AS SK_DATA
        SUBSTR(SUBSTR(TO_CHAR(A.DAT_REFERENCIA, 'YYYYMMDD'),5),1,2) AS MES_SK,
        A.COD_PLATAFORMA,
        A.COD_ATENDIMENTO,
        A.NUM_NTC,
        A.DAT_INICIO_ATENDIMENTO,
        A.DAT_FIM_ATENDIMENTO

FROM DWH.BI_FP_ATENDIMENTO_MINT A
LEFT JOIN DWH.BI_DIM_CANAL_MINT B ON A.DW_NUM_CANAL_MINT = B.DW_NUM_CANAL_MINT
WHERE A.COD_SHORT_CODE = 9652 --- SHORT CODE WPP
---      AND NUM_NTC = '11980299093' -- INTERAÇÃO EM 20/12/2020
AND A.DAT_REFERENCIA BETWEEN TO_DATE(&DATA_INICIAL, 'RRRRMMDD') AND  TO_DATE(&DATA_FINAL, 'RRRRMMDD')
),

TMP_SQD_AA_WTSAPP_02 AS

(SELECT  /*+PARALLEL (32)*/
        A.DAT_REFERENCIA
       ,A.MES_SK
       ,A.COD_PLATAFORMA
       ,A.COD_ATENDIMENTO
       ,A.NUM_NTC
       ,A.DAT_INICIO_ATENDIMENTO
       ,A.DAT_FIM_ATENDIMENTO
       ,B.DW_NOS_MINT 
       ,B.DW_MENU_MINT
       ,C.DSC_NOS_MINT
       ,D.PARA AS DSC_PARA_MINT
FROM TMP_SQD_AA_WTSAPP_01 A
LEFT JOIN DWH.BI_FP_NAVEGACAO_MINT@BASECLARO B ON A.COD_ATENDIMENTO = B.COD_ATENDIMENTO AND A.DAT_REFERENCIA  = B.DAT_REFERENCIA
LEFT JOIN DWH.BI_DIM_NOS_MINT@BASECLARO C ON B.DW_NOS_MINT = C.DW_NOS_MINT
LEFT JOIN INTMKT.DE_PARA_MINT@BASECLARO D ON C.DSC_NOS_MINT = D.DE
WHERE B.DW_NUM_CANAL_MINT = 2),


------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------
------------DEXTINÇÃO DOS NÚMEROS DOS CLIENTES QUE ACESSARAM O CANAL WHATSAPP-------------
------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------

TMP_SQD_AA_WTSAPP_03 AS 


(SELECT 
       DISTINCT(NUM_NTC),
       MES_SK AS MES_SAFRA_WHATS,
       COD_PLATAFORMA AS SEGMENTO_WHATS
FROM TMP_SQD_AA_WTSAPP_02),

------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------
------------VERIFICA O ANALITICO DE LIGAÇÕES PARA CLARO MÓVEL-------------
------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------

CONSULTA_ANALITICO_CR AS

(

       SELECT /*+ PARALLEL (32)*/
       'Claro Móvel' AS NM_MARCA,
       'CMV' AS NM_SUB_MARCA,
       FT.SK_ATENDIMENTO_URA AS CALL_ID,
       FT.DT_INICIO_LIGACAO,
       SUBSTR(SUBSTR(FT.DT_INICIO_LIGACAO,5),1,2) AS MES_SK,
       FT.NR_HORA_INICIO_LIGACAO,
       FT.CD_NUMERO_TELEFONE AS NUM_NTC,
       CASE
           WHEN FT.FN_LIGACAO_IDENTIFICADA = 1 THEN 'Sim' ELSE 'Não' END AS FG_LIG_IDENTIFICADA,
       STS.COD_SUB_STS_ATU STATUS_CLIENTE,
       CASE
           WHEN FT.FN_CORPORATIVO = 1 THEN 'Sim' ELSE 'Não' END AS FN_CORPORATIVO,
       CASE
            WHEN VA.NM_VISAO_ANALISE_BI = 'Contact Rate Direcionado ao Humano' THEN 'Direcionado Humano' ELSE 'Retido URA' END FC_DIRECIONADO_RETIDO,
       ATM.NM_TIPO_MOTIVO_BI AS NM_MOTIVO_URA,
       STM.NM_SUB_TIPO_MOTIVO_BI AS NM_SUB_MOTIVO_URA,
       ATL.NM_TIPO_LIGACAO_BI AS NM_EXPURGO_URA,
       PP.DSC_PLANO_PRECO_BI AS TP_PLANO_CLIENTE
FROM INTMKT.FT_ATENDIMENTO_URA@BASECLARO FT
LEFT JOIN DWH.BI_DIM_UN_NEGOCIO@BASECLARO UN
ON UN.DW_UN_NEGOCIO = FT.SK_REGIONAL
LEFT JOIN DWH.BI_DIM_AREA@BASECLARO AR
ON AR.DW_AREA = FT.SK_AREA
LEFT JOIN DWH.BI_DIM_PLATAFORMA@BASECLARO PLA
ON PLA.COD_PLATAFORMA = FT.SK_PLATAFORMA
LEFT JOIN INTMKT.DS_ATENDIMENTO_PLATAFORMA@BASECLARO APL
ON APL.SK_ATENDIMENTO_PLATAFORMA = FT.SK_ATENDIMENTO_PLATAFORMA
LEFT JOIN DWH.BI_DIM_STATUS@BASECLARO STS
ON STS.STS_DW = FT.SK_STATUS
LEFT JOIN DWH.BI_DIM_TIPO_CLIENTE@BASECLARO TC
ON TC.DW_TIPO_CLIENTE = FT.SK_TIPO_CLIENTE
LEFT JOIN DWH.BI_DIM_FAIXA_HORA@BASECLARO FXH
ON FXH.DW_FAIXA_HORA = FT.SK_FAIXA_HORA
LEFT JOIN DWH.BI_DIM_MENU_URA@BASECLARO MNU
ON MNU.DW_MENU_URA = FT.SK_MENU_URA
LEFT JOIN DWH.BI_DIM_AUTOMACAO_URA@BASECLARO ARA
ON ARA.DW_AUTOMACAO_URA = FT.SK_AUTOMACAO_URA
LEFT JOIN DWH.BI_DIM_NAVEGACAO_URA@BASECLARO NVU
ON NVU.DW_NAVEGACAO_URA = FT.SK_NAVEGACAO_URA
LEFT JOIN DWH.BI_DIM_TIPO_DESCONEXAO_URA@BASECLARO TDU
ON TDU.DW_TIPO_DESCONEXAO = FT.SK_TIPO_DESCONEXAO
LEFT JOIN INTMKT.DS_ATENDIMENTO_TIPO_MOTIVO@BASECLARO ATM
ON ATM.SK_ATENDIMENTO_TIPO_MOTIVO = FT.SK_ATENDIMENTO_TIPO_MOTIVO
LEFT JOIN INTMKT.DS_ATENDIMENTO_SCRIPT@BASECLARO ATS
ON ATS.SK_ATENDIMENTO_SCRIPT = FT.SK_ATENDIMENTO_SCRIPT
LEFT JOIN INTMKT.DS_ATENDIMENTO_TIPO_LIGACAO@BASECLARO ATL
ON ATL.SK_ATENDIMENTO_TIPO_LIGACAO = FT.SK_ATENDIMENTO_TIPO_LIGACAO
LEFT JOIN DWH.BI_DIM_CICLO@BASECLARO CCC
ON CCC.DW_CICLO = FT.SK_NUMERO_CICLO
LEFT JOIN DWH.BI_DIM_PLANO_PRECO@BASECLARO PP
ON PP.DW_PLANO = FT.SK_PLANO
LEFT JOIN INTMKT.DS_ATENDIMENTO_SUB_TIPO_MOTIVO@BASECLARO STM
ON STM.SK_ATENDIMENTO_SUB_TIPO_MOTIVO = FT.SK_ATENDIMENTO_SUB_TIPO_MOTIVO
LEFT JOIN INTMKT.DS_VISAO_ANALISE@BASECLARO VA
ON VA.SK_VISAO_ANALISE = FT.SK_VISAO_ANALISE
LEFT JOIN INTMKT.DS_ATENDIMENTO_FAIXA_AGING@BASECLARO FA
ON FA.SK_ATENDIMENTO_FAIXA_AGING = FT.SK_ATENDIMENTO_FAIXA_AGING
LEFT JOIN INTMKT.DS_ATENDIMENTO_FAIXA_ETARIA@BASECLARO FE
ON FE.SK_ATENDIMENTO_FAIXA_ETARIA = FT.SK_ATENDIMENTO_FAIXA_ETARIA
LEFT JOIN INTMKT.DS_AGRUPAMENTO_TIPO_CLIENTE@BASECLARO ATC
ON ATC.SK_AGRUPAMENTO_TIPO_CLIENTE = FT.SK_AGRUPAMENTO_TIPO_CLIENTE
LEFT JOIN INTMKT.DS_CALENDARIO@BASECLARO CAL
ON CAL.SK_CALENDARIO = FT.SK_DATA
LEFT JOIN INTMKT.DS_FLAG@BASECLARO FL
ON FL.SK_FLAG = FT.SK_FC_ASSINANTE_ZB
WHERE FT.SK_DATA >= &DATA_INICIAL AND FT.SK_DATA <= &DATA_FINAL),

------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------
------------CONSOLIDA O ANALITICO DE LIGAÇÕES E AGRUPA-------------
------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------


CONSOLIDADO_LIGACOES_MOVEL AS

(SELECT 
      MES_SK AS MES_LIGACAO,
      NUM_NTC,
      NM_MOTIVO_URA,
      COUNT(*) AS QTD_LIG
FROM CONSULTA_ANALITICO_CR
WHERE FC_DIRECIONADO_RETIDO = 'Direcionado Humano'
      GROUP BY
              MES_SK,
              NUM_NTC,
              NM_MOTIVO_URA)


-----------------------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------------------------
------------ANALISE FINAL VERIFICAR QUAL CR DOS CLIENTES QUE ACESSARAM O CANAL DO WHATSAPP NO MÊS ANTERIOR E SUBSEQUENTE-------------
----------------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------

SELECT
--      A.NUM_NTC, - NTC_ORIGEM
      A.SEGMENTO_WHATS,
      A.MES_SAFRA_WHATS,
      B.MES_LIGACAO,
      SUM(B.QTD_LIG)
FROM TMP_SQD_AA_WTSAPP_03 A
LEFT JOIN CONSOLIDADO_LIGACOES_MOVEL B
          ON A.NUM_NTC = B.NUM_NTC
GROUP BY
--      A.NUM_NTC, - NTC_ORIGEM
      A.SEGMENTO_WHATS,
      A.MES_SAFRA_WHATS,
      B.MES_LIGACAO,
      B.QTD_LIG

