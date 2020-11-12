--QUERY V2 RET WPP

---===========================================================================================================================
-- Ação: Lançamento Canal WhatsApp 
-- Inicio: 04/11/19
--objetivo: RETENÇÃO NO ATENDIMENTO DO CANAL  

--RELAÇÃO DE CLIENTES UTILIZADOS PARA TESTE:
--NUM_NTC = '11920023824'   --09.01.2020 -> 11920023824 CLIENTES QUE ACESSOU WPP  
--NUM_NTC = '11976881122'   --02.03.2020 -> CLIENTE TRANSBORDO WPP
--NUM_NTC = '51991419490'   --07.02.2020 -> CLIENTE TRANSBORDO WPP
--NUM_NTC = '11980299093' -- 20.02.2020 --> INTERAÇÃO NO WPP
--NUM_NTC = '11930002132' -- 24.02.2020 --> LIGOU E ACESSOU 
--NUM_NTC = '11930003116' -- 14.02.2020 --> ACESSOU E ACESSOU

---===========================================================================================================================
---FEV.2020 524.621 QT_ACESSOS
---FEV.2020 7.428.993 QT_LIGACOES
---FEV.2020 6.711 QT_ACESSOS_TRANSBORDO
--SELECT COUNT(DISTINCT(NUM_NTC || DT_INI_ATENDIMENTO)) QT_ACESSOS  FROM TMP_SQDAA_WPP_RET_P1 ---FEV.2020 524.621 QT_ACESSOS
--SELECT COUNT(DISTINCT(NUM_NTC || DT_INI_LIGACAO)) QT_LIGACOES  FROM TMP_SQDAA_WPP_RET_P2 ---FEV.2020 7.428.993 QT_LIGACOES
--SELECT COUNT(DISTINCT(NUM_NTC || DT_INI_LIGACAO)) QT_ACESSOS  FROM TMP_SQDAA_WPP_RET_P3 ---FEV.2020 524.621 QT_ACESSOS


DROP TABLE TMP_SQDAA_WPP_RET_P1;
DROP TABLE TMP_SQDAA_WPP_RET_P2;
DROP TABLE TMP_SQDAA_WPP_RET_P3A;
DROP TABLE TMP_SQDAA_WPP_RET_P3B;
DROP TABLE TMP_SQDAA_WPP_RET_P4;
DROP TABLE TMP_SQDAA_WPP_RET_P5;
DROP TABLE TMP_FT_RETIDO_DIGITAL_WHATSAPP;
DROP TABLE TMP_RET_DIG_WHATSAPP_DTREF;

CREATE TABLE TMP_SQDAA_WPP_RET_P1 COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS
SELECT DISTINCT /*+parallel (32)*/ ------------------------------------------------------------------------ETAPA 1 ACESSO WPP
        A.DAT_REFERENCIA
        ,SUBSTR(TO_CHAR(A.DAT_INICIO_ATENDIMENTO, 'YYYYMMDD'),1,6) AS SAFRA --ok
        ,TO_CHAR(A.DAT_INICIO_ATENDIMENTO, 'YYYYMMDD') AS SK_DATA --ok
        ,A.COD_PLATAFORMA --ok
        ,A.NUM_NTC --ok
        ,A.DAT_INICIO_ATENDIMENTO AS DT_INI_ATENDIMENTO
        ,TRUNC(A.DAT_INICIO_ATENDIMENTO) AS DAT_INICIO_ATENDIMENTO --ok
        ,TO_CHAR(A.NUM_NTC ||' '||TO_CHAR(A.DAT_INICIO_ATENDIMENTO, 'YYYYMMDD') ||' '||REPLACE(SUBSTR(TO_CHAR(A.DAT_INICIO_ATENDIMENTO,'dd/mm/yyyy hh24:mi:ss'),12,8),':','')) AS SK_ACESSO 
        ,A.COD_SHORT_CODE AS DW_SHOTE_CODE_MINT
    ,SYSDATE                       AS DAT_CRIACAO
FROM DWH.BI_FP_ATENDIMENTO_MINT A
LEFT JOIN DWH.BI_DIM_CANAL_MINT B ON A.DW_NUM_CANAL_MINT = B.DW_NUM_CANAL_MINT
WHERE A.COD_SHORT_CODE = 9652 --- SHORT CODE WPP
AND A.DAT_INICIO_ATENDIMENTO BETWEEN TO_DATE(&DATA_INICIAL, 'RRRRMMDD') AND  TO_DATE(&DATA_FINAL, 'RRRRMMDD')

GROUP BY  A.DAT_REFERENCIA
        ,SUBSTR(TO_CHAR(A.DAT_INICIO_ATENDIMENTO, 'YYYYMMDD'),1,6)
        ,TO_CHAR(A.DAT_INICIO_ATENDIMENTO, 'YYYYMMDD')
        ,A.COD_PLATAFORMA
        ,A.NUM_NTC
        ,A.DAT_INICIO_ATENDIMENTO
    ,A.COD_SHORT_CODE
;


CREATE TABLE TMP_SQDAA_WPP_RET_P2 COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS
SELECT /*+ PARALLEL (32)*/ ------------------------------------------------------------------------ETAPA 2 LIGACOES CR
       LAST_DAY(TO_DATE(FT.DT_INICIO_LIGACAO,'YYYY/MM/DD')) AS DAT_REFERENCIA       
       ,FT.DT_INICIO_LIGACAO SK_DATA       
       ,'CMV' AS NM_MARCA
       ,FT.SK_ATENDIMENTO_URA AS COD_CHAMADA -- CALL_ID
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
---AND       atl.nm_tipo_ligacao_bi = 'Ligações Válidas' -- FILTRO OK (SÃO CLIENTES QUE NÃO TIVERAM ABANDONO)
---AND va.nm_visao_analise_bi = 'Contact Rate Direcionado ao Humano' -- FILTRO OK
;
 
CREATE TABLE TMP_SQDAA_WPP_RET_P3A COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS
SELECT DISTINCT /*+parallel(32) */ ------------------------------------------------------------------------ETAPA 3.1 ACESSO COM CR
       AC.SK_ACESSO
       ,CASE WHEN AC.DT_INI_ATENDIMENTO > CR.DT_INI_LIGACAO THEN 1 ELSE 0 END FLG_ACESSA_LIGA 
       ,SYSDATE                       AS DAT_CRIACAO
FROM TMP_SQDAA_WPP_RET_P1 AC -- WPP
INNER JOIN TMP_SQDAA_WPP_RET_P2 CR ON AC.NUM_NTC = CR.NUM_NTC -- cr
      AND TRUNC(AC.DT_INI_ATENDIMENTO) = TRUNC(CR.DT_INI_LIGACAO) ---------->>>>> SOMENTE D+0
WHERE COD_PLATAFORMA <> '-2'
GROUP BY AC.SK_ACESSO
         ,CASE WHEN AC.DT_INI_ATENDIMENTO > CR.DT_INI_LIGACAO THEN 1 ELSE 0 END    
ORDER BY  AC.SK_ACESSO
;   
 
CREATE TABLE TMP_SQDAA_WPP_RET_P3B COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS
SELECT DISTINCT /*+parallel(32) */ ------------------------------------------------------------------------ETAPA 3.2 ACESSO SOMENTE NÃO RETIDO
       AC.*
FROM TMP_SQDAA_WPP_RET_P3A AC -- WPP
WHERE AC.FLG_ACESSA_LIGA = 0
ORDER BY  AC.SK_ACESSO
;   

CREATE TABLE TMP_SQDAA_WPP_RET_P4 COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS
SELECT DISTINCT /*PARALLEL (32) */ ----------------------------------------------------------------ETAPA 4 TRANSBORDO BLIP_TEL
        TO_CHAR(TO_DATE(T.DAT_INICIO_ATENDIMENTO, 'DD/MM/RRRR'),'RRRRMM') AS SAFRA --ok
        ,TO_CHAR(TO_DATE(T.DAT_INICIO_ATENDIMENTO, 'DD/MM/RRRR'),'RRRRMMDD') AS SK_DATA --ok
        ,TO_CHAR(T.NUM_NTC ||' '||TO_CHAR(T.DAT_INICIO_ATENDIMENTO, 'YYYYMMDD') ||' '||T.HOR_INICIO_ATENDIMENTO) AS SK_ACESSO 
        ,TO_DATE(TO_DATE(T.DAT_INICIO_ATENDIMENTO,'DD/MM/YYYY') ||' ' ||T.HOR_INICIO_ATENDIMENTO,'DD/MM/RRRR HH24:MI:SS') AS DT_INI_CHAT
        ,T.NUM_NTC
        ,T.COD_LOGIN_ATEND_CRIACAO
        ,MC.DW_METODO_CONTATO
        ,MC.DSC_METODO_CONTATO AS METODO_CONTATO
        ,CASE WHEN T.NUM_NTC = AC.NUM_NTC 
              AND TRUNC(AC.DT_INI_ATENDIMENTO) = TRUNC(T.DAT_INICIO_ATENDIMENTO) THEN 0 ELSE 1 END AS FLG_ACESSA_TRANSBORDO

FROM BI_FP_ASSINANTE_ATEND_FECHADO T
LEFT JOIN BI_DIM_MOTIVO_ATENDIMENTO M ON T.DW_MOTIVO_ATENDIMENTO = M.DW_MOTIVO_ATENDIMENTO
LEFT JOIN BI_DIM_METODO_CONTATO MC    ON T.DW_METODO_CONTATO = MC.DW_METODO_CONTATO
INNER JOIN TMP_SQDAA_WPP_RET_P1 AC    ON T.NUM_NTC = AC.NUM_NTC -- ACESSO
      AND TRUNC(AC.DT_INI_ATENDIMENTO) = TRUNC(T.DAT_INICIO_ATENDIMENTO) ---------->>>>> SOMENTE D+0
WHERE T.DAT_INICIO_ATENDIMENTO BETWEEN TO_DATE(&DATA_INICIAL, 'RRRRMMDD') AND  TO_DATE(&DATA_FINAL, 'RRRRMMDD')
      AND T.DW_METODO_CONTATO IN (781)---WhatsApp
      AND T.COD_LOGIN_ATEND_CRIACAO NOT IN ('USSD')
GROUP BY 
        TO_CHAR(TO_DATE(T.DAT_INICIO_ATENDIMENTO, 'DD/MM/RRRR'),'RRRRMM')
        ,TO_CHAR(TO_DATE(T.DAT_INICIO_ATENDIMENTO, 'DD/MM/RRRR'),'RRRRMMDD')
    ,TO_CHAR(T.NUM_NTC ||' '||TO_CHAR(T.DAT_INICIO_ATENDIMENTO, 'YYYYMMDD') ||' '||T.HOR_INICIO_ATENDIMENTO)
        ,TO_DATE(TO_DATE(T.DAT_INICIO_ATENDIMENTO,'DD/MM/YYYY') ||' ' ||T.HOR_INICIO_ATENDIMENTO,'DD/MM/RRRR HH24:MI:SS')
        ,T.NUM_NTC
        ,T.COD_LOGIN_ATEND_CRIACAO
        ,MC.DW_METODO_CONTATO
        ,MC.DSC_METODO_CONTATO
        ,CASE WHEN T.NUM_NTC = AC.NUM_NTC 
              AND TRUNC(AC.DT_INI_ATENDIMENTO) = TRUNC(T.DAT_INICIO_ATENDIMENTO) THEN 0 ELSE 1 END
ORDER BY T.NUM_NTC ,TO_DATE(TO_DATE(T.DAT_INICIO_ATENDIMENTO,'DD/MM/YYYY') ||' ' ||T.HOR_INICIO_ATENDIMENTO,'DD/MM/RRRR HH24:MI:SS')
;

    
CREATE TABLE TMP_FT_RETIDO_DIGITAL_WHATSAPP COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS
SELECT DISTINCT ------------------------------------------------------------------------ETAPA 5 ACESSO + CR + TRANSBORDO
/*OK*/  A.SK_ACESSO 
/*OK*/  ,DAT_REFERENCIA
/*OK*/  ,A.SAFRA 
/*OK*/  ,A.NUM_NTC 
        ,A.SK_DATA 
/*OK*/  ,A.COD_PLATAFORMA 
        ,A.DAT_INICIO_ATENDIMENTO
        ,A.DT_INI_ATENDIMENTO
/*OK*/  ,A.DW_SHORT_CODE_MINT
/*OK*/  ,'WhatsApp' AS DSC_METODO_CONTATO
        ,CASE WHEN B.SK_ACESSO IS NULL THEN 1 ELSE 0 END AS FLG_ACESSA_LIGA
        ,CASE WHEN C.SK_ACESSO IS NULL THEN 1 ELSE 0 END AS FLG_ACESSA_TRANSBORDA
/*OK*/  ,CASE WHEN B.SK_ACESSO IS NULL 
               AND C.SK_ACESSO IS NULL THEN 'RETIDO DIGITAL' ELSE 'ACESSOU E LIGOU' END DSC_VISAO_ANALISE
/*OK*/  ,'D 0' AS DSC_SAFRA              
        ,SYSDATE                       AS DAT_CRIACAO
FROM TMP_SQDAA_WPP_RET_P1 A

LEFT JOIN TMP_SQDAA_WPP_RET_P3B B --->>LIGA URA
     ON A.SK_ACESSO = B.SK_ACESSO

LEFT JOIN TMP_SQDAA_WPP_RET_P4 C --->>TRANSBORDO
     ON A.SK_ACESSO = C.SK_ACESSO

WHERE COD_PLATAFORMA <> '-2'

GROUP BY  
/*OK*/  A.SK_ACESSO
/*OK*/  ,A.DAT_REFERENCIA
/*OK*/  ,A.SAFRA 
/*OK*/  ,A.NUM_NTC 
/*OK*/  ,A.SK_DATA 
/*OK*/  ,A.COD_PLATAFORMA 
/*OK*/  ,A.DAT_INICIO_ATENDIMENTO
/*OK*/  ,A.DW_SHOTE_CODE_MINT
/*OK*/  ,A.DT_INI_ATENDIMENTO
/*OK*/  ,CASE WHEN B.SK_ACESSO IS NULL THEN 1 ELSE 0 END
/*OK*/  ,CASE WHEN C.SK_ACESSO IS NULL THEN 1 ELSE 0 END
/*OK*/  ,CASE WHEN B.SK_ACESSO IS NULL 
               AND C.SK_ACESSO IS NULL THEN 'RETIDO DIGITAL' ELSE 'ACESSOU E LIGOU' END  
       
ORDER BY A.SK_ACESSO
;


CREATE TABLE TMP_RET_DIG_WHATSAPP_DTREF COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS
SELECT DISTINCT ------------------------------------------------------------------------ETAPA 6  TABELA AGREGADA
SAFRA ,MAX(DAT_INICIO_ATENDIMENTO) AS DAT_REFERENCIA FROM TMP_SQDAA_WPP_RET_P1 GROUP BY SAFRA;


INSERT INTO AGG_RET_DIG_WHATSAPP ------------------------------------------------------------------------ETAPA 6.1  TABELA AGREGADA
SELECT DISTINCT       
/*OK*/ A.SAFRA
/*OK*/ ,MAX(B.DAT_REFERENCIA) AS DAT_REFERENCIA
/*OK*/ ,A.DW_SHORT_CODE_MINT
/*OK*/ ,A.DSC_METODO_CONTATO
/*OK*/ ,A.COD_PLATAFORMA
/*OK*/ ,A.DSC_SAFRA
/*OK*/ ,A.DSC_VISAO_ANALISE
/*OK*/ ,COUNT(DISTINCT(A.NUM_NTC)) AS QT_USU_UNICOS
/*OK*/ ,COUNT(DISTINCT(A.SK_ACESSO)) AS QT_ACESSO
/*OK*/ ,SYSDATE                       AS DAT_CRIACAO
       
 FROM TMP_FT_RETIDO_DIGITAL_WHATSAPP A
 LEFT JOIN TMP_RET_DIG_WHATSAPP_DTREF B ON A.SAFRA = B.SAFRA
 GROUP BY     
/*OK*/ A.SAFRA
/*OK*/ ,B.DAT_REFERENCIA
/*OK*/ ,A.DW_SHORT_CODE_MINT
/*OK*/ ,A.DSC_METODO_CONTATO
/*OK*/ ,A.COD_PLATAFORMA
/*OK*/ ,A.DSC_SAFRA
/*OK*/ ,A.DSC_VISAO_ANALISE

ORDER BY 
/*OK*/ A.SAFRA
/*OK*/ ,MAX(B.DAT_REFERENCIA)
/*OK*/ ,A.DW_SHORT_CODE_MINT
/*OK*/ ,A.DSC_METODO_CONTATO
/*OK*/ ,A.COD_PLATAFORMA
/*OK*/ ,A.DSC_SAFRA
/*OK*/ ,A.DSC_VISAO_ANALISE;
  ;





