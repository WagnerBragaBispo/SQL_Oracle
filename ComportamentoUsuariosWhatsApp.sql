/*
  SPRINT 28 - Abertura da origem dos acessos ao WhatsApp
  OBJETIVO: A ORIGEM DO ACESSO E PARA ONDE ESTÃO MIGRANDO
  
Importante respondermos algumas perguntas:
•  Clientes que acessam whatsapp acessam também outros canais? Na mesma proporção? Por qual motivo? ---- separar ura/ath
•  Clientes que acessam whatsapp antes tinham o comportamento de ligação e agora usam o canal? Deixaram de ligar por causa do whatsapp? Reduzimos quanto do CR? --- taxa de contato
•  Clientes que não interagiam, agora acessa whatsapp? Se sim, qual o custo disso?
•  Clientes que não ligavam, agora acessam o whatsapp? Se sim, qual o custo evitado?
•	 Retenção digital do canal whatsapp (móvel/net/tv) de jan a atual. 


NESTOR: 
Caros,
Segue agenda para combinarmos como seguir com estudo de ganhos com Whatsapp.
Fiz algumas anotações da reunião com o Celso.
Onde podemos enxergar o quanto está reduzindo custo na central ?
Quem usa whatsapp vai pro SAC depois? Qual retenção do canal? Comportamento entre net/claro móvel (pre/pos/controle) e DTH
Qual % de resolução no Whatsapp?
Volume de clientes que buscavam o sac e agora migraram para o whatsapp?
Volume de clientes que não buscavam o sac e buscaram o whatsapp (custo evitado de call center)?

Volume de usuários originados com os disparos ativos e comportamento em relação ao central?


*/

/*---------------------------------------------------------------------------------------------------------------------------------------------------
  ANALISE ABAIXO É SOBRE:
  Clientes que acessam whatsapp acessam também outros canais? Na mesma proporção? Por qual motivo?
---------------------------------------------------------------------------------------------------------------------------------------------------*/



--ETAPA 1 ACESSO WHATSAPP_P1
DROP TABLE TMP_SQDAA_ACESSO_WHATSAPP_P1;
CREATE TABLE TMP_SQDAA_ACESSO_WHATSAPP_P1 COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS
SELECT DISTINCT /*+parallel (32)*/ ------------------------------------------------------------------------ETAPA 1 ACESSO WPP
        A.DAT_REFERENCIA
        ,SUBSTR(TO_CHAR(A.DAT_INICIO_ATENDIMENTO, 'YYYYMMDD'),1,6) AS SAFRA --ok
        ,TO_CHAR(A.DAT_INICIO_ATENDIMENTO, 'YYYYMMDD') AS SK_DATA --ok
        ,A.COD_PLATAFORMA --ok
        ,A.NUM_NTC --ok
        ,A.DAT_INICIO_ATENDIMENTO AS DT_INI_ATENDIMENTO
        ,TRUNC(A.DAT_INICIO_ATENDIMENTO) AS DAT_INICIO_ATENDIMENTO --ok
        ,TO_CHAR(A.NUM_NTC ||' '||TO_CHAR(A.DAT_INICIO_ATENDIMENTO, 'YYYYMMDD') ||' '||REPLACE(SUBSTR(TO_CHAR(A.DAT_INICIO_ATENDIMENTO,'dd/mm/yyyy hh24:mi:ss'),12,8),':','')) AS SK_ACESSO 
        ,A.COD_SHORT_CODE
        ,'WhatsApp' AS DSC_METODO_CONTATO       
        ,SYSDATE                       AS DAT_CRIACAO
FROM DWH.BI_FP_ATENDIMENTO_MINT A
LEFT JOIN DWH.BI_DIM_CANAL_MINT B ON A.DW_NUM_CANAL_MINT = B.DW_NUM_CANAL_MINT
WHERE A.COD_SHORT_CODE = 9652 --- SHORT CODE WPP
AND A.DAT_INICIO_ATENDIMENTO BETWEEN TO_DATE(&DATA_INICIAL, 'RRRRMMDD') AND  TO_DATE(&DATA_FINAL, 'RRRRMMDD')
AND A.COD_PLATAFORMA NOT IN ('-2','-3') --> SOMENTE CLIENTES MÓVEIS

GROUP BY  A.DAT_REFERENCIA
        ,SUBSTR(TO_CHAR(A.DAT_INICIO_ATENDIMENTO, 'YYYYMMDD'),1,6)
        ,TO_CHAR(A.DAT_INICIO_ATENDIMENTO, 'YYYYMMDD')
        ,A.COD_PLATAFORMA
        ,A.NUM_NTC
        ,A.DAT_INICIO_ATENDIMENTO
    ,A.COD_SHORT_CODE
;

--ETAPA 2 ACESSO USSD
DROP TABLE TMP_SQDAA_ACESSO_WHATSAPP_P2;
CREATE TABLE TMP_SQDAA_ACESSO_WHATSAPP_P2 COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS
SELECT DISTINCT /*+parallel (32)*/ ------------------------------------------------------------------------ETAPA 1 ACESSO USSD
        A.DAT_REFERENCIA
        ,SUBSTR(TO_CHAR(A.DAT_INICIO_ATENDIMENTO, 'YYYYMMDD'),1,6) AS SAFRA --ok
        ,TO_CHAR(A.DAT_INICIO_ATENDIMENTO, 'YYYYMMDD') AS SK_DATA --ok
        ,A.COD_PLATAFORMA --ok
        ,A.NUM_NTC --ok
        ,A.DAT_INICIO_ATENDIMENTO AS DT_INI_ATENDIMENTO
        ,A.COD_SHORT_CODE
        ,A.COD_ATENDIMENTO
        ,A.DW_NUM_CANAL_MINT
        ,A.NUM_PROTOCOLO
        ,TRUNC(A.DAT_INICIO_ATENDIMENTO) AS DAT_INICIO_ATENDIMENTO --ok
        ,TO_CHAR(A.NUM_NTC ||' '||TO_CHAR(A.DAT_INICIO_ATENDIMENTO, 'YYYYMMDD') ||' '||REPLACE(SUBSTR(TO_CHAR(A.DAT_INICIO_ATENDIMENTO,'dd/mm/yyyy hh24:mi:ss'),12,8),':','')) AS SK_ACESSO 
        ,'USSD' AS DSC_METODO_CONTATO
        ,CASE WHEN TO_CHAR(A.NUM_NTC ||' '||TO_CHAR(A.DAT_INICIO_ATENDIMENTO, 'YYYYMMDD') ||' '||REPLACE(SUBSTR(TO_CHAR(A.DAT_INICIO_ATENDIMENTO,'dd/mm/yyyy hh24:mi:ss'),12,8),':','')) 
                   < B.SK_ACESSO THEN 1 ELSE 0 END AS FLG_ACESSOU_ANTES           
        ,SYSDATE                       AS DAT_CRIACAO
 
FROM DWH.BI_FP_ATENDIMENTO_MINT A
INNER JOIN TMP_SQDAA_ACESSO_WHATSAPP_P1 B
           ON A.NUM_NTC = B.NUM_NTC
           
WHERE A.DAT_INICIO_ATENDIMENTO BETWEEN TO_DATE(&DATA_INICIAL, 'RRRRMMDD') AND  TO_DATE(&DATA_FINAL, 'RRRRMMDD')
AND A.COD_SHORT_CODE = 1052 --- SHORT CODE USSD
AND A.COD_PLATAFORMA NOT IN ('-2','-3') -->SOMENTE CLIENTES CLARO
AND A.DW_NUM_CANAL_MINT = 2 --> CANAL USSD VER NA TABELA VER TABELA "DWH.BI_DIM_CANAL_MINT"
AND A.NUM_PROTOCOLO IS NOT NULL --> SOMENTE ACESSOS QUE POSSUEM PROTOCOLOS

GROUP BY  A.DAT_REFERENCIA
        ,SUBSTR(TO_CHAR(A.DAT_INICIO_ATENDIMENTO, 'YYYYMMDD'),1,6)
        ,TO_CHAR(A.DAT_INICIO_ATENDIMENTO, 'YYYYMMDD')
        ,A.COD_PLATAFORMA
        ,A.NUM_NTC
        ,A.DAT_INICIO_ATENDIMENTO
        ,A.COD_SHORT_CODE
        ,A.COD_ATENDIMENTO
        ,A.DW_NUM_CANAL_MINT 
        ,A.NUM_PROTOCOLO
        ,CASE WHEN TO_CHAR(A.NUM_NTC ||' '||TO_CHAR(A.DAT_INICIO_ATENDIMENTO, 'YYYYMMDD') ||' '||REPLACE(SUBSTR(TO_CHAR(A.DAT_INICIO_ATENDIMENTO,'dd/mm/yyyy hh24:mi:ss'),12,8),':','')) 
                   < B.SK_ACESSO THEN 1 ELSE 0 END     
;



-- ETAPA 3 ACESSO MCM
DROP TABLE TMP_SQDAA_ACESSO_WHATSAPP_P3;
CREATE TABLE TMP_SQDAA_ACESSO_WHATSAPP_P3 COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS
SELECT  /*+ PARALLEL (32)*/
       LAST_DAY(MC.DAT_INICIO_ATENDIMENTO) AS DAT_REFERENCIA 
       ,TO_NUMBER(TO_CHAR(MC.DAT_INICIO_ATENDIMENTO, 'RRRRMMDD')) SK_DATA
       ,TO_DATE(TO_CHAR(MC.DAT_INICIO_ATENDIMENTO, 'RRRRMMDD')||' '||MC.HOR_INICIO_ATENDIMENTO,'RRRRMMDD HH24MISS') AS DAT_INI_ATENDIMENTO
       ,TO_DATE(TO_CHAR(MC.DAT_FIM_ATENDIMENTO, 'RRRRMMDD')||' '||MC.HOR_FIM_ATENDIMENTO,'RRRRMMDD HH24MISS') AS DAT_FIM_ATENDIMENTO
       ,MC.NUM_NTC
       ,MC.DW_METODO_CONTATO
     --  ,DM.DSC_METODO_CONTATO
       ,TO_CHAR(MC.NUM_NTC ||' '||TO_CHAR(MC.DAT_INICIO_ATENDIMENTO, 'RRRRMMDD')||' '||MC.HOR_INICIO_ATENDIMENTO) AS SK_ACESSO
       ,'MCM' AS DSC_METODO_CONTATO
       ,CASE WHEN TO_CHAR(MC.NUM_NTC ||' '||TO_DATE(TO_CHAR(MC.DAT_INICIO_ATENDIMENTO, 'RRRRMMDD')||' '||MC.HOR_INICIO_ATENDIMENTO,'RRRRMMDD HH24MISS')) <
             AC.SK_ACESSO THEN 1 ELSE 0 END AS FLG_ACESSOU_ANTES             
       ,SYSDATE AS DAT_CRIACAO
       

      FROM DWH.BI_FP_ASSINANTE_ATEND_FECHADO MC 
INNER JOIN TMP_SQDAA_ACESSO_WHATSAPP_P1 AC
           ON MC.NUM_NTC = AC.NUM_NTC
           
LEFT JOIN BI_DIM_MOTIVO_ATENDIMENTO M
          ON MC.DW_MOTIVO_ATENDIMENTO = M.DW_MOTIVO_ATENDIMENTO
LEFT JOIN BI_DIM_METODO_CONTATO DM
          ON MC.DW_METODO_CONTATO = DM.DW_METODO_CONTATO
WHERE MC.DW_METODO_CONTATO IN (81,411,451,701,709,791)     -------EXPURDO 681 App Flex 552 Minha Claro Empresa    
  
      AND UPPER(MC.DSC_OBSERVACAO_ATENDIMENTO) LIKE '%MINHACLAROWEB%AUTENTICA%USU%'
      AND MC.DAT_INICIO_ATENDIMENTO BETWEEN TO_DATE(&DATA_INICIAL, 'RRRRMMDD') AND  TO_DATE(&DATA_FINAL, 'RRRRMMDD')
     -- AND MC.DAT_INICIO_ATENDIMENTO BETWEEN TO_DATE(20200301, 'RRRRMMDD') AND  TO_DATE(20200331, 'RRRRMMDD')
 ;
  
--ETAPA 4 LIGACOES CR
DROP TABLE TMP_SQDAA_ACESSO_WHATSAPP_P4;
CREATE TABLE TMP_SQDAA_ACESSO_WHATSAPP_P4 COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS
SELECT /*+ PARALLEL (32)*/ ------------------------------------------------------------------------ETAPA 2 LIGACOES CR
       LAST_DAY(TO_DATE(FT.DT_INICIO_LIGACAO,'YYYY/MM/DD')) AS DAT_REFERENCIA       
       ,FT.DT_INICIO_LIGACAO SK_DATA       
       ,'CMV' AS NM_MARCA
       ,FT.SK_ATENDIMENTO_URA AS COD_CHAMADA -- CALL_ID
       ,TO_CHAR(FT.CD_NUMERO_TELEFONE ||' '||FT.DT_INICIO_LIGACAO ||' '||FT.NR_HORA_INICIO_LIGACAO) AS SK_LIGA
       ,TO_DATE(TO_DATE(FT.DT_INICIO_LIGACAO,'YYYY/MM/DD') ||' ' ||FT.NR_HORA_INICIO_LIGACAO,'DD/MM/RRRR HH24:MI:SS') AS DT_INI_LIGACAO
       ,TO_DATE(TO_DATE(FT.DT_FIM_LIGACAO,'YYYY/MM/DD') ||' ' ||FT.NR_HORA_FIM_LIGACAO,'DD/MM/RRRR HH24:MI:SS') AS DT_FIM_LIGACAO
       ,FT.CD_NUMERO_TELEFONE                           AS NUM_NTC
       ,'CallCenter' AS DSC_METODO_CONTATO 
       ,CASE WHEN TO_CHAR(FT.CD_NUMERO_TELEFONE ||' '||FT.DT_INICIO_LIGACAO ||' '||FT.NR_HORA_INICIO_LIGACAO) <
             AC.SK_ACESSO THEN 1 ELSE 0 END AS FLG_ACESSOU_ANTES
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
         WHEN UPPER(VA.NM_VISAO_ANALISE_BI) = 'CONTACT RATE DIRECIONADO AO HUMANO' THEN
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
INNER JOIN TMP_SQDAA_ACESSO_WHATSAPP_P1 AC ON AC.NUM_NTC = FT.CD_NUMERO_TELEFONE ------> BASE ACESSO DO WHATSAPP
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

-- ETAPA 5 AGRUPAR AS TABELAS 
DROP TABLE TMP_SQDAA_ACESSO_WHATSAPP_P5;
CREATE TABLE TMP_SQDAA_ACESSO_WHATSAPP_P5 COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS
SELECT DISTINCT NUM_NTC ,DSC_METODO_CONTATO FROM TMP_SQDAA_ACESSO_WHATSAPP_P2
UNION ALL 
SELECT DISTINCT NUM_NTC ,DSC_METODO_CONTATO FROM TMP_SQDAA_ACESSO_WHATSAPP_P3
UNION ALL 
SELECT DISTINCT NUM_NTC ,DSC_METODO_CONTATO FROM TMP_SQDAA_ACESSO_WHATSAPP_P4
;


-- ETAPA 6 FT -ANALISE SOMENTE ACESSA
DROP TABLE BI_FT_WHATSAPP_COMPORT_ACESSO;
CREATE TABLE BI_FT_WHATSAPP_COMPORT_ACESSO COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS
SELECT  /*+ PARALLEL (32)*/
        AC.SAFRA
        ,'CMV' AS NM_MARCA
        ,'WHATSAPP' AS DSC_METODO_CONTATO
        ,AC.COD_SHORT_CODE
        ,AC.COD_PLATAFORMA
        ,AC.NUM_NTC
        ,COUNT(DISTINCT(AC.SK_ACESSO)) AS QT_ACESSO
        ,CASE WHEN CN.NUM_NTC IS NULL THEN 0 ELSE 1 END AS FLG_ACESSOU_CANAIS       
        ,CASE WHEN CN.NUM_NTC IS NULL THEN 'SOMENTE ACESSOU WHASTAPP' ELSE 'ACESSOU OUTROS CANAIS' END AS DSC_ACESSOU_CANAIS
        ,CASE WHEN CN.DSC_METODO_CONTATO = 'CallCenter' THEN 1 ELSE 0 END AS FLG_LIGOU  
        ,CASE WHEN CN.DSC_METODO_CONTATO = 'MCM'  THEN 1 ELSE 0 END AS FLG_MCM
        ,CASE WHEN CN.DSC_METODO_CONTATO = 'USSD'  THEN 1 ELSE 0 END AS FLG_USSD      
        /*,CASE WHEN CN.DSC_METODO_CONTATO IN('CallCenter','MCM','USSD') THEN 1 ELSE 0 END AS DSC_ANALISE     */
     FROM TMP_SQDAA_ACESSO_WHATSAPP_P1 AC --> BASE DE ACESSO WHATSAPP
LEFT JOIN TMP_SQDAA_ACESSO_WHATSAPP_P5 CN ON CN.NUM_NTC = AC.NUM_NTC --> agregado TODOS os CANAIS = USSD + MCM + CALL CENTER

GROUP BY AC.SAFRA
        ,AC.COD_PLATAFORMA
        ,AC.NUM_NTC
       ,AC.COD_SHORT_CODE
       ,AC.COD_PLATAFORMA
       ,CASE WHEN CN.NUM_NTC IS NULL THEN 0 ELSE 1 END       
       ,CASE WHEN CN.NUM_NTC IS NULL THEN 'SOMENTE ACESSOU WHASTAPP' ELSE 'ACESSOU OUTROS CANAIS' END 
       ,CASE WHEN CN.DSC_METODO_CONTATO = 'CallCenter' THEN 1 ELSE 0 END  
       ,CASE WHEN CN.DSC_METODO_CONTATO = 'MCM'  THEN 1 ELSE 0 END
       ,CASE WHEN CN.DSC_METODO_CONTATO = 'USSD'  THEN 1 ELSE 0 END
      /* ,CASE WHEN CN.DSC_METODO_CONTATO IN('CallCenter','MCM','USSD') THEN 1 ELSE 0 END    */
; 

-----------------------------------------------------> RESUMO 1
SELECT  
      AC.SAFRA
      ,AC.COD_PLATAFORMA
      ,AC.COD_SHORT_CODE
      ,AC.DSC_METODO_CONTATO
      ,AC.COD_PLATAFORMA
      ,COUNT(NUM_NTC) AS QT_USU_UNICOS
      ,SUM(QT_ACESSO) AS QT_ACESSO
      ,AC.DSC_ACESSOU_CANAIS
      ,SUM(AC.FLG_ACESSOU_CANAIS) AS QT_FLG_ACESSOU_CANAIS
      ,SUM(AC.FLG_LIGOU) AS QT_LIGOU
      ,SUM(AC.FLG_MCM) AS QT_MCM
      ,SUM(AC.FLG_USSD) AS QT_USSD
--select *    
FROM BI_FT_WHATSAPP_COMPORT_ACESSO AC

GROUP BY AC.SAFRA
      ,AC.COD_PLATAFORMA
      ,AC.COD_SHORT_CODE
      ,AC.COD_PLATAFORMA
      ,AC.DSC_METODO_CONTATO
      ,AC.DSC_ACESSOU_CANAIS
;
-----------------------------------------------------> RESUMO 2 DIRECIONADO/ RETIDO
WITH
PRI_LIGACAO_P1 AS 
(
SELECT 
            LG1.NUM_NTC
            ,MIN(LG1.SK_LIGA) AS SK_LIGA
FROM TMP_SQDAA_ACESSO_WHATSAPP_P4 LG1
GROUP BY LG1.NUM_NTC
),
PRI_LIGACAO_P2 AS 
(
SELECT DISTINCT
            LG1.NUM_NTC
            ,MIN(LG2.DSC_DIRECIONADO_RETIDO) AS DSC_DIRECIONADO_RETIDO
FROM PRI_LIGACAO_P1 LG1
LEFT JOIN TMP_SQDAA_ACESSO_WHATSAPP_P4 LG2 ON LG1.SK_LIGA = LG2.SK_LIGA
GROUP BY LG1.NUM_NTC
)

SELECT  
      AC.SAFRA
      ,AC.COD_PLATAFORMA
      ,AC.COD_SHORT_CODE
      ,AC.DSC_METODO_CONTATO
      ,AC.COD_PLATAFORMA
      ,COUNT(AC.NUM_NTC) AS QT_USU_UNICOS
      ,SUM(AC.QT_ACESSO) AS QT_ACESSO
      ,AC.DSC_ACESSOU_CANAIS
      ,LG.DSC_DIRECIONADO_RETIDO
      ,SUM(AC.FLG_LIGOU) AS QT_LIGOU

--select *    
     FROM BI_FT_WHATSAPP_COMPORT_ACESSO AC
LEFT JOIN PRI_LIGACAO_P2 LG ON LG.NUM_NTC = AC.NUM_NTC
WHERE AC.FLG_LIGOU = 1

GROUP BY AC.SAFRA
      ,AC.COD_PLATAFORMA
      ,AC.COD_SHORT_CODE
      ,AC.DSC_METODO_CONTATO
      ,AC.COD_PLATAFORMA
      ,AC.DSC_ACESSOU_CANAIS
      ,LG.DSC_DIRECIONADO_RETIDO   
;
--AC.NUM_NTC = 11930071948 --> CLIENTE ACESSOU WHATSAPP EM ABR/2020 E LIGOU


/*--====================================================================================================================
  ANALISE ABAIXO É SOBRE:
  Clientes que acessam whatsapp antes tinham o comportamento de ligação e agora usam o canal? Deixaram de ligar por causa do whatsapp? Reduzimos quanto do CR?
--====================================================================================================================*/

--ETAPA 8 ACESSO WHATSAPP_UNICOS
  DROP TABLE TMP_SQDAA_WHATSAPP_UU_P1;
CREATE TABLE TMP_SQDAA_WHATSAPP_UU_P1 COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS
SELECT DISTINCT /*+parallel (32)*/ ------------------------------------------------------------------------ETAPA 1 DATA REFERENCIA
        MAX(A.DAT_INICIO_ATENDIMENTO) AS DT_REFERENCIA
        ,SUBSTR(TO_CHAR(MAX(A.DAT_INICIO_ATENDIMENTO), 'YYYYMMDD'),1,6) AS SAFRA --ok
        
FROM DWH.BI_FP_ATENDIMENTO_MINT A
LEFT JOIN DWH.BI_DIM_CANAL_MINT B ON A.DW_NUM_CANAL_MINT = B.DW_NUM_CANAL_MINT
WHERE A.COD_SHORT_CODE = 9652 --- SHORT CODE WPP
AND A.DAT_INICIO_ATENDIMENTO BETWEEN TO_DATE(&DATA_INICIAL, 'RRRRMMDD') AND  TO_DATE(&DATA_FINAL, 'RRRRMMDD')
AND A.COD_PLATAFORMA NOT IN ('-2','-3')



--ETAPA 9 ACESSO WHATSAPP_UNICOS
DROP TABLE TMP_SQDAA_WHATSAPP_UU_P2;
CREATE TABLE TMP_SQDAA_WHATSAPP_UU_P2 COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS
SELECT DISTINCT /*+parallel (32)*/ ------------------------------------------------------------------------ETAPA 2 PRIMEIRO ACESSO
        C.DT_REFERENCIA
        ,A.COD_PLATAFORMA --ok
        ,A.NUM_NTC --ok
        ,MIN(A.DAT_INICIO_ATENDIMENTO) AS DT_PRI_ATENDIMENTO
        ,A.COD_SHORT_CODE
        ,'WhatsApp' AS DSC_METODO_CONTATO       
        ,SYSDATE                       AS DAT_CRIACAO
FROM DWH.BI_FP_ATENDIMENTO_MINT A
LEFT JOIN DWH.BI_DIM_CANAL_MINT B ON A.DW_NUM_CANAL_MINT = B.DW_NUM_CANAL_MINT
LEFT JOIN TMP_SQDAA_WHATSAPP_UU_P1 C ON 202005 = C.SAFRA
WHERE A.COD_SHORT_CODE = 9652 --- SHORT CODE WPP
AND A.DAT_INICIO_ATENDIMENTO BETWEEN TO_DATE(&DATA_INICIAL, 'RRRRMMDD') AND  TO_DATE(&DATA_FINAL, 'RRRRMMDD')
AND A.COD_PLATAFORMA NOT IN ('-2','-3') --> SOMENTE CLIENTES MÓVEIS

GROUP BY C.DT_REFERENCIA
         ,A.COD_PLATAFORMA
         ,A.NUM_NTC --ok
         ,A.COD_SHORT_CODE
;
--ETAPA 10 LIGACOES CR
DROP TABLE TMP_SQDAA_WHATSAPP_UU_P3;
CREATE TABLE TMP_SQDAA_WHATSAPP_UU_P3 COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS
SELECT /*+ PARALLEL (32)*/ ------------------------------------------------------------------------ETAPA 3 LIGACOES CR
       LAST_DAY(TO_DATE(FT.DT_INICIO_LIGACAO,'YYYY/MM/DD')) AS DAT_REFERENCIA       
       ,SUBSTR(FT.DT_INICIO_LIGACAO,1,6) AS SAFRA
       ,FT.DT_INICIO_LIGACAO SK_DATA       
       ,'CMV' AS NM_MARCA
       ,FT.SK_ATENDIMENTO_URA AS COD_CHAMADA -- CALL_ID
       ,TO_CHAR(FT.CD_NUMERO_TELEFONE ||' '||FT.DT_INICIO_LIGACAO ||' '||FT.NR_HORA_INICIO_LIGACAO) AS SK_LIGA
       ,TO_DATE(TO_DATE(FT.DT_INICIO_LIGACAO,'YYYY/MM/DD') ||' ' ||FT.NR_HORA_INICIO_LIGACAO,'DD/MM/RRRR HH24:MI:SS') AS DT_INI_LIGACAO
       ,TO_DATE(TO_DATE(FT.DT_FIM_LIGACAO,'YYYY/MM/DD') ||' ' ||FT.NR_HORA_FIM_LIGACAO,'DD/MM/RRRR HH24:MI:SS') AS DT_FIM_LIGACAO
       ,FT.CD_NUMERO_TELEFONE                           AS NUM_NTC
       ,'CallCenter' AS DSC_METODO_CONTATO 
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
         WHEN UPPER(VA.NM_VISAO_ANALISE_BI) = 'CONTACT RATE DIRECIONADO AO HUMANO' THEN
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
INNER JOIN TMP_SQDAA_WHATSAPP_UU_P2 AC ON AC.NUM_NTC = FT.CD_NUMERO_TELEFONE ------> BASE ACESSO DO WHATSAPP
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

WHERE FT.SK_DATA BETWEEN 20190101 AND 20201231
;

--=========================TOTAL DE CLIENTES COM OU SEM INTERAÇÃO NO ATH

WITH 
INTERACAO1 AS 
(
SELECT * FROM TMP_SQDAA_WHATSAPP_UU_P3 LG WHERE LG.DSC_DIRECIONADO_RETIDO = 'DIRECIONADO HUMANO' AND LG.DSC_EXPURGO_URA = 'Ligações Válidas'
),

INTERACAO2 AS
(
SELECT /*+ PARALLEL (32)*/ ------------------------------------------------------------------------SEM INTERAÇÃO OU COM INTERAÇÃO
      AC.DT_REFERENCIA
      ,AC.COD_PLATAFORMA
      ,AC.COD_SHORT_CODE
      ,AC.DSC_METODO_CONTATO
      ,CASE WHEN LG.NUM_NTC IS NULL THEN 0 ELSE 1 END AS FLG_INTERACAO
      ,CASE WHEN LG.NUM_NTC IS NULL THEN 'SEM LIGACAO ATH' ELSE 'COM LIGACAO ATH' END AS DSC_INTERACAO
      ,COUNT(DISTINCT(AC.NUM_NTC)) AS QT_USUARIOS
     
FROM TMP_SQDAA_WHATSAPP_UU_P2 AC
LEFT JOIN INTERACAO1 LG ON LG.NUM_NTC = AC.NUM_NTC 

GROUP BY AC.DT_REFERENCIA
      ,AC.COD_PLATAFORMA
      ,AC.COD_SHORT_CODE
      ,AC.DSC_METODO_CONTATO
      ,LG.DSC_DIRECIONADO_RETIDO
      ,CASE WHEN LG.NUM_NTC IS NULL THEN 0 ELSE 1 END
      ,CASE WHEN LG.NUM_NTC IS NULL THEN 'SEM LIGACAO ATH' ELSE 'COM LIGACAO ATH' END
)
SELECT * FROM INTERACAO2;



--=========================ANALISE ANTES E DEPOIS DO ACESSO (INICIO)
WITH 
LIGACAO_ANT_DEP_1 AS 
(
SELECT /*+ PARALLEL (32)*/ ------------------------------------------------------------------------ETAPA 1 SEPARANDO SOMENTE ATH
* 
FROM TMP_SQDAA_WHATSAPP_UU_P3 LG ---> BASE DE CLIENTES DO WHATSAPP QUE FAZEM LIGAÇÕES DOS CLIENTES QUE UTILIZAM 
WHERE LG.DSC_DIRECIONADO_RETIDO = 'DIRECIONADO HUMANO' AND LG.DSC_EXPURGO_URA = 'Ligações Válidas'
),

LIGACAO_ANT_DEP_2 AS 
(
SELECT  /*+ PARALLEL (32)*/ ------------------------------------------------------------------------ETAPA 2 SEPARANDO COM/SEM INTERAÇÃO
      AC.DT_REFERENCIA
      ,AC.COD_PLATAFORMA ---> EXPURGO PQ TEM USUÁRIOS QUE MIGROU DE PLATAFORMA
      ,AC.NUM_NTC
      ,MIN(AC.DT_PRI_ATENDIMENTO) AS DT_PRI_ATENDIMENTO
      ,CASE WHEN LG.NUM_NTC IS NULL THEN 0 ELSE 1 END AS FLG_INTERACAO
      ,CASE WHEN LG.NUM_NTC IS NULL THEN 'SEM LIGACAO ATH' ELSE 'COM LIGACAO ATH' END AS DSC_INTERACAO
           
     FROM TMP_SQDAA_WHATSAPP_UU_P2 AC --> BASE DE ACESSO
LEFT JOIN LIGACAO_ANT_DEP_1 LG ON LG.NUM_NTC = AC.NUM_NTC 

GROUP BY AC.DT_REFERENCIA
      ,AC.COD_PLATAFORMA
      ,AC.NUM_NTC
      ,CASE WHEN LG.NUM_NTC IS NULL THEN 0 ELSE 1 END
      ,CASE WHEN LG.NUM_NTC IS NULL THEN 'SEM LIGACAO ATH' ELSE 'COM LIGACAO ATH' END 
ORDER BY AC.NUM_NTC
),

/*
SELECT AC.COD_PLATAFORMA ,COUNT(DISTINCT(AC.NUM_NTC)) QT_USUARIOS  FROM LIGACAO_ANT_DEP_2 AC WHERE AC.FLG_INTERACAO = 1 GROUP BY AC.COD_PLATAFORMA
--3.846.221 ATÉ 26/05 TOTAL DE USUÁRIOS USUÁRIOS  
-- SOMENTE COM LIGAÇÃO
*/
LIGACAO_ANT_DEP_3 AS 
(
SELECT  /*+ PARALLEL (32)*/ ------------------------------------------------------------------------EATPA 3 
     AC.NUM_NTC
     ,AC.DSC_INTERACAO
     ,AC.COD_PLATAFORMA
     ,LG.SAFRA
     ,CASE WHEN TRUNC(LG.DT_INI_LIGACAO) <= TRUNC(AC.DT_PRI_ATENDIMENTO) THEN 1 ELSE 0 END AS FLG_PERFIL_LIGACAO
     ,CASE WHEN TRUNC(LG.DT_INI_LIGACAO) <= TRUNC(AC.DT_PRI_ATENDIMENTO) THEN 'LIGOU ANTES' ELSE 'LIGOU DEPOIS' END AS DSC_PERFIL_LIGACAO
     ,COUNT(LG.DT_INI_LIGACAO) AS QT_LIGACAO
       
     FROM LIGACAO_ANT_DEP_2 AC
LEFT JOIN LIGACAO_ANT_DEP_1 LG ON AC.NUM_NTC = LG.NUM_NTC
     WHERE AC.FLG_INTERACAO = 1 -- SOMENTE COM LIGACAÇÃO
GROUP BY AC.NUM_NTC
      ,AC.DSC_INTERACAO
      ,AC.COD_PLATAFORMA
      ,LG.SAFRA
      ,CASE WHEN TRUNC(LG.DT_INI_LIGACAO) <= TRUNC(AC.DT_PRI_ATENDIMENTO) THEN 1 ELSE 0 END
      ,CASE WHEN TRUNC(LG.DT_INI_LIGACAO) <= TRUNC(AC.DT_PRI_ATENDIMENTO) THEN 'LIGOU ANTES' ELSE 'LIGOU DEPOIS' END
ORDER BY AC.NUM_NTC
)
/*
SELECT 
      DSC_PERFIL_LIGACAO
      ,COD_PLATAFORMA
      ,SAFRA
      ,COUNT(DISTINCT(NUM_NTC)) AS QT_USUARIOS
      ,SUM(QT_LIGACAO) AS QT_LIGACAO
      
FROM LIGACAO_ANT_DEP_3
GROUP BY DSC_PERFIL_LIGACAO
      ,COD_PLATAFORMA
      ,SAFRA
     */
SELECT /*DSC_PERFIL_LIGACAO 
,-*/COD_PLATAFORMA  ,COUNT(DISTINCT(NUM_NTC)) AS QT_USUARIOS 
FROM LIGACAO_ANT_DEP_3 
GROUP BY /*DSC_PERFIL_LIGACAO ,*/COD_PLATAFORMA;


--=========================ANALISE ANTES E DEPOIS DO ACESSO (F I M) 


SELECT /*+ PARALLEL (32)*/
      AC.DT_REFERENCIA
      ,AC.COD_SHORT_CODE
      ,AC.DSC_METODO_CONTATO
      ,CASE WHEN LG.NUM_NTC IS NULL THEN 0 ELSE 1 END AS FLG_ACESSA_LIGA
      ,CASE WHEN LG.NUM_NTC IS NULL THEN 'SOMENTE ACESSA' ELSE 'ACESSA E LIGA' END AS DSC_ACESSA_LIGA
      ,COUNT(DISTINCT(AC.NUM_NTC)) AS QT_USUARIOS
     
FROM TMP_SQDAA_WHATSAPP_UU_P2 AC
LEFT JOIN TMP_SQDAA_WHATSAPP_UU_P3 LG ON LG.NUM_NTC = AC.NUM_NTC AND TRUNC(AC.DT_PRI_ATENDIMENTO) < TRUNC(LG.DT_INI_LIGACAO)
GROUP BY AC.DT_REFERENCIA
      ,AC.COD_SHORT_CODE
      ,AC.DSC_METODO_CONTATO
      ,CASE WHEN LG.NUM_NTC IS NULL THEN 0 ELSE 1 END
      ,CASE WHEN LG.NUM_NTC IS NULL THEN 'SOMENTE ACESSA' ELSE 'ACESSA E LIGA' END
     
;
 SELECT /*+ PARALLEL (32)*/
      AC.DT_REFERENCIA
      ,AC.COD_SHORT_CODE
      ,AC.DSC_METODO_CONTATO
      ,CASE WHEN LG.NUM_NTC IS NULL THEN 0 ELSE 1 END AS FLG_ACESSA_LIGA
      ,CASE WHEN LG.NUM_NTC IS NULL THEN 'SOMENTE ACESSA' ELSE 'ACESSA E LIGA' END AS DSC_ACESSA_LIGA
      ,COUNT(DISTINCT(AC.NUM_NTC)) AS QT_USUARIOS
     
FROM TMP_SQDAA_WHATSAPP_UU_P2 AC
LEFT JOIN TMP_SQDAA_WHATSAPP_UU_P3 LG ON LG.NUM_NTC = AC.NUM_NTC AND TRUNC(AC.DT_PRI_ATENDIMENTO) >= TRUNC(LG.DT_INI_LIGACAO)
GROUP BY AC.DT_REFERENCIA
      ,AC.COD_SHORT_CODE
      ,AC.DSC_METODO_CONTATO
      ,CASE WHEN LG.NUM_NTC IS NULL THEN 0 ELSE 1 END
      ,CASE WHEN LG.NUM_NTC IS NULL THEN 'SOMENTE ACESSA' ELSE 'ACESSA E LIGA' END
     
;   
;





 