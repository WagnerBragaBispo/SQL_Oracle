/* Sprint 29 - Card 59008 - Estruturar abertura de motivos retidos no Canal WhatsApp
   

    Tabelas:
    
    U92047747.BI_FT_RET_DIG_WHATSAPP --> OK
    INTMKT.DE_PARA_MINT --> OK

    DWH.BI_DIM_CANAL_MINT
    DWH.BI_DIM_FUNCIONALIDADE_MINT
    DWH.BI_DIM_INPUT_ASSINANTE_MINT
    DWH.BI_DIM_MENU_MINT
    DWH.BI_DIM_NOS_MINT --> OK
    DWH.BI_DIM_STATUS_MINT
    DWH.BI_DIM_TIPO_ASSINATURA_MINT
    DWH.BI_DIM_TIPO_INTERACAO_MINT
    DWH.BI_DIM_TIPO_SERVICO_MINT
    DWH.BI_FP_ATENDIMENTO_MINT --> OK
    DWH.BI_FP_NAVEGACAO_MINT
*/
WITH 
WHTSAPP_MOT_RET_P1 AS (

SELECT /*+parallel (32)*/
        B.SAFRA
        ,A.DAT_REFERENCIA
        ,A.COD_PLATAFORMA
        ,C.DW_NOS_MINT
        ,COUNT(DISTINCT(B.SK_ACESSO)) AS QT_ACESSOS_RETIDO
      --,COUNT(DISTINCT(B.NUN_NTC)) AS QT_UU
  
FROM DWH.BI_FP_ATENDIMENTO_MINT A
INNER JOIN U92047747.BI_FT_RET_DIG_WHATSAPP B ON TO_CHAR(A.NUM_NTC ||' '||TO_CHAR(A.DAT_INICIO_ATENDIMENTO, 'YYYYMMDD') ||' '||REPLACE(SUBSTR(TO_CHAR(A.DAT_INICIO_ATENDIMENTO,'dd/mm/yyyy hh24:mi:ss'),12,8),':','')) = B.SK_ACESSO
LEFT JOIN DWH.BI_FP_NAVEGACAO_MINT C  ON A.COD_ATENDIMENTO = C.COD_ATENDIMENTO AND TRUNC(A.DAT_REFERENCIA)  = TRUNC(C.DAT_REFERENCIA)

WHERE A.COD_SHORT_CODE = 9652 --- SHORT CODE WPP
  AND A.DAT_INICIO_ATENDIMENTO BETWEEN TO_DATE(20200601, 'RRRRMMDD') AND  TO_DATE(20200630, 'RRRRMMDD')
  AND B.DAT_INICIO_ATENDIMENTO BETWEEN TO_DATE(20200601, 'RRRRMMDD') AND  TO_DATE(20200630, 'RRRRMMDD')
  AND B.DSC_VISAO_ANALISE = 'RETIDO DIGITAL'
  AND B.DSC_SAFRA = 'D 0'
GROUP BY 
         B.SAFRA
        ,A.DAT_REFERENCIA
        ,A.COD_PLATAFORMA
        ,A.COD_ATENDIMENTO
        ,C.DW_NOS_MINT
)
SELECT /*+PARALLEL (32)*/ DISTINCT
    A.SAFRA
    ,A.COD_PLATAFORMA
    ,B.DSC_NOS_MINT
    ,SUM(A.QT_ACESSOS_RETIDO) AS QT_ACESSOS_RETIDO
FROM WHTSAPP_MOT_RET_P1 A
INNER JOIN DWH.BI_DIM_NOS_MINT B ON A.DW_NOS_MINT = B.DW_NOS_MINT
GROUP BY A.SAFRA
    ,A.COD_PLATAFORMA
    ,B.DSC_NOS_MINT;

