/*AGREDADA BI*/

--=================================================================================================
--                VISÃO INADIMPLÊNCIA POR CONTRATO               
--                        TABELA :  OPS_ALTERYX.BI_AGG_CLIENTE_CMV AG
--===================================================================================================

SELECT /*+PARALLEL (30)*/ 
       CMV.NUM_NTC
      ,CMV.cod_plataforma 
      ,AG.QTD_FATURA_DIGITAL_MV
      ,AG.FLG_INADIMPLENTE
      ,MAX(AG.QTD_DIAS_DIVIDA) AS QTD_DIAS_DIVIDA
      ,MAX(AG.DAT_ENTRADA_COBRANCA) AS DAT_ENTRADA_COBRANCA
      ,MAX(AG.DAT_ULT_COBRANCA) AS DAT_ULT_COBRANCA
      ,SUM(AG.VAL_DIVIDA) AS VL_TOTAL_DIVIDA
      ,COUNT(DISTINCT(AG.DW_NUM_CLIENTE)) AS QTDE_CLI


FROM OPS_ALTERYX.BI_AGG_CLIENTE_CMV AG

LEFT JOIN OPS_ALTERYX.BI_FP_BASE_ASSINANTE_CMV CMV
     ON CMV.DW_NUM_CLIENTE = AG.DW_NUM_CLIENTE

WHERE AG.DAT_MOVIMENTO = &DT_MOVIMENTO
      AND CMV.DAT_MOVIMENTO = &DT_MOVIMENTO 
      AND AG.QTD_PLAT_PREPG_MV = 0          ----EXCLUINDO PRÉ PAGO
      AND AG.QTD_PLAT_CNTRL_MV = 1          --- SOMENTE CONLTROLE
      AND AG.FLG_INADIMPLENTE = 1           --- SOMENTE INADIMPLENTE
GROUP BY
       CMV.NUM_NTC
      ,CMV.cod_plataforma 
      ,AG.QTD_FATURA_DIGITAL_MV
      ,AG.FLG_INADIMPLENTE
      ,AG.QTD_DIAS_DIVIDA
      ,AG.VAL_DIVIDA
      ,AG.DAT_ENTRADA_COBRANCA
      ,AG.DAT_ULT_COBRANCA
;
--=================================================================================================
--                VISÃO INADIMPLÊNCIA --- CONSOLIDADO               
--                        TABELA :  OPS_ALTERYX.BI_AGG_CLIENTE_CMV AG
--                          PRODUTO :  APENAS CONTROLE
--===================================================================================================

SELECT /*+PARALLEL (30)*/ 
      AG.DAT_MOVIMENTO
      ,CMV.cod_plataforma 
      ,AG.QTD_FATURA_DIGITAL_MV
      ,AVG(AG.QTD_DIAS_DIVIDA) AS QTD_DIAS_DIVIDA
      ,MAX(AG.QTD_DIAS_DIVIDA) AS QTD_DIAS_DIVIDA_MAX
      ,MIN(AG.QTD_DIAS_DIVIDA) AS QTD_DIAS_DIVIDA_MIN
      ,(SUM(AG.VAL_DIVIDA)/ COUNT(DISTINCT(AG.DW_NUM_CLIENTE))) AS VL_DIVIDA_MEDIA
      ,COUNT(DISTINCT(AG.DW_NUM_CLIENTE)) AS QTDE_CLI


FROM OPS_ALTERYX.BI_AGG_CLIENTE_CMV AG

LEFT JOIN OPS_ALTERYX.BI_FP_BASE_ASSINANTE_CMV CMV
     ON CMV.DW_NUM_CLIENTE = AG.DW_NUM_CLIENTE

WHERE AG.DAT_MOVIMENTO = &DT_MOVIMENTO
      AND CMV.DAT_MOVIMENTO = &DT_MOVIMENTO 
      AND AG.QTD_PLAT_PREPG_MV = 0          ----EXCLUINDO PRÉ PAGO
      AND AG.QTD_PLAT_CNTRL_MV = 1          --- SOMENTE CONLTROLE
      AND AG.FLG_INADIMPLENTE = 1           --- SOMENTE INADIMPLENTE
GROUP BY
       AG.DAT_MOVIMENTO
      ,CMV.cod_plataforma 
      ,AG.QTD_FATURA_DIGITAL_MV
;
