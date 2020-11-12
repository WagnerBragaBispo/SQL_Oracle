Base do Retorno Print Laser (impresso, e-mail e whatsapp): U92039197.CRITICA_FATURAMENTO

Script para consultar CRF:
SELECT 'Claro Móvel' AS NM_MARCA,
       'CMV' AS NM_SUB_MARCA,
       FT.SK_ATENDIMENTO_URA AS CALL_ID,
       FT.DT_INICIO_LIGACAO,
       FT.NR_HORA_INICIO_LIGACAO,
       FT.DT_FIM_LIGACAO,
       FT.NR_HORA_FIM_LIGACAO,
       FT.NR_DURACAO_ATENDIMENTO,
       UN.DSC_UN_NEGOCIO AS REGIONAL,
       FT.NR_URA AS NUM_DNIS,
       AR.COD_AREA AS NR_DDD,
       CLT.NUM_CPF,
       CLT.NUM_CNPJ,
       FT.CD_NUMERO_TELEFONE AS NUM_NTC,
       FT.CD_NUMERO_TELEFONE_ANI AS NUM_NTC_ANI,
       FT.CD_NAVEGACAO_CLIENTE_URA,
       PLA.DSC_PLATAFORMA AS COD_PLATAFORMA,
       APL.NM_AGRUPAMENTO_PLATAFORMA_BI AS NM_AGRUPAMENTO_PLATAFORMA,
       APL.NM_ATENDIMENTO_PLATAFORMA_BI AS NM_PLATAFORMA,
       CASE WHEN FT.FN_LIGACAO_IDENTIFICADA = 1 THEN 'Sim' ELSE 'Não' END AS FG_LIG_IDENTIFICADA,
       STS.COD_SUB_STS_ATU STATUS_CLIENTE, 
       CASE WHEN FT.FN_CORPORATIVO = 1 THEN 'Sim' ELSE 'Não' END AS FN_CORPORATIVO,
       TDU.COD_TIPO_DESCONEXAO,
       TDU.DSC_TIPO_DESCONEXAO AS TIPO_DESCONEXAO,
       CASE WHEN VA.NM_VISAO_ANALISE_BI = 'Contact Rate Direcionado ao Humano' THEN 'Direcionado Humano' ELSE 'Retido URA'
       END FC_DIRECIONADO_RETIDO,
       FT.CD_ATENDIMENTO_SCRIPT AS NR_SCRIPTPOINT,
       ATS.NM_ATENDIMENTO_SCRIPT_BI AS DS_SCRIPTPOINT,
       ATM.NM_TIPO_MOTIVO_BI AS NM_MOTIVO_URA,
       STM.NM_SUB_TIPO_MOTIVO_BI AS NM_SUB_MOTIVO_URA,
       ATL.NM_TIPO_LIGACAO_BI AS NM_EXPURGO_URA,
       CCC.DSC_CICLO AS DT_VENCIMENTO_FATURA,
       FA.NM_ATENDIMENTO_FAIXA_AGING_BI AS NM_TEMPO_BASE,
       FE.NM_ATENDIMENTO_FAIXA_ETARIA_BI AS NM_FAIXA_ETARIA,
       ATC.NM_AGRUPAMENTO_TIPO_CLIENTE AS NM_TP_CLIENTE,
       PP.DSC_PLANO_PRECO_BI AS TP_PLANO_CLIENTE,
       CASE WHEN FT.FN_DIA_UTIL = 1 THEN 'Útil' ELSE 'Não Útil' END AS FN_DIA_UTIL,
       FT.CD_RAT_PROD_PRI,
       FT.CD_RAT_PROD_SEC,
       FT.CD_RAT_MOT_PRI,
       FT.QT_LIGACAO_REAL,
       FT.QT_RAT_PROD_PRI,
       FT.QT_RAT_PROD_SEC,
       FT.QT_RAT_PROD_RES,
       FT.QT_RAT_PROD_FINAL,
       FT.QT_RAT_MOT_PRI,
       FT.QT_RAT_MOT_SEC,
       FT.QT_RAT_MOT_RES,
       FT.QT_RAT_MOT_FINAL
  FROM INTMKT.FT_ATENDIMENTO_URA FT
  LEFT JOIN DWH.BI_DIM_UN_NEGOCIO UN
    ON UN.DW_UN_NEGOCIO = FT.SK_REGIONAL
  LEFT JOIN DWH.BI_DIM_AREA AR
    ON AR.DW_AREA = FT.SK_AREA
  LEFT JOIN DWH.BI_DIM_PLATAFORMA PLA
    ON PLA.COD_PLATAFORMA = FT.SK_PLATAFORMA
  LEFT JOIN INTMKT.DS_ATENDIMENTO_PLATAFORMA APL
    ON APL.SK_ATENDIMENTO_PLATAFORMA = FT.SK_ATENDIMENTO_PLATAFORMA
  LEFT JOIN DWH.BI_DIM_STATUS STS
    ON STS.STS_DW = FT.SK_STATUS
  LEFT JOIN DWH.BI_DIM_TIPO_CLIENTE TC
    ON TC.DW_TIPO_CLIENTE = FT.SK_TIPO_CLIENTE
  LEFT JOIN DWH.BI_DIM_FAIXA_HORA FXH
    ON FXH.DW_FAIXA_HORA = FT.SK_FAIXA_HORA
  LEFT JOIN DWH.BI_DIM_MENU_URA MNU
    ON MNU.DW_MENU_URA = FT.SK_MENU_URA
  LEFT JOIN DWH.BI_DIM_AUTOMACAO_URA ARA
    ON ARA.DW_AUTOMACAO_URA = FT.SK_AUTOMACAO_URA
  LEFT JOIN DWH.BI_DIM_NAVEGACAO_URA NVU
    ON NVU.DW_NAVEGACAO_URA = FT.SK_NAVEGACAO_URA
  LEFT JOIN DWH.BI_DIM_TIPO_DESCONEXAO_URA TDU
    ON TDU.DW_TIPO_DESCONEXAO = FT.SK_TIPO_DESCONEXAO
  LEFT JOIN INTMKT.DS_ATENDIMENTO_TIPO_MOTIVO ATM
    ON ATM.SK_ATENDIMENTO_TIPO_MOTIVO = FT.SK_ATENDIMENTO_TIPO_MOTIVO
  LEFT JOIN INTMKT.DS_ATENDIMENTO_SCRIPT ATS
    ON ATS.SK_ATENDIMENTO_SCRIPT = FT.SK_ATENDIMENTO_SCRIPT
  LEFT JOIN INTMKT.DS_ATENDIMENTO_TIPO_LIGACAO ATL
    ON ATL.SK_ATENDIMENTO_TIPO_LIGACAO = FT.SK_ATENDIMENTO_TIPO_LIGACAO
  LEFT JOIN DWH.BI_DIM_CICLO CCC
    ON CCC.DW_CICLO = FT.SK_NUMERO_CICLO
  LEFT JOIN DWH.BI_DIM_PLANO_PRECO PP
    ON PP.DW_PLANO = FT.SK_PLANO
  LEFT JOIN INTMKT.DS_ATENDIMENTO_SUB_TIPO_MOTIVO STM
    ON STM.SK_ATENDIMENTO_SUB_TIPO_MOTIVO =
       FT.SK_ATENDIMENTO_SUB_TIPO_MOTIVO
  LEFT JOIN INTMKT.DS_VISAO_ANALISE VA
    ON VA.SK_VISAO_ANALISE = FT.SK_VISAO_ANALISE
  LEFT JOIN INTMKT.DS_ATENDIMENTO_FAIXA_AGING FA
    ON FA.SK_ATENDIMENTO_FAIXA_AGING = FT.SK_ATENDIMENTO_FAIXA_AGING
  LEFT JOIN INTMKT.DS_ATENDIMENTO_FAIXA_ETARIA FE
    ON FE.SK_ATENDIMENTO_FAIXA_ETARIA = FT.SK_ATENDIMENTO_FAIXA_ETARIA
  LEFT JOIN INTMKT.DS_AGRUPAMENTO_TIPO_CLIENTE ATC
    ON ATC.SK_AGRUPAMENTO_TIPO_CLIENTE = FT.SK_AGRUPAMENTO_TIPO_CLIENTE
  LEFT JOIN INTMKT.DS_CALENDARIO CAL
    ON CAL.SK_CALENDARIO = FT.SK_DATA
  LEFT JOIN INTMKT.DS_FLAG FL
    ON FL.SK_FLAG = FT.SK_FC_ASSINANTE_ZB
  LEFT JOIN DWH.BI_DIM_ASSINANTE ASS
    ON ASS.DW_NUM_NTC = FT.SK_NUMERO_TELEFONE
  LEFT JOIN DWH.BI_DIM_CLIENTE CLT
    ON CLT.DW_NUM_CLIENTE = ASS.DW_NUM_CLIENTE
WHERE FT.SK_DATA >= 20190301
   AND ATL.NM_TIPO_LIGACAO_BI = 'Ligações Válidas'
   AND ATM.NM_TIPO_MOTIVO_BI = 'Financeiro'
  AND (FT.CD_NUMERO_TELEFONE in ())

 
LEONARDO BARBOSA VILELA
UNIDADE CORPORATIVA

FIN | Faturamento
T.: 55 11 4313-4836 C.: 55 11 9 8010-9022
Leonardo.vilela@claro.com.br 

www.claro.com.br


--==========================================================================================================
--	Script pego com Leonardo do Faturamento com cruzamento da base de retorno da Print Laser (fornecedora de Fatura Claro), carregado manualmente pela equipe de faturamento 
--==========================================================================================================

--Base do Retorno Print Laser (impresso, e-mail e whatsapp): U92039197.CRITICA_FATURAMENTO

-- Descrição = 550 recusa do email ou 571 redundancia 
	

SELECT    'Claro Móvel'        AS nm_marca, 
          'CMV'                 AS nm_sub_marca, 
          ft.sk_atendimento_ura AS call_id, 
          ft.dt_inicio_ligacao, 
          ft.nr_hora_inicio_ligacao, 
          ft.dt_fim_ligacao, 
          ft.nr_hora_fim_ligacao, 
          ft.nr_duracao_atendimento, 
          un.dsc_un_negocio AS regional, 
          ft.nr_ura         AS num_dnis, 
          ar.cod_area       AS nr_ddd, 
          clt.num_cpf, 
          clt.num_cnpj, 
          ft.cd_numero_telefone     AS num_ntc, 
          ft.cd_numero_telefone_ani AS num_ntc_ani, 
          ft.cd_navegacao_cliente_ura, 
          pla.dsc_plataforma               AS cod_plataforma, 
          apl.nm_agrupamento_plataforma_bi AS nm_agrupamento_plataforma, 
          apl.nm_atendimento_plataforma_bi AS nm_plataforma, 
          CASE 
                    WHEN ft.fn_ligacao_identificada = 1 THEN 'Sim' 
                    ELSE 'Não' 
          END                 AS fg_lig_identificada, 
          sts.cod_sub_sts_atu    status_cliente, 
          CASE 
                    WHEN ft.fn_corporativo = 1 THEN 'Sim' 
                    ELSE 'Não' 
          END AS fn_corporativo, 
          tdu.cod_tipo_desconexao, 
          tdu.dsc_tipo_desconexao AS tipo_desconexao, 
          CASE 
                    WHEN va.nm_visao_analise_bi = 'Contact Rate Direcionado ao Humano' THEN 'Direcionado Humano'
                    ELSE 'Retido URA' 
          END                               fc_direcionado_retido, 
          ft.cd_atendimento_script          AS nr_scriptpoint, 
          ats.nm_atendimento_script_bi      AS ds_scriptpoint, 
          atm.nm_tipo_motivo_bi             AS nm_motivo_ura, 
          stm.nm_sub_tipo_motivo_bi         AS nm_sub_motivo_ura, 
          atl.nm_tipo_ligacao_bi            AS nm_expurgo_ura, 
          ccc.dsc_ciclo                     AS dt_vencimento_fatura, 
          fa.nm_atendimento_faixa_aging_bi  AS nm_tempo_base, 
          fe.nm_atendimento_faixa_etaria_bi AS nm_faixa_etaria, 
          atc.nm_agrupamento_tipo_cliente   AS nm_tp_cliente, 
          pp.dsc_plano_preco_bi             AS tp_plano_cliente, 
          CASE 
                    WHEN ft.fn_dia_util = 1 THEN 'Útil' 
                    ELSE 'Não Útil' 
          END AS fn_dia_util, 
          ft.cd_rat_prod_pri, 
          ft.cd_rat_prod_sec, 
          ft.cd_rat_mot_pri, 
          ft.qt_ligacao_real, 
          ft.qt_rat_prod_pri, 
          ft.qt_rat_prod_sec, 
          ft.qt_rat_prod_res, 
          ft.qt_rat_prod_final, 
          ft.qt_rat_mot_pri, 
          ft.qt_rat_mot_sec, 
          ft.qt_rat_mot_res, 
          ft.qt_rat_mot_final 
FROM      intmkt.ft_atendimento_ura FT 
LEFT JOIN dwh.bi_dim_un_negocio UN 
ON        un.dw_un_negocio = ft.sk_regional 
LEFT JOIN dwh.bi_dim_area AR 
ON        ar.dw_area = ft.sk_area 
LEFT JOIN dwh.bi_dim_plataforma PLA 
ON        pla.cod_plataforma = ft.sk_plataforma 
LEFT JOIN intmkt.ds_atendimento_plataforma APL 
ON        apl.sk_atendimento_plataforma = ft.sk_atendimento_plataforma 
LEFT JOIN dwh.bi_dim_status STS 
ON        sts.sts_dw = ft.sk_status 
LEFT JOIN dwh.bi_dim_tipo_cliente TC 
ON        tc.dw_tipo_cliente = ft.sk_tipo_cliente 
LEFT JOIN dwh.bi_dim_faixa_hora FXH 
ON        fxh.dw_faixa_hora = ft.sk_faixa_hora 
LEFT JOIN dwh.bi_dim_menu_ura MNU 
ON        mnu.dw_menu_ura = ft.sk_menu_ura 
LEFT JOIN dwh.bi_dim_automacao_ura ARA 
ON        ara.dw_automacao_ura = ft.sk_automacao_ura 
LEFT JOIN dwh.bi_dim_navegacao_ura NVU 
ON        nvu.dw_navegacao_ura = ft.sk_navegacao_ura 
LEFT JOIN dwh.bi_dim_tipo_desconexao_ura TDU 
ON        tdu.dw_tipo_desconexao = ft.sk_tipo_desconexao 
LEFT JOIN intmkt.ds_atendimento_tipo_motivo ATM 
ON        atm.sk_atendimento_tipo_motivo = ft.sk_atendimento_tipo_motivo 
LEFT JOIN intmkt.ds_atendimento_script ATS 
ON        ats.sk_atendimento_script = ft.sk_atendimento_script 
LEFT JOIN intmkt.ds_atendimento_tipo_ligacao ATL 
ON        atl.sk_atendimento_tipo_ligacao = ft.sk_atendimento_tipo_ligacao 
LEFT JOIN dwh.bi_dim_ciclo CCC 
ON        ccc.dw_ciclo = ft.sk_numero_ciclo 
LEFT JOIN dwh.bi_dim_plano_preco PP 
ON        pp.dw_plano = ft.sk_plano 
LEFT JOIN intmkt.ds_atendimento_sub_tipo_motivo STM 
ON        stm.sk_atendimento_sub_tipo_motivo = ft.sk_atendimento_sub_tipo_motivo 
LEFT JOIN intmkt.ds_visao_analise VA 
ON        va.sk_visao_analise = ft.sk_visao_analise 
LEFT JOIN intmkt.ds_atendimento_faixa_aging FA 
ON        fa.sk_atendimento_faixa_aging = ft.sk_atendimento_faixa_aging 
LEFT JOIN intmkt.ds_atendimento_faixa_etaria FE 
ON        fe.sk_atendimento_faixa_etaria = ft.sk_atendimento_faixa_etaria 
LEFT JOIN intmkt.ds_agrupamento_tipo_cliente ATC 
ON        atc.sk_agrupamento_tipo_cliente = ft.sk_agrupamento_tipo_cliente 
LEFT JOIN intmkt.ds_calendario CAL 
ON        cal.sk_calendario = ft.sk_data 
LEFT JOIN intmkt.ds_flag FL 
ON        fl.sk_flag = ft.sk_fc_assinante_zb 
LEFT JOIN dwh.bi_dim_assinante ASS 
ON        ass.dw_num_ntc = ft.sk_numero_telefone 
LEFT JOIN dwh.bi_dim_cliente CLT 
ON        clt.dw_num_cliente = ass.dw_num_cliente 
WHERE     ft.sk_data >= 20190301 
AND       atl.nm_tipo_ligacao_bi = 'Ligações Válidas' 
AND       atm.nm_tipo_motivo_bi = 'Financeiro' 
AND       ( 
                    ft.cd_numero_telefone IN ())


