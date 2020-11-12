

--=================================================================================================
--filtros
-- COD_PLATAFORMA = 'AUTOC' --- CLARO CONTROLE
-- CR.FC_DIRECIONADO_RETIDO = 'Direcionado Humano' E 'Retido URA'
-- NM_EXPURGO_URA = 'Ligações Válidas'

cr.NM_MOTIVO_URA
select * from U92277452.FT_SQD_AA_CR_AUTOC; -- BASE CRIADA PELO TOMÉ
--===================================================================================================



select /*+PARALLEL (30)*/  
--    CLI.NUM_CPF
--    ,CMV.NUM_NTC
--    ,FAT.NUM_CONTA_CLI
   CR.NM_MOTIVO_URA
   ,CR.NM_AGRUPAMENTO_PLATAFORMA 
   ,FAT.FLG_FATURA_DIGITAL
   ,FAT.FLG_DEBITO_AUTOMATICO
   ,SUM(CR.QT_LIGACAO_REAL) AS QT_LIGACAO_REAL
   ,SUM(CR.QT_LIGACOES_RATEIO) AS QT_LIGACOES_RATEIO                   --- total de ligações com rateio validação com IN

FROM DWH.BI_DIM_CLIENTE CLI 
LEFT JOIN OPS_ALTERYX.BI_FP_BASE_ASSINANTE_CMV CMV
     ON CMV.DW_NUM_CLIENTE = CLI.DW_NUM_CLIENTE
      
LEFT JOIN OPS_ALTERYX.BI_FP_FATURA_CMV FAT
     ON FAT.DW_NUM_CLIENTE = CMV.DW_NUM_CLIENTE
    AND FAT.DAT_MOVIMENTO =  CMV.DAT_MOVIMENTO

LEFT JOIN U92277452.FT_SQD_AA_CR_04 CR               --- U92277452.FT_SQD_AA_CR_AUTOC
     ON CR.NUM_NTC = CMV.NUM_NTC
          
     where CMV.DAT_MOVIMENTO = TO_DATE('01/02/2019','DD/MM/YYYY') --- base inicial do mês
       --AND CMV.COD_PLATAFORMA = 'AUTOC'                           --- base Claro Controle
       AND FAT.DAT_MOVIMENTO = TO_DATE('01/02/2019','DD/MM/YYYY') --- base inicial do mês
       AND CR.DT_INICIO_LIGACAO BETWEEN 20190201 AND 20190228     --- período de ligações
       AND CR.FC_DIRECIONADO_RETIDO = 'Direcionado Humano'
GROUP BY 
   CR.NM_MOTIVO_URA
   ,CR.NM_AGRUPAMENTO_PLATAFORMA 
   ,FAT.FLG_FATURA_DIGITAL
   ,FAT.FLG_DEBITO_AUTOMATICO
;
-- tempo 759,631 seconds base Jul/19 
-- tempo xxx seconds base Ago/19


--==================================================================================================================

SELECT /*+ PARALLEL (32)*/ 
     dig.nm_grupo
     ,dig.num_ntc
     ,sum(case when CR.NUM_CPF is null  
                then 0 else CR.QT_LIGACAO_REAL end) QT_LIGACAO_REAL
     ,count(dig.num_ntc) as qt_ntc_dig 
     /*,sum(case when CR.DT_INICIO_LIGACAO BETWEEN 20190801 AND 20190831 
               then CR.QT_LIGACAO_REAL else 0 end) QT_LIG_REAL_201908
     ,sum(case when CR.DT_INICIO_LIGACAO between 20190801 and 20190831 
               then CR.QT_LIGACOES_RATEIO else 0 end) QT_LIG_RATEIO_201908*/



FROM U92277452.TMP_SQD_AA_CLI_DIG_CTRL_CMV dig

left join DWH.BI_DIM_CLIENTE CLI
     on cli.DW_NUM_CLIENTE = dig.DW_NUM_CLIENTE

LEFT JOIN U92277452.FT_SQD_AA_CR_04 CR               --- U92277452.FT_SQD_AA_CR_AUTOC
     ON cli.NUM_CPF = CR.NUM_CPF
          
GROUP BY 
   dig.nm_grupo
     ,dig.num_ntc
order by dig.nm_grupo
;

--==================================================================================================================
SELECT nm_grupo 
       ,cod_operadora 
       ,num_contrato 
       ,num_cpf_cnpj 
       ,cod_tipo_pessoa 
       ,fg_pme 
       ,fg_pj
       ,fg_pf
       
  FROM u92277452.TMP_SQD_AA_CLI_DIG_NET;
  
SELECT * FROM U92277452.FT_SQD_AA_CR_04 CR;


select /*+PARALLEL (30)*/  
   CR.NM_MOTIVO_URA
   ,CR.NM_AGRUPAMENTO_PLATAFORMA 
   ,DIG.NM_GRUPO
   ,COUNT(TO_CHAR(DIG.NUM_CPF_CNPJ,'00000000000')) AS QT_NTC
   ,SUM(CR.QT_LIGACAO_REAL) AS QT_LIGACAO_REAL
   ,SUM(CR.QT_LIGACOES_RATEIO) AS QT_LIGACOES_RATEIO                   --- total de ligações com rateio validação com IN

FROM U92277452.FT_SQD_AA_CR_04 CR               --- U92277452.FT_SQD_AA_CR_AUTOC
LEFT JOIN U92277452.TMP_SQD_AA_CLI_DIG_CTRL_CMV DIG
          
     --ON TO_CHAR(CR.NUM_NTC,'00000000000') = TO_CHAR(DIG.NUM_NTC,'00000000000')
     ON TO_CHAR(CR.NUM_CPF,'00000000000') = TO_CHAR(DIG.NUM_CPF_CNPJ,'00000000000')
          
     where 
       DIG.NUM_CPF_CNPJ <> -3
     AND CR.DT_INICIO_LIGACAO BETWEEN 20190801 AND 20190831     --- período de ligações
     
       
GROUP BY 
  CR.NM_MOTIVO_URA
   ,CR.NM_AGRUPAMENTO_PLATAFORMA 
   ,DIG.NM_GRUPO
 ORDER BY CR.NM_AGRUPAMENTO_PLATAFORMA DESC
          ,DIG.NM_GRUPO ASC
;
--==================================================================================================================
select /*+PARALLEL (30)*/  
   CR.NM_MOTIVO_URA
   ,CR.NM_AGRUPAMENTO_PLATAFORMA 
   ,DIG.NM_GRUPO
   ,COUNT(TO_CHAR(DIG.NUM_CPF_CNPJ,'00000000000')) AS QT_NTC
   ,SUM(CR.QT_LIGACAO_REAL) AS QT_LIGACAO_REAL
   ,SUM(CR.QT_LIGACOES_RATEIO) AS QT_LIGACOES_RATEIO                   --- total de ligações com rateio validação com IN

FROM U92277452.FT_SQD_AA_CR_04 CR               --- U92277452.FT_SQD_AA_CR_AUTOC
LEFT JOIN U92277452.TMP_SQD_AA_CLI_POS_CMV DIG
          
     --ON TO_CHAR(CR.NUM_NTC,'00000000000') = TO_CHAR(DIG.NUM_NTC,'00000000000')
     ON TO_CHAR(CR.NUM_CPF,'00000000000') = TO_CHAR(DIG.NUM_CPF_CNPJ,'00000000000')
          
     where 
       DIG.NUM_CPF_CNPJ <> -3
     AND CR.DT_INICIO_LIGACAO BETWEEN 20190801 AND 20190831     --- período de ligações
     
       
GROUP BY 
  CR.NM_MOTIVO_URA
   ,CR.NM_AGRUPAMENTO_PLATAFORMA 
   ,DIG.NM_GRUPO
 ORDER BY CR.NM_AGRUPAMENTO_PLATAFORMA DESC
          ,DIG.NM_GRUPO ASC
;
--==================================================================================================================
select * from all_tables  table_name like '%TMP_SQD_AA%';
