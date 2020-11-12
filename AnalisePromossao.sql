---=======================================================================================================
select * from BI_FP_ASS_PROMOCAO_PRE 
select * from BI_DIM_CANAL_PROMOCAO_PRE 
select * from BI_DIM_PROMOCAO_PRE
select * from BI_DIM_PLATAFORMA
select * from BI_DIM_UN_NEGOCIO
SELECT * FROM BI_DIM_AREA
SELECT * FROM BI_DIM_SUB_TIPO_OPER_PROM    
select * from U92277452.TMP_SQD_AA_CR_CMV
---=======================================================================================================

select  
       FP.DAT_MOVIMENTO
       ,FP.NUM_NTC
       ,FP.DAT_ATIVACAO_LINHA_ASS
       ,FP.DAT_EFETIVACAO_PROMOCAO
--       ,FP.HOR_EFETIVACAO_PROMOCAO
       ,DM.DSC_PROMOCAO_PRE
--       ,DM.DW_PROMOCAO_PAI
       ,CN.DSC_CANAL_PROMOCAO_PRE
       ,PS8.DAT_LIG
       ,PS8.DSC_OBSERVACAO_ATENDIMENTO
from BI_FP_ASS_PROMOCAO_PRE FP 
LEFT JOIN BI_DIM_PROMOCAO_PRE DM
     ON FP.DW_PROMOCAO_PRE = DM.DW_PROMOCAO_PRE
LEFT JOIN BI_DIM_CANAL_PROMOCAO_PRE CN
     ON FP.DW_CANAL_PROMOCAO_PRE = CN.DW_CANAL_PROMOCAO_PRE
LEFT JOIN U92277452.TMP_SQD_AA_CR_CMV PS8
     ON FP.NUM_NTC = PS8.NUM_NTC

WHERE /*FP.NUM_NTC IN (86994473301, 81994986974)*/
     FP.NUM_NTC  in (select NUM_NTC from U92277452.TMP_SQD_AA_CR_CMV group by NUM_NTC )
GROUP BY  FP.DAT_MOVIMENTO
       ,FP.NUM_NTC
       ,FP.DAT_ATIVACAO_LINHA_ASS
       ,FP.DAT_EFETIVACAO_PROMOCAO
--       ,FP.HOR_EFETIVACAO_PROMOCAO
       ,DM.DSC_PROMOCAO_PRE
--       ,DM.DW_PROMOCAO_PAI
       ,CN.DSC_CANAL_PROMOCAO_PRE
       ,PS8.DAT_LIG
       ,PS8.DSC_OBSERVACAO_ATENDIMENTO
       
ORDER BY FP.NUM_NTC ,FP.DAT_EFETIVACAO_PROMOCAO ASC;


---=======================================================================================================
--                                              Qtde de promoção no WPP
---=======================================================================================================

select  
       --FP.DAT_MOVIMENTO
       CN.DSC_CANAL_PROMOCAO_PRE
--       ,TO_CHAR(DAT_EFETIVACAO_PROMOCAO,'YYYYMM') AS SAFRA_EFETIVACAO_PROMOCAO
       ,DM.DSC_PROMOCAO_PRE
       ,count(distinct(FP.NUM_NTC)) as QTDE_NTC

from BI_FP_ASS_PROMOCAO_PRE FP 
LEFT JOIN BI_DIM_PROMOCAO_PRE DM
     ON FP.DW_PROMOCAO_PRE = DM.DW_PROMOCAO_PRE
LEFT JOIN BI_DIM_CANAL_PROMOCAO_PRE CN
     ON FP.DW_CANAL_PROMOCAO_PRE = CN.DW_CANAL_PROMOCAO_PRE
--LEFT JOIN U92277452.TMP_SQD_AA_CR_CMV PS8                         ---tabela dropada pelo Tomé
  --   ON FP.NUM_NTC = PS8.NUM_NTC

WHERE FP.DAT_EFETIVACAO_PROMOCAO BETWEEN TO_DATE(&DT_INICIO,'YYYYMMDD') AND TO_DATE(&DT_FIM,'YYYYMMDD')
--      AND CN.DSC_CANAL_PROMOCAO_PRE = 'WPP'
--     FP.NUM_NTC  in (select NUM_NTC from U92277452.TMP_SQD_AA_CR_CMV group by NUM_NTC )
GROUP BY  --FP.DAT_MOVIMENTO
      CN.DSC_CANAL_PROMOCAO_PRE
          ,DM.DSC_PROMOCAO_PRE
       
ORDER BY CN.DSC_CANAL_PROMOCAO_PRE,
DM.DSC_PROMOCAO_PRE ASC;

