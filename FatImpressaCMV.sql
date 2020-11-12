select COUNT (NUM_CONTA_CLI) from bi_dim_doc_conta where 1=1 --num_conta_cli in (933601071,122375646)
and cod_sts_atu in ('O','N')
AND IND_IMPRIME_CONTA = 8
AND ((IND_COMBO_MULTI = 'N') OR (IND_COMBO_MULTI IS NULL))
and num_conta_cli in 
                  (select num_conta_cli 
                  from bi_fp_receita_fatura_det 
                  where dat_fatura between to_date('01/09/2019','dd/mm/yyyy') and to_date('30/09/2019','dd/mm/yyyy'))


