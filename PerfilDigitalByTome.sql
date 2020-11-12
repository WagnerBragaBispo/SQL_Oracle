--=================================================================================================================
--          TABELAS DIPOSNIBILIZADAS PELO TOME 
            

/*SELECT * FROM U92277452.TMP_SQD_AA_CLI_DIG_CTRL_CMV;
SELECT * FROM U92277452.TMP_SQD_AA_CLI_POS_CMV;
SELECT * FROM U92277452.TMP_SQD_AA_CLI_DIG_NET;

SELECT * FROM U92277452.TMP_SQD_AA_CR_CMV*/
--=================================================================================================================



SELECT /*+PARALLEL (30)*/ 
       t.cod_plataforma
       ,t.nm_grupo
       ,t.flg_fatura_digital
       ,t.flg_debito_automatico
       ,count(distinct(t.num_ntc)) as qtd_ntc
FROM U92277452.TMP_SQD_AA_CLI_DIG_CTRL_CMV t
group by t.cod_plataforma
       ,t.nm_grupo
       ,t.flg_fatura_digital
       ,t.flg_debito_automatico
order by t.cod_plataforma
       ,t.nm_grupo
;


SELECT /*+PARALLEL (30)*/ 
       t.cod_plataforma
       ,t.nm_grupo
       ,t.flg_fatura_digital
       ,t.flg_debito_automatico
       ,count(distinct(t.num_ntc)) as qtd_ntc
FROM U92277452.TMP_SQD_AA_CLI_POS_CMV t
group by t.cod_plataforma
       ,t.nm_grupo
       ,t.flg_fatura_digital
       ,t.flg_debito_automatico
order by t.cod_plataforma
       ,t.nm_grupo
;
