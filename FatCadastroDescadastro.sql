/* --------------------------------------------------------------------------------------------------------
 QUERY - CONSULTA MCM (Minha Claro/Minha Claro APP) FATURA DIGITAL CADASTRO E DESCADASTRADO
-----------------------------------------------------------------------------------------------------------*/

SELECT /*+PARALLEL (32)*/
    SUBSTR(TO_CHAR(A.DAT_INICIO_ATENDIMENTO, 'YYYYMMDD'),1,6) AS SAFRA --ok
	,TO_CHAR(A.NUM_NTC ||' '||TO_CHAR((A.DAT_CRIACAO_DW)+1, 'YYYYMMDD')) AS SK_ACESSO
    ,TO_DATE(TO_CHAR(A.DAT_INICIO_ATENDIMENTO, 'RRRRMMDD')||' '||A.HOR_INICIO_ATENDIMENTO,'RRRRMMDD HH24MISS') AS DAT_INI_ATENDIMENTO
    ,A.DAT_CRIACAO_DW
    ,A.DW_NUM_NTC
	,A.NUM_NTC
    ,A.DW_AREA
    ,A.DW_METODO_CONTATO
    ,A.DW_MOTIVO_ATENDIMENTO
    ,B.DSC_METODO_CONTATO
  --,A.DSC_OBSERVACAO_ATENDIMENTO 
    ,CASE WHEN A.NUM_CPF IN (-3) THEN A.NUM_CNPJ ELSE A.NUM_CPF END AS NUM_CPF_CNPJ
    ,CASE WHEN A.DSC_OBSERVACAO_ATENDIMENTO LIKE '%cadastramento no Fatura Digital%' OR A.DSC_OBSERVACAO_ATENDIMENTO LIKE '%Adicionado contato no Fatura Digital%' THEN 'CADASTRADO FATURA DIGITAL'
          WHEN A.DSC_OBSERVACAO_ATENDIMENTO LIKE '%descadastramento do Fatura Digital%' THEN 'DESCADASTRADO FATURA DIGITAL'  ELSE 'OUTROS' END AS DSC_CAD_FD
    ,CASE WHEN A.DSC_OBSERVACAO_ATENDIMENTO LIKE '%cadastramento no Fatura Digital%' OR A.DSC_OBSERVACAO_ATENDIMENTO LIKE '%Adicionado contato no Fatura Digital%' THEN 1
          WHEN A.DSC_OBSERVACAO_ATENDIMENTO LIKE '%descadastramento do Fatura Digital%' THEN 0  ELSE 99 END AS FLG_CAD_FD      
    
      FROM DWH.BI_FP_ASSINANTE_ATEND_FECHADO A
 LEFT JOIN DWH.BI_DIM_METODO_CONTATO B    ON A.DW_METODO_CONTATO = B.DW_METODO_CONTATO
  WHERE A.DAT_CRIACAO_DW BETWEEN TO_DATE(20191231, 'RRRRMMDD') AND  TO_DATE(20200630, 'RRRRMMDD') 
  AND A.DW_METODO_CONTATO in (701,791)
  AND 
    (A.DSC_OBSERVACAO_ATENDIMENTO LIKE '%cadastramento no Fatura Digital%'
    OR A.DSC_OBSERVACAO_ATENDIMENTO LIKE '%descadastramento do Fatura Digital%'
    OR A.DSC_OBSERVACAO_ATENDIMENTO LIKE '%Adicionado contato no Fatura Digital%')
; 
