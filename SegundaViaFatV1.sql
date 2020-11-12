---==============================================================================================================
            --- Phelipe criação das tabelas
---==============================================================================================================
--DROP TABLE CMV_PS8
--CREATE TABLE CMV_PS8 COMPRESS FOR QUERY HIGH PARALLEL 32 PCTFREE 0 NOLOGGING AS        
SELECT /*PARALLEL (8) */
        TO_NUMBER(TO_CHAR(DAT_INICIO_ATENDIMENTO, 'RRRRMMDD')) AS DAT_LIG, 
        T.NUM_NTC,
        T.NUM_INTERACAO_OLTP,
        MC.DSC_METODO_CONTATO,
        M.DSC_MOTIVO_ATEND_NIVEL_2,
        M.DSC_MOTIVO_ATEND_NIVEL_3,
        M.DSC_MOTIVO_ATEND_NIVEL_4,
        M.DSC_MOTIVO_ATEND_NIVEL_5
FROM BI_FP_ASSINANTE_ATEND_FECHADO T
LEFT JOIN BI_DIM_MOTIVO_ATENDIMENTO M
     ON T.DW_MOTIVO_ATENDIMENTO = M.DW_MOTIVO_ATENDIMENTO
LEFT JOIN BI_DIM_METODO_CONTATO MC
     ON T.DW_METODO_CONTATO = MC.DW_METODO_CONTATO
               
WHERE T.DAT_INICIO_ATENDIMENTO BETWEEN '01/06/2019' AND '31/08/2019'        
      AND DSC_MOTIVO_ATEND_NIVEL_4 = '2ª Via da Conta'
      AND COD_TIPO_METODO IN('COBR', 'IPHN', 'RETE') --- ATENDIMENTO HUMANO
GROUP BY
        TO_NUMBER(TO_CHAR(DAT_INICIO_ATENDIMENTO, 'RRRRMMDD')), 
        T.NUM_NTC,
        T.NUM_INTERACAO_OLTP,
        MC.DSC_METODO_CONTATO,
        M.DSC_MOTIVO_ATEND_NIVEL_2,
        M.DSC_MOTIVO_ATEND_NIVEL_3,
        M.DSC_MOTIVO_ATEND_NIVEL_4,
        M.DSC_MOTIVO_ATEND_NIVEL_5
