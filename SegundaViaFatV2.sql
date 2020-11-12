---==============================================================================================================
            --- Phelipe criação das tabelas
---==============================================================================================================
DROP TABLE CMV_PS8
CREATE TABLE CMV_PS8 COMPRESS FOR QUERY HIGH PARALLEL 32 PCTFREE 0 NOLOGGING AS        
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
      AND COD_TIPO_METODO IN('COBR', 'IPHN', 'RETE')
GROUP BY
        TO_NUMBER(TO_CHAR(DAT_INICIO_ATENDIMENTO, 'RRRRMMDD')), 
        T.NUM_NTC,
        T.NUM_INTERACAO_OLTP,
        MC.DSC_METODO_CONTATO,
        M.DSC_MOTIVO_ATEND_NIVEL_2,
        M.DSC_MOTIVO_ATEND_NIVEL_3,
        M.DSC_MOTIVO_ATEND_NIVEL_4,
        M.DSC_MOTIVO_ATEND_NIVEL_5
SELECT * FROM CMV_PS8
--------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------
DROP TABLE CMV_ASS_JUL_19
CREATE TABLE CMV_ASS_JUL_19 COMPRESS FOR QUERY HIGH PARALLEL 32 PCTFREE 0 NOLOGGING AS
SELECT
NUM_NTC
FROM OPS_ALTERYX.BI_FP_BASE_ASSINANTE_CMV
WHERE COD_PLATAFORMA = 'AUTOC'
AND DAT_MOVIMENTO = '01/08/2019'
SELECT * FROM CMV_ASS_JUL_19
--------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------
CREATE TABLE CMV_2VIA_CONTROE COMPRESS FOR QUERY HIGH PARALLEL 32 PCTFREE 0 NOLOGGING AS
SELECT 
       P.DAT_LIG,
       P.NUM_NTC,
       P.NUM_INTERACAO_OLTP,
       P.DSC_METODO_CONTATO,
       P.DSC_MOTIVO_ATEND_NIVEL_2,
       P.DSC_MOTIVO_ATEND_NIVEL_3,
       P.DSC_MOTIVO_ATEND_NIVEL_4,
       P.DSC_MOTIVO_ATEND_NIVEL_5
FROM CMV_PS8 P
LEFT JOIN CMV_ASS_JUL_19 A
ON A.NUM_NTC = P.NUM_NTC

SELECT COUNT(*) FROM CMV_2VIA_CONTROE
GRANT SELECT ON CMV_2VIA_CONTROE TO PUBLIC

select * from all_all_tables where table_name like '%CMV_2VIA_CONTROE%';

select * from U92277452.CMV_2VIA_CONTROE;



---==============================================================================================================
            --- Cross com as tabelas PS8 (Agregada)
---==============================================================================================================


select /*+PARALLEL (30)*/
       ps8.DAT_LIG
       ,count(ps8.num_ntc) as qtde_ntc
    from U92277452.CMV_2VIA_CONTROE ps8
group by ps8.DAT_LIG order by ps8.DAT_LIG desc;

/*Soma de QTDE_NTC	
mês	Total
06	 265.074 
07	 277.114 
08	 246.996*/ 

/*select unique DAT_MOVIMENTO from OPS_ALTERYX.BI_FP_BASE_ASSINANTE_CMV CMV;

SELECT 
       PS8.*
       ,CLI.DW_NUM_CLIENTE
FROM U92277452.CMV_2VIA_CONTROE A
LEFT JOIN OPS_ALTERYX.BI_FP_BASE_ASSINANTE_CMV cmv
     PS8.NUM_NTC = CMV.NUM_NTC
WHERE PS8.DAT_LIG BETWEEN 20190601 AND 20190630
;*/
--PS8 JUN/2019 - 265.074 QTDE_NTC



select /*+PARALLEL (30)*/  

   FAT.DAT_MOVIMENTO
   ,FAT.FLG_FATURA_DIGITAL
   ,FAT.FLG_DEBITO_AUTOMATICO
   ,PS8.DSC_MOTIVO_ATEND_NIVEL_4
   ,COUNT(PS8.NUM_NTC) AS QTDE_NTC_PS8
  

FROM DWH.BI_DIM_CLIENTE CLI 
LEFT JOIN OPS_ALTERYX.BI_FP_BASE_ASSINANTE_CMV CMV
     ON CMV.DW_NUM_CLIENTE = CLI.DW_NUM_CLIENTE
       
LEFT JOIN OPS_ALTERYX.BI_FP_FATURA_CMV FAT
     ON FAT.DW_NUM_CLIENTE = CMV.DW_NUM_CLIENTE
    AND FAT.DAT_MOVIMENTO =  CMV.DAT_MOVIMENTO

LEFT JOIN U92277452.CMV_2VIA_CONTROE PS8
     ON PS8.NUM_NTC = CMV.NUM_NTC

     where 
     /*  CMV.COD_PLATAFORMA = 'AUTOC'
       AND CMV.COD_STATUS = 'A'
       AND*/ FAT.DAT_MOVIMENTO = '01/06/2019'
       AND CMV.DAT_MOVIMENTO = '01/06/2019'
       AND PS8.DAT_LIG BETWEEN 20190601 AND 20190630
        
 
GROUP BY   FAT.DAT_MOVIMENTO
          ,FAT.FLG_FATURA_DIGITAL
          ,FAT.FLG_DEBITO_AUTOMATICO
          ,PS8.DSC_MOTIVO_ATEND_NIVEL_4
;

---==============================================================================================================
            --- Cross com as tabelas PS8 (analítico)
---==============================================================================================================


select /*+PARALLEL (30)*/  

   FAT.DAT_MOVIMENTO
   ,FAT.FLG_FATURA_DIGITAL
   ,FAT.FLG_DEBITO_AUTOMATICO
   ,PS8.DAT_LIG
   ,PS8.NUM_NTC
   ,PS8.NUM_INTERACAO_OLTP AS NUM_ATENDIMENTO
   ,PS8.DSC_MOTIVO_ATEND_NIVEL_4

FROM DWH.BI_DIM_CLIENTE CLI 
LEFT JOIN OPS_ALTERYX.BI_FP_BASE_ASSINANTE_CMV CMV
     ON CMV.DW_NUM_CLIENTE = CLI.DW_NUM_CLIENTE
       
LEFT JOIN OPS_ALTERYX.BI_FP_FATURA_CMV FAT
     ON FAT.DW_NUM_CLIENTE = CMV.DW_NUM_CLIENTE
    AND FAT.DAT_MOVIMENTO =  CMV.DAT_MOVIMENTO

LEFT JOIN U92277452.CMV_2VIA_CONTROE PS8
     ON PS8.NUM_NTC = CMV.NUM_NTC

     where 
     /*  CMV.COD_PLATAFORMA = 'AUTOC'
       AND CMV.COD_STATUS = 'A'
       AND*/ FAT.DAT_MOVIMENTO = '01/06/2019'
       AND CMV.DAT_MOVIMENTO = '01/06/2019'
       AND PS8.DAT_LIG BETWEEN 20190601 AND 20190630
        
 
GROUP BY   FAT.DAT_MOVIMENTO
   ,FAT.FLG_FATURA_DIGITAL
   ,FAT.FLG_DEBITO_AUTOMATICO
   ,PS8.DAT_LIG
   ,PS8.NUM_NTC
   ,PS8.NUM_INTERACAO_OLTP
   ,PS8.DSC_MOTIVO_ATEND_NIVEL_4
ORDER BY PS8.NUM_NTC ,PS8.DSC_MOTIVO_ATEND_NIVEL_4 ASC
;



---==============================================================================================================
            --- Cross com as tabelas PS8 ( PRINT LASER )

--Base do Retorno Print Laser (impresso, e-mail e whatsapp): U92039197.CRITICA_FATURAMENTO
-- Descrição = 550 recusa do email ou 571 redundancia 
---==============================================================================================================

SELECT 
        CMV.NUM_NTC 
       ,CLI.NUM_CPF
       ,TO_CHAR(TO_DATE(PRINT.DATA,'YYYYMMDD'),'YYYYMM') 
       ,PRINT.*
 
FROM DWH.BI_DIM_CLIENTE CLI 

LEFT JOIN OPS_ALTERYX.BI_FP_BASE_ASSINANTE_CMV CMV
     ON CMV.DW_NUM_CLIENTE = CLI.DW_NUM_CLIENTE

LEFT JOIN U92039197.CRITICA_FATURAMENTO PRINT
     ON CLI.NUM_CPF = PRINT.CPF

WHERE CMV.DAT_MOVIMENTO = TO_DATE('01/07/2019','DD/MM/YYYY')
      AND PRINT.DATA BETWEEN TO_DATE('01/07/2019','DD/MM/YYYY') AND TO_DATE('31/07/2019','DD/MM/YYYY')
      AND CMV.NUM_NTC IN (SELECT PS8.NUM_NTC QTDE_ATEND FROM U92277452.CMV_2VIA_CONTROE PS8 
                          WHERE PS8.DAT_LIG BETWEEN 20190701 AND 20190701)
;

---AGREGADA
select /*+PARALLEL (30)*/  

   FAT.DAT_MOVIMENTO
   ,FAT.FLG_FATURA_DIGITAL
   ,FAT.FLG_DEBITO_AUTOMATICO
   ,PS8.DSC_MOTIVO_ATEND_NIVEL_4
   ,COUNT(PS8.NUM_NTC) AS QTDE_NTC_PS8
   ,COUNT(PS8.NUM_INTERACAO_OLTP) AS QTDE_ATEND_PS8
  

FROM DWH.BI_DIM_CLIENTE CLI 
LEFT JOIN OPS_ALTERYX.BI_FP_BASE_ASSINANTE_CMV CMV
     ON CMV.DW_NUM_CLIENTE = CLI.DW_NUM_CLIENTE
       
LEFT JOIN OPS_ALTERYX.BI_FP_FATURA_CMV FAT
     ON FAT.DW_NUM_CLIENTE = CMV.DW_NUM_CLIENTE
    AND FAT.DAT_MOVIMENTO =  CMV.DAT_MOVIMENTO

LEFT JOIN U92277452.CMV_2VIA_CONTROE PS8
     ON PS8.NUM_NTC = CMV.NUM_NTC

LEFT JOIN U92039197.CRITICA_FATURAMENTO PRINT
     ON CLI.NUM_CPF = PRINT.CPF

     where 
       CMV.COD_PLATAFORMA = 'AUTOC'
       AND CMV.COD_STATUS = 'A'
       AND FAT.DAT_MOVIMENTO = '01/06/2019'
       AND CMV.DAT_MOVIMENTO = '01/06/2019'
       AND PS8.DAT_LIG BETWEEN 20190601 AND 20190630
       AND PRINT.DATA BETWEEN TO_DATE(20190601 AND 20190630 
 
GROUP BY   FAT.DAT_MOVIMENTO
          ,FAT.FLG_FATURA_DIGITAL
          ,FAT.FLG_DEBITO_AUTOMATICO
          ,PS8.DSC_MOTIVO_ATEND_NIVEL_4
;


