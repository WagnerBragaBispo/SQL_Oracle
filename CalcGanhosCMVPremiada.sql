---===========================================================================================================================
-- Ação: Minha Claro Premiada 
-- Inicio: 13/11/19
-- Fim: 13/02/2020
--objetivo: acesso

---===========================================================================================================================
 

WITH
       ACESSOS_RESIDENCIAL AS 
                                       
                                        (SELECT         /*+PARALLEL (12)*/
                                                         SK_DATA,
                                                         COUNT(*) AS QTD_ACSS_TV
                                                         
                                               FROM INN.DW_IDM_LOG A
                                                   WHERE SK_DATA >= TO_NUMBER(&DATA_INICIAL)
                                                     AND SK_DATA <= TO_NUMBER(&DATA_FINAL)
                                                     AND UPPER(CLIENT_ID) LIKE '%TV%'
                                                     GROUP BY
                                                     SK_DATA), 
                                                     
ACESSOS_MOVEL AS 
                                        (SELECT /*+ PARALLEL (32)*/

                                         TO_NUMBER(TO_CHAR(DAT_INICIO_ATENDIMENTO, 'RRRRMMDD')) SK_DATA
                                         ,COUNT(*) AS QTD_ACSS_CMV
                                  FROM DWH.BI_FP_ASSINANTE_ATEND_FECHADO A
                                         WHERE DW_METODO_CONTATO IN (81, 411, 451,681,701,552)
                                               AND DAT_INICIO_ATENDIMENTO BETWEEN TO_DATE(&DATA_INICIAL, 'RRRRMMDD') AND  TO_DATE(&DATA_FINAL, 'RRRRMMDD')
                                             -- NOVO FILTRO  -- SOMENTE USUÁRIOS QUE AUTENTICARAM -- EX: MinhaClaroWebPF: Autenticação de usuário
                                                AND UPPER(A.DSC_OBSERVACAO_ATENDIMENTO) LIKE '%MINHACLAROWEB%AUTENTICA%USU%'
                                          GROUP BY
                                              TO_NUMBER(TO_CHAR(DAT_INICIO_ATENDIMENTO, 'RRRRMMDD')))       


SELECT 
        A.SK_DATA,
        A.QTD_ACSS_TV,
        B.QTD_ACSS_CMV
FROM ACESSOS_RESIDENCIAL A
FULL OUTER JOIN ACESSOS_MOVEL B
ON A.SK_DATA = B.SK_DATA
ORDER BY SK_DATA



SELECT /*+ PARALLEL (32)*/
       TO_NUMBER(TO_CHAR(DAT_INICIO_ATENDIMENTO, 'RRRRMMDD')) SK_DATA
       ,COUNT(*) AS QTD_ACSS_CMV
FROM DWH.BI_FP_ASSINANTE_ATEND_FECHADO A
       WHERE DW_METODO_CONTATO IN (81, 411, 451,681,701,552)
             AND DAT_INICIO_ATENDIMENTO BETWEEN TO_DATE(&DATA_INICIAL, 'RRRRMMDD') AND  TO_DATE(&DATA_FINAL, 'RRRRMMDD')
             AND UPPER(A.DSC_OBSERVACAO_ATENDIMENTO) LIKE '%MINHACLAROWEB%AUTENTICA%USU%'
        GROUP BY
            TO_NUMBER(TO_CHAR(DAT_INICIO_ATENDIMENTO, 'RRRRMMDD'))



---===========================================================================================================================
-- Ação: Minha Claro Premiada                                          (fim da querie)
---===========================================================================================================================
