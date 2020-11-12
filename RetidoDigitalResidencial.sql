

/*Segue abaixo o exemplo de trazer quais canais de atendimentos estão cadastrados após a tag “origem”*/

-- EXEMPLO PARA EXTRAIR E VERIFICAR OS CAMPOS DO                  
SELECT /*+parallel (12)*/ DISTINCT TRIM(SUBSTR(NM_DESC_PEDIDO,INSTR(NM_DESC_PEDIDO,'|origem:')+8)) AS TESTE
FROM INN.EXT_ADMINSITE 
WHERE  TRUNC (DT_HR_SOLICITACAO) >= TO_DATE('09/10/2019','DD/MM/YYYY') AND  NM_DESC_PEDIDO LIKE '%|origem:%';

SELECT *  FROM INN.EXT_ADMINSITE 
WHERE TRIM(SUBSTR(NM_DESC_PEDIDO,INSTR(NM_DESC_PEDIDO,'|origem:')+8)) IN ('NETAPP','MINHANET','MEUTECNICO','BOT','NETAPPNOVO','MIND');

/*O Segundo exemplo é o filtro que podemos aplicar para trazer somente os canais que a Ligia gerou.

Isso é somente para validação.

Abraços*/


---==================================================================================================================
-- QUERY PARA REGISTRAR O ACESSO (ÁREA LOGADA)
-- FONTE: PHELIPE
--
---==================================================================================================================


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
