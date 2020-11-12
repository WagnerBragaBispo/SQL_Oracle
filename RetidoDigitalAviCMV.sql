


/*---===========================================================================================================================


Base de contact rate


---===========================================================================================================================*/

DROP TABLE TMP_SQDAA_CR_CMV;
CREATE TABLE TMP_SQDAA_CR_CMV COMPRESS FOR QUERY HIGH PARALLEL 256 NOLOGGING AS
SELECT /*+ PARALLEL (32)*/ ------------------------------------------------------------------------ETAPA 2 LIGACOES CR
       LAST_DAY(TO_DATE(FT.DT_INICIO_LIGACAO,'YYYY/MM/DD')) AS DAT_REFERENCIA       
       ,FT.DT_INICIO_LIGACAO SK_DATA       
       ,'CMV' AS NM_MARCA
       ,FT.SK_ATENDIMENTO_URA AS COD_CHAMADA -- CALL_ID
       ,TO_CHAR(FT.CD_NUMERO_TELEFONE ||' '||FT.DT_INICIO_LIGACAO ||' '||FT.NR_HORA_INICIO_LIGACAO) AS SK_LIGA
       ,TO_DATE(TO_DATE(FT.DT_INICIO_LIGACAO,'YYYY/MM/DD') ||' ' ||FT.NR_HORA_INICIO_LIGACAO,'DD/MM/RRRR HH24:MI:SS') AS DT_INI_LIGACAO
       ,TO_DATE(TO_DATE(FT.DT_FIM_LIGACAO,'YYYY/MM/DD') ||' ' ||FT.NR_HORA_FIM_LIGACAO,'DD/MM/RRRR HH24:MI:SS') AS DT_FIM_LIGACAO
       ,FT.CD_NUMERO_TELEFONE                           AS NUM_NTC
       ,CASE
         WHEN FT.FN_LIGACAO_IDENTIFICADA = 1 THEN
          'SIM'
         ELSE
          'NAO'
       END                                AS FLG_LIG_IDENTIFICADA
       ,STS.COD_SUB_STS_ATU DSC_STS_CLIENTE
       ,CASE
         WHEN FT.FN_CORPORATIVO = 1 THEN
          'SIM'
         ELSE
          'NAO'
       END                               AS FLG_CORPORATIVO
       ,CASE
         WHEN VA.NM_VISAO_ANALISE_BI = 'CONTACT RATE DIRECIONADO AO HUMANO' THEN
          'DIRECIONADO HUMANO'
         ELSE
          'RETIDO URA'
       END                           AS DSC_DIRECIONADO_RETIDO
       ,ATM.NM_TIPO_MOTIVO_BI         AS DSC_MOTIVO_URA
       ,STM.NM_SUB_TIPO_MOTIVO_BI     AS DSC_SUB_MOTIVO_URA
       ,ATL.NM_TIPO_LIGACAO_BI        AS DSC_EXPURGO_URA
       ,PP.DSC_PLANO_PRECO_BI         AS DSC_PLANO_CLIENTE
       ,SYSDATE                       AS DAT_CRIACAO

  FROM INTMKT.FT_ATENDIMENTO_URA FT -- OK
  LEFT JOIN DWH.BI_DIM_STATUS STS -- OK
    ON STS.STS_DW = FT.SK_STATUS
  LEFT JOIN INTMKT.DS_ATENDIMENTO_TIPO_MOTIVO ATM -- OK
    ON ATM.SK_ATENDIMENTO_TIPO_MOTIVO = FT.SK_ATENDIMENTO_TIPO_MOTIVO

  LEFT JOIN INTMKT.DS_ATENDIMENTO_TIPO_LIGACAO ATL -- OK
    ON ATL.SK_ATENDIMENTO_TIPO_LIGACAO = FT.SK_ATENDIMENTO_TIPO_LIGACAO
  LEFT JOIN DWH.BI_DIM_PLANO_PRECO PP -- OK
    ON PP.DW_PLANO = FT.SK_PLANO
  LEFT JOIN INTMKT.DS_ATENDIMENTO_SUB_TIPO_MOTIVO STM -- OK
   ON STM.SK_ATENDIMENTO_SUB_TIPO_MOTIVO =
       FT.SK_ATENDIMENTO_SUB_TIPO_MOTIVO
  LEFT JOIN INTMKT.DS_VISAO_ANALISE VA -- OK
    ON VA.SK_VISAO_ANALISE = FT.SK_VISAO_ANALISE
  LEFT JOIN intmkt.ds_atendimento_tipo_ligacao ATL 
ON        atl.sk_atendimento_tipo_ligacao = ft.sk_atendimento_tipo_ligacao 

WHERE FT.SK_DATA BETWEEN &DATA_INICIAL AND &DATA_FINAL
---AND       atl.nm_tipo_ligacao_bi = 'Ligações Válidas' -- FILTRO OK (SÃO CLIENTES QUE NÃO TIVERAM ABANDONO)
---AND va.nm_visao_analise_bi = 'Contact Rate Direcionado ao Humano' -- FILTRO OK
;


/*--=====================================================================================================================
			TRATATIVA DO SAS
--=======================================================================================================================*/

LIBNAME P00DW1 ORACLE PATH=P00DW1_SAS SCHEMA=DWH USER=U92047747 PASSWORD="Wagner*202008";

proc sql;     connect to oracle (USER="U92047747" PASSWORD="Wagner*202008" path = "P00DW1_SAS");        
	create table WORK.TMP_SQDAA_CR_CMV AS SELECT * from connection to oracle         
		(
			SELECT  * FROM U92047747.TMP_SQDAA_CR_CMV
 
		);run;



DATA TMP_SQDAA_CR_CMV_P1;
		SET WORK.TMP_SQDAA_CR_CMV;

HORA = COMPRESS(PUT(HOUR(DT_INI_LIGACAO),Z2.)||PUT(minute(DT_INI_LIGACAO),Z2.)||PUT(second(DT_INI_LIGACAO),Z2.));

LAST_DAY = PUT(datepart(DAT_REFERENCIA),yymmddn8.)*1;

SK_LIGA0  = COMPRESS(NUM_NTC) ||' '|| COMPRESS(SK_DATA)   ||' '|| COMPRESS(HORA);
SK_LIGA1  = COMPRESS(NUM_NTC) ||' '|| COMPRESS(SK_DATA+1) ||' '|| COMPRESS(HORA);
SK_LIGA2  = COMPRESS(NUM_NTC) ||' '|| COMPRESS(SK_DATA+2) ||' '|| COMPRESS(HORA);
SK_LIGA3  = COMPRESS(NUM_NTC) ||' '|| COMPRESS(SK_DATA+3) ||' '|| COMPRESS(HORA);
SK_LIGA7  = COMPRESS(NUM_NTC) ||' '|| COMPRESS(SK_DATA+7) ||' '|| COMPRESS(HORA);
SK_LIGA30 = COMPRESS(NUM_NTC) ||' '|| COMPRESS(LAST_DAY)  ||' '|| '235959';

RUN;
/*--=====================================================================================================================
			ACESSOS	--> ETAPA 2
									MOVEL
--=======================================================================================================================*/

DATA TMP_ACESSO_AVI_MOVEL_202007_P1;
SET WAGNER.TMP_ACESSO_AVI_MOVEL_202007;

SK_DATA = SUBSTR(COMPRESS(CREATEDAT,"-"),1,8)*1;
SAFRA = SUBSTR(COMPRESS(CREATEDAT,"-"),1,6)*1;
SK_DATA_DT = input(put(SK_DATA,8.),yymmdd8.);
format SK_DATA_DT yymmddn8.;
DT_REFERENCIA = PUT(intnx('month',SK_DATA_DT,1)-1,yymmddn8.)*1;

HORA = COMPRESS(SUBSTR(COMPRESS(CREATEDAT,":"),12,6));


SK_ACESSO0 = FONE ||' '|| COMPRESS(SK_DATA) 	||' '|| COMPRESS(HORA);
SK_ACESSO1 = FONE ||' '|| COMPRESS(SK_DATA+1) ||' '|| COMPRESS(HORA);
SK_ACESSO2 = FONE ||' '|| COMPRESS(SK_DATA+2) ||' '|| COMPRESS(HORA);
SK_ACESSO3 = FONE ||' '|| COMPRESS(SK_DATA+3) ||' '|| COMPRESS(HORA);
SK_ACESSO7 = FONE ||' '|| COMPRESS(SK_DATA+7) ||' '|| COMPRESS(HORA);
SK_ACESSO30 = FONE||' '|| COMPRESS(DT_REFERENCIA) ||' '|| '235959';

RUN;

O que precisa ser feito: Recriar o indicador para Móvel e Residencial dos clientes, trazendo a base ativa considerando as seguintes divisão:

Clientes que ligam e acessam;
Clientes que só acessa;
Clientes que só liga;
Clientes que não interagiram no mês;
Dos clientes que acessam, trazer um subDvisão considerando quais canais os clientes acessaram (USSD, MCM, MCR, NovoAPP, WhatsApp, ChatBot);
+ detalhes: Precisamo analisar como será a quebra dos clientes Combo Multi que acessam somente o Residencial em alguns canais.

O card pode ser concluído quando (específico): Fase1 trazer a base Ativa com a relação do Claro ID ou CPF onde podemos relacionar os clientes; Fase2 realizar as quebras e SubDivisão; Fase 3: processar os meses  anteriores.

