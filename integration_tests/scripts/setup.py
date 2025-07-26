# Databricks notebook source
# MAGIC %pip install --upgrade pip

# COMMAND ----------

# MAGIC %pip install -r ../../requirements.txt

# COMMAND ----------

import os
os.environ["pipeline"] = "databricks"

# COMMAND ----------

from datta_pipeline_library.core.base_config import (
    BaseConfig,
    CommonConfig,
    EnvConfig,
    GreatExpectationsConfig,
)
from datta_pipeline_library.helpers.adls import configure_spark_to_use_spn_to_write_to_adls_gen2
from datta_pipeline_library.helpers.spn import AzureSPN

# COMMAND ----------

# DBTITLE 1,Parameters
unique_repo_branch_id = dbutils.widgets.get(name="unique_repo_branch_id")
unique_repo_branch_id_schema = dbutils.widgets.get(name="unique_repo_branch_id_schema")
repos_path = dbutils.widgets.get(name="repos_path")
env = dbutils.widgets.get(name="env")

common_conf = CommonConfig.from_file("../../conf/common/common_conf.json")
env_conf = EnvConfig.from_file(f"../../conf/{env}/conf.json")

kv = env_conf.kv_key

# values from key vault
tenant_id = dbutils.secrets.get(scope=kv, key="AZ-AS-SPN-DATTA-TENANT-ID")
spn_client_id = dbutils.secrets.get(scope=kv, key=env_conf.spn_client_id_key)
spn_client_secret = dbutils.secrets.get(scope=kv, key=env_conf.spn_client_secret_key)

spn = AzureSPN(tenant_id, spn_client_id, spn_client_secret)

#gx_config = GreatExpectationsConfig(azure_conn_str)

#base_config = BaseConfig.from_confs(env_conf, common_conf, gx_config)
base_config = BaseConfig.from_confs(env_conf, common_conf)
base_config.set_unique_id(unique_repo_branch_id)
base_config.set_unique_id_schema(unique_repo_branch_id_schema)

# COMMAND ----------

configure_spark_to_use_spn_to_write_to_adls_gen2(env_conf.storage_account, spn)

# COMMAND ----------

# DBTITLE 1,Configuration
uc_catalog = base_config.get_uc_catalog_name()
euh_schema = base_config.get_uc_euh_schema()
eh_schema = base_config.get_uc_eh_schema()

euh_folder_path = base_config.get_euh_folder_path()
eh_folder_path = base_config.get_eh_folder_path()

print("unity catalog: ", uc_catalog)
print("euh schema: ", euh_schema)
print("eh schema: ", eh_schema)
print("euh folder path: ", euh_folder_path)
print("eh folder path: ", eh_folder_path)

# COMMAND ----------

# MAGIC %md ## Delete UC schemas and tables

# COMMAND ----------

euh_table_list = ['bkpf', 'glpca', 'mseg', 'msego2', 'ds1hm_mt_bb', 'ds1hm_mt_bb01', 'rprctr_control_table']
if env == "dev":
    for table_name in euh_table_list:
        spark.sql(f"DROP TABLE IF EXISTS `{uc_catalog}`.`{euh_schema}`.table_name")

# COMMAND ----------

eh_table_list = ['fact_fi_act_line_item']
if env == "dev":
    for table_name in eh_table_list:
        spark.sql(f"DROP TABLE IF EXISTS `{uc_catalog}`.`{eh_schema}`.table_name")

# COMMAND ----------

if env == "dev":
    spark.sql(f"DROP SCHEMA IF EXISTS `{uc_catalog}`.`{euh_schema}` CASCADE")

# COMMAND ----------

if env == "dev":
    spark.sql(f"DROP SCHEMA IF EXISTS `{uc_catalog}`.`{eh_schema}` CASCADE")

# COMMAND ----------

# MAGIC %md ## Delete ADLS folders

# COMMAND ----------

# DBTITLE 1,Delete EUH folder
if env == "dev":
    dbutils.fs.rm(euh_folder_path, recurse=True)

# COMMAND ----------

# DBTITLE 1,Delete EH folder
if env == "dev":
    dbutils.fs.rm(eh_folder_path, recurse=True)

# COMMAND ----------

# MAGIC %md ## Insert data into euh layer tables

# COMMAND ----------

# DBTITLE 1,Create UC schemas
if env == "dev":
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{uc_catalog}`.`{euh_schema}`")

# COMMAND ----------

if env == "dev":
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{uc_catalog}`.`{eh_schema}`")

# COMMAND ----------

# DBTITLE 1,DS1HM_MT_BB create statement
if env == "dev": 
  bb_table_path = euh_folder_path + "/DS1HM_MT_BB"
  spark.sql(f"""CREATE TABLE `{uc_catalog}`.`{euh_schema}`.ds1hm_mt_bb (
    MANDT STRING,
    RYEAR STRING,
    RBUKRS STRING,
    RLDNR STRING,
    DOCCT STRING,
    DOCNR STRING,
    DOCLN STRING,
    GL_SIRID STRING,
    LFGJA STRING,
    LFMON STRING,
    REFACTIV STRING,
    RHOART STRING,
    REFDOCNR STRING,
    REFRYEAR STRING,
    REFDOCLN STRING,
    BWART STRING,
    SHKZG STRING,
    MGRP STRING,
    KEYFIG STRING,
    MB_FLG STRING,
    EXGTYP STRING,
    EXGNUM STRING,
    EBELN STRING,
    EBELP STRING,
    LIFNR STRING,
    GR1 STRING,
    GR2 STRING,
    GR3 STRING,
    PRPRD STRING,
    KAUFN STRING,
    KDPOS STRING,
    KUNAG STRING,
    VBELN STRING,
    POSNR STRING,
    SHTYPE STRING,
    SHNUMBER STRING,
    RSNUM STRING,
    RSPOS STRING,
    NOMTK STRING,
    NOMIT STRING,
    DOCIND STRING,
    MBLNR STRING,
    MJAHR STRING,
    ZEILE STRING,
    FKBER STRING,
    KTGRD STRING,
    ZDMFR STRING,
    BUKRS STRING,
    LAND1 STRING,
    PTYPE1 STRING,
    WERKS STRING,
    BZIRK STRING,
    MATNR STRING,
    PRCTR STRING,
    HMPRCTR STRING,
    UMBKS STRING,
    UMWRK STRING,
    PTYPE2 STRING,
    UMMAT STRING,
    BSARK STRING,
    OIC_MOT STRING,
    VSBED STRING,
    OIC_AORGIN STRING,
    OIC_ADESTN STRING,
    WGT_UOM_KG STRING,
    WGT_QTY DECIMAL(13,3),
    VOL_UOM_L15 STRING,
    VOL_QTY DECIMAL(13,3),
    MEINS STRING,
    LFIMG DECIMAL(13,3),
    ERFME STRING,
    ERFMG DECIMAL(13,3),
    QTYFLAG STRING,
    TIMSTP STRING,
    SYSTEM_ID STRING,
    LAST_ACTION_CD STRING,
    LAST_DTM TIMESTAMP,
    yrmono STRING,
    ingested_at TIMESTAMP,
    AEDATTM TIMESTAMP,
    OPFLAG STRING)
  USING delta
  LOCATION '{bb_table_path}'""")

# COMMAND ----------

# DBTITLE 1,DS1HM_MT_BB01 create statement
if env == "dev": 
    bb01_table_path = euh_folder_path + "/DS1HM_MT_BB01"
    spark.sql(f"""CREATE TABLE `{uc_catalog}`.`{euh_schema}`.ds1hm_mt_bb01 (
    MANDT STRING,
    RYEAR STRING,
    RBUKRS STRING,
    RLDNR STRING,
    DOCCT STRING,
    DOCNR STRING,
    DOCLN STRING,
    LFMON STRING,
    KONNR STRING,
    VGBEL STRING,
    REFDOCNR STRING,
    FKBER STRING,
    TIMSTP STRING,
    PARCEL_ID STRING,
    KTPNR STRING,
    VGPOS STRING,
    ZSTI_REFNO STRING,
    ZSTI_REFITEM STRING,
    ZSTI_REVNO STRING,
    DEALNO STRING,
    DEALTYPE STRING,
    DEALGROUP STRING,
    DDATFRM DATE,
    DDATTO DATE,
    ZXBLDAT DATE,
    OID_EXTBOL STRING,
    RACCT STRING,
    BGI_CARGOID STRING,
    BGI_DELIVERYID STRING,
    BGI_PARCELID STRING,
    VOL_QTY_GAL DECIMAL(13,3),
    VOL_UOM_GAL STRING,
    VOL_QTY_BB6 DECIMAL(13,3),
    VOL_UOM_BB6 STRING,
    ZACT_TYPE STRING,
    LAST_ACTION_CD STRING,
    LAST_DTM TIMESTAMP,
    SYSTEM_ID STRING,
    yrmono STRING,
    ingested_at TIMESTAMP,
    AEDATTM TIMESTAMP,
    OPFLAG STRING)
    USING delta
    LOCATION '{bb01_table_path}'""")

# COMMAND ----------

# DBTITLE 1,GLPCA create statement
if env == "dev":     
    glpca_table_path = euh_folder_path + "/GLPCA"
    spark.sql(f"""CREATE TABLE `{uc_catalog}`.`{euh_schema}`.glpca (
    RCLNT STRING,
    GL_SIRID STRING,
    RLDNR STRING,
    RRCTY STRING,
    RVERS STRING,
    RYEAR STRING,
    RTCUR STRING,
    RUNIT STRING,
    DRCRK STRING,
    POPER STRING,
    DOCCT STRING,
    DOCNR STRING,
    DOCLN STRING,
    RBUKRS STRING,
    RPRCTR STRING,
    RHOART STRING,
    RFAREA STRING,
    KOKRS STRING,
    RACCT STRING,
    HRKFT STRING,
    RASSC STRING,
    EPRCTR STRING,
    ACTIV STRING,
    AFABE STRING,
    OCLNT STRING,
    SBUKRS STRING,
    SPRCTR STRING,
    SHOART STRING,
    SFAREA STRING,
    TSL DECIMAL(15,2),
    HSL DECIMAL(15,2),
    KSL DECIMAL(15,2),
    MSL DECIMAL(15,3),
    CPUDT DATE,
    CPUTM STRING,
    USNAM STRING,
    SGTXT STRING,
    AUTOM STRING,
    DOCTY STRING,
    BLDAT DATE,
    BUDAT DATE,
    WSDAT DATE,
    REFDOCNR STRING,
    REFRYEAR STRING,
    REFDOCLN STRING,
    REFDOCCT STRING,
    REFACTIV STRING,
    AWTYP STRING,
    AWORG STRING,
    WERKS STRING,
    GSBER STRING,
    KOSTL STRING,
    LSTAR STRING,
    AUFNR STRING,
    AUFPL STRING,
    ANLN1 STRING,
    ANLN2 STRING,
    MATNR STRING,
    BWKEY STRING,
    BWTAR STRING,
    ANBWA STRING,
    KUNNR STRING,
    LIFNR STRING,
    RMVCT STRING,
    EBELN STRING,
    EBELP STRING,
    KSTRG STRING,
    ERKRS STRING,
    PAOBJNR STRING,
    PASUBNR STRING,
    PS_PSP_PNR STRING,
    KDAUF STRING,
    KDPOS STRING,
    FKART STRING,
    VKORG STRING,
    VTWEG STRING,
    AUBEL STRING,
    AUPOS STRING,
    SPART STRING,
    VBELN STRING,
    POSNR STRING,
    VKGRP STRING,
    VKBUR STRING,
    VBUND STRING,
    LOGSYS STRING,
    ALEBN STRING,
    AWSYS STRING,
    VERSA STRING,
    STFLG STRING,
    STOKZ STRING,
    STAGR STRING,
    GRTYP STRING,
    REP_MATNR STRING,
    CO_PRZNR STRING,
    IMKEY STRING,
    DABRZ DATE,
    VALUT DATE,
    RSCOPE STRING,
    AWREF_REV STRING,
    AWORG_REV STRING,
    BWART STRING,
    BLART STRING,
    ZZRBLGP STRING,
    SYSTEM_ID STRING,
    LAST_ACTION_CD STRING,
    LAST_DTM TIMESTAMP,
    yrmono STRING,
    ingested_at TIMESTAMP,
    AEDATTM TIMESTAMP,
    OPFLAG STRING)
    USING delta
    LOCATION '{glpca_table_path}'""")

# COMMAND ----------

# DBTITLE 1,MSEG create statement
if env == "dev":    
    mseg_table_path = euh_folder_path + "/MSEG"
    spark.sql(f"""CREATE TABLE `{uc_catalog}`.`{euh_schema}`.mseg (
    MANDT STRING,
    MBLNR STRING,
    MJAHR STRING,
    ZEILE STRING,
    LINE_ID STRING,
    PARENT_ID STRING,
    LINE_DEPTH STRING,
    MAA_URZEI STRING,
    BWART STRING,
    XAUTO STRING,
    MATNR STRING,
    WERKS STRING,
    LGORT STRING,
    CHARG STRING,
    INSMK STRING,
    ZUSCH STRING,
    ZUSTD STRING,
    SOBKZ STRING,
    LIFNR STRING,
    KUNNR STRING,
    KDAUF STRING,
    KDPOS STRING,
    KDEIN STRING,
    PLPLA STRING,
    SHKZG STRING,
    WAERS STRING,
    DMBTR DECIMAL(13,2),
    BNBTR DECIMAL(13,2),
    BUALT DECIMAL(13,2),
    SHKUM STRING,
    DMBUM DECIMAL(13,2),
    BWTAR STRING,
    MENGE DECIMAL(13,3),
    MEINS STRING,
    ERFMG DECIMAL(13,3),
    ERFME STRING,
    BPMNG DECIMAL(13,3),
    BPRME STRING,
    EBELN STRING,
    EBELP STRING,
    LFBJA STRING,
    LFBNR STRING,
    LFPOS STRING,
    SJAHR STRING,
    SMBLN STRING,
    SMBLP STRING,
    ELIKZ STRING,
    SGTXT STRING,
    EQUNR STRING,
    WEMPF STRING,
    ABLAD STRING,
    GSBER STRING,
    KOKRS STRING,
    PARGB STRING,
    PARBU STRING,
    KOSTL STRING,
    PROJN STRING,
    AUFNR STRING,
    ANLN1 STRING,
    ANLN2 STRING,
    XSKST STRING,
    XSAUF STRING,
    XSPRO STRING,
    XSERG STRING,
    GJAHR STRING,
    XRUEM STRING,
    XRUEJ STRING,
    BUKRS STRING,
    BELNR STRING,
    BUZEI STRING,
    BELUM STRING,
    BUZUM STRING,
    RSNUM STRING,
    RSPOS STRING,
    KZEAR STRING,
    PBAMG DECIMAL(29,3),
    KZSTR STRING,
    UMMAT STRING,
    UMWRK STRING,
    UMLGO STRING,
    UMCHA STRING,
    UMZST STRING,
    UMZUS STRING,
    UMBAR STRING,
    UMSOK STRING,
    KZBEW STRING,
    KZVBR STRING,
    KZZUG STRING,
    WEUNB STRING,
    PALAN DECIMAL(11,0),
    LGNUM STRING,
    LGTYP STRING,
    LGPLA STRING,
    BESTQ STRING,
    BWLVS STRING,
    TBNUM STRING,
    TBPOS STRING,
    XBLVS STRING,
    VSCHN STRING,
    NSCHN STRING,
    DYPLA STRING,
    UBNUM STRING,
    TBPRI STRING,
    TANUM STRING,
    WEANZ STRING,
    GRUND STRING,
    EVERS STRING,
    EVERE STRING,
    IMKEY STRING,
    KSTRG STRING,
    PAOBJNR STRING,
    PRCTR STRING,
    PS_PSP_PNR STRING,
    NPLNR STRING,
    AUFPL STRING,
    APLZL STRING,
    AUFPS STRING,
    VPTNR STRING,
    FIPOS STRING,
    SAKTO STRING,
    BSTMG DECIMAL(13,3),
    BSTME STRING,
    XWSBR STRING,
    EMLIF STRING,
    EXBWR DECIMAL(38,18),
    VKWRT DECIMAL(38,18),
    AKTNR STRING,
    ZEKKN STRING,
    VFDAT DATE,
    CUOBJ_CH STRING,
    EXVKW DECIMAL(38,18),
    PPRCTR STRING,
    RSART STRING,
    GEBER STRING,
    FISTL STRING,
    MATBF STRING,
    UMMAB STRING,
    BUSTM STRING,
    BUSTW STRING,
    MENGU STRING,
    WERTU STRING,
    LBKUM DECIMAL(13,3),
    SALK3 DECIMAL(38,18),
    VPRSV STRING,
    FKBER STRING,
    DABRBZ DATE,
    VKWRA DECIMAL(13,2),
    DABRZ DATE,
    XBEAU STRING,
    LSMNG DECIMAL(13,3),
    LSMEH STRING,
    KZBWS STRING,
    QINSPST STRING,
    URZEI STRING,
    J_1BEXBASE DECIMAL(13,2),
    MWSKZ STRING,
    TXJCD STRING,
    EMATN STRING,
    J_1AGIRUPD STRING,
    VKMWS STRING,
    HSDAT DATE,
    BERKZ STRING,
    MAT_KDAUF STRING,
    MAT_KDPOS STRING,
    MAT_PSPNR STRING,
    XWOFF STRING,
    BEMOT STRING,
    PRZNR STRING,
    LLIEF STRING,
    LSTAR STRING,
    XOBEW STRING,
    GRANT_NBR STRING,
    ZUSTD_T156M STRING,
    SPE_GTS_STOCK_TY STRING,
    KBLNR STRING,
    KBLPOS STRING,
    XMACC STRING,
    VGART_MKPF STRING,
    BUDAT_MKPF DATE,
    CPUDT_MKPF DATE,
    CPUTM_MKPF STRING,
    USNAM_MKPF STRING,
    XBLNR_MKPF STRING,
    TCODE2_MKPF STRING,
    VBELN_IM STRING,
    VBELP_IM STRING,
    SGT_SCAT STRING,
    SGT_UMSCAT STRING,
    `/BEV2/ED_KZ_VER` STRING,
    `/BEV2/ED_USER` STRING,
    `/BEV2/ED_AEDAT` DATE,
    `/BEV2/ED_AETIM` STRING,
    `/CWM/MENGE` DECIMAL(13,3),
    `/CWM/MEINS` STRING,
    `/CWM/ERFMG` DECIMAL(13,3),
    `/CWM/ERFME` STRING,
    `/CWM/BUSTW` STRING,
    `/CWM/SMBLU` STRING,
    `/CWM/FI` INT,
    ZXBUDAT DATE,
    ZXOIB_BLTIME STRING,
    ZXBLDAT DATE,
    ZNOMTK STRING,
    ZNOMIT STRING,
    ZIMPERNO STRING,
    ZPASSNO STRING,
    ZREPTVT STRING,
    ZCARRIER STRING,
    ZSTI_REFNO STRING,
    ZSTI_REFITEM STRING,
    OINAVNW DECIMAL(13,2),
    OICONDCOD STRING,
    CONDI STRING,
    OIKNUMV STRING,
    OIEXGPTR STRING,
    OIMATPST STRING,
    OIMATIE STRING,
    OIEXGTYP STRING,
    OIFEETOT DECIMAL(13,2),
    OIFEEPST STRING,
    OIFEEDT DATE,
    OIMATREF STRING,
    OIEXGNUM STRING,
    OINETCYC STRING,
    OIJ1BNFFIM STRING,
    OITRKNR STRING,
    OITRKJR STRING,
    OIEXTNR STRING,
    OIITMNR STRING,
    OIFTIND STRING,
    OIPRIOP STRING,
    OITRIND STRING,
    OIGHNDL STRING,
    OIUMBAR STRING,
    OISBREL STRING,
    OIBASPROD STRING,
    OIBASVAL DECIMAL(13,2),
    OIGLERF DECIMAL(13,3),
    OIGLSKU DECIMAL(13,3),
    OIGLBPR DECIMAL(13,3),
    OIGLBST DECIMAL(13,3),
    OIGLCALC STRING,
    OIBBSWQTY DOUBLE,
    OIASTBW STRING,
    OIVBELN STRING,
    OIPOSNR STRING,
    OIPIPEVAL STRING,
    OIC_LIFNR STRING,
    OIC_DCITYC STRING,
    OIC_DCOUNC STRING,
    OIC_DREGIO STRING,
    OIC_DLAND1 STRING,
    OIC_OCITYC STRING,
    OIC_OCOUNC STRING,
    OIC_OREGIO STRING,
    OIC_OLAND1 STRING,
    OIC_PORGIN STRING,
    OIC_PDESTN STRING,
    OIC_PTRIP STRING,
    OIC_PBATCH STRING,
    OIC_MOT STRING,
    OIC_AORGIN STRING,
    OIC_ADESTN STRING,
    OIC_TRUCKN STRING,
    OIA_BASELO STRING,
    OICERTF1 STRING,
    OIDATFM1 DATE,
    OIDATTO1 DATE,
    OIH_LICTP STRING,
    OIH_LICIN STRING,
    OIH_LCFOL STRING,
    OIH_FOLQTY DECIMAL(13,3),
    OID_EXTBOL STRING,
    OID_MISCDL STRING,
    OIBOMHEAD STRING,
    OIB_TIMESTAMP STRING,
    OIB_GUID_OPEN STRING,
    OIB_GUID_CLOSE STRING,
    OIB_SOCNR STRING,
    OITAXFROM STRING,
    OITAXTO STRING,
    OIHANTYP STRING,
    OITAXGRP STRING,
    OIPRICIE STRING,
    OIINVREC STRING,
    OIOILCON DECIMAL(5,2),
    OIOILCON2 DECIMAL(5,2),
    OIFUTDT DATE,
    OIFUTDT2 DATE,
    OIUOMQT STRING,
    OITAXQT DECIMAL(13,3),
    OIFUTQT DECIMAL(13,3),
    OIFUTQT2 DECIMAL(13,3),
    OITAXGRP2 STRING,
    OIEDBAL_GI STRING,
    OIEDBALM_GI STRING,
    OICERTF1_GI STRING,
    OIDATFM1_GI DATE,
    OIDATTO1_GI DATE,
    OIH_LICTP_GI STRING,
    OIH_LICIN_GI STRING,
    OIH_LCFOL_GI STRING,
    OIH_FOLQTY_GI DECIMAL(13,3),
    OIHANTYP_GI STRING,
    OIO_VSTEL STRING,
    OIO_SPROC STRING,
    ZZEILE STRING,
    LAST_ACTION_CD STRING,
    LAST_DTM TIMESTAMP,
    SYSTEM_ID STRING,
    ZOIVBELN_ID STRING,
    ZZNOMTK_ID STRING,
    ZVGART_ID STRING,
    ZOIEXGNUM_ID STRING,
    ZBWART_ID STRING,
    ZLIFNR_ID STRING,
    ZOICLIFNR_ID STRING,
    ZOICTRUCKN_ID STRING,
    ZOICMOT_ID STRING,
    ZWERKS_ID STRING,
    ZOIOVSTEL_ID STRING,
    ZOIHANTYP_ID STRING,
    ZUMWRK_ID STRING,
    ZMATNR_ID STRING,
    ZBWTAR_ID STRING,
    ZKUNNR_ID STRING,
    ZWEMPF_ID STRING,
    ZUSNAMMKPF_ID STRING,
    ZOICOLAND1_ID STRING,
    ZOICDLAND1_ID STRING,
    ZOIHLICTP_ID STRING,
    ZOICERTF1_ID STRING,
    ZOIPOSNR_ID STRING,
    ZMBLNR_ID STRING,
    ZMJAHR_ID STRING,
    ZZEILE_ID STRING,
    ZOITAXGRP_ID STRING,
    ZEBELN_ID STRING,
    ZKDAUF_ID STRING,
    ZBUKRS_ID STRING,
    ZOIC_OCITYC_ID STRING,
    ZDIC_OICITYC_ID STRING,
    ZOITAXFROM_ID STRING,
    ZOITAXTO_ID STRING,
    ZLGORT_ID STRING,
    ZUMLGO_ID STRING,
    ZSTOR_LOC_ID STRING,
    ZPART_STOR_LOC_ID STRING,
    ZEXC_DUTY_LIC_ID STRING,
    ZERFME_ID STRING,
    ZORIGIN_CITY_ID STRING,
    ZDESTINATION_CITY_ID STRING,
    ZMBLNR_YR_ALT_ID STRING,
    SGT_RCAT STRING,
    `/CWM/TY2TQ` STRING,
    `/CWM/UMTY2TQ` STRING,
    DISUB_OWNER STRING,
    FSH_SEASON_YEAR STRING,
    FSH_SEASON STRING,
    FSH_COLLECTION STRING,
    FSH_THEME STRING,
    FSH_UMSEA_YR STRING,
    FSH_UMSEA STRING,
    FSH_UMCOLL STRING,
    FSH_UMTHEME STRING,
    SGT_CHINT STRING,
    FSH_DEALLOC_QTY DECIMAL(13,3),
    WRF_CHARSTC1 STRING,
    WRF_CHARSTC2 STRING,
    WRF_CHARSTC3 STRING,
    ZMATERIAL_NUMBER STRING,
    yrmono STRING,
    ingested_at TIMESTAMP,
    AEDATTM TIMESTAMP,
    OPFLAG STRING)
    USING delta
    LOCATION '{mseg_table_path}'""")

# COMMAND ----------

# DBTITLE 1,MSEGO2 create statement
if env == "dev":
    msego2_table_path = euh_folder_path + "/MSEGO2"
    spark.sql(f"""CREATE TABLE `{uc_catalog}`.`{euh_schema}`.msego2 (
    MANDT STRING,
    MBLNR STRING,
    MJAHR STRING,
    ZEILE STRING,
    MSEHI STRING,
    LINE_ID STRING,
    PARENT_ID STRING,
    LINE_DEPTH STRING,
    MAA_URZEI STRING,
    ADQNT DOUBLE,
    ADQNTP DECIMAL(13,3),
    MANEN STRING,
    ZMBLNR_ID STRING,
    ZMJAHR_ID STRING,
    ZZEILE_ID STRING,
    ZMSEHI_ID STRING,
    LAST_ACTION_CD STRING,
    LAST_DTM TIMESTAMP,
    SYSTEM_ID STRING,
    yrmono STRING,
    ingested_at TIMESTAMP,
    AEDATTM TIMESTAMP,
    OPFLAG STRING)
    USING delta
    LOCATION '{msego2_table_path}'""")

# COMMAND ----------

# DBTITLE 1,BKPF create statement
if env == "dev":
    bkpf_table_path = euh_folder_path + "/BKPF"
    spark.sql(f"""CREATE TABLE `{uc_catalog}`.`{euh_schema}`.bkpf (
    MANDT STRING,
    BUKRS STRING,
    BELNR STRING,
    GJAHR STRING,
    BLART STRING,
    BLDAT DATE,
    BUDAT DATE,
    MONAT STRING,
    CPUDT DATE,
    CPUTM STRING,
    AEDAT DATE,
    UPDDT DATE,
    WWERT DATE,
    USNAM STRING,
    TCODE STRING,
    BVORG STRING,
    XBLNR STRING,
    DBBLG STRING,
    STBLG STRING,
    STJAH STRING,
    BKTXT STRING,
    WAERS STRING,
    KURSF DECIMAL(9,5),
    KZWRS STRING,
    KZKRS DECIMAL(9,5),
    BSTAT STRING,
    XNETB STRING,
    FRATH DECIMAL(13,2),
    XRUEB STRING,
    GLVOR STRING,
    GRPID STRING,
    DOKID STRING,
    ARCID STRING,
    IBLAR STRING,
    AWTYP STRING,
    AWKEY STRING,
    FIKRS STRING,
    HWAER STRING,
    HWAE2 STRING,
    HWAE3 STRING,
    KURS2 DECIMAL(9,5),
    KURS3 DECIMAL(9,5),
    BASW2 STRING,
    BASW3 STRING,
    UMRD2 STRING,
    UMRD3 STRING,
    XSTOV STRING,
    STODT DATE,
    XMWST STRING,
    CURT2 STRING,
    CURT3 STRING,
    KUTY2 STRING,
    KUTY3 STRING,
    XSNET STRING,
    AUSBK STRING,
    XUSVR STRING,
    DUEFL STRING,
    AWSYS STRING,
    TXKRS DECIMAL(9,5),
    LOTKZ STRING,
    XWVOF STRING,
    STGRD STRING,
    PPNAM STRING,
    BRNCH STRING,
    NUMPG STRING,
    ADISC STRING,
    XREF1_HD STRING,
    XREF2_HD STRING,
    XREVERSAL STRING,
    REINDAT DATE,
    RLDNR STRING,
    LDGRP STRING,
    PROPMANO STRING,
    XBLNR_ALT STRING,
    VATDATE DATE,
    DOCCAT STRING,
    XSPLIT STRING,
    CASH_ALLOC STRING,
    FOLLOW_ON STRING,
    XREORG STRING,
    SUBSET STRING,
    KURST STRING,
    KURSX DECIMAL(28,14),
    KUR2X DECIMAL(28,14),
    KUR3X DECIMAL(28,14),
    XMCA STRING,
    RESUBMISSION DATE,
    `/SAPF15/STATUS` STRING,
    PSOTY STRING,
    PSOAK STRING,
    PSOKS STRING,
    PSOSG STRING,
    PSOFN STRING,
    INTFORM STRING,
    INTDATE DATE,
    PSOBT DATE,
    PSOZL STRING,
    PSODT DATE,
    PSOTM STRING,
    FM_UMART STRING,
    CCINS STRING,
    CCNUM STRING,
    SSBLK STRING,
    BATCH STRING,
    SNAME STRING,
    SAMPLED STRING,
    EXCLUDE_FLAG STRING,
    BLIND STRING,
    OFFSET_STATUS STRING,
    OFFSET_REFER_DAT DATE,
    PENRC STRING,
    KNUMV STRING,
    OINETNUM STRING,
    OINJAHR STRING,
    OININD STRING,
    RECHN STRING,
    ZCPUDT DATE,
    ZCPUTM STRING,
    ZPAYDUDT DATE,
    ZAWKEY_REF_ORGUNIT STRING,
    AWKEY_REFDOC STRING,
    Z_CONCAT_KEY STRING,
    LAST_ACTION_CD STRING,
    LAST_DTM TIMESTAMP,
    SYSTEM_ID STRING,
    ZFINT_HEADER_ID STRING,
    ZUSNAM_ID STRING,
    ZBLART_ID STRING,
    ZSTBLG_ID STRING,
    ZAWTYP_ID STRING,
    ZHWAER_ID STRING,
    ZCUR_ID STRING,
    ZSH_COMP_ID STRING,
    ZHWAE2_ID STRING,
    ZHWAE3_ID STRING,
    CTXKRS DECIMAL(9,5),
    PPDAT DATE,
    PPTME STRING,
    ZFISCAL_YEAR_PERIOD STRING,
    PYBASTYP STRING,
    Z_TAX_DCMT_SOURCE_ID STRING,
    Z_PURC_SOURCE_ID STRING,
    PYBASNO STRING,
    PYBASDAT DATE,
    PYIBAN STRING,
    INWARDNO_HD STRING,
    INWARDDT_HD DATE,
    yrmono STRING,
    ingested_at TIMESTAMP,
    AEDATTM TIMESTAMP,
    OPFLAG STRING)
    USING delta
    LOCATION '{bkpf_table_path}'"""
    )

# COMMAND ----------

# DBTITLE 1,RPRCTR_CONTROL_TABLE create statement
if env == "dev":
    rprctr_table_path = euh_folder_path + "/RPRCTR_CONTROL_TABLE"
    spark.sql(f"""CREATE TABLE `{uc_catalog}`.`{euh_schema}`.rprctr_control_table (
    RPRCTR STRING,
    ingested_at TIMESTAMP,
    AEDATTM TIMESTAMP,
    OPFLAG STRING)
    USING delta
    LOCATION '{rprctr_table_path}'""")

# COMMAND ----------

# DBTITLE 1,Load sample data into EUH tables
if env == "dev":
    
    #ds1hm_mt_bb

    spark.sql(f"""INSERT INTO `{uc_catalog}`.`{euh_schema}`.ds1hm_mt_bb
VALUES 
('110','2021','ARZ1','8A','A','1810189346','302','','0','0','','0','','0','0','','','','','','','','','0','','','0','0','','','0','','','0','','','0','0','','0','','','0','0','','','','','','','','','','','','','','','','','','','','','','0','','0','','0','','0','','2.02101E+13',null,null,null,'DS1_HM_MT_BB_20230808_163336_001.parquet',current_timestamp(),null,null),
('110','2022','GB30','8A','A','1983777417','4','','0','0','','0','','0','0','','','','','','','','','0','','','0','0','','','0','','','0','','','0','0','','0','','','0','0','GHOST','','','','','','','','','','','','','','','','','','','','','0','','0','','0','','0','','2.02204E+13','1001','U','2022-04-28T07:37:31.000+0000','DS1_HM_MT_BB_20230810_002600_001.parquet',current_timestamp(),null,null),
('110','2022','US16','8A','A','1986318264','6','22354997521','2022','5','SD00','5','1412129794','2022','1','','H','50','I1SO','X','','','','0','','','0','0','','268984450','40','12639312','734041650','40','','','0','0','','0','','','0','0','SV34','1','D','US16','US','ZST','Y393','USZ150','400007387','800001','300064','','','','','TAS','1','10','','','KG','-3427.777','L15','-4569.759','KG','-3427.777','UG6','-1208','A','2.02205E+13','1001','C','2022-09-13T13:16:07.000+0000','DS1_HM_MT_BB_20230810_041724_001.parquet',current_timestamp(),null,null),
('110','2022','US16','8A','A','1985833144','2','22348723138','2022','4','RMRP','2','5230476851','2022','0','','S','','','','ZBLN','1002965','4538133803','10','69062056','5080462114','2022','2','','','0','','','0','','','0','0','','0','','','0','0','OP37','','D','US16','US','ZTP','Y747','USZ108','400007423','300064','300064','','','','','','','','','','KG','0','L15','0','KG','0','','0','B','2.02205E+13','1001','C','2022-09-13T13:16:23.000+0000','DS1_HM_MT_BB_20230810_041724_001.parquet',current_timestamp(),null,null),
('110','2021','ARZ1','8A','A','1811430644','57','','0','0','','0','','0','0','','','','','','','','','0','','','0','0','','','0','','','0','','','0','0','','0','','','0','0','','','','','','','','','','','','','','','','','','','','','','0','','0','','0','','0','','2.02101E+13',null,null,null,'DS1_HM_MT_BB_20230808_163336_001.parquet',current_timestamp(),null,null),
('110','2022','GB30','8A','A','1983778742','134','22326288295','2022','4','SD00','5','1411830057','2022','9','','H','50','I1SO','X','','','','0','','','0','0','','268805295','10','12497204','733855545','10','','','0','0','','0','','','0','0','SV43','1','D','GB30','TH','ZST','T033','THZ01','550051279','100034','600010','','','','','OCR','1','11','','','','0','','0','KAR','0','KAR','0','','2.02204E+13','1001','C','2022-09-13T13:14:40.000+0000','DS1_HM_MT_BB_20230810_002600_001.parquet',current_timestamp(),null,null),
('110','2022','US16','8A','A','1986318316','2','22354999900','2022','5','SD00','5','1412129861','2022','1','','H','50','I1SO','X','','','','0','','','0','0','','268976949','20','12592469','734032768','20','','','0','0','','0','','','0','0','','1','D','US16','US','ZSH','Y370','USZ101','400007375','800001','300055','','','','','TAS','1','10','','','','0','','0','KG','0','UG6','0','','2.02205E+13','1001','C','2022-09-13T13:16:07.000+0000','DS1_HM_MT_BB_20230810_041724_001.parquet',current_timestamp(),null,null),
('110','2022','US16','8A','A','1985834079','1','22348740823','2022','4','RMRP','35','5230477001','2022','0','','S','','','','ZBLN','1002965','4538160441','10','69062056','5080528307','2022','3','','','0','','','0','','','0','0','','0','','','0','0','','','D','US16','US','ZTP','Y750','USZ108','400007423','300064','300064','','','','','','','','','','KG','0','L15','0','KG','0','','0','B','2.02205E+13','1001','C','2022-09-13T13:16:25.000+0000','DS1_HM_MT_BB_20230810_041724_001.parquet',current_timestamp(),null,null),
('110','2021','ARZ1','8A','A','1811430644','116','','0','0','','0','','0','0','','','','','','','','','0','','','0','0','','','0','','','0','','','0','0','','0','','','0','0','','','','','','','','','','','','','','','','','','','','','','0','','0','','0','','0','','2.02101E+13',null,null,null,'DS1_HM_MT_BB_20230808_163336_001.parquet',current_timestamp(),null,null),
('110','2022','GB30','8A','A','1983778742','152','22326288313','2022','4','SD00','35','1411830057','2022','10','','H','','','','','','','0','','','0','0','','268805295','20','12497204','733855545','20','','','0','0','','0','','','0','0','','1','D','GB30','TH','ZST','T033','THZ01','550051310','9999999900','600010','','','','','OCR','1','11','','','','0','','0','KAR','0','KAR','0','','2.02204E+13','1001','C','2022-09-13T13:14:40.000+0000','DS1_HM_MT_BB_20230810_002600_001.parquet',current_timestamp(),null,null)""")
    
#ds1hm_mt_bb01

    spark.sql(f"""INSERT INTO `{uc_catalog}`.`{euh_schema}`.ds1hm_mt_bb01
VALUES 
('110','2021','ARZ1','8A','A','1810189346','282','0','','','','','1705487585237','','0','0','','0','0','','','',null,null,null,'','','','','','0','','0','','',null,null,null,'HM_MT_BB01_20230807_053514_004.parquet',current_timestamp(),null,null),
('110','2022','ZA03','8A','A','1959589165','62','2','','','1408687977','','1705487585237','','0','0','','0','0','','','',null,null,null,'','6210010','','','','0','','0','','',null,null,null,'DS1HM_MT_BB01_INIT2',current_timestamp(),null,null),
('110','2021','ARZ1','8A','A','1810189346','342','0','','','','','1705487585237','','0','0','','0','0','','','',null,null,null,'','','','','','0','','0','','',null,null,null,'HM_MT_BB01_20230807_053514_004.parquet',current_timestamp(),null,null),
('110','2022','ZA03','8A','A','1959589371','45','2','','','1408687936','','1705487585237','','0','0','','0','0','','','',null,null,null,'','6210010','','','','0','','0','','',null,null,null,'DS1HM_MT_BB01_INIT2',current_timestamp(),null,null),
('110','2021','ARZ1','8A','A','1810820215','318','0','','','','','1705487585237','','0','0','','0','0','','','',null,null,null,'','','','','','0','','0','','',null,null,null,'HM_MT_BB01_20230807_053514_004.parquet',current_timestamp(),null,null),
('110','2022','ZA03','8A','A','1959589721','32','2','','','1408687946','SV43','1705487585237','','0','0','','0','0','','','',null,null,null,'','6000100','','','','0','','0','','',null,null,null,'DS1HM_MT_BB01_INIT2',current_timestamp(),null,null),
('110','2021','ARZ1','8A','A','1811430905','4','0','','','','','1705487585237','','0','0','','0','0','','','',null,null,null,'','','','','','0','','0','','',null,null,null,'HM_MT_BB01_20230807_053514_004.parquet',current_timestamp(),null,null),
('110','2022','ZA03','8A','A','1959590735','13','2','','','1408687971','SV43','1705487585237','','0','0','','0','0','','','',null,null,null,'','6000100','','','','0','','0','','',null,null,null,'DS1HM_MT_BB01_INIT2',current_timestamp(),null,null),
('110','2021','ARZ1','8A','A','1811431532','7','0','','','','','1705487585237','','0','0','','0','0','','','',null,null,null,'','','','','','0','','0','','',null,null,null,'HM_MT_BB01_20230807_053514_004.parquet',current_timestamp(),null,null),
('110','2022','ZA03','8A','A','1959590858','18','2','','','1408687975','','1705487585237','','0','0','','0','0','','','',null,null,null,'','3620200','','','','0','','0','','',null,null,null,'DS1HM_MT_BB01_INIT2',current_timestamp(),null,null)""")
    
#glpca

    spark.sql(f"""INSERT INTO `{uc_catalog}`.`{euh_schema}`.glpca
VALUES 
('110','22374548860','8A','0','0','2022','PKR','','S','5','A','1987625093','679','PK01','100001','35','SV06','OP01','3170300','','','','RMRP','0','110','','','0','','3092.8','3092.8','16.67','0',null,'8:21:38','PHDEL4','','','',null,null,null,'5230524561','2022','679','W','RMRP','RMRP','2022','P209','','PK01000236','','','0','','','','P209','','','','68059155','','4538018786','100','','','0','0','0','','0','','','','','0','','','0','','','','','','','','','','','','','','',null,null,'OC','','','','RS','0','','',null,'GLPCA_INIT3',current_timestamp(),null,null),
('110','21405393808','8A','0','0','2021','CAD','KG','H','10','A','1914479495','1','CA48','800001','2','SV31','OP01','6380303','','','','RFBU','0','110','','','0','','-97.65','-97.65','-79.11','-92.218',null,'8:01:40','R_CPS_RFC','SVMS:-ENHD-ETRF','','',null,null,null,'4012802556','2021','1','W','RFBU','BKPFF','CA482021','C282','','CA48900004','','','0','','','400004665','','','','','','','','0','','','0','0','0','','0','','','','','0','','','0','','','','','','','','','','','','','','',null,null,'OC','','','','HC','0','','',null,'GLPCA_20230812_082950_001.parquet',current_timestamp(),null,null),
('110','22097420995','8A','0','0','2022','USD','','H','3','A','1966684754','90','US52','100034','5','SV44','OP01','6355027','','','','SD00','0','110','','','0','','-4.32','-4.32','-4.32','0',null,'3:38:12','B_BILUS52_08','','','',null,null,null,'1409564951','2022','6','W','SD00','VBRK','','U246','','','','','0','','','550042741','','UT','','12573062','','','','0','','','2109110898','0','0','','0','ZRAD','US52','8','399904752','70','3','','6','S79','A076','','','','','','','','','','','','',null,null,'PA','','','','RV','0','','',null,'GLPCA_INIT2',current_timestamp(),null,null),
('110','22516945830','8A','0','0','2022','EUR','','S','6','A','1998866183','4','DE01','9999999900','35','','OP01','3620100','','','','SD00','0','110','','','0','','18.08','18.08','19.41','0',null,'16:17:46','B_BILDE_14','','','',null,null,null,'1413643313','2022','2','W','SD00','VBRK','','D267','','','','','0','','','400001812','','TA','','12245867','','','','0','','','0','0','0','','0','ZSB1','DE01','14','401168633','20','2','','2','M48','A022','','','','','','','','','','','','',null,null,'','','','','RV','0','','',null,'GLPCA_INIT3',current_timestamp(),null,null),
('110','22374549229','8A','0','0','2022','USD','KG','H','5','A','1987625350','1','US16','300055','2','SV31','OP01','6380303','','','','RFBU','0','110','','','0','','-813.87','-813.87','-813.87','-856.944',null,'8:21:25','R_CPS_RFC','SVMS:-ENHD-ETRF','','',null,null,null,'4027237290','2022','1','W','RFBU','BKPFF','US162022','Y370','','US16900145','','','0','','','400007375','','','','','','','','0','','','0','0','0','','0','','','','','0','','','0','','','','','','','','','','','','','','',null,null,'OC','','','','HC','0','','',null,'GLPCA_INIT3',current_timestamp(),null,null),
('110','21405396193','8A','0','0','2021','CAD','KG','H','10','A','1914480062','1','CA48','800001','2','SV31','OP01','6380303','','','','RFBU','0','110','','','0','','-314.05','-314.05','-254.44','-296.576',null,'8:01:46','R_CPS_RFC','SVMS:-ENHD-ETRF','','',null,null,null,'4012802588','2021','1','W','RFBU','BKPFF','CA482021','C282','','CA48900004','','','0','','','400004665','','','','','','','','0','','','0','0','0','','0','','','','','0','','','0','','','','','','','','','','','','','','',null,null,'OC','','','','HC','0','','',null,'GLPCA_20230812_082950_001.parquet',current_timestamp(),null,null),
('110','22097421431','8A','0','0','2022','USD','','S','3','A','1966684858','76','US52','600010','5','SV38','OP01','6355580','','','','SD00','0','110','','','0','','3.92','3.92','3.92','0',null,'3:38:12','B_BILUS52_08','','','',null,null,null,'1409564954','2022','5','W','SD00','VBRK','','U246','','','','','0','','','550039860','','UT','','12573062','','','','0','','','2109111185','0','0','','0','ZRAD','US52','8','399904759','50','3','','5','S79','A076','','','','','','','','','','','','',null,null,'PA','','','','RV','0','','',null,'GLPCA_INIT2',current_timestamp(),null,null),
('110','22516947071','8A','0','0','2022','EUR','','S','6','A','1998866280','8','DE01','9999999900','35','','OP01','3620100','','','','SD00','0','110','','','0','','1.77','1.77','1.9','0',null,'16:17:46','B_BILDE_14','','','',null,null,null,'1413643400','2022','4','W','SD00','VBRK','','D267','','','','','0','','','400001952','','TA','','12245867','','','','0','','','0','0','0','','0','ZSB1','DE01','14','401169805','40','2','','4','M48','A022','','','','','','','','','','','','',null,null,'','','','','RV','0','','',null,'GLPCA_INIT3',current_timestamp(),null,null),
('110','22374549653','8A','0','0','2022','TRY','KG','S','5','A','1987625521','2','TR04','310001','2','SV30','OP01','6380203','','','','RFBU','0','110','','','0','','313442.85','313442.85','21122.91','17434.403',null,'8:21:29','R_CPS_RFC','SVMS:-Offset-ENHDETRF','','',null,null,null,'4009280932','2022','2','W','RFBU','BKPFF','TR042022','T102','','TR04900002','','','0','','','400006000','','','','','','','','0','','','0','0','0','','0','','','','','0','','','0','','','','','','','','','','','','','','',null,null,'OC','','','','HC','0','','',null,'GLPCA_INIT3',current_timestamp(),null,null),
('110','21405403705','8A','0','0','2021','USD','','S','10','A','1914477377','2','US16','800001','5','OP20','OP01','6920450','','','','SD00','0','110','','','0','','23.67','23.67','23.67','0',null,'8:02:18','B_BILUS16_14','','','',null,null,null,'1402769205','2021','1','W','SD00','VBRK','','Y017','','US16000841','','','0','','','400007375','','UT','','12308038','','','','0','','','0','0','0','','0','ZTF3','US16','14','262627379','10','2','','1','LLB','A022','','','','','','','','','','','','',null,null,'OC','','','','RV','0','','',null,'GLPCA_20230812_154003_001.parquet',current_timestamp(),null,null)""")
    
#mseg

    spark.sql(f"""INSERT INTO `{uc_catalog}`.`{euh_schema}`.mseg
VALUES 
('110','3198096407','2021','1','0','0','0','0','','','','','','','','','','','','','','0','0','','','','0','0','0','','0','','0','','0','','0','','','0','0','','0','0','','0','','','','','','','','','','','','','','','','','','','0','','','','','0','','0','0','0','','0','','','','','','','','','','','','','','0','','','','','0','0','0','','','','','0','','0','0','0','','','','','0','','0','','0','0','0','','','','0','','','','0','0','','0',null,'0','0','','','','','','','','','','','0','0','','',null,'0',null,'','0','','','','0','0','','','','','',null,'','','0','0','','','','','','','','','','','0','','',null,null,'0:00:00','','','','','0','','','','',null,'0:00:00','0','','0','','','0','0',null,'0:00:00',null,'','0','','','','','','0','0','','','','','','','','0','',null,'','','','','','0','','0','','','','','','0','','0','0','0','0','0','','0','','','0','','','','','','','','','','','','','','','','','','','','',null,null,'','','','0','','','','0','','','','','','','','','','0','0',null,null,'','0','0','0','','','','',null,null,'','','','0','','','','1','A','2022-07-04T07:06:34.000+0000','1001','','','','','','','','','','','','','','','','','','','','','','','1001000000','10010720920630','10012021','10010001','','','','','','1001ZDIC_OICITYC_ID','','','','','','','','','','','10010720920630','','','','','','','','','','','','','','0','','','','','MSEG_2021_3199800000.parquet',current_timestamp(),null,null),
('110','5073211467','2021','1','1','0','0','0','101','','550053335','T026','WHS1','UT','X','','','','','','','0','0','','S','THB','18048.54','0','18048.54','','0','UT','44','KAR','44','KAR','0','','','0','0','','0','0','','0','','','','','','','OP01','','','','','6377762','','','','','','','2021','','','GB30','','0','','0','0','0','','0','2','','','','','','','','','F','','','','0','','','','','0','0','0','','','','','0','','0','1','0','','','','','0','600010','0','','0','0','1','','','6350120','44','KAR','','','0','0','','0',null,'0','0','600010','','','','550053335','','MF01','WF01','X','X','402','164898.01','S','',null,'0',null,'','0','','','6','1','0','','','','','',null,'','','0','0','','','','','','','','2','','','0','','WF',null,null,'10:43:26','THF247','','MB31','','0','','','','',null,'0:00:00','0','','0','','','0','0',null,'17:43:26',null,'','0','','','','','','0','0','','','','','','','','0','',null,'','','','','','0','','0','','','','','','0','','0','0','0','0','0','','0','','','0','','','','','','TH','','','','TH','','','','','','T026','#','','','',null,null,'','','','0','','','','20210408104326','','','','UT','UT','HC','UA','','','100','0',null,null,'L','528','0','0','','','','',null,null,'','','','0','','','','1','C','2021-04-08T10:44:28.000+0000','1001','','','1001WF','','1001101','','','','','1001T026','','1001HC','','10010720920630','1001UT','','','1001THF247','1001TH','1001TH','','','1001000000','10010720920630','10012021','10010001','1001UA','','','1001GB30','','1001ZDIC_OICITYC_ID','1001UT','1001UT','1001WHS1','','1001WHS1T026','','','1001KAR','1001TH','1001TH','10010720920630','','','','','','','','','','','','','','0','','','','550053335','MSEG_2021_5076000000.parquet',current_timestamp(),null,null),
('110','3198096775','2021','3','0','0','0','0','','','','','','','','','','','','','','0','0','','','','0','0','0','','0','','0','','0','','0','','','0','0','','0','0','','0','','','','','','','','','','','','','','','','','','','0','','','','','0','','0','0','0','','0','','','','','','','','','','','','','','0','','','','','0','0','0','','','','','0','','0','0','0','','','','','0','','0','','0','0','0','','','','0','','','','0','0','','0',null,'0','0','','','','','','','','','','','0','0','','',null,'0',null,'','0','','','','0','0','','','','','',null,'','','0','0','','','','','','','','','','','0','','',null,null,'0:00:00','','','','','0','','','','',null,'0:00:00','0','','0','','','0','0',null,'0:00:00',null,'','0','','','','','','0','0','','','','','','','','0','',null,'','','','','','0','','0','','','','','','0','','0','0','0','0','0','','0','','','0','','','','','','','','','','','','','','','','','','','','',null,null,'','','','0','','','','0','','','','','','','','','','0','0',null,null,'','0','0','0','','','','',null,null,'','','','0','','','','3','A','2022-07-04T07:06:34.000+0000','1001','','','','','','','','','','','','','','','','','','','','','','','1001000000','10010720920630','10012021','10010003','','','','','','1001ZDIC_OICITYC_ID','','','','','','','','','','','10010720920630','','','','','','','','','','','','','','0','','','','','MSEG_2021_3199800000.parquet',current_timestamp(),null,null),
('110','5073211577','2021','1','1','0','0','0','101','','550052837','D133','WHS1','11729533','F','','','','','','','0','0','','S','EUR','895.23','0','895.23','','0','UT','45','KAR','45','KAR','0','','','0','0','','0','0','','0','X','','','','','','OP01','','','','','6365301','','','','','','','2021','','','DE01','','0','','0','0','0','','0','2','','','','','','','','','F','','','','0','','','','','0','0','0','','','','','0','','0','1','0','','','','','0','600010','0','','0','0','1','','','6350120','45','KAR','','','0','0','','0',null,'0','0','600010','','','','550052837','','MF01','WF01','X','X','0','0','S','',null,'0',null,'','0','','','6','1','0','','','','','',null,'','','0','0','','','','','','','','F','','','0','','WF',null,null,'10:47:23','TPDEG803','','MB31','','0','','','','',null,'0:00:00','0','','0','','','0','0',null,'10:47:22',null,'','0','','','UT','','','0','0','','','','','','','','0','',null,'','','','','','0','','0','','','','','','0','','0','0','0','0','0','','0','','','0','','','','','','DE','','','','DE','','','','','','D133','#','','','',null,null,'','','','0','','','','20210408104326','','','','UT','UT','HC','UA','','','100','0',null,null,'KG','605.52','0','0','','','','',null,null,'','','','0','','','','1','C','2021-04-08T10:47:58.000+0000','1001','','','1001WF','','1001101','','','','','1001D133','','1001HC','','10010720920630','1001UT','','','1001TPDEG803','1001DE','1001DE','','','1001000000','10010720920630','10012021','10010001','1001UA','','','1001DE01','','1001ZDIC_OICITYC_ID','1001UT','1001UT','1001WHS1','','1001WHS1D133','','','1001KAR','1001DE','1001DE','10010720920630','','','','','','','','','','','','','','0','','','','550052837','MSEG_2021_5076000000.parquet',current_timestamp(),null,null),
('110','3198097557','2021','11','0','0','0','0','','','','','','','','','','','','','','0','0','','','','0','0','0','','0','','0','','0','','0','','','0','0','','0','0','','0','','','','','','','','','','','','','','','','','','','0','','','','','0','','0','0','0','','0','','','','','','','','','','','','','','0','','','','','0','0','0','','','','','0','','0','0','0','','','','','0','','0','','0','0','0','','','','0','','','','0','0','','0',null,'0','0','','','','','','','','','','','0','0','','',null,'0',null,'','0','','','','0','0','','','','','',null,'','','0','0','','','','','','','','','','','0','','',null,null,'0:00:00','','','','','0','','','','',null,'0:00:00','0','','0','','','0','0',null,'0:00:00',null,'','0','','','','','','0','0','','','','','','','','0','',null,'','','','','','0','','0','','','','','','0','','0','0','0','0','0','','0','','','0','','','','','','','','','','','','','','','','','','','','',null,null,'','','','0','','','','0','','','','','','','','','','0','0',null,null,'','0','0','0','','','','',null,null,'','','','0','','','','11','A','2022-07-04T07:06:34.000+0000','1001','','','','','','','','','','','','','','','','','','','','','','','1001000000','10010720920630','10012021','10010011','','','','','','1001ZDIC_OICITYC_ID','','','','','','','','','','','10010720920630','','','','','','','','','','','','','','0','','','','','MSEG_2021_3199800000.parquet',current_timestamp(),null,null),
('110','5073211711','2021','1','1','0','0','0','Z11','','400006910','Y356','0','UT','','','','','69052725','','','0','0','','S','USD','16605.6','0','16290.65','','0','UT','25739.537','KG','9071','UG6','9071','UG6','4534697666','10','2021','5073211711','1','0','','0','X','','','','','','OP01','','','','','','','','','','','','2021','','','US16','','0','','0','0','0','','0','2','','','','','','','','','B','','','','0','','','','','0','0','0','','','','','0','','0','1','0','','','','','0','300057','0','','0','0','0','','','','215.976','BB6','','69052725','0','0','','0',null,'0','0','300057','','','','400006910','','ME01','WE01','X','X','0','0','S','',null,'0',null,'','0','','','','1','0','','','','','',null,'','','0','0','','','','','','','','F','','','0','','WE',null,null,'10:50:14','B_MODALL','','MB01','720920630','10','','','','',null,'0:00:00','0','','0','','','0','0',null,'0:00:00',null,'','0','','','','','','0','0','','','','','','','','0','',null,'','','','','','0','','0','','','','','','0','','0','0','0','0','0','','0','','720920630','10','X','','1390','155','MI','US','1390','155','MI','US','','','2019','','1','Y356','#','','','',null,null,'','','','0','467212','','','20210408104326','','','','TA','UT','AA','BS','X','','100','0',null,null,'UG6','9071','0','0','','','','',null,null,'','','','0','','','','1','C','2021-04-08T10:51:35.000+0000','1001','10010720920630','','1001WE','','1001Z11','10010720920630','','','100101','1001Y356','','1001AA','','10010720920630','1001UT','','','1001B_MODALL','1001US','1001US','','','1001000010','10010720920630','10012021','10010001','1001BS','10010720920630','','1001US16','10011390','1001ZDIC_OICITYC_ID','1001TA','1001UT','10010000','','10010000Y356','','','1001UG6','1001USMI1390','1001USMI1390','10010720920630','','','','','','','','','','','','','','0','','','','400006910','MSEG_2021_5076000000.parquet',current_timestamp(),null,null),
('110','3198099913','2021','3','0','0','0','0','','','','','','','','','','','','','','0','0','','','','0','0','0','','0','','0','','0','','0','','','0','0','','0','0','','0','','','','','','','','','','','','','','','','','','','0','','','','','0','','0','0','0','','0','','','','','','','','','','','','','','0','','','','','0','0','0','','','','','0','','0','0','0','','','','','0','','0','','0','0','0','','','','0','','','','0','0','','0',null,'0','0','','','','','','','','','','','0','0','','',null,'0',null,'','0','','','','0','0','','','','','',null,'','','0','0','','','','','','','','','','','0','','',null,null,'0:00:00','','','','','0','','','','',null,'0:00:00','0','','0','','','0','0',null,'0:00:00',null,'','0','','','','','','0','0','','','','','','','','0','',null,'','','','','','0','','0','','','','','','0','','0','0','0','0','0','','0','','','0','','','','','','','','','','','','','','','','','','','','',null,null,'','','','0','','','','0','','','','','','','','','','0','0',null,null,'','0','0','0','','','','',null,null,'','','','0','','','','3','A','2022-07-04T07:06:34.000+0000','1001','','','','','','','','','','','','','','','','','','','','','','','1001000000','10010720920630','10012021','10010003','','','','','','1001ZDIC_OICITYC_ID','','','','','','','','','','','10010720920630','','','','','','','','','','','','','','0','','','','','MSEG_2021_3199800000.parquet',current_timestamp(),null,null),
('110','5073212034','2021','1','1','0','0','0','101','','900003861','N506','','','','','','','68049987','','','0','0','','S','EUR','933.73','0','0','','0','','1','EA','1','EA','1','EA','4533586031','100','2021','5073212034','1','0','','0','X','','','NLJKQF','FLESSENREK AN.HS 7 MLO','','OP01','','','','','83730460','','','','','','','2021','','','NL02','','0','','0','0','0','','0','2','','','','','','','','','B','V','','','0','','','','','0','0','0','','','','','0','','0','0','0','','','','','0','200029','0','','0','0','0','','','7480800','1','EA','','','0','0','','0',null,'0','0','800041','','','','900003861','','ME02','WE06','','','0','0','V','',null,'0',null,'','0','','','','1','0','','','','','',null,'','','0','0','','','','','','','','F','','','0','','WE',null,null,'11:02:41','SNRSTY','2 181 601 993','MIGO_GR','','0','','','','',null,'0:00:00','0','','0','','','0','0',null,'11:02:00',null,'','0','','','','','','0','0','','','','','','','','0','',null,'','','','','','0','','0','','','','','','0','','0','0','0','0','0','','0','','','0','','','','','','NL','','','','NL','','','','','','N506','#','','','',null,null,'','','','0','','','','20210408104326','','','','','','','','','','0','0',null,null,'','0','0','0','','','','',null,null,'','','','0','','','','1','C','2021-04-08T11:02:57.000+0000','1001','','','1001WE','','1001101','10010720920630','','','','1001N506','','','','10010720920630','','','1001NLJKQF','1001SNRSTY','1001NL','1001NL','','','1001000000','10010720920630','10012021','10010001','','10010720920630','','1001NL02','','1001ZDIC_OICITYC_ID','','','','','1001N506','','','1001EA','1001NL','1001NL','10010720920630','','','','','','','','','','','','','','0','','','','900003861','MSEG_2021_5076000000.parquet',current_timestamp(),null,null),
('110','3198101012','2021','5','0','0','0','0','','','','','','','','','','','','','','0','0','','','','0','0','0','','0','','0','','0','','0','','','0','0','','0','0','','0','','','','','','','','','','','','','','','','','','','0','','','','','0','','0','0','0','','0','','','','','','','','','','','','','','0','','','','','0','0','0','','','','','0','','0','0','0','','','','','0','','0','','0','0','0','','','','0','','','','0','0','','0',null,'0','0','','','','','','','','','','','0','0','','',null,'0',null,'','0','','','','0','0','','','','','',null,'','','0','0','','','','','','','','','','','0','','',null,null,'0:00:00','','','','','0','','','','',null,'0:00:00','0','','0','','','0','0',null,'0:00:00',null,'','0','','','','','','0','0','','','','','','','','0','',null,'','','','','','0','','0','','','','','','0','','0','0','0','0','0','','0','','','0','','','','','','','','','','','','','','','','','','','','',null,null,'','','','0','','','','0','','','','','','','','','','0','0',null,null,'','0','0','0','','','','',null,null,'','','','0','','','','5','A','2022-07-04T07:06:34.000+0000','1001','','','','','','','','','','','','','','','','','','','','','','','1001000000','10010720920630','10012021','10010005','','','','','','1001ZDIC_OICITYC_ID','','','','','','','','','','','10010720920630','','','','','','','','','','','','','','0','','','','','MSEG_2021_3199800000.parquet',current_timestamp(),null,null),
('110','5073212700','2021','1','1','0','0','0','101','','400004707','C143','113','UT','','','','','69038954','','','0','0','','S','CAD','0','0','0','','0','UT','3160.492','KG','3160.492','KG','3724','L15','4534698253','10','0','','0','0','','0','X','','','','','','OP01','','','','','','','','','','','','2021','','','CA48','','0','','0','0','0','','0','2','','','','','','','','','B','','','','0','','','','','0','0','0','','','','','0','','0','1','0','','','','','0','310001','0','','0','0','0','','','','3724','L15','','69038954','0','0','','0',null,'0','0','310001','','','','400004707','','ME01','WE01','X','X','25520881.55','0','S','',null,'0',null,'','0','','','','1','0','','','','','',null,'','','0','0','','','','','','','','F','','','0','','WE',null,null,'11:24:21','B_MODALL','','MB01','','0','','','','',null,'0:00:00','0','','0','','','0','0',null,'11:00:08',null,'','0','','','','','','0','0','','','1201987497','69038954','3','X','ZTI','0','',null,'','1001312','','','','0','','0','','','','','','0','','0','0','0','0','0','','0','','','0','','','','FRT','MB','CA','','','MB','CA','','','29,702,971','','1','C143','#','','','',null,null,'','','','0','1156250','','','20210408104326','','','92941','UT','UT','HC','IA','','','100','0',null,null,'L15','3724','0','0','','','','',null,null,'','','','0','','','','1','C','2021-04-08T11:25:25.000+0000','1001','','','1001WE','10010720920630','1001101','10010720920630','','','100101','1001C143','','1001HC','','10010720920630','1001UT','','','1001B_MODALL','1001CA','1001CA','','','1001000000','10010720920630','10012021','10010001','1001IA','10010720920630','','1001CA48','','1001ZDIC_OICITYC_ID','1001UT','1001UT','10010113','','10010113C143','','','1001KG','1001CAMB','1001CAMB','10010720920630','','','','','','','','','','','','','','0','','','','400004707','MSEG_2021_5076000000.parquet',current_timestamp(),null,null)""")
    
#msego2

    spark.sql(f"""INSERT INTO `{uc_catalog}`.`{euh_schema}`.msego2
VALUES 
('110','3179719162','2020','6','L','13','3','0','0','5706.624','5706.624','X',null,null,null,null,null,null,null,'MSEGO2_INIT3',current_timestamp(),null,null),
('110','3225060129','2022','1','L20','1','0','0','0','8619.544','8619.544','','1704261931624','10012022','10010001','1001L20','C','2023-01-17T13:22:49.000+0000','1001','MSEG02_20230804_194255_003.parquet',current_timestamp(),null,null),
('110','3179719162','2020','7','BBL','4','0','0','0','5.11879896','5.119','',null,null,null,null,null,null,null,'MSEGO2_INIT3',current_timestamp(),null,null),
('110','3225060139','2022','2','L20','2','0','0','0','589.2867903','589.287','','1704261931624','10012022','10010002','1001L20','C','2023-01-17T13:22:49.000+0000','1001','MSEG02_20230804_194255_003.parquet',current_timestamp(),null,null),
('110','3197961775','2021','1','UGL','0','0','0','0','0','0','','1704261931624','10012021','10010001','1001UGL','A','2022-07-01T13:32:38.000+0000','1001','MSEGO2_2021_3200800000.parquet',current_timestamp(),null,null),
('110','3225060139','2022','3','BB6','3','0','0','0','12.24320576','12.243','','1704261931624','10012022','10010003','1001BB6','C','2023-01-17T13:22:52.000+0000','1001','MSEG02_20230804_194255_003.parquet',current_timestamp(),null,null),
('110','3197962654','2021','1','LB','0','0','0','0','0','0','','1704261931624','10012021','10010001','1001LB','A','2022-07-01T13:32:38.000+0000','1001','MSEGO2_2021_3200800000.parquet',current_timestamp(),null,null),
('110','3225060143','2022','1','UG6','1','0','0','0','2591.393729','2591.394','','1704261931624','10012022','10010001','1001UG6','C','2023-01-17T13:22:52.000+0000','1001','MSEG02_20230804_194255_003.parquet',current_timestamp(),null,null),
('110','3198029690','2021','1','L','0','0','0','0','0','0','','1704261931624','10012021','10010001','1001L','A','2022-07-01T13:32:38.000+0000','1001','MSEGO2_2021_3200800000.parquet',current_timestamp(),null,null),
('110','3225060144','2022','3','L15','3','0','0','0','176.7','176.7','','1704261931624','10012022','10010003','1001L15','C','2023-01-17T13:22:52.000+0000','1001','MSEG02_20230804_194255_003.parquet',current_timestamp(),null,null)""")
    
#bkpf

    spark.sql(f"""INSERT INTO `{uc_catalog}`.`{euh_schema}`.bkpf
VALUES 
('110','ARZ1','37006','2021','RV',null,null,'1',null,'21:30:08',null,null,null,'B_BIL_IAARZ1','','','1943622293','','','0','','USD','85.12','','0','','','0','','SD00','','','','','VBRK','1943622293','','ARS','USD','','0','0','1','','3','','',null,'','30','','M','','','','','','','85.12','','','','','1943','0','','','','',null,'','','','',null,'','','','','','','G','0','0','0','',null,'','','','','','','',null,null,'',null,'0:00:00','','','','','','','','','','',null,'','','','0','','',null,'21:30:08',null,'','1943622293','ARZ100000370062021','C','2021-01-11T21:30:35.000+0000','1001','10010000037006ARZ12021','1001B_BIL_IAARZ1','1001RV','','1001VBRK','1001ARS','1001USD','1001ARZ1','1001USD','','0',null,'0:00:00','1.2021','','0000037006ARZ12021','19436222932021','',null,'','',null,'BKPF_20230804_200444_001.parquet',current_timestamp(),null,null),
('110','CH01','3096538','2022','RV',null,null,'3',null,'20:54:11',null,null,null,'B_BILCH01_14','','','','','','0','','CHF','1','','0','','','0','','SD00','','','','','VBRK','1409333247','','CHF','USD','','1.08085','0','1','','3','','',null,'','30','','M','','','','','','','0','','','','','','0','','','','',null,'','','','',null,'','','','','','','G','0','0','0','',null,'','','','','','','',null,null,'',null,'0:00:00','','','','','','','','','','',null,'','','','0','','',null,'20:54:11',null,'','1409333247','CH0100030965382022','C','2022-03-04T21:01:53.000+0000','1001','10010003096538CH012022','1001B_BILCH01_14','1001RV','','1001VBRK','1001CHF','1001CHF','1001CH01','1001USD','','0',null,'0:00:00','3.2022','','0003096538CH012022','19436222932021','',null,'','',null,'BKPF_20230830_151556_64.parquet',current_timestamp(),'2023-08-30T15:15:56.622+0000',''),
('110','ARZ1','37144','2021','RV',null,null,'2',null,'19:05:35',null,null,null,'PHMGA9','/DS1/SD_C_SBCR212','','1943062241','','','0','','USD','83.9','','0','','','0','','SD00','','','','','VBRK','1944687815','','ARS','USD','','0','0','1','','3','','',null,'','30','','M','','','','','','','83.9','','','','','1943','0','','','','',null,'','','','',null,'','','','','','','G','0','0','0','',null,'','','','','','','',null,null,'',null,'0:00:00','','','','','','','','','','',null,'','','','0','','',null,'19:05:35',null,'','1944687815','ARZ100000371442021','C','2021-02-04T19:05:49.000+0000','1001','10010000037144ARZ12021','1001PHMGA9','1001RV','','1001VBRK','1001ARS','1001USD','1001ARZ1','1001USD','','0',null,'0:00:00','2.2021','','0000037144ARZ12021','19436222932021','',null,'','',null,'BKPF_20230804_200444_001.parquet',current_timestamp(),null,null),
('110','CH01','3096685','2022','RV',null,null,'3',null,'20:56:58',null,null,null,'B_BILCH01_14','','','','','','0','','CHF','1','','0','','','0','','SD00','','','','','VBRK','1409333431','','CHF','USD','','1.08085','0','1','','3','','',null,'','30','','M','','','','','','','0','','','','','','0','','','','',null,'','','','',null,'','','','','','','G','0','0','0','',null,'','','','','','','',null,null,'',null,'0:00:00','','','','','','','','','','',null,'','','','0','','',null,'20:56:58',null,'','1409333431','CH0100030966852022','C','2022-03-04T21:01:53.000+0000','1001','10010003096685CH012022','1001B_BILCH01_14','1001RV','','1001VBRK','1001CHF','1001CHF','1001CH01','1001USD','','0',null,'0:00:00','3.2022','','0003096685CH012022','19436222932021','',null,'','',null,'BKPF_20230830_151556_64.parquet',current_timestamp(),'2023-08-30T15:15:56.622+0000',''),
('110','ARZ1','37364','2021','RV',null,null,'3',null,'21:30:21',null,null,null,'B_BIL_IAARZ1','','','1946269562','','','0','','USD','90.78','','0','','','0','','SD00','','','','','VBRK','1946269562','','ARS','USD','','0','0','1','','3','','',null,'','30','','M','','','','','','','90.78','','','','','1946','0','','','','',null,'','','','',null,'','','','','','','G','0','0','0','',null,'','','','','','','',null,null,'',null,'0:00:00','','','','','','','','','','',null,'','','','0','','',null,'21:30:21',null,'','1946269562','ARZ100000373642021','C','2021-03-12T21:30:24.000+0000','1001','10010000037364ARZ12021','1001B_BIL_IAARZ1','1001RV','','1001VBRK','1001ARS','1001USD','1001ARZ1','1001USD','','0',null,'0:00:00','3.2021','','0000037364ARZ12021','19436222932021','',null,'','',null,'BKPF_20230804_200444_001.parquet',current_timestamp(),null,null),
('110','CH01','3096744','2022','RV',null,null,'3',null,'20:58:14',null,null,null,'B_BILCH01_14','','','','','','0','','CHF','1','','0','','','0','','SD00','','','','','VBRK','1409333490','','CHF','USD','','1.08085','0','1','','3','','',null,'','30','','M','','','','','','','0','','','','','','0','','','','',null,'','','','',null,'','','','','','','G','0','0','0','',null,'','','','','','','',null,null,'',null,'0:00:00','','','','','','','','','','',null,'','','','0','','',null,'20:58:14',null,'','1409333490','CH0100030967442022','C','2022-03-04T21:01:53.000+0000','1001','10010003096744CH012022','1001B_BILCH01_14','1001RV','','1001VBRK','1001CHF','1001CHF','1001CH01','1001USD','','0',null,'0:00:00','3.2022','','0003096744CH012022','19436222932021','',null,'','',null,'BKPF_20230830_151556_64.parquet',current_timestamp(),'2023-08-30T15:15:56.622+0000',''),
('110','ARZ1','37443','2021','RV',null,null,'3',null,'21:30:18',null,null,null,'B_BIL_IAARZ1','','','1946781815','','','0','','USD','91.64','','0','','','0','','SD00','','','','','VBRK','1946781815','','ARS','USD','','0','0','1','','3','','',null,'','30','','M','','','','','','','91.64','','','','','1946','0','','','','',null,'','','','',null,'','','','','','','G','0','0','0','',null,'','','','','','','',null,null,'',null,'0:00:00','','','','','','','','','','',null,'','','','0','','',null,'21:30:18',null,'','1946781815','ARZ100000374432021','C','2021-03-24T21:31:39.000+0000','1001','10010000037443ARZ12021','1001B_BIL_IAARZ1','1001RV','','1001VBRK','1001ARS','1001USD','1001ARZ1','1001USD','','0',null,'0:00:00','3.2021','','0000037443ARZ12021','19436222932021','',null,'','',null,'BKPF_20230804_200444_001.parquet',current_timestamp(),null,null),
('110','CH01','3096860','2022','RV',null,null,'3',null,'15:01:02',null,null,null,'B_BILCH01_14','','','1409360364','','','0','','CHF','1','','0','','','0','','SD00','','','','','VBRK','1409360364','','CHF','USD','','1.0879','0','1','','3','','',null,'','30','','M','','','','','','','0','','','','','','0','','','','',null,'','','','',null,'','','','','','','G','0','0','0','',null,'','','','','','','',null,null,'',null,'0:00:00','','','','','','','','','','',null,'','','','0','','',null,'15:01:02',null,'','1409360364','CH0100030968602022','C','2022-03-05T15:01:26.000+0000','1001','10010003096860CH012022','1001B_BILCH01_14','1001RV','','1001VBRK','1001CHF','1001CHF','1001CH01','1001USD','','0',null,'0:00:00','3.2022','','0003096860CH012022','19436222932021','',null,'','',null,'BKPF_20230830_151556_64.parquet',current_timestamp(),'2023-08-30T15:15:56.622+0000',''),
('110','ARZ1','37538','2021','RV',null,null,'4',null,'15:00:26',null,null,null,'PHMGA9','VF02','','1947257851','','','0','','USD','90.85','','0','','','0','','SD00','','','','','VBRK','1947488389','','ARS','USD','','0','0','1','','3','','',null,'','30','','M','','','','','','','90.85','','','','','1947','0','','','','',null,'','','','',null,'','','','','','','G','0','0','0','',null,'','','','','','','',null,null,'',null,'0:00:00','','','','','','','','','','',null,'','','','0','','',null,'15:00:26',null,'','1947488389','ARZ100000375382021','C','2021-04-08T15:02:18.000+0000','1001','10010000037538ARZ12021','1001PHMGA9','1001RV','','1001VBRK','1001ARS','1001USD','1001ARZ1','1001USD','','0',null,'0:00:00','4.2021','','0000037538ARZ12021','19436222932021','',null,'','',null,'BKPF_20230804_200444_001.parquet',current_timestamp(),null,null),
('110','CH01','3097652','2022','RV',null,null,'3',null,'19:00:20',null,null,null,'B_BIL_IACH01','','','1409398321','','','0','','USD','0.9192','','0','','','0','','SD00','','','','','VBRK','1409398321','','CHF','USD','','0','0','1','','3','','',null,'','30','','M','','','','','','','0','','','','','','0','','','','',null,'','','','',null,'','','','','','','G','0','0','0','',null,'','','','','','','',null,null,'',null,'0:00:00','','','','','','','','','','',null,'','','','0','','',null,'19:00:20',null,'','1409398321','CH0100030976522022','U','2022-03-23T11:09:15.000+0000','1001','10010003097652CH012022','1001B_BIL_IACH01','1001RV','','1001VBRK','1001CHF','1001USD','1001CH01','1001USD','','0',null,'0:00:00','3.2022','','0003097652CH012022','19436222932021','',null,'','',null,'BKPF_20230830_151556_64.parquet',current_timestamp(),'2023-08-30T15:15:56.622+0000','')""")
    
#rprctr_control_table

    spark.sql(f"""INSERT INTO `{uc_catalog}`.`{euh_schema}`.rprctr_control_table
VALUES 
('100001',current_timestamp(),null,null),
('100014',current_timestamp(),null,null),
('100055',current_timestamp(),null,null),
('300053',current_timestamp(),null,null),
('300055',current_timestamp(),null,null),
('300056',current_timestamp(),null,null),
('300057',current_timestamp(),null,null),
('300060',current_timestamp(),null,null),
('300061',current_timestamp(),null,null),
('300062',current_timestamp(),null,null)""")
