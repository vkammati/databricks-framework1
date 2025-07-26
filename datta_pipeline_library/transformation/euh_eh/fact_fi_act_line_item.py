# Databricks notebook source
# DBTITLE 1,Calling Common Functions
# MAGIC %run ./common_functions

# COMMAND ----------

# DBTITLE 1,Importing Required Libraries & Selecting the max_run_start_time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from delta.tables import *
spark = SparkSession.getActiveSession()
spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")

table_name="FACT_FI_ACT_LINE_ITEM"
path = eh_folder_path+"/"+table_name

# COMMAND ----------

# DBTITLE 1,Reading Full data from EUH tables
df_GLPCA_full = spark.sql(f" select * from `{uc_catalog_name}`.`{uc_euh_schema}`.glpca").withColumnRenamed("LAST_DTM","GLPCA_LAST_DTM").withColumnRenamed("LAST_ACTION_CD","GLPCA_LAST_ACTION_CD").withColumnRenamed("OPFLAG","GLPCA_OPFLAG")
df_DS1_HM_MT_BB_full = spark.sql(f" select * from `{uc_catalog_name}`.`{uc_euh_schema}`.ds1hm_mt_bb").withColumnRenamed("LAST_DTM","DS1_HM_MT_BB_LAST_DTM").withColumnRenamed("LAST_ACTION_CD","DS1_HM_MT_BB_LAST_ACTION_CD")
df_HM_MT_BB01_full = spark.sql(f" select * from `{uc_catalog_name}`.`{uc_euh_schema}`.ds1hm_mt_bb01").withColumnRenamed("LAST_DTM","HM_MT_BB01_LAST_DTM").withColumnRenamed("LAST_ACTION_CD","HM_MT_BB01_LAST_ACTION_CD")
df_BKPF_full = spark.sql(f" select * from `{uc_catalog_name}`.`{uc_euh_schema}`.bkpf").withColumnRenamed("LAST_DTM","BKPF_LAST_DTM").withColumnRenamed("LAST_ACTION_CD","BKPF_LAST_ACTION_CD")
df_MSEG_full = spark.sql(f" select * from `{uc_catalog_name}`.`{uc_euh_schema}`.mseg").withColumnRenamed("LAST_DTM","MSEG_LAST_DTM").withColumnRenamed("LAST_ACTION_CD","MSEG_LAST_ACTION_CD")
df_MSEGO2_full = spark.sql(f" select * from `{uc_catalog_name}`.`{uc_euh_schema}`.msego2").withColumnRenamed("LAST_DTM","MSEGO2_LAST_DTM").withColumnRenamed("LAST_ACTION_CD","MSEGO2_LAST_ACTION_CD")
df_RPRCTR_CONTROL_TABLE_full = spark.sql(f" select * from `{uc_catalog_name}`.`{uc_euh_schema}`.rprctr_control_table").withColumnRenamed("ingested_at","rpctr_ingested_at")

# COMMAND ----------

df_BKPF_full = bkpf_filter(df_BKPF_full)
df_MSEG_full = client_filter(df_MSEG_full)
df_MSEGO2_full = mseg02_filter(df_MSEGO2_full)
df_DS1_HM_MT_BB_full = client_filter(df_DS1_HM_MT_BB_full)
df_HM_MT_BB01_full = client_filter(df_HM_MT_BB01_full)

# COMMAND ----------

#Retain only Aecorsoft fields
df_GLPCA_full = combine_date_columns_fcb(df_GLPCA_full,'GLPCA_LAST_DTM','GLPCA_LAST_ACTION_CD','AEDATTM','GLPCA_OPFLAG')
df_BKPF_full = combine_date_columns_fcb(df_BKPF_full,'BKPF_LAST_DTM','BKPF_LAST_ACTION_CD','AEDATTM','OPFLAG')
df_MSEG_full = combine_date_columns_fcb(df_MSEG_full,'MSEG_LAST_DTM','MSEG_LAST_ACTION_CD','AEDATTM','OPFLAG')
df_MSEGO2_full = combine_date_columns_fcb(df_MSEGO2_full,'MSEGO2_LAST_DTM','MSEGO2_LAST_ACTION_CD','AEDATTM','OPFLAG')
df_DS1_HM_MT_BB_full = combine_date_columns_fcb(df_DS1_HM_MT_BB_full,'DS1_HM_MT_BB_LAST_DTM','DS1_HM_MT_BB_LAST_ACTION_CD','AEDATTM','OPFLAG')
df_HM_MT_BB01_full = combine_date_columns_fcb(df_HM_MT_BB01_full,'HM_MT_BB01_LAST_DTM','HM_MT_BB01_LAST_ACTION_CD','AEDATTM','OPFLAG')

# COMMAND ----------

if load_type == "DELTA":
    df_max_run_start_time = get_latest_delta("sede-x-DATTA-FI-EH-workflow", uc_catalog_name, uc_raw_schema)
    print(df_max_run_start_time)

# COMMAND ----------

# DBTITLE 1,BKPF - Delta Records processing (I, U and D)
if load_type == "DELTA":
    df_BKPF_delta = spark.sql(f"select * from `{uc_catalog_name}`.`{uc_euh_schema}`.bkpf where OPFLAG != 'D' and ingested_at >= '{df_max_run_start_time}'")

    df_BKPF_delete_records = spark.sql(f"select * from `{uc_catalog_name}`.`{uc_euh_schema}`.bkpf where OPFLAG == 'D' and ingested_at >= '{df_max_run_start_time}'")
    
    df_BKPF_delta = bkpf_filter(df_BKPF_delta)
    df_BKPF_delete_records = bkpf_filter(df_BKPF_delete_records)
    
    df_BKPF_delta.createOrReplaceTempView("df_BKPF_delta_tv")
    df_BKPF_delete_records.createOrReplaceTempView("df_BKPF_delete_records_tv")
    
    df_BKPF_delta_bkpf = spark.sql(f"select BUKRS,BELNR,GJAHR,BLART,TCODE,XBLNR from df_BKPF_delta_tv")

    df_BKPF_delete_bkpf = spark.sql(f"select BUKRS,BELNR,GJAHR,BLART,TCODE,XBLNR from df_BKPF_delete_records_tv")
    
    df_BKPF_delta_bkpf.createOrReplaceTempView("df_BKPF_delta_bkpf")

    df_BKPF_delete_bkpf.createOrReplaceTempView("df_BKPF_delete_bkpf")

    df_GLPCA_BKPF_delta = spark.sql(f"select glpca.GL_SIRID from `{uc_catalog_name}`.`{uc_euh_schema}`.glpca as glpca where exists ( select 1 from df_BKPF_delta_bkpf as a where a.GJAHR = glpca.RYEAR and a.BUKRS == glpca.RBUKRS  and a.BELNR == glpca.REFDOCNR)")

    df_GLPCA_BKPF_delete = spark.sql(f"select glpca.GL_SIRID from `{uc_catalog_name}`.`{uc_euh_schema}`.glpca as glpca where exists ( select 1 from df_BKPF_delete_bkpf as a where a.GJAHR = glpca.RYEAR  and a.BUKRS == glpca.RBUKRS and a.BELNR == glpca.REFDOCNR)")                                                                                                             

# COMMAND ----------

# DBTITLE 1,MSEG - Delta Processing (I, U and D)
if load_type == "DELTA":   
    df_MSEG_delta = spark.sql(f"select * from `{uc_catalog_name}`.`{uc_euh_schema}`.mseg where OPFLAG != 'D' and ingested_at >= '{df_max_run_start_time}'")

    df_MSEG_delete_records = spark.sql(f"select * from `{uc_catalog_name}`.`{uc_euh_schema}`.mseg where OPFLAG == 'D' and ingested_at >= '{df_max_run_start_time}'")
   
    df_MSEG_delta = client_filter(df_MSEG_delta)
    df_MSEG_delete_records = client_filter(df_MSEG_delete_records)
    
    df_MSEG_delta.createOrReplaceTempView("df_MSEG_delta_tv")
    df_MSEG_delete_records.createOrReplaceTempView("df_MSEG_delete_records_tv")
    
    df_MSEG_delta_mseg = spark.sql(f" select MANDT,MBLNR,MJAHR,ZEILE,BWART,MATNR,WERKS,SHKZG,UMMAT,UMWRK from df_MSEG_delta_tv")

    df_MSEG_delete_mseg = spark.sql(f" select MANDT,MBLNR,MJAHR,ZEILE,BWART,MATNR,WERKS,SHKZG,UMMAT,UMWRK from  df_MSEG_delete_records_tv")

    df_MSEG_delta_mseg.createOrReplaceTempView("df_MSEG_delta_mseg")
    df_MSEG_delete_mseg.createOrReplaceTempView("df_MSEG_delete_mseg")

    df_GLPCA_MSEG_delta = spark.sql(f"""select glpca.GL_SIRID 
                                    from `{uc_catalog_name}`.`{uc_euh_schema}`.glpca as glpca
                                    where exists (
                                        select bkpf.GJAHR,bkpf.BUKRS,bkpf.BELNR from `{uc_catalog_name}`.`{uc_euh_schema}`.bkpf as bkpf 
                                        where exists (
                                            select 1 from df_MSEG_delta_mseg as mseg 
                                            where mseg.MBLNR = substring(bkpf.XBLNR,1,10) and mseg.ZEILE = substring(bkpf.XBLNR,-4,4)
                                            ) 
                                        and bkpf.GJAHR = glpca.RYEAR and bkpf.BUKRS = glpca.RBUKRS and bkpf.BELNR = glpca.REFDOCNR
                                        )""")
    
    df_GLPCA_MSEG_delete = spark.sql(f"""select glpca.GL_SIRID 
                                    from `{uc_catalog_name}`.`{uc_euh_schema}`.glpca as glpca
                                    where exists (
                                        select bkpf.GJAHR,bkpf.BUKRS,bkpf.BELNR from `{uc_catalog_name}`.`{uc_euh_schema}`.bkpf as bkpf 
                                        where exists (
                                            select 1 from df_MSEG_delete_mseg as mseg 
                                            where mseg.MBLNR = substring(bkpf.XBLNR,1,10) and mseg.ZEILE = substring(bkpf.XBLNR,-4,4)
                                            ) 
                                        and bkpf.GJAHR = glpca.RYEAR and bkpf.BUKRS = glpca.RBUKRS  and bkpf.BELNR = glpca.REFDOCNR
                                        )""")

# COMMAND ----------

# DBTITLE 1,MSEG02 - Delta Processing ( I, U and D)
if load_type == "DELTA":   
    df_MSEGO2_delta = spark.sql(f"""select * from `{uc_catalog_name}`.`{uc_euh_schema}`.msego2 where OPFLAG != 'D' and ingested_at >= '{df_max_run_start_time}'""")

    df_MSEGO2_delete_records = spark.sql(f"""select * from `{uc_catalog_name}`.`{uc_euh_schema}`.msego2 where OPFLAG == 'D' and ingested_at >= '{df_max_run_start_time}'""")
   
    df_MSEGO2_delta = mseg02_filter(df_MSEGO2_delta)
    df_MSEGO2_delete_records = mseg02_filter(df_MSEGO2_delete_records)
    
    df_MSEGO2_delta.createOrReplaceTempView("df_MSEGO2_delta_tv")
    df_MSEGO2_delete_records.createOrReplaceTempView("df_MSEGO2_delete_records_tv")
    
    df_MSEGO2_delta_msego2 = spark.sql(f"""select MANDT,MBLNR,MJAHR,ZEILE,MSEHI,ADQNT from df_MSEGO2_delta_tv""")

    df_MSEGO2_delete_msego2 = spark.sql(f"""select MANDT,MBLNR,MJAHR,ZEILE,MSEHI,ADQNT from  df_MSEGO2_delete_records_tv""")

    df_MSEGO2_delta_msego2.createOrReplaceTempView("df_MSEGO2_delta_msego2")
    df_MSEGO2_delete_msego2.createOrReplaceTempView("df_MSEGO2_delete_msego2")

    df_GLPCA_MSEGO2_delta = spark.sql(f"""select glpca.GL_SIRID 
                                    from `{uc_catalog_name}`.`{uc_euh_schema}`.glpca as glpca
                                    where exists (
                                        select bkpf.GJAHR,bkpf.BUKRS,bkpf.BELNR from `{uc_catalog_name}`.`{uc_euh_schema}`.bkpf as bkpf 
                                        where exists (
                                            select mseg.MBLNR,mseg.ZEILE from `{uc_catalog_name}`.`{uc_euh_schema}`.mseg as mseg 
                                             where exists (
                                                select 1 from df_MSEGO2_delta_msego2 as msego2 
                                                where msego2.MBLNR = mseg.MBLNR and msego2.MJAHR = mseg.MJAHR and msego2.ZEILE = mseg.ZEILE
                                                )
                                                and mseg.MBLNR = substring(bkpf.XBLNR,1,10) and mseg.ZEILE = substring(bkpf.XBLNR,-4,4)
                                            )
                                        and bkpf.GJAHR = glpca.RYEAR and bkpf.BUKRS = glpca.RBUKRS  and bkpf.BELNR = glpca.REFDOCNR)""")
    
    df_GLPCA_MSEGO2_delete = spark.sql(f"""select glpca.GL_SIRID 
                                    from `{uc_catalog_name}`.`{uc_euh_schema}`.glpca as glpca
                                    where exists (
                                        select bkpf.GJAHR,bkpf.BUKRS,bkpf.BELNR from `{uc_catalog_name}`.`{uc_euh_schema}`.bkpf as bkpf 
                                        where exists (
                                            select mseg.MBLNR,mseg.ZEILE from `{uc_catalog_name}`.`{uc_euh_schema}`.mseg as mseg 
                                             where exists (
                                                select 1 from df_MSEGO2_delete_msego2 as msego2 
                                                where msego2.MBLNR = mseg.MBLNR and msego2.MJAHR = mseg.MJAHR and msego2.ZEILE = mseg.ZEILE
                                                )
                                                and mseg.MBLNR = substring(bkpf.XBLNR,1,10) and mseg.ZEILE = substring(bkpf.XBLNR,-4,4)
                                            )
                                        and bkpf.GJAHR = glpca.RYEAR and bkpf.BUKRS = glpca.RBUKRS  and bkpf.BELNR = glpca.REFDOCNR)""")

# COMMAND ----------

# DBTITLE 1,ds1hm_mt_bb delta logic
if load_type == "DELTA":
    df_DS1HM_MT_BB_delta = spark.sql(f"select * from `{uc_catalog_name}`.`{uc_euh_schema}`.ds1hm_mt_bb where ingested_at >= '{df_max_run_start_time}'")
    df_DS1HM_MT_BB_delta = client_filter(df_DS1HM_MT_BB_delta).select("MANDT","DOCCT","GL_SIRID","UMWRK","OIC_MOT","WGT_QTY","VOL_UOM_L15","VOL_QTY")
    df_DS1HM_MT_BB_delta.createOrReplaceTempView("df_DS1HM_MT_BB_delta_tv")
    
    df_GLPCA_DS1HM_MT_BB_delta = spark.sql(f"""select glpca.GL_SIRID from `{uc_catalog_name}`.`{uc_euh_schema}`.glpca as glpca where exists ( select 1 from df_DS1HM_MT_BB_delta_tv as a where a.GL_SIRID = glpca.GL_SIRID and a.DOCCT = glpca.DOCCT)""")                                                                       

# COMMAND ----------

# DBTITLE 1,ds1hm_mt_bb01 delta logic
if load_type == "DELTA":
    df_DS1MT_BB01_delta = spark.sql(f"select * from `{uc_catalog_name}`.`{uc_euh_schema}`.ds1hm_mt_bb01 where ingested_at >= '{df_max_run_start_time}'")
    df_DS1MT_BB01_delta = client_filter(df_DS1MT_BB01_delta).select("MANDT","RYEAR","RBUKRS","RLDNR","DOCCT","DOCNR","DOCLN","KONNR","VGBEL","PARCEL_ID","ZSTI_REFNO","DEALNO","ZXBLDAT","OID_EXTBOL","BGI_CARGOID","BGI_DELIVERYID","BGI_PARCELID","VOL_QTY_GAL","VOL_UOM_GAL","VOL_QTY_BB6","VOL_UOM_BB6")
    df_DS1MT_BB01_delta.createOrReplaceTempView("df_DS1MT_BB01_delta_tv")
    
    df_GLPCA_DS1HM_MT_BB01_delta = spark.sql(f"""select glpca.GL_SIRID from `{uc_catalog_name}`.`{uc_euh_schema}`.glpca as glpca where exists ( select 1 from df_DS1MT_BB01_delta_tv as a where a.RLDNR = glpca.RLDNR and a.RYEAR = glpca.RYEAR and a.DOCNR = glpca.DOCNR and a.DOCLN = glpca.DOCLN and a.RBUKRS = glpca.RBUKRS and a.DOCCT = glpca.DOCCT)""")                                                                                              


# COMMAND ----------

# DBTITLE 1,Full and Delta Load Logic
if load_type == "DELTA":   

    df_GLPCA_delta = spark.sql(f"select * from `{uc_catalog_name}`.`{uc_euh_schema}`.glpca where ingested_at >= '{df_max_run_start_time}'").withColumnRenamed("LAST_DTM","GLPCA_LAST_DTM").withColumnRenamed("LAST_ACTION_CD","GLPCA_LAST_ACTION_CD").withColumnRenamed("OPFLAG","GLPCA_OPFLAG")

    df_GLPCA_delta =df_GLPCA_delta.withColumn("Source_ID",concat(lit(gsap_source_system_name),lit('_'),col('RCLNT')))

    """ Applying required filters for each tables """
    df_GLPCA_delta = glpca_filter(df_GLPCA_delta)

    #Separate deleted records from inserted / updated records. This is done to have nulls in all fields except primary keys for delete records in driving table.
    df_GLPCA_delta_deleted=df_GLPCA_delta.filter(F.col('GLPCA_OPFLAG')=='D').select('GL_SIRID','GLPCA_OPFLAG','Source_ID', 'RCLNT').withColumnRenamed("GL_SIRID", "Rec_No_Line_Itm_Rec").withColumnRenamed("RCLNT", "Client")

    # *** DELTA records logic
    df_GLPCA_delta_GL_SIRID = df_GLPCA_delta.filter(F.col('GLPCA_OPFLAG')!='D').select("GL_SIRID")
    
    df_GLPCA_final_delta = df_GLPCA_delta_GL_SIRID.union(df_GLPCA_BKPF_delta).union(df_GLPCA_MSEG_delta).union(df_GLPCA_MSEGO2_delta).union(df_GLPCA_DS1HM_MT_BB01_delta).union(df_GLPCA_DS1HM_MT_BB_delta).distinct()

    df_GLPCA_final_delete = df_GLPCA_BKPF_delete.union(df_GLPCA_MSEG_delete).union(df_GLPCA_MSEGO2_delete).distinct()

    df_GLPCA_final_delta_delete = df_GLPCA_final_delta.union(df_GLPCA_final_delete)

    df_RPRCTR_CONTROL_TABLE= df_RPRCTR_CONTROL_TABLE_full
    
    df_GLPCA_final_delta_delete.createOrReplaceTempView("df_GLPCA_final_delta_delete")

    df_GLPCA_final = spark.sql(f"""select * FROM `{uc_catalog_name}`.`{uc_euh_schema}`.glpca AS glpca WHERE EXISTS (SELECT GL_SIRID FROM df_GLPCA_final_delta_delete WHERE glpca.GL_SIRID = GL_SIRID)""").withColumn("Source_ID",concat(lit(gsap_source_system_name),lit('_'),col('RCLNT'))).withColumnRenamed("LAST_DTM","GLPCA_LAST_DTM").withColumnRenamed("LAST_ACTION_CD","GLPCA_LAST_ACTION_CD").withColumnRenamed("OPFLAG","GLPCA_OPFLAG")

    df_GLPCA = glpca_filter(df_GLPCA_final)

    df_GLPCA = df_GLPCA.join(df_RPRCTR_CONTROL_TABLE,["RPRCTR"], "inner")

    df_DS1_HM_MT_BB = df_DS1_HM_MT_BB_full
    df_HM_MT_BB01 = df_HM_MT_BB01_full
    
    df_BKPF = df_BKPF_full
    df_MSEG = df_MSEG_full
    df_MSEGO2 = df_MSEGO2_full

    df_GLPCA = df_GLPCA.select("RCLNT","GL_SIRID","RLDNR","RYEAR","RTCUR","DRCRK","POPER","DOCCT","DOCNR","DOCLN","RBUKRS","RPRCTR","RFAREA","RACCT","SPRCTR","HSL","KSL","BLDAT","BUDAT","REFDOCNR","REFRYEAR","REFDOCLN","REFDOCCT","WERKS","MATNR","KUNNR","LIFNR","EBELN","KDAUF","VTWEG","SPART","BWART","BLART", "Source_ID", "ingested_at", "GLPCA_OPFLAG")

    df_GLPCA = df_GLPCA.withColumnRenamed("BLART","GLPCA_BLART") \
        .withColumnRenamed("BWART","GLPCA_BWART") \
        .withColumnRenamed("DOCCT","GLPCA_DOCCT") \
        .withColumnRenamed("DOCLN","GLPCA_DOCLN") \
        .withColumnRenamed("DOCNR","GLPCA_DOCNR") \
        .withColumnRenamed("DRCRK","GLPCA_DRCRK") \
        .withColumnRenamed("EBELN","GLPCA_EBELN") \
        .withColumnRenamed("GL_SIRID","GLPCA_GL_SIRID") \
        .withColumnRenamed("HSL","GLPCA_HSL") \
        .withColumnRenamed("KDAUF","GLPCA_KDAUF") \
        .withColumnRenamed("KSL","GLPCA_KSL") \
        .withColumnRenamed("KUNNR","GLPCA_KUNNR") \
        .withColumnRenamed("LIFNR","GLPCA_LIFNR") \
        .withColumnRenamed("MATNR","GLPCA_MATNR") \
        .withColumnRenamed("POPER","GLPCA_POPER") \
        .withColumnRenamed("RACCT","GLPCA_RACCT") \
        .withColumnRenamed("RBUKRS","GLPCA_RBUKRS") \
        .withColumnRenamed("RCLNT","GLPCA_RCLNT") \
        .withColumnRenamed("REFDOCCT","GLPCA_REFDOCCT") \
        .withColumnRenamed("REFDOCLN","GLPCA_REFDOCLN") \
        .withColumnRenamed("REFDOCNR","GLPCA_REFDOCNR") \
        .withColumnRenamed("REFRYEAR","GLPCA_REFRYEAR") \
        .withColumnRenamed("RFAREA","GLPCA_RFAREA") \
        .withColumnRenamed("RLDNR","GLPCA_RLDNR") \
        .withColumnRenamed("RPRCTR","GLPCA_RPRCTR") \
        .withColumnRenamed("RTCUR","GLPCA_RTCUR") \
        .withColumnRenamed("RYEAR","GLPCA_RYEAR") \
        .withColumnRenamed("SPART","GLPCA_SPART") \
        .withColumnRenamed("SPRCTR","GLPCA_SPRCTR") \
        .withColumnRenamed("VTWEG","GLPCA_VTWEG") \
        .withColumnRenamed("WERKS","GLPCA_WERKS")

    """ Reading DS1_HM_MT_BB Table """

    df_DS1_HM_MT_BB = df_DS1_HM_MT_BB.select("MANDT","DOCCT","GL_SIRID","UMWRK","OIC_MOT","WGT_QTY","VOL_UOM_L15","VOL_QTY")

    df_DS1_HM_MT_BB = df_DS1_HM_MT_BB.withColumnRenamed("GL_SIRID","BB_GL_SIRID")\
        .withColumnRenamed("OIC_MOT","BB_OIC_MOT")\
        .withColumnRenamed("WGT_QTY","BB_WGT_QTY")\
        .withColumnRenamed("VOL_UOM_L15","BB_VOL_UOM_L15")\
        .withColumnRenamed("VOL_QTY","BB_VOL_QTY")\
        .withColumnRenamed("UMWRK","BB_UMWRK")

    df_DS1_HM_MT_BB = df_DS1_HM_MT_BB.withColumn("BB_VOL_GAL",col("BB_VOL_QTY"))\
                                     .withColumn("BB_VOL_BBL",col("BB_VOL_QTY"))

    """ Joining GLPCA Table with df_DS1_HM_MT_BB Table and creating df_glpca_bb """

    df_glpca_bb = df_GLPCA.join(df_DS1_HM_MT_BB,((df_GLPCA.GLPCA_GL_SIRID == df_DS1_HM_MT_BB.BB_GL_SIRID)&(df_GLPCA.GLPCA_DOCCT == df_DS1_HM_MT_BB.DOCCT)),how="leftouter")

    df_glpca_bb = df_glpca_bb.select(*[col(column_name) for column_name in df_glpca_bb.columns if column_name not in {'MANDT','BB_GL_SIRID','DOCCT'}])

    """ Reading HM_MT_BB01 Table """
    df_HM_MT_BB01 = df_HM_MT_BB01.select("MANDT","RYEAR","RBUKRS","RLDNR","DOCCT","DOCNR","DOCLN","KONNR","VGBEL","PARCEL_ID","ZSTI_REFNO","DEALNO","ZXBLDAT","OID_EXTBOL","BGI_CARGOID","BGI_DELIVERYID","BGI_PARCELID","VOL_QTY_GAL","VOL_UOM_GAL","VOL_QTY_BB6","VOL_UOM_BB6")
    
    df_HM_MT_BB01 = df_HM_MT_BB01.withColumnRenamed("KONNR","BB01_KONNR") \
        .withColumnRenamed("VGBEL","BB01_VGBEL") \
        .withColumnRenamed("PARCEL_ID","BB01_PARCEL_ID") \
        .withColumnRenamed("ZSTI_REFNO","BB01_ZSTI_REFNO") \
        .withColumnRenamed("DEALNO","BB01_DEALNO") \
        .withColumnRenamed("ZXBLDAT","BB01_ZXBLDAT") \
        .withColumnRenamed("OID_EXTBOL","BB01_OID_EXTBOL") \
        .withColumnRenamed("BGI_CARGOID","BGI_CARGOID") \
        .withColumnRenamed("BGI_DELIVERYID","BGI_DELIVERYID") \
        .withColumnRenamed("BGI_PARCELID","BGI_PARCELID")
    
    """ Joining df_glpca_bb with df_HM_MT_BB01 Table and creating df_glpca_bb_bb01 """

    df_glpca_bb_bb01 = df_glpca_bb.join(df_HM_MT_BB01,((df_glpca_bb.GLPCA_RLDNR == df_HM_MT_BB01.RLDNR)&(df_glpca_bb.GLPCA_RYEAR == df_HM_MT_BB01.RYEAR)&(df_glpca_bb.GLPCA_DOCNR == df_HM_MT_BB01.DOCNR)&(df_glpca_bb.GLPCA_DOCLN == df_HM_MT_BB01.DOCLN)&(df_glpca_bb.GLPCA_RBUKRS == df_HM_MT_BB01.RBUKRS)&(df_glpca_bb.GLPCA_DOCCT == df_HM_MT_BB01.DOCCT)),how = "leftouter")

    df_glpca_bb_bb01 = df_glpca_bb_bb01.select(*[col(column_name) for column_name in df_glpca_bb_bb01.columns if column_name not in {'GLPCA_DOCCT','MANDT','RYEAR','RBUKRS','RLDNR','DOCCT','DOCNR','DOCLN'}])

    """ Reading BKPF Table """

    df_BKPF = df_BKPF.select("BUKRS","BELNR","GJAHR","BLART","TCODE","XBLNR")
    df_BKPF = df_BKPF.withColumnRenamed("BUKRS","BKPF_BUKRS") \
                        .withColumnRenamed("BELNR","BKPF_BELNR") \
                        .withColumnRenamed("GJAHR","BKPF_GJAHR") \
                        .withColumnRenamed("BLART","BKPF_BLART") \
                        .withColumnRenamed("TCODE","BKPF_TCODE") \
                        .withColumnRenamed("XBLNR","BKPF_XBLNR") 
    
    df_BKPF =df_BKPF.withColumn("BKPF_XBLNR_LEFT10",substring(df_BKPF.BKPF_XBLNR,1,10))
    df_BKPF =df_BKPF.withColumn("BKPF_XBLNR_RIGHT4",substring(df_BKPF.BKPF_XBLNR,-4,4))

    """ Joining df_glpca_bb_bb01 with df_BKPF Table and creating df_glpca_bb_bb01_bkpf """

    df_glpca_bb_bb01_bkpf = df_glpca_bb_bb01.join(df_BKPF,((df_glpca_bb_bb01.GLPCA_RYEAR == df_BKPF.BKPF_GJAHR)&(df_glpca_bb_bb01.GLPCA_RBUKRS == df_BKPF.BKPF_BUKRS)&(df_glpca_bb_bb01.GLPCA_REFDOCNR == df_BKPF.BKPF_BELNR)),how="leftouter")

    df_glpca_bb_bb01_bkpf = df_glpca_bb_bb01_bkpf.select(*[col(column_name) for column_name in df_glpca_bb_bb01_bkpf.columns if column_name not in {'BKPF_BUKRS','BKPF_BELNR','BKPF_GJAHR','BKPF_BLART','BKPF_TCODE','BKPF_XBLNR'}])

    """ Reading MSEG Table """

    df_MSEG = df_MSEG.select("MANDT","MBLNR","MJAHR","ZEILE","BWART","MATNR","WERKS","SHKZG","UMMAT","UMWRK")
    df_MSEG = df_MSEG.withColumnRenamed("MATNR","MSEG_MATNR")\
        .withColumnRenamed("UMWRK","MSEG_UMWRK")\
        .withColumnRenamed("WERKS","MSEG_WERKS")\
        .withColumnRenamed("UMMAT","MSEG_UMMAT")\
        .withColumnRenamed("MANDT","MSEG_MANDT")\
        .withColumnRenamed("MBLNR","MSEG_MBLNR")\
        .withColumnRenamed("MJAHR","MSEG_MJAHR")\
        .withColumnRenamed("ZEILE","MSEG_ZEILE")\
        .withColumnRenamed("BWART","MSEG_BWART")\
        .withColumnRenamed("SHKZG","MSEG_SHKZG")

    """ Joining df_glpca_bb_bb01_bkpf with df_MSEG Table and creating df_glpca_bb_bb01_bkpf_mseg """

    df_glpca_bb_bb01_bkpf_mseg = df_glpca_bb_bb01_bkpf.join(df_MSEG,((df_glpca_bb_bb01_bkpf.BKPF_XBLNR_LEFT10 == df_MSEG.MSEG_MBLNR)&(df_glpca_bb_bb01_bkpf.BKPF_XBLNR_RIGHT4 == df_MSEG.MSEG_ZEILE)),how="leftouter")\
        .withColumn("Z_NEW_PLANT",when(df_MSEG.MSEG_SHKZG != df_glpca_bb_bb01_bkpf.GLPCA_DRCRK,df_MSEG.MSEG_WERKS).otherwise(df_MSEG.MSEG_UMWRK))\
        .withColumn("Z_NEW_UMWRK",when(df_MSEG.MSEG_SHKZG == df_glpca_bb_bb01_bkpf.GLPCA_DRCRK,df_MSEG.MSEG_WERKS).otherwise(df_MSEG.MSEG_UMWRK))\
        .withColumn("Z_NEW_MATNR",when(df_MSEG.MSEG_SHKZG != df_glpca_bb_bb01_bkpf.GLPCA_DRCRK,df_MSEG.MSEG_MATNR).otherwise(df_MSEG.MSEG_UMMAT))\
        .withColumn("Z_NEW_UMMAT",when(df_MSEG.MSEG_SHKZG == df_glpca_bb_bb01_bkpf.GLPCA_DRCRK,df_MSEG.MSEG_MATNR).otherwise(df_MSEG.MSEG_UMMAT))

    df_glpca_bb_bb01_bkpf_mseg = df_glpca_bb_bb01_bkpf_mseg.select(*[col(column_name) for column_name in df_glpca_bb_bb01_bkpf_mseg.columns if column_name not in {'MANDT','MSEG_MANDT'}])

    """ Reading MSEGO2 Table """

    df_MSEGO2 = df_MSEGO2.select("MANDT","MBLNR","MJAHR","ZEILE","MSEHI","ADQNT")

    df_MSEGO2 = df_MSEGO2.withColumnRenamed("MBLNR","MSEGO2_MBLNR") \
                .withColumnRenamed("MJAHR","MSEGO2_MJAHR") \
                .withColumnRenamed("ZEILE","MSEGO2_ZEILE") \
                .withColumnRenamed("MSEHI","MSEGO2_MSEHI") \
                .withColumnRenamed("ADQNT","MSEGO2_ADQNT") 

    """ Joining df_glpca_bb_bb01_bkpf_mseg with df_MSEGO2 Table and creating df_glpca_bb_bb01_bkpf_mseg_msego2 """

    df_glpca_bb_bb01_bkpf_mseg_msego2 = df_glpca_bb_bb01_bkpf_mseg.join(df_MSEGO2,((df_glpca_bb_bb01_bkpf_mseg.MSEG_MBLNR == df_MSEGO2.MSEGO2_MBLNR)&(df_glpca_bb_bb01_bkpf_mseg.MSEG_MJAHR == df_MSEGO2.MSEGO2_MJAHR)&(df_glpca_bb_bb01_bkpf_mseg.MSEG_ZEILE == df_MSEGO2.MSEGO2_ZEILE)),"leftouter").withColumn("Z_MSEGO2_ADQNT",when(df_glpca_bb_bb01_bkpf_mseg.GLPCA_DRCRK == 'S',df_MSEGO2.MSEGO2_ADQNT).otherwise(((df_MSEGO2.MSEGO2_ADQNT)*-1)))\
                               .withColumn("CM_GL_FA_001",concat(col("GLPCA_RACCT"),col("GLPCA_RFAREA")))

    df_glpca_bb_bb01_bkpf_mseg_msego2 = df_glpca_bb_bb01_bkpf_mseg_msego2.select(*[col(column_name) for column_name in df_glpca_bb_bb01_bkpf_mseg_msego2.columns if column_name not in {'MSEG_MATNR','MSEG_WERKS','MSEG_UMMAT','MSEG_UMWRK','MANDT','MSEGO2_MBLNR','MSEGO2_MJAHR','MSEGO2_ZEILE','MSEGO2_MSEHI'}])

    df_glpca_bb_bb01_bkpf_mseg_msego2 = df_glpca_bb_bb01_bkpf_mseg_msego2.withColumn("CL_RR_MM_MATERIAL_KEY",substring(df_glpca_bb_bb01_bkpf_mseg_msego2.Z_NEW_MATNR,10,18))\
                       .withColumn("CL_RR_SENDING_RECEIVING_MAT",substring(df_glpca_bb_bb01_bkpf_mseg_msego2.Z_NEW_UMMAT,10,18))\
                       .withColumn("CL_RR_UOM",lit("GAL"))\
                       .withColumn("CL_RR_CURRENCY",lit("Local Currency"))\
                       .withColumn("CL_RR_MOVEMENT_TYPE_PLUS",when((col("GLPCA_BWART") == '551') |(col("GLPCA_BWART") == '552'),"Scrapping")\
                                                            .when((col("GLPCA_BWART") == '701') |(col("GLPCA_BWART") == '702'),"Warehouse Gains & losses")\
                                                            .when((col("GLPCA_BWART") == '951') |(col("GLPCA_BWART") == '952'),"InTransit Gains & losses")\
                                                            .when((col("GLPCA_BWART") == 'Y51') |(col("GLPCA_BWART") == 'Y52'),"Tank Level Gains & losses")\
                                                            .otherwise(""))\
                       .withColumn("CL_RR_PROFIT_CENTER",substring(df_glpca_bb_bb01_bkpf_mseg_msego2.GLPCA_RPRCTR,5,10))\
                       .withColumn("CL_RR_MOVEMENT_GROUP",lit(""))\
                       .withColumn("CL_RR_GL_ACCOUNT_KEY",substring(df_glpca_bb_bb01_bkpf_mseg_msego2.GLPCA_RACCT,4,10))\
                       .withColumn("CL_RR_MATERIAL_KEY",substring(df_glpca_bb_bb01_bkpf_mseg_msego2.GLPCA_MATNR,10,18))
    
    df_glpca_bb_bb01_bkpf_mseg_msego2 = df_glpca_bb_bb01_bkpf_mseg_msego2.withColumnRenamed("GLPCA_RCLNT", "Client")\
                        .withColumnRenamed("GLPCA_GL_SIRID", "Rec_No_Line_Itm_Rec")\
                        .withColumnRenamed("GLPCA_RLDNR", "Ledger")\
                        .withColumnRenamed("GLPCA_RYEAR", "Fiscal_Year")\
                        .withColumnRenamed("GLPCA_RTCUR", "Currency_Key")\
                        .withColumnRenamed("GLPCA_DRCRK", "Debit_Credit_Indicator")\
                        .withColumnRenamed("GLPCA_POPER", "Posting_Period")\
                        .withColumnRenamed("GLPCA_DOCNR", "Accounting_Document_No")\
                        .withColumnRenamed("GLPCA_DOCLN", "Document_Line")\
                        .withColumnRenamed("GLPCA_RBUKRS", "Company_Code")\
                        .withColumnRenamed("GLPCA_RPRCTR", "Profit_Center")\
                        .withColumnRenamed("GLPCA_RFAREA", "Functional_Area")\
                        .withColumnRenamed("GLPCA_RACCT", "Account_No")\
                        .withColumnRenamed("GLPCA_SPRCTR", "Partner_Profit_Center")\
                        .withColumnRenamed("GLPCA_HSL", "Amt_Comp_Code_Curr")\
                        .withColumnRenamed("GLPCA_KSL", "Amt_PC_Local_Curr")\
                        .withColumnRenamed("BLDAT", "Document_Date")\
                        .withColumnRenamed("BUDAT", "Document_Posting_Date")\
                        .withColumnRenamed("GLPCA_REFDOCNR", "Ref_Document_No")\
                        .withColumnRenamed("GLPCA_REFRYEAR", "Ref_Fiscal_Year")\
                        .withColumnRenamed("GLPCA_REFDOCLN", "Line_Itm_Account_Doc_No")\
                        .withColumnRenamed("GLPCA_REFDOCCT", "Ref_Document_Type")\
                        .withColumnRenamed("GLPCA_WERKS", "Plant")\
                        .withColumnRenamed("GLPCA_MATNR", "Material_Number")\
                        .withColumnRenamed("GLPCA_KUNNR", "Customer_Number")\
                        .withColumnRenamed("GLPCA_LIFNR", "Account_No_Supplier")\
                        .withColumnRenamed("GLPCA_EBELN", "Purchasing_Document_No")\
                        .withColumnRenamed("GLPCA_KDAUF", "Sales_Order_Doc_No")\
                        .withColumnRenamed("GLPCA_VTWEG", "Distribution_Channel")\
                        .withColumnRenamed("GLPCA_SPART", "Division")\
                        .withColumnRenamed("GLPCA_BWART", "Mvmt_Type")\
                        .withColumnRenamed("GLPCA_BLART", "Document_Type")\
                        .withColumnRenamed("BB_UMWRK", "Receiving_Issue_Plant")\
                        .withColumnRenamed("BB_OIC_MOT", "Ext_Mode_Transport")\
                        .withColumnRenamed("BB_WGT_QTY", "Weight_Qty")\
                        .withColumnRenamed("BB_VOL_UOM_L15", "Volume_Unit_L15")\
                        .withColumnRenamed("BB_VOL_QTY", "Volume_Qty")\
                        .withColumnRenamed("BB_VOL_GAL", "Volume_GAL")\
                        .withColumnRenamed("BB_VOL_BBL", "Volume_BBL")\
                        .withColumnRenamed("BB01_KONNR", "No_Principal_Prchg_Agmt")\
                        .withColumnRenamed("BB01_VGBEL", "Doc_No_Ref_Doc")\
                        .withColumnRenamed("BB01_PARCEL_ID", "Parcel_ID")\
                        .withColumnRenamed("BB01_ZSTI_REFNO", "Smart_Contract_No")\
                        .withColumnRenamed("BB01_DEALNO", "OIL_TSW_Deal_Number")\
                        .withColumnRenamed("BB01_ZXBLDAT", "Document_Date_In_Doc")\
                        .withColumnRenamed("BB01_OID_EXTBOL", "External_Bill_Of_Lading")\
                        .withColumnRenamed("BGI_CARGOID", "Endur_Cargo_ID")\
                        .withColumnRenamed("BGI_DELIVERYID", "BGI_Delivery_ID")\
                        .withColumnRenamed("BGI_PARCELID", "Endur_Parcel_ID")\
                        .withColumnRenamed("VOL_QTY_GAL", "Volume_Qty_GAL")\
                        .withColumnRenamed("VOL_UOM_GAL", "Volume_Unit_GAL")\
                        .withColumnRenamed("VOL_QTY_BB6", "Volume_Qty_BB6")\
                        .withColumnRenamed("VOL_UOM_BB6", "Volume_Unit_BB6")\
                        .withColumnRenamed("BKPF_XBLNR_LEFT10", "CL_LT_Reference_Doc_No")\
                        .withColumnRenamed("BKPF_XBLNR_RIGHT4", "CL_RT_Reference_Doc_No")\
                        .withColumnRenamed("MSEG_MBLNR", "Material_Document_Number")\
                        .withColumnRenamed("MSEG_MJAHR", "Material_Document_Year")\
                        .withColumnRenamed("MSEG_ZEILE", "Material_Document_Itm")\
                        .withColumnRenamed("MSEG_BWART", "MM_Mvmt_Type")\
                        .withColumnRenamed("MSEG_SHKZG", "Debit_Or_Credit_Indicator")\
                        .withColumnRenamed("Z_NEW_PLANT", "MM_Plant")\
                        .withColumnRenamed("Z_NEW_UMWRK", "MM_Receiving_Plant")\
                        .withColumnRenamed("Z_NEW_MATNR", "MM_Material")\
                        .withColumnRenamed("Z_NEW_UMMAT", "MM_Receiving_Material")\
                        .withColumnRenamed("MSEGO2_ADQNT", "Add_Oil_Gas_Qty")\
                        .withColumnRenamed("Z_MSEGO2_ADQNT", "MM_Volume_L15")\
                        .withColumnRenamed("CM_GL_FA_001", "CL_Account_No_Functional_Area")\
                        .withColumnRenamed("CL_RR_MM_MATERIAL_KEY", "MM_Material_Key")\
                        .withColumnRenamed("CL_RR_SENDING_RECEIVING_MAT", "CL_Receiving_Issuing_Mat_Key")\
                        .withColumnRenamed("CL_RR_UOM", "CL_UOM_In_GAL")\
                        .withColumnRenamed("CL_RR_CURRENCY", "CL_Lcl_Currency")\
                        .withColumnRenamed("CL_RR_MOVEMENT_TYPE_PLUS", "CL_Mvmt_Type_Plus")\
                        .withColumnRenamed("CL_RR_PROFIT_CENTER", "CL_Profit_Center")\
                        .withColumnRenamed("CL_RR_MOVEMENT_GROUP", "CL_Mvmt_Group")\
                        .withColumnRenamed("CL_RR_GL_ACCOUNT_KEY", "CL_GL_Account_Key")\
                        .withColumnRenamed("CL_RR_MATERIAL_KEY", "Material_Key")

    #If delta changes are from driving table, then retain driving table's OPFLAG, else update it to U
    df_glpca_bb_bb01_bkpf_mseg_msego2 = df_glpca_bb_bb01_bkpf_mseg_msego2.withColumn('GLPCA_OPFLAG',F.when(F.col('ingested_at') >= df_max_run_start_time,F.col('GLPCA_OPFLAG')).otherwise(F.lit('U')))

    #Union deleted records from driving table keeping only primary keys and OPFLAG as D
    df_glpca_bb_bb01_bkpf_mseg_msego2 = df_glpca_bb_bb01_bkpf_mseg_msego2.unionByName(df_GLPCA_delta_deleted, allowMissingColumns=True).drop('ingested_at')

    df_glpca_bb_bb01_bkpf_mseg_msego2 = df_glpca_bb_bb01_bkpf_mseg_msego2.withColumnRenamed("GLPCA_OPFLAG", "OPFLAG").distinct()

    deltaTable = DeltaTable.forName(spark, f"`{uc_catalog_name}`.`{uc_eh_schema}`.{table_name}")

    (deltaTable.alias('target') \
    .merge(df_glpca_bb_bb01_bkpf_mseg_msego2.alias('source'), "target.Source_ID = source.Source_ID and target.Rec_No_Line_Itm_Rec = source.Rec_No_Line_Itm_Rec")	
    .whenMatchedUpdate( set =
    {
      "Client": "source.Client",
        "Rec_No_Line_Itm_Rec": "source.Rec_No_Line_Itm_Rec",
        "Ledger": "source.Ledger",
        "Fiscal_Year": "source.Fiscal_Year",
        "Currency_Key": "source.Currency_Key",
        "Debit_Credit_Indicator": "source.Debit_Credit_Indicator",
        "Posting_Period": "source.Posting_Period",
        "Accounting_Document_No": "source.Accounting_Document_No",
        "Document_Line": "source.Document_Line",
        "Company_Code": "source.Company_Code",
        "Profit_Center": "source.Profit_Center",
        "Functional_Area": "source.Functional_Area",
        "Account_No": "source.Account_No",
        "Partner_Profit_Center": "source.Partner_Profit_Center",
        "Amt_Comp_Code_Curr": "source.Amt_Comp_Code_Curr",
        "Amt_PC_Local_Curr": "source.Amt_PC_Local_Curr",
        "Document_Date": "source.Document_Date",
        "Document_Posting_Date": "source.Document_Posting_Date",
        "Ref_Document_No": "source.Ref_Document_No",
        "Ref_Fiscal_Year": "source.Ref_Fiscal_Year",
        "Line_Itm_Account_Doc_No": "source.Line_Itm_Account_Doc_No",
        "Ref_Document_Type": "source.Ref_Document_Type",
        "Plant": "source.Plant",
        "Material_Number": "source.Material_Number",
        "Customer_Number": "source.Customer_Number",
        "Account_No_Supplier": "source.Account_No_Supplier",
        "Purchasing_Document_No": "source.Purchasing_Document_No",
        "Sales_Order_Doc_No": "source.Sales_Order_Doc_No",
        "Distribution_Channel": "source.Distribution_Channel",
        "Division": "source.Division",
        "Mvmt_Type": "source.Mvmt_Type",
        "Document_Type": "source.Document_Type",
        "Receiving_Issue_Plant": "source.Receiving_Issue_Plant",
        "Ext_Mode_Transport": "source.Ext_Mode_Transport",
        "Weight_Qty": "source.Weight_Qty",
        "Volume_Unit_L15": "source.Volume_Unit_L15",
        "Volume_Qty": "source.Volume_Qty",
        "Volume_GAL": "source.Volume_GAL",
        "Volume_BBL": "source.Volume_BBL",
        "No_Principal_Prchg_Agmt": "source.No_Principal_Prchg_Agmt",
        "Doc_No_Ref_Doc": "source.Doc_No_Ref_Doc",
        "Parcel_ID": "source.Parcel_ID",
        "Smart_Contract_No": "source.Smart_Contract_No",
        "OIL_TSW_Deal_Number": "source.OIL_TSW_Deal_Number",
        "Document_Date_In_Doc": "source.Document_Date_In_Doc",
        "External_Bill_Of_Lading": "source.External_Bill_Of_Lading",
        "Endur_Cargo_ID": "source.Endur_Cargo_ID",
        "BGI_Delivery_ID": "source.BGI_Delivery_ID",
        "Endur_Parcel_ID": "source.Endur_Parcel_ID",
        "Volume_Qty_GAL": "source.Volume_Qty_GAL",
        "Volume_Unit_GAL": "source.Volume_Unit_GAL",
        "Volume_Qty_BB6": "source.Volume_Qty_BB6",
        "Volume_Unit_BB6": "source.Volume_Unit_BB6",
        "CL_LT_Reference_Doc_No": "source.CL_LT_Reference_Doc_No",
        "CL_RT_Reference_Doc_No": "source.CL_RT_Reference_Doc_No",
        "Material_Document_Number": "source.Material_Document_Number",
        "Material_Document_Year": "source.Material_Document_Year",
        "Material_Document_Itm": "source.Material_Document_Itm",
        "MM_Mvmt_Type": "source.MM_Mvmt_Type",
        "Debit_Or_Credit_Indicator": "source.Debit_Or_Credit_Indicator",
        "MM_Plant": "source.MM_Plant",
        "MM_Receiving_Plant": "source.MM_Receiving_Plant",
        "MM_Material": "source.MM_Material",
        "MM_Receiving_Material": "source.MM_Receiving_Material",
        "Add_Oil_Gas_Qty": "source.Add_Oil_Gas_Qty",
        "MM_Volume_L15": "source.MM_Volume_L15",
        "CL_Account_No_Functional_Area": "source.CL_Account_No_Functional_Area",
        "MM_Material_Key": "source.MM_Material_Key",
        "CL_Receiving_Issuing_Mat_Key": "source.CL_Receiving_Issuing_Mat_Key",
        "CL_UOM_In_GAL": "source.CL_UOM_In_GAL",
        "CL_Lcl_Currency": "source.CL_Lcl_Currency",
        "CL_Mvmt_Type_Plus": "source.CL_Mvmt_Type_Plus",
        "CL_Profit_Center": "source.CL_Profit_Center",
        "CL_Mvmt_Group": "source.CL_Mvmt_Group",
        "CL_GL_Account_Key": "source.CL_GL_Account_Key",
        "Material_Key": "source.Material_Key",
        "ingested_at": current_timestamp(),
        "Source_ID": "source.Source_ID",
        "OPFLAG" : "source.OPFLAG"
    }) \
    .whenNotMatchedInsert(values =
    {
      "Client": "source.Client",
        "Rec_No_Line_Itm_Rec": "source.Rec_No_Line_Itm_Rec",
        "Ledger": "source.Ledger",
        "Fiscal_Year": "source.Fiscal_Year",
        "Currency_Key": "source.Currency_Key",
        "Debit_Credit_Indicator": "source.Debit_Credit_Indicator",
        "Posting_Period": "source.Posting_Period",
        "Accounting_Document_No": "source.Accounting_Document_No",
        "Document_Line": "source.Document_Line",
        "Company_Code": "source.Company_Code",
        "Profit_Center": "source.Profit_Center",
        "Functional_Area": "source.Functional_Area",
        "Account_No": "source.Account_No",
        "Partner_Profit_Center": "source.Partner_Profit_Center",
        "Amt_Comp_Code_Curr": "source.Amt_Comp_Code_Curr",
        "Amt_PC_Local_Curr": "source.Amt_PC_Local_Curr",
        "Document_Date": "source.Document_Date",
        "Document_Posting_Date": "source.Document_Posting_Date",
        "Ref_Document_No": "source.Ref_Document_No",
        "Ref_Fiscal_Year": "source.Ref_Fiscal_Year",
        "Line_Itm_Account_Doc_No": "source.Line_Itm_Account_Doc_No",
        "Ref_Document_Type": "source.Ref_Document_Type",
        "Plant": "source.Plant",
        "Material_Number": "source.Material_Number",
        "Customer_Number": "source.Customer_Number",
        "Account_No_Supplier": "source.Account_No_Supplier",
        "Purchasing_Document_No": "source.Purchasing_Document_No",
        "Sales_Order_Doc_No": "source.Sales_Order_Doc_No",
        "Distribution_Channel": "source.Distribution_Channel",
        "Division": "source.Division",
        "Mvmt_Type": "source.Mvmt_Type",
        "Document_Type": "source.Document_Type",
        "Receiving_Issue_Plant": "source.Receiving_Issue_Plant",
        "Ext_Mode_Transport": "source.Ext_Mode_Transport",
        "Weight_Qty": "source.Weight_Qty",
        "Volume_Unit_L15": "source.Volume_Unit_L15",
        "Volume_Qty": "source.Volume_Qty",
        "Volume_GAL": "source.Volume_GAL",
        "Volume_BBL": "source.Volume_BBL",
        "No_Principal_Prchg_Agmt": "source.No_Principal_Prchg_Agmt",
        "Doc_No_Ref_Doc": "source.Doc_No_Ref_Doc",
        "Parcel_ID": "source.Parcel_ID",
        "Smart_Contract_No": "source.Smart_Contract_No",
        "OIL_TSW_Deal_Number": "source.OIL_TSW_Deal_Number",
        "Document_Date_In_Doc": "source.Document_Date_In_Doc",
        "External_Bill_Of_Lading": "source.External_Bill_Of_Lading",
        "Endur_Cargo_ID": "source.Endur_Cargo_ID",
        "BGI_Delivery_ID": "source.BGI_Delivery_ID",
        "Endur_Parcel_ID": "source.Endur_Parcel_ID",
        "Volume_Qty_GAL": "source.Volume_Qty_GAL",
        "Volume_Unit_GAL": "source.Volume_Unit_GAL",
        "Volume_Qty_BB6": "source.Volume_Qty_BB6",
        "Volume_Unit_BB6": "source.Volume_Unit_BB6",
        "CL_LT_Reference_Doc_No": "source.CL_LT_Reference_Doc_No",
        "CL_RT_Reference_Doc_No": "source.CL_RT_Reference_Doc_No",
        "Material_Document_Number": "source.Material_Document_Number",
        "Material_Document_Year": "source.Material_Document_Year",
        "Material_Document_Itm": "source.Material_Document_Itm",
        "MM_Mvmt_Type": "source.MM_Mvmt_Type",
        "Debit_Or_Credit_Indicator": "source.Debit_Or_Credit_Indicator",
        "MM_Plant": "source.MM_Plant",
        "MM_Receiving_Plant": "source.MM_Receiving_Plant",
        "MM_Material": "source.MM_Material",
        "MM_Receiving_Material": "source.MM_Receiving_Material",
        "Add_Oil_Gas_Qty": "source.Add_Oil_Gas_Qty",
        "MM_Volume_L15": "source.MM_Volume_L15",
        "CL_Account_No_Functional_Area": "source.CL_Account_No_Functional_Area",
        "MM_Material_Key": "source.MM_Material_Key",
        "CL_Receiving_Issuing_Mat_Key": "source.CL_Receiving_Issuing_Mat_Key",
        "CL_UOM_In_GAL": "source.CL_UOM_In_GAL",
        "CL_Lcl_Currency": "source.CL_Lcl_Currency",
        "CL_Mvmt_Type_Plus": "source.CL_Mvmt_Type_Plus",
        "CL_Profit_Center": "source.CL_Profit_Center",
        "CL_Mvmt_Group": "source.CL_Mvmt_Group",
        "CL_GL_Account_Key": "source.CL_GL_Account_Key",
        "Material_Key": "source.Material_Key",
        "ingested_at": current_timestamp(),
        "Source_ID": "source.Source_ID",
        "OPFLAG" : "source.OPFLAG"
    })
    .execute()
    )       
else:
    """ Reading GLPCA Table to Fetch data for last 2 years"""

    
    df_GLPCA_full = glpca_filter(df_GLPCA_full)
    #Separate updates and inserts from deletes
    df_GLPCA_deleted=df_GLPCA_full.filter(F.col('GLPCA_OPFLAG')=='D').select('GL_SIRID','GLPCA_OPFLAG','RCLNT').withColumnRenamed("GL_SIRID", "Rec_No_Line_Itm_Rec").withColumnRenamed("RCLNT", "Client")
    
    df_GLPCA = df_GLPCA_full.filter(F.col('GLPCA_OPFLAG')!='D')
    df_DS1_HM_MT_BB = df_DS1_HM_MT_BB_full.filter(F.col('OPFLAG')!='D')
    df_HM_MT_BB01 = df_HM_MT_BB01_full.filter(F.col('OPFLAG')!='D')
    df_BKPF = df_BKPF_full.filter(F.col('OPFLAG')!='D')
    df_MSEG = df_MSEG_full.filter(F.col('OPFLAG')!='D')
    df_MSEGO2= df_MSEGO2_full.filter(F.col('OPFLAG')!='D')
    df_RPRCTR_CONTROL_TABLE= df_RPRCTR_CONTROL_TABLE_full
    
    df_DS1_HM_MT_BB = client_filter(df_DS1_HM_MT_BB)
    df_HM_MT_BB01 = client_filter(df_HM_MT_BB01)
    df_BKPF = bkpf_filter(df_BKPF)
    df_MSEG = client_filter(df_MSEG)
    df_MSEGO2 = mseg02_filter(df_MSEGO2)
    
    df_GLPCA  = df_GLPCA.withColumn("Current_Year", date_format(current_date(), "YYYY-MM-dd"))
    df_GLPCA  = df_GLPCA.withColumn("Previous_Year", add_months(col("Current_Year"),-24))
    df_GLPCA = df_GLPCA.filter(df_GLPCA.BUDAT >= df_GLPCA.Previous_Year)
    
    df_GLPCA = df_GLPCA.join(df_RPRCTR_CONTROL_TABLE,["RPRCTR"], "inner")
    df_GLPCA = df_GLPCA.select("RCLNT","GL_SIRID","RLDNR","RYEAR","RTCUR","DRCRK","POPER","DOCCT","DOCNR","DOCLN","RBUKRS","RPRCTR","RFAREA","RACCT","SPRCTR","HSL","KSL","BLDAT","BUDAT","REFDOCNR","REFRYEAR","REFDOCLN","REFDOCCT","WERKS","MATNR","KUNNR","LIFNR","EBELN","KDAUF","VTWEG","SPART","BWART","BLART", "GLPCA_OPFLAG")
    
    df_GLPCA = df_GLPCA.withColumnRenamed("BLART","GLPCA_BLART") \
        .withColumnRenamed("BWART","GLPCA_BWART") \
        .withColumnRenamed("DOCCT","GLPCA_DOCCT") \
        .withColumnRenamed("DOCLN","GLPCA_DOCLN") \
        .withColumnRenamed("DOCNR","GLPCA_DOCNR") \
        .withColumnRenamed("DRCRK","GLPCA_DRCRK") \
        .withColumnRenamed("EBELN","GLPCA_EBELN") \
        .withColumnRenamed("GL_SIRID","GLPCA_GL_SIRID") \
        .withColumnRenamed("HSL","GLPCA_HSL") \
        .withColumnRenamed("KDAUF","GLPCA_KDAUF") \
        .withColumnRenamed("KSL","GLPCA_KSL") \
        .withColumnRenamed("KUNNR","GLPCA_KUNNR") \
        .withColumnRenamed("LIFNR","GLPCA_LIFNR") \
        .withColumnRenamed("MATNR","GLPCA_MATNR") \
        .withColumnRenamed("POPER","GLPCA_POPER") \
        .withColumnRenamed("RACCT","GLPCA_RACCT") \
        .withColumnRenamed("RBUKRS","GLPCA_RBUKRS") \
        .withColumnRenamed("RCLNT","GLPCA_RCLNT") \
        .withColumnRenamed("REFDOCCT","GLPCA_REFDOCCT") \
        .withColumnRenamed("REFDOCLN","GLPCA_REFDOCLN") \
        .withColumnRenamed("REFDOCNR","GLPCA_REFDOCNR") \
        .withColumnRenamed("REFRYEAR","GLPCA_REFRYEAR") \
        .withColumnRenamed("RFAREA","GLPCA_RFAREA") \
        .withColumnRenamed("RLDNR","GLPCA_RLDNR") \
        .withColumnRenamed("RPRCTR","GLPCA_RPRCTR") \
        .withColumnRenamed("RTCUR","GLPCA_RTCUR") \
        .withColumnRenamed("RYEAR","GLPCA_RYEAR") \
        .withColumnRenamed("SPART","GLPCA_SPART") \
        .withColumnRenamed("SPRCTR","GLPCA_SPRCTR") \
        .withColumnRenamed("VTWEG","GLPCA_VTWEG") \
        .withColumnRenamed("WERKS","GLPCA_WERKS")
    
    """ Reading DS1_HM_MT_BB Table """

    df_DS1_HM_MT_BB = df_DS1_HM_MT_BB.select("MANDT","DOCCT","GL_SIRID","UMWRK","OIC_MOT","WGT_QTY","VOL_UOM_L15","VOL_QTY")

    df_DS1_HM_MT_BB = df_DS1_HM_MT_BB.withColumnRenamed("GL_SIRID","BB_GL_SIRID")\
        .withColumnRenamed("OIC_MOT","BB_OIC_MOT")\
        .withColumnRenamed("WGT_QTY","BB_WGT_QTY")\
        .withColumnRenamed("VOL_UOM_L15","BB_VOL_UOM_L15")\
        .withColumnRenamed("VOL_QTY","BB_VOL_QTY")\
        .withColumnRenamed("UMWRK","BB_UMWRK")

    df_DS1_HM_MT_BB = df_DS1_HM_MT_BB.withColumn("BB_VOL_GAL",col("BB_VOL_QTY"))\
                                     .withColumn("BB_VOL_BBL",col("BB_VOL_QTY"))

    """ Joining GLPCA Table with df_DS1_HM_MT_BB Table and creating df_glpca_bb """

    df_glpca_bb = df_GLPCA.join(df_DS1_HM_MT_BB,((df_GLPCA.GLPCA_GL_SIRID == df_DS1_HM_MT_BB.BB_GL_SIRID)&(df_GLPCA.GLPCA_DOCCT == df_DS1_HM_MT_BB.DOCCT)),how="leftouter")

    df_glpca_bb = df_glpca_bb.select(*[col(column_name) for column_name in df_glpca_bb.columns if column_name not in {'MANDT','BB_GL_SIRID','DOCCT'}])

    """ Reading HM_MT_BB01 Table """
    
    df_HM_MT_BB01 = df_HM_MT_BB01.select("MANDT","RYEAR","RBUKRS","RLDNR","DOCCT","DOCNR","DOCLN","KONNR","VGBEL","PARCEL_ID","ZSTI_REFNO","DEALNO","ZXBLDAT","OID_EXTBOL","BGI_CARGOID","BGI_DELIVERYID","BGI_PARCELID","VOL_QTY_GAL","VOL_UOM_GAL","VOL_QTY_BB6","VOL_UOM_BB6")

    df_HM_MT_BB01 = df_HM_MT_BB01.withColumnRenamed("KONNR","BB01_KONNR") \
        .withColumnRenamed("VGBEL","BB01_VGBEL") \
        .withColumnRenamed("PARCEL_ID","BB01_PARCEL_ID") \
        .withColumnRenamed("ZSTI_REFNO","BB01_ZSTI_REFNO") \
        .withColumnRenamed("DEALNO","BB01_DEALNO") \
        .withColumnRenamed("ZXBLDAT","BB01_ZXBLDAT") \
        .withColumnRenamed("OID_EXTBOL","BB01_OID_EXTBOL") \
        .withColumnRenamed("BGI_CARGOID","BGI_CARGOID") \
        .withColumnRenamed("BGI_DELIVERYID","BGI_DELIVERYID") \
        .withColumnRenamed("BGI_PARCELID","BGI_PARCELID")
    
    """ Joining df_glpca_bb with df_HM_MT_BB01 Table and creating df_glpca_bb_bb01 """

    df_glpca_bb_bb01 = df_glpca_bb.join(df_HM_MT_BB01,((df_glpca_bb.GLPCA_RLDNR == df_HM_MT_BB01.RLDNR)&(df_glpca_bb.GLPCA_RYEAR == df_HM_MT_BB01.RYEAR)&(df_glpca_bb.GLPCA_DOCNR == df_HM_MT_BB01.DOCNR)&(df_glpca_bb.GLPCA_DOCLN == df_HM_MT_BB01.DOCLN)&(df_glpca_bb.GLPCA_RBUKRS == df_HM_MT_BB01.RBUKRS)&(df_glpca_bb.GLPCA_DOCCT == df_HM_MT_BB01.DOCCT)),how = "leftouter")

    df_glpca_bb_bb01 = df_glpca_bb_bb01.select(*[col(column_name) for column_name in df_glpca_bb_bb01.columns if column_name not in {'GLPCA_DOCCT','MANDT','RYEAR','RBUKRS','RLDNR','DOCCT','DOCNR','DOCLN'}])

    
    """ Reading BKPF Table """

    df_BKPF = df_BKPF.select("BUKRS","BELNR","GJAHR","BLART","TCODE","XBLNR")
    df_BKPF = df_BKPF.withColumnRenamed("BUKRS","BKPF_BUKRS") \
                        .withColumnRenamed("BELNR","BKPF_BELNR") \
                        .withColumnRenamed("GJAHR","BKPF_GJAHR") \
                        .withColumnRenamed("BLART","BKPF_BLART") \
                        .withColumnRenamed("TCODE","BKPF_TCODE") \
                        .withColumnRenamed("XBLNR","BKPF_XBLNR") 
    
    df_BKPF =df_BKPF.withColumn("BKPF_XBLNR_LEFT10",substring(df_BKPF.BKPF_XBLNR,1,10))
    df_BKPF =df_BKPF.withColumn("BKPF_XBLNR_RIGHT4",substring(df_BKPF.BKPF_XBLNR,-4,4))

    """ Joining df_glpca_bb_bb01 with df_BKPF Table and creating df_glpca_bb_bb01_bkpf """

    df_glpca_bb_bb01_bkpf = df_glpca_bb_bb01.join(df_BKPF,((df_glpca_bb_bb01.GLPCA_RYEAR == df_BKPF.BKPF_GJAHR)&(df_glpca_bb_bb01.GLPCA_RBUKRS == df_BKPF.BKPF_BUKRS)&(df_glpca_bb_bb01.GLPCA_REFDOCNR == df_BKPF.BKPF_BELNR)),how="leftouter")

    df_glpca_bb_bb01_bkpf = df_glpca_bb_bb01_bkpf.select(*[col(column_name) for column_name in df_glpca_bb_bb01_bkpf.columns if column_name not in {'BKPF_BUKRS','BKPF_BELNR','BKPF_GJAHR','BKPF_BLART','BKPF_TCODE','BKPF_XBLNR'}])

    """ Reading MSEG Table """

    df_MSEG = df_MSEG.select("MANDT","MBLNR","MJAHR","ZEILE","BWART","MATNR","WERKS","SHKZG","UMMAT","UMWRK")
    df_MSEG = df_MSEG.withColumnRenamed("MATNR","MSEG_MATNR")\
        .withColumnRenamed("UMWRK","MSEG_UMWRK")\
        .withColumnRenamed("WERKS","MSEG_WERKS")\
        .withColumnRenamed("UMMAT","MSEG_UMMAT")\
        .withColumnRenamed("MANDT","MSEG_MANDT")\
        .withColumnRenamed("MBLNR","MSEG_MBLNR")\
        .withColumnRenamed("MJAHR","MSEG_MJAHR")\
        .withColumnRenamed("ZEILE","MSEG_ZEILE")\
        .withColumnRenamed("BWART","MSEG_BWART")\
        .withColumnRenamed("SHKZG","MSEG_SHKZG")

    """ Joining df_glpca_bb_bb01_bkpf with df_MSEG Table and creating df_glpca_bb_bb01_bkpf_mseg """

    df_glpca_bb_bb01_bkpf_mseg = df_glpca_bb_bb01_bkpf.join(df_MSEG,((df_glpca_bb_bb01_bkpf.BKPF_XBLNR_LEFT10 == df_MSEG.MSEG_MBLNR)&(df_glpca_bb_bb01_bkpf.BKPF_XBLNR_RIGHT4 == df_MSEG.MSEG_ZEILE)),how="leftouter")\
        .withColumn("Z_NEW_PLANT",when(df_MSEG.MSEG_SHKZG != df_glpca_bb_bb01_bkpf.GLPCA_DRCRK,df_MSEG.MSEG_WERKS).otherwise(df_MSEG.MSEG_UMWRK))\
        .withColumn("Z_NEW_UMWRK",when(df_MSEG.MSEG_SHKZG == df_glpca_bb_bb01_bkpf.GLPCA_DRCRK,df_MSEG.MSEG_WERKS).otherwise(df_MSEG.MSEG_UMWRK))\
        .withColumn("Z_NEW_MATNR",when(df_MSEG.MSEG_SHKZG != df_glpca_bb_bb01_bkpf.GLPCA_DRCRK,df_MSEG.MSEG_MATNR).otherwise(df_MSEG.MSEG_UMMAT))\
        .withColumn("Z_NEW_UMMAT",when(df_MSEG.MSEG_SHKZG == df_glpca_bb_bb01_bkpf.GLPCA_DRCRK,df_MSEG.MSEG_MATNR).otherwise(df_MSEG.MSEG_UMMAT))

    df_glpca_bb_bb01_bkpf_mseg = df_glpca_bb_bb01_bkpf_mseg.select(*[col(column_name) for column_name in df_glpca_bb_bb01_bkpf_mseg.columns if column_name not in {'MANDT','MSEG_MANDT'}])

    """ Reading MSEGO2 Table """

    df_MSEGO2 = df_MSEGO2.select("MANDT","MBLNR","MJAHR","ZEILE","MSEHI","ADQNT")

    df_MSEGO2 = df_MSEGO2.withColumnRenamed("MBLNR","MSEGO2_MBLNR") \
                .withColumnRenamed("MJAHR","MSEGO2_MJAHR") \
                .withColumnRenamed("ZEILE","MSEGO2_ZEILE") \
                .withColumnRenamed("MSEHI","MSEGO2_MSEHI") \
                .withColumnRenamed("ADQNT","MSEGO2_ADQNT") 

    """ Joining df_glpca_bb_bb01_bkpf_mseg with df_MSEGO2 Table and creating df_glpca_bb_bb01_bkpf_mseg_msego2 """

    df_glpca_bb_bb01_bkpf_mseg_msego2 = df_glpca_bb_bb01_bkpf_mseg.join(df_MSEGO2,((df_glpca_bb_bb01_bkpf_mseg.MSEG_MBLNR == df_MSEGO2.MSEGO2_MBLNR)&(df_glpca_bb_bb01_bkpf_mseg.MSEG_MJAHR == df_MSEGO2.MSEGO2_MJAHR)&(df_glpca_bb_bb01_bkpf_mseg.MSEG_ZEILE == df_MSEGO2.MSEGO2_ZEILE)),"leftouter").withColumn("Z_MSEGO2_ADQNT",when(df_glpca_bb_bb01_bkpf_mseg.GLPCA_DRCRK == 'S',df_MSEGO2.MSEGO2_ADQNT).otherwise(((df_MSEGO2.MSEGO2_ADQNT)*-1)))\
                               .withColumn("CM_GL_FA_001",concat(col("GLPCA_RACCT"),col("GLPCA_RFAREA")))

    df_glpca_bb_bb01_bkpf_mseg_msego2 = df_glpca_bb_bb01_bkpf_mseg_msego2.select(*[col(column_name) for column_name in df_glpca_bb_bb01_bkpf_mseg_msego2.columns if column_name not in {'MSEG_MATNR','MSEG_WERKS','MSEG_UMMAT','MSEG_UMWRK','MANDT','MSEGO2_MBLNR','MSEGO2_MJAHR','MSEGO2_ZEILE','MSEGO2_MSEHI'}])

    df_glpca_bb_bb01_bkpf_mseg_msego2 = df_glpca_bb_bb01_bkpf_mseg_msego2.withColumn("CL_RR_MM_MATERIAL_KEY",substring(df_glpca_bb_bb01_bkpf_mseg_msego2.Z_NEW_MATNR,10,18))\
                       .withColumn("CL_RR_SENDING_RECEIVING_MAT",substring(df_glpca_bb_bb01_bkpf_mseg_msego2.Z_NEW_UMMAT,10,18))\
                       .withColumn("CL_RR_UOM",lit("GAL"))\
                       .withColumn("CL_RR_CURRENCY",lit("Local Currency"))\
                       .withColumn("CL_RR_MOVEMENT_TYPE_PLUS",when((col("GLPCA_BWART") == '551') |(col("GLPCA_BWART") == '552'),"Scrapping")\
                                                            .when((col("GLPCA_BWART") == '701') |(col("GLPCA_BWART") == '702'),"Warehouse Gains & losses")\
                                                            .when((col("GLPCA_BWART") == '951') |(col("GLPCA_BWART") == '952'),"InTransit Gains & losses")\
                                                            .when((col("GLPCA_BWART") == 'Y51') |(col("GLPCA_BWART") == 'Y52'),"Tank Level Gains & losses")\
                                                            .otherwise(""))\
                       .withColumn("CL_RR_PROFIT_CENTER",substring(df_glpca_bb_bb01_bkpf_mseg_msego2.GLPCA_RPRCTR,5,10))\
                       .withColumn("CL_RR_MOVEMENT_GROUP",lit(""))\
                       .withColumn("CL_RR_GL_ACCOUNT_KEY",substring(df_glpca_bb_bb01_bkpf_mseg_msego2.GLPCA_RACCT,4,10))\
                       .withColumn("CL_RR_MATERIAL_KEY",substring(df_glpca_bb_bb01_bkpf_mseg_msego2.GLPCA_MATNR,10,18))
    
    df_glpca_bb_bb01_bkpf_mseg_msego2 =df_glpca_bb_bb01_bkpf_mseg_msego2.withColumn("ingested_at",current_timestamp()).withColumn("Source_ID",concat(lit(gsap_source_system_name),lit('_'),col('GLPCA_RCLNT')))
    
    df_glpca_bb_bb01_bkpf_mseg_msego2 = df_glpca_bb_bb01_bkpf_mseg_msego2.withColumnRenamed("GLPCA_RCLNT", "Client")\
                        .withColumnRenamed("GLPCA_GL_SIRID", "Rec_No_Line_Itm_Rec")\
                        .withColumnRenamed("GLPCA_RLDNR", "Ledger")\
                        .withColumnRenamed("GLPCA_RYEAR", "Fiscal_Year")\
                        .withColumnRenamed("GLPCA_RTCUR", "Currency_Key")\
                        .withColumnRenamed("GLPCA_DRCRK", "Debit_Credit_Indicator")\
                        .withColumnRenamed("GLPCA_POPER", "Posting_Period")\
                        .withColumnRenamed("GLPCA_DOCNR", "Accounting_Document_No")\
                        .withColumnRenamed("GLPCA_DOCLN", "Document_Line")\
                        .withColumnRenamed("GLPCA_RBUKRS", "Company_Code")\
                        .withColumnRenamed("GLPCA_RPRCTR", "Profit_Center")\
                        .withColumnRenamed("GLPCA_RFAREA", "Functional_Area")\
                        .withColumnRenamed("GLPCA_RACCT", "Account_No")\
                        .withColumnRenamed("GLPCA_SPRCTR", "Partner_Profit_Center")\
                        .withColumnRenamed("GLPCA_HSL", "Amt_Comp_Code_Curr")\
                        .withColumnRenamed("GLPCA_KSL", "Amt_PC_Local_Curr")\
                        .withColumnRenamed("BLDAT", "Document_Date")\
                        .withColumnRenamed("BUDAT", "Document_Posting_Date")\
                        .withColumnRenamed("GLPCA_REFDOCNR", "Ref_Document_No")\
                        .withColumnRenamed("GLPCA_REFRYEAR", "Ref_Fiscal_Year")\
                        .withColumnRenamed("GLPCA_REFDOCLN", "Line_Itm_Account_Doc_No")\
                        .withColumnRenamed("GLPCA_REFDOCCT", "Ref_Document_Type")\
                        .withColumnRenamed("GLPCA_WERKS", "Plant")\
                        .withColumnRenamed("GLPCA_MATNR", "Material_Number")\
                        .withColumnRenamed("GLPCA_KUNNR", "Customer_Number")\
                        .withColumnRenamed("GLPCA_LIFNR", "Account_No_Supplier")\
                        .withColumnRenamed("GLPCA_EBELN", "Purchasing_Document_No")\
                        .withColumnRenamed("GLPCA_KDAUF", "Sales_Order_Doc_No")\
                        .withColumnRenamed("GLPCA_VTWEG", "Distribution_Channel")\
                        .withColumnRenamed("GLPCA_SPART", "Division")\
                        .withColumnRenamed("GLPCA_BWART", "Mvmt_Type")\
                        .withColumnRenamed("GLPCA_BLART", "Document_Type")\
                        .withColumnRenamed("BB_UMWRK", "Receiving_Issue_Plant")\
                        .withColumnRenamed("BB_OIC_MOT", "Ext_Mode_Transport")\
                        .withColumnRenamed("BB_WGT_QTY", "Weight_Qty")\
                        .withColumnRenamed("BB_VOL_UOM_L15", "Volume_Unit_L15")\
                        .withColumnRenamed("BB_VOL_QTY", "Volume_Qty")\
                        .withColumnRenamed("BB_VOL_GAL", "Volume_GAL")\
                        .withColumnRenamed("BB_VOL_BBL", "Volume_BBL")\
                        .withColumnRenamed("BB01_KONNR", "No_Principal_Prchg_Agmt")\
                        .withColumnRenamed("BB01_VGBEL", "Doc_No_Ref_Doc")\
                        .withColumnRenamed("BB01_PARCEL_ID", "Parcel_ID")\
                        .withColumnRenamed("BB01_ZSTI_REFNO", "Smart_Contract_No")\
                        .withColumnRenamed("BB01_DEALNO", "OIL_TSW_Deal_Number")\
                        .withColumnRenamed("BB01_ZXBLDAT", "Document_Date_In_Doc")\
                        .withColumnRenamed("BB01_OID_EXTBOL", "External_Bill_Of_Lading")\
                        .withColumnRenamed("BGI_CARGOID", "Endur_Cargo_ID")\
                        .withColumnRenamed("BGI_DELIVERYID", "BGI_Delivery_ID")\
                        .withColumnRenamed("BGI_PARCELID", "Endur_Parcel_ID")\
                        .withColumnRenamed("VOL_QTY_GAL", "Volume_Qty_GAL")\
                        .withColumnRenamed("VOL_UOM_GAL", "Volume_Unit_GAL")\
                        .withColumnRenamed("VOL_QTY_BB6", "Volume_Qty_BB6")\
                        .withColumnRenamed("VOL_UOM_BB6", "Volume_Unit_BB6")\
                        .withColumnRenamed("BKPF_XBLNR_LEFT10", "CL_LT_Reference_Doc_No")\
                        .withColumnRenamed("BKPF_XBLNR_RIGHT4", "CL_RT_Reference_Doc_No")\
                        .withColumnRenamed("MSEG_MBLNR", "Material_Document_Number")\
                        .withColumnRenamed("MSEG_MJAHR", "Material_Document_Year")\
                        .withColumnRenamed("MSEG_ZEILE", "Material_Document_Itm")\
                        .withColumnRenamed("MSEG_BWART", "MM_Mvmt_Type")\
                        .withColumnRenamed("MSEG_SHKZG", "Debit_Or_Credit_Indicator")\
                        .withColumnRenamed("Z_NEW_PLANT", "MM_Plant")\
                        .withColumnRenamed("Z_NEW_UMWRK", "MM_Receiving_Plant")\
                        .withColumnRenamed("Z_NEW_MATNR", "MM_Material")\
                        .withColumnRenamed("Z_NEW_UMMAT", "MM_Receiving_Material")\
                        .withColumnRenamed("MSEGO2_ADQNT", "Add_Oil_Gas_Qty")\
                        .withColumnRenamed("Z_MSEGO2_ADQNT", "MM_Volume_L15")\
                        .withColumnRenamed("CM_GL_FA_001", "CL_Account_No_Functional_Area")\
                        .withColumnRenamed("CL_RR_MM_MATERIAL_KEY", "MM_Material_Key")\
                        .withColumnRenamed("CL_RR_SENDING_RECEIVING_MAT", "CL_Receiving_Issuing_Mat_Key")\
                        .withColumnRenamed("CL_RR_UOM", "CL_UOM_In_GAL")\
                        .withColumnRenamed("CL_RR_CURRENCY", "CL_Lcl_Currency")\
                        .withColumnRenamed("CL_RR_MOVEMENT_TYPE_PLUS", "CL_Mvmt_Type_Plus")\
                        .withColumnRenamed("CL_RR_PROFIT_CENTER", "CL_Profit_Center")\
                        .withColumnRenamed("CL_RR_MOVEMENT_GROUP", "CL_Mvmt_Group")\
                        .withColumnRenamed("CL_RR_GL_ACCOUNT_KEY", "CL_GL_Account_Key")\
                        .withColumnRenamed("CL_RR_MATERIAL_KEY", "Material_Key")

    #Add deleted records with all columns except primary keys and opflag as nulls              
    df_glpca_bb_bb01_bkpf_mseg_msego2 = df_glpca_bb_bb01_bkpf_mseg_msego2.unionByName(df_GLPCA_deleted, allowMissingColumns=True)

    df_glpca_bb_bb01_bkpf_mseg_msego2 = df_glpca_bb_bb01_bkpf_mseg_msego2.withColumnRenamed("GLPCA_OPFLAG", "OPFLAG").distinct()

    df_glpca_bb_bb01_bkpf_mseg_msego2.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(f"{path}/")
                     

# COMMAND ----------

# DBTITLE 1,Delete Logic in EH layer
if load_type == "DELTA":

    df_GLPCA_delete_records = spark.sql(f"select GL_SIRID, RCLNT from `{uc_catalog_name}`.`{uc_euh_schema}`.glpca where OPFLAG == 'D' and ingested_at >= '{df_max_run_start_time}'")
    df_GLPCA_delete_records = df_GLPCA_delete_records.withColumn("Source_ID_delete",concat(lit(gsap_source_system_name),lit('_'),col('RCLNT')))

    df_GLPCA_delete_records.createOrReplaceTempView("GLPCA_delete_records")

    spark.sql(f"""UPDATE `{uc_catalog_name}`.`{uc_eh_schema}`.{table_name} AS fact_table 
              SET Ledger = null,
                Fiscal_Year = null,
                Currency_Key = null,
                Debit_Credit_Indicator = null,
                Posting_Period = null,
                Accounting_Document_No = null,
                Document_Line = null,
                Company_Code = null,
                Profit_Center = null,
                Functional_Area = null,
                Account_No = null,
                Partner_Profit_Center = null,
                Amt_Comp_Code_Curr = null,
                Amt_PC_Local_Curr = null,
                Document_Date = null,
                Document_Posting_Date = null,
                Ref_Document_No = null,
                Ref_Fiscal_Year = null,
                Line_Itm_Account_Doc_No = null,
                Ref_Document_Type = null,
                Plant = null,
                Material_Number = null,
                Customer_Number = null,
                Account_No_Supplier = null,
                Purchasing_Document_No = null,
                Sales_Order_Doc_No = null,
                Distribution_Channel = null,
                Division = null,
                Mvmt_Type = null,
                Document_Type = null,
                Receiving_Issue_Plant = null,
                Ext_Mode_Transport = null,
                Weight_Qty = null,
                Volume_Unit_L15 = null,
                Volume_Qty = null,
                Volume_GAL = null,
                Volume_BBL = null,
                No_Principal_Prchg_Agmt = null,
                Doc_No_Ref_Doc = null,
                Parcel_ID = null,
                Smart_Contract_No = null,
                OIL_TSW_Deal_Number = null,
                Document_Date_In_Doc = null,
                External_Bill_Of_Lading = null,
                Endur_Cargo_ID = null,
                BGI_Delivery_ID = null,
                Endur_Parcel_ID = null,
                Volume_Qty_GAL = null,
                Volume_Unit_GAL = null,
                Volume_Qty_BB6 = null,
                Volume_Unit_BB6 = null,
                CL_LT_Reference_Doc_No = null,
                CL_RT_Reference_Doc_No = null,
                Material_Document_Number = null,
                Material_Document_Year = null,
                Material_Document_Itm = null,
                MM_Mvmt_Type = null,
                Debit_Or_Credit_Indicator = null,
                MM_Plant = null,
                MM_Receiving_Plant = null,
                MM_Material = null,
                MM_Receiving_Material = null,
                Add_Oil_Gas_Qty = null,
                MM_Volume_L15 = null,
                CL_Account_No_Functional_Area = null,
                MM_Material_Key = null,
                CL_Receiving_Issuing_Mat_Key = null,
                CL_UOM_In_GAL = null,
                CL_Lcl_Currency = null,
                CL_Mvmt_Type_Plus = null,
                CL_Profit_Center = null,
                CL_Mvmt_Group = null,
                CL_GL_Account_Key = null,
                Material_Key = null,
                ingested_at = current_timestamp(),
                OPFLAG = 'D'
              WHERE EXISTS (SELECT GL_SIRID FROM GLPCA_delete_records WHERE fact_table.Rec_No_Line_Itm_Rec = GL_SIRID and fact_table.Source_ID = Source_ID_delete)""")
