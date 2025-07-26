# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp,date_format
from pyspark.sql.functions import col, trim
from datetime import datetime
from delta.tables import *
spark = SparkSession.getActiveSession()

# COMMAND ----------


fact_table_name="FACT_FI_GL_BALANCE_TSAP"
fact_path = eh_folder_path+"/"+fact_table_name

# COMMAND ----------

#TO BE UPDATED
GLIDXA_TSAP_Column=['Client','Reference_Document_Number','Reference_Procedure','Reference_Organisational_Units','Ledger','Document_Type','Fiscal_Year','Doc_Number','Posting_Date_In_The_Document','Record_Type','Version','Company','Company_Code','FI_SL_Document_Type','Accounting_Document_Number','LAST_DTM','ingested_at']

df_GLIDXA_TSAP=df_GLIDXA_TSAP.select(*GLIDXA_TSAP_Column)

# COMMAND ----------

#PIPELINE NAME TO BE UPDATED
if load_type == "DELTA":
    df_max_run_start_time = get_latest_delta("sede-x-DATTA-FI-EH-D-HANA-workflow", uc_catalog_name, uc_raw_schema)
    print(df_max_run_start_time)

# COMMAND ----------

#JOIN CONDITIONS AND COLUMNS TO BE UPDATED
if load_type == "DELTA":   

    df_GLIDXA_TSAP_delta = df_GLIDXA_TSAP.filter(f.col('ingested_at') >= df_max_run_start_time).drop('ingested_at')

    df_GLIDXA_TSAP_delta = df_GLIDXA_TSAP_delta.withColumn("Source_ID",concat(f.lit(tsap_source_system_name),f.lit('_'),col('Client')))

    deltaTable = DeltaTable.forName(spark, f"`{uc_catalog_name}`.`{uc_eh_schema}`.{fact_table_name}")

    (deltaTable.alias('target') \
    .merge(df_GLIDXA_TSAP_delta.alias('source'), "target.Source_ID = source.Source_ID and target.Reference_Document_Number = source.Reference_Document_Number and target.Reference_Procedure = source.Reference_Procedure and target.Reference_Organisational_Units = source.Reference_Organisational_Units and target.Ledger = source.Ledger and target.Document_Type = source.Document_Type and target.Fiscal_Year = source.Fiscal_Year and target.Doc_Number = source.Doc_Number")	#to be updated
    .whenMatchedUpdate( set =
    {
      'Posting_Date_In_The_Document':'source.Posting_Date_In_The_Document',
      'Record_Type':'source.Record_Type',
      'Version':'source.Version',
      'Company':'source.Company',
      'Company_Code':'source.Company_Code',
      'FI_SL_Document_Type':'source.FI_SL_Document_Type',
      'Accounting_Document_Number':'source.Accounting_Document_Number',
      "LAST_DTM": 'source.LAST_DTM',
      "Ingested_At": f.current_timestamp(),   
      'Source_ID':concat(f.lit(tsap_source_system_name),f.lit('_'),'source.Client')
    }) \
    .whenNotMatchedInsert(values =    {         
      'Reference_Document_Number':'source.Reference_Document_Number',
      'Reference_Procedure':'source.Reference_Procedure',
      'Reference_Organisational_Units':'source.Reference_Organisational_Units',
      'Ledger':'source.Ledger',
      'Document_Type':'source.Document_Type',
      'Fiscal_Year':'source.Fiscal_Year',
      'Doc_Number':'source.Doc_Number',
      'Posting_Date_In_The_Document':'source.Posting_Date_In_The_Document',
      'Record_Type':'source.Record_Type',
      'Version':'source.Version',
      'Company':'source.Company',
      'Company_Code':'source.Company_Code',
      'FI_SL_Document_Type':'source.FI_SL_Document_Type',
      'Accounting_Document_Number':'source.Accounting_Document_Number',
      "LAST_DTM": 'source.LAST_DTM',
      "Ingested_At": f.current_timestamp(),   
      'Source_ID':concat(f.lit(tsap_source_system_name),f.lit('_'),'source.Client')
    })
    .execute()
    )  

    print("fact_fi_gl_balance_tsap delta written")     

else:
    df_GLIDXA_TSAP=df_GLIDXA_TSAP.drop('ingested_at').withColumn("Ingested_At", f.current_timestamp()).withColumn("Source_ID",concat(f.lit(tsap_source_system_name),f.lit('_'),col('Client'))).drop('Client')

    df_GLIDXA_TSAP.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(f"{fact_path}/")
    print("fact_fi_gl_balance_tsap init written")


# COMMAND ----------

if load_type == "DELTA":
  #Update Keys
  df_GLIDXA_TSAP_delete_records = spark.sql(f"""select DISTINCT RCLNT as Client, 
                                             AWREF as Reference_Document_Number,
                                             AWTYP as Reference_Procedure,
                                            AWORG as Reference_Organisational_Units,
                                            RLDNR as Ledger,
                                            DOCCT as Document_Type,
                                            RYEAR as Fiscal_Year,
                                            DOCNR as Doc_Number 
                                             from `{uc_catalog_name}`.`{uc_raw_schema}`.glidxa_tsap 
                                             where LAST_ACTION_CD = 'D' 
                                             and ingested_at >= '{df_max_run_start_time}'""")    

  df_GLIDXA_TSAP_delete_records = df_GLIDXA_TSAP_delete_records.withColumn("Source_ID",concat(f.lit(tsap_source_system_name),f.lit('_'),col('Client'))).drop('Client')

  df_GLIDXA_TSAP_delete_records.createOrReplaceTempView("df_GLIDXA_TSAP_delete_records")

    #Update Keys
  spark.sql(f"""DELETE FROM `{uc_catalog_name}`.`{uc_eh_schema}`.{fact_table_name} AS fact_table WHERE EXISTS (SELECT 1 FROM df_GLIDXA_TSAP_delete_records 
              WHERE fact_table.Source_ID = Source_ID 
              and fact_table.Reference_Document_Number = Reference_Document_Number
              and fact_table.Reference_Procedure = Reference_Procedure 
            and fact_table.Reference_Organisational_Units = Reference_Organisational_Units 
            and fact_table.Ledger = Ledger 
            and fact_table.Document_Type = Document_Type 
            and fact_table.Fiscal_Year = Fiscal_Year 
            and fact_table.Doc_Number = Doc_Number)""")
