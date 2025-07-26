# Databricks notebook source
pip install msal

# COMMAND ----------

pip install adal

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql import SparkSession
spark = SparkSession.getActiveSession()

# COMMAND ----------

from datta_pipeline_library.core.base_config import (
    BaseConfig,
    CollibraConfig,
    CommonConfig,
    EnvConfig
)
from datta_pipeline_library.helpers.adls import configure_spark_to_use_spn_to_write_to_adls_gen2
from datta_pipeline_library.helpers.spn import AzureSPN
# from datta_pipeline_library.edc.collibra import fetch_business_metadata
from datta_pipeline_library.helpers.uc import (
    get_catalog_name,
    get_raw_schema_name,
    get_curated_schema_name,
    get_eh_schema_name,
)

# COMMAND ----------

env = dbutils.widgets.get(name="env")
repos_path = dbutils.widgets.get(name="repos_path")
unique_repo_branch_id = dbutils.widgets.get(name="unique_repo_branch_id")
unique_repo_branch_id_schema = dbutils.widgets.get(name="unique_repo_branch_id_schema")
load_type=dbutils.widgets.get(name="load_type")

common_conf = CommonConfig.from_file(f"/Workspace/{repos_path.strip('/')}/conf/common/common_conf.json")
print("common_conf !!!")
env_conf = EnvConfig.from_file(f"/Workspace/{repos_path.strip('/')}/conf/{env}/conf.json")

base_config = BaseConfig.from_confs(env_conf, common_conf)
if unique_repo_branch_id:
    base_config.set_unique_id(unique_repo_branch_id)

if unique_repo_branch_id_schema:
    base_config.set_unique_id_schema(unique_repo_branch_id_schema)

# COMMAND ----------

kv = env_conf.kv_key

# values from key vault
tenant_id = dbutils.secrets.get(scope=kv, key="AZ-AS-SPN-DATTA-TENANT-ID")
spn_client_id = dbutils.secrets.get(scope=kv, key=env_conf.spn_client_id_key)
spn_client_secret = dbutils.secrets.get(scope=kv, key=env_conf.spn_client_secret_key)
edc_user_id = dbutils.secrets.get(scope=kv, key=env_conf.edc_user_id_key)
edc_user_pwd = dbutils.secrets.get(scope=kv, key=env_conf.edc_user_pwd_key)

spn = AzureSPN(tenant_id, spn_client_id, spn_client_secret)
configure_spark_to_use_spn_to_write_to_adls_gen2(env_conf.storage_account, spn)

# COMMAND ----------

uc_catalog_name = base_config.get_uc_catalog_name()
print("uc_catalog_name : ",uc_catalog_name)

uc_raw_schema = base_config.get_uc_raw_schema()
print("uc_raw_schema : ",uc_raw_schema)
uc_euh_schema = base_config.get_uc_euh_schema()
print("uc_euh_schema : ",uc_euh_schema)
uc_eh_schema = base_config.get_uc_eh_schema()
print("uc_eh_schema : ",uc_eh_schema)
uc_curated_schema = base_config.get_uc_curated_schema()
print("uc_curated_schema : ",uc_curated_schema)

euh_folder_path = base_config.get_euh_folder_path()
print("euh_folder_path : ",euh_folder_path)
eh_folder_path = base_config.get_eh_folder_path()
print("eh_folder_path : ",eh_folder_path)
curated_folder_path = base_config.get_curated_folder_path()
print("curated_folder_path : ",curated_folder_path)

tbl_owner_grp = base_config.get_tbl_owner_grp()
print("tbl_owner_grp : ",tbl_owner_grp)
tbl_read_grp = base_config.get_tbl_read_grp()
print("tbl_read_grp : ",tbl_owner_grp)

uc_eh_schema = uc_eh_schema.replace("-gsap", "")
eh_folder_path = eh_folder_path.replace("/gsap","")
print("uc_eh_schema : ",uc_eh_schema)
print("eh_folder_path : ",eh_folder_path)


# COMMAND ----------

''' assignPermission This function assigns Permission to all the tables created '''
def assignPermission(catalog,schema,table_name,tbl_owner,tbl_read):
    spark.sql(f"ALTER table `{catalog}`.`{schema}`.{table_name} owner to `{tbl_owner}`")
    print("Table Owner is assigned")
    spark.sql(f"GRANT ALL PRIVILEGES ON TABLE `{catalog}`.`{schema}`.{table_name} TO `{tbl_owner}`")
    print("All privileges access given to tbl owner")
    spark.sql(f"GRANT SELECT ON TABLE `{catalog}`.`{schema}`.{table_name} TO `{tbl_read}`")
    print("Reader access granted")

# COMMAND ----------

""" Filter 'column' with RCLNT and RACCT """

def glpca_filter(df):
    filtered_df = df.filter((df.RCLNT == 110) & ((df.RACCT >= "0006000000") & (df.RACCT <= "0009999999")))
    return filtered_df

""" Filter 'column' with MANDT """

def client_filter(df):
    filtered_df = df.filter(df.MANDT==110)
    return filtered_df

""" Filter 'column' with BLART and TCODE """

def bkpf_filter(df):
    filtered_df = df.filter(col("BLART").isin('HC','HF'))
    filtered_df = filtered_df.filter(col("TCODE").isin('/DS1/HM_C_DAILY_T','FB08'))
    return filtered_df

""" Filter 'column' with MANDT and MSEHI """

def mseg02_filter(df):
    filtered_df = df.filter((df.MANDT==110)&(df.MSEHI=='L15'))
    return filtered_df


""" Function to Combine LAST_DTM, AEDATTM, LAST_ACTION_CD, OPFLAG"""
def combine_date_columns_fcb(df,hana_last_dtm='LAST_DTM',hana_last_action='LAST_ACTION_CD', aecorsoft_last_dtm='AEDATTM', aecorsoft_last_action='OPFLAG'):
    df = df.withColumn(aecorsoft_last_dtm, F.when(F.col(aecorsoft_last_dtm).isNull(), F.col(hana_last_dtm)).otherwise(F.col(aecorsoft_last_dtm)))
    df = df.withColumn(aecorsoft_last_action, F.when(F.col(aecorsoft_last_action).isNull(), F.col(hana_last_action)).otherwise(F.col(aecorsoft_last_action)))
    df = df.withColumn(aecorsoft_last_action,F.regexp_replace(aecorsoft_last_action, 'C', 'I'))
    df=df.drop(hana_last_dtm,hana_last_action)
    return df

# COMMAND ----------

if env=='dev':
    stn_source_system_name = 'D80'
    tsap_source_system_name = 'D81'
    gsap_source_system_name = 'D94'
elif env=='tst':
    stn_source_system_name = 'A80'
    tsap_source_system_name = 'T81'
    gsap_source_system_name = 'C94'   
elif env=='pre':
    stn_source_system_name = 'B80'
    tsap_source_system_name = 'Q81'
    gsap_source_system_name = 'Z94'
elif env=='prd':
    stn_source_system_name = 'P80'
    tsap_source_system_name = 'P81'
    gsap_source_system_name = 'P94'
else:
    source_system_name=''
    gsap_source_system_name = ''

# COMMAND ----------

def get_latest_delta(dlt_workflow_name, catalog, schema):
    df_max_run_start_time = spark.sql(f"""select run_start_date from `{catalog}`.`{schema}`.process_status where run_id in (select max(run_id) as max_run_id from `{catalog}`.`{schema}`.process_status where status = 'completed' and dlt_workflow_name = '{dlt_workflow_name}') and dlt_workflow_name = '{dlt_workflow_name}'""").first()[0]
                            
    return df_max_run_start_time
