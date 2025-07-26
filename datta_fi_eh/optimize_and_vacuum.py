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
    get_euh_schema_name,
    get_eh_schema_name,
)

# COMMAND ----------

env = dbutils.widgets.get(name="env")
repos_path = dbutils.widgets.get(name="repos_path")
unique_repo_branch_id = dbutils.widgets.get(name="unique_repo_branch_id")
unique_repo_branch_id_schema = dbutils.widgets.get(name="unique_repo_branch_id_schema")


# COMMAND ----------

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

uc_eh_schema = base_config.get_uc_eh_schema()

uc_eh_schema = uc_eh_schema.replace("-gsap","")
print("uc_eh_schema : ",uc_eh_schema)


# COMMAND ----------

spark.sql(f"""VACUUM `{uc_catalog_name}`.`{uc_eh_schema}`.fact_fi_gl_accounting_stn""")
spark.sql(f"""VACUUM `{uc_catalog_name}`.`{uc_eh_schema}`.fact_fi_gl_accounting_tsap""")
spark.sql(f"""VACUUM `{uc_catalog_name}`.`{uc_eh_schema}`.fact_fi_gl_balance_stn""")
spark.sql(f"""VACUUM `{uc_catalog_name}`.`{uc_eh_schema}`.fact_fi_gl_balance_tsap""")

# COMMAND ----------

spark.sql(f"""OPTIMIZE `{uc_catalog_name}`.`{uc_eh_schema}`.fact_fi_gl_accounting_stn""")
spark.sql(f"""OPTIMIZE `{uc_catalog_name}`.`{uc_eh_schema}`.fact_fi_gl_accounting_tsap""")
spark.sql(f"""OPTIMIZE `{uc_catalog_name}`.`{uc_eh_schema}`.fact_fi_gl_balance_stn""")
spark.sql(f"""OPTIMIZE `{uc_catalog_name}`.`{uc_eh_schema}`.fact_fi_gl_balance_tsap""")

