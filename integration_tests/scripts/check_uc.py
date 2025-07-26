# Databricks notebook source
# MAGIC %pip install --upgrade pip

# COMMAND ----------

requirements_filepath = dbutils.widgets.get(name="requirements_filepath").strip("/")

# NB: using `with open(...) as f:` raises error "IndentationError: expected an indented block"
requirements_file = open(f"/Workspace/{requirements_filepath}")
libraries = requirements_file.read().splitlines()
libraries_str = " ".join(libraries)
requirements_file.close()

%pip install $libraries_str

# COMMAND ----------

import json
import os
import requests
from requests.exceptions import HTTPError
import time

os.environ["pipeline"] = "databricks"

from pyspark.sql.functions import col, collect_set

from ci_cd_helpers.auth import get_dbx_http_header
from ci_cd_helpers.azure import generate_spn_ad_token
from ci_cd_helpers.workflows import poll_active_runs
from datta_pipeline_library.core.base_config import (
    BaseConfig,
    CommonConfig,
    EnvConfig,
    GreatExpectationsConfig,
)

# COMMAND ----------

# DBTITLE 1,Parameters
unique_repo_branch_id = dbutils.widgets.get(name="unique_repo_branch_id")
unique_repo_branch_id_schema = dbutils.widgets.get(name="unique_repo_branch_id_schema")
env = dbutils.widgets.get(name="env")
#register_table_to_uc_workflow_id = dbutils.widgets.get(name="register_table_to_uc_workflow_id")
#host = dbutils.widgets.get(name="host")

# COMMAND ----------

# DBTITLE 1,Configuration
common_conf = CommonConfig.from_file("../../conf/common/common_conf.json")
env_conf = EnvConfig.from_file(f"../../conf/{env}/conf.json")

kv = env_conf.kv_key

tenant_id = dbutils.secrets.get(scope=kv, key="AZ-AS-SPN-DATTA-TENANT-ID")
spn_client_id = dbutils.secrets.get(scope=kv, key=env_conf.spn_client_id_key)
spn_client_secret = dbutils.secrets.get(scope=kv, key=env_conf.spn_client_secret_key)
#azure_conn_str = dbutils.secrets.get(scope=kv, key=env_conf.azure_connection_string_key)

#gx_config = GreatExpectationsConfig(azure_conn_str)

#base_config = BaseConfig.from_confs(env_conf, common_conf, gx_config)
base_config = BaseConfig.from_confs(env_conf, common_conf)
base_config.set_unique_id(unique_repo_branch_id)
base_config.set_unique_id_schema(unique_repo_branch_id_schema)

spn_ad_token = generate_spn_ad_token(tenant_id, spn_client_id, spn_client_secret)
dbx_http_header = get_dbx_http_header(spn_ad_token)

uc_catalog = base_config.get_uc_catalog_name()

euh_schema = base_config.get_uc_euh_schema()
eh_schema = base_config.get_uc_eh_schema()

# COMMAND ----------

# DBTITLE 1,All schemas and tables
expected_schema_to_tables = {
    euh_schema: {
        "bkpf",
        "ds1hm_mt_bb",
        "glpca",
        "ds1hm_mt_bb01",
        "mseg",
        "msego2",
        "rprctr_control_table",
    },
    eh_schema: {
        "fact_fi_act_line_item",
    },
}

# COMMAND ----------

# DBTITLE 1,Wait for register_table_to_uc job to finish all runs
#poll_active_runs(register_table_to_uc_workflow_id, host, dbx_http_header)

# COMMAND ----------

# DBTITLE 1,Check that tables/views exist
actual_schema_to_tables = (
    spark
        .table("system.information_schema.tables")
        .filter(
            (col("table_catalog") == uc_catalog) &
            (col("table_schema").isin(*expected_schema_to_tables.keys()))
        )
        .select("table_schema", "table_name")
        .filter(col('table_name').isin(['bkpf', 'ds1hm_mt_bb', 'glpca', 'ds1hm_mt_bb01', 'mseg', 'msego2',        'rprctr_control_table', 'fact_fi_act_line_item']))
        .groupBy("table_schema")
        .agg(collect_set("table_name").alias("tables"))
        .collect()
)

actual_schema_to_tables = {
    row.table_schema: set(row.tables)
    for row in actual_schema_to_tables
}

# COMMAND ----------

actual_schema_to_tables_formatted_for_display = {
    schema: sorted(list(tables))
    for schema, tables in actual_schema_to_tables.items()
}

print(json.dumps(actual_schema_to_tables_formatted_for_display, indent=4))

# COMMAND ----------

error_message = f"""Schemas and tables don't match:
- Expected schemas and tables:
{expected_schema_to_tables}

- Actual schemas and tables:
{actual_schema_to_tables}
"""

assert actual_schema_to_tables == expected_schema_to_tables, error_message
