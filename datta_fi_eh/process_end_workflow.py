# Databricks notebook source
# MAGIC %run ./config_to_read_outside_DLT

# COMMAND ----------

host=workspace_url 
#added for Databricks
if workflow_frequency == 'D-HANA':
  job_name= fi_d_hana_dlt_workflow_name
  dlt_workflow_name = fi_d_hana_dlt_workflow_name
  
else:
  job_name= dlt_workflow_name
  dlt_workflow_name = dlt_workflow_name

spnvalue = spnvalue

# COMMAND ----------

process_table_name='process_status'
table_name='`'+uc_catalog_name+'`.`'+uc_raw_schema+'`'+'.'+process_table_name

http_header = get_dbx_http_header(spnvalue)
dlt_workflow_job_id=get_job_id(job_name, host, http_header)

# COMMAND ----------

from datta_pipeline_library.core.process_status import update_table
update_table(table_name,dlt_workflow_name,dlt_workflow_job_id)
