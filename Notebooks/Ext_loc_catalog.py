# Databricks notebook source
# DBTITLE 1,Run logging framework
# MAGIC %run /Users/shreya.dani22@gmail.com/Final_project/Logging

# COMMAND ----------

# DBTITLE 1,Log notebook start
log_message("INFO", "ext_loc_catalog notebook started")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL LOCATION if not EXISTS ext_loc_bronze
# MAGIC URL 'abfss://bronze@finalprojstore.dfs.core.windows.net/'
# MAGIC WITH (STORAGE CREDENTIAL project_cred);

# COMMAND ----------

# DBTITLE 1,Log bronze ext location
log_message("INFO", "External location 'ext_loc_bronze' created at abfss://bronze@finalprojstore.dfs.core.windows.net/")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL LOCATION if not EXISTS ext_loc_silver
# MAGIC URL 'abfss://silver@finalprojstore.dfs.core.windows.net/'
# MAGIC WITH (STORAGE CREDENTIAL project_cred);

# COMMAND ----------

# DBTITLE 1,Log silver ext location
log_message("INFO", "External location 'ext_loc_silver' created at abfss://silver@finalprojstore.dfs.core.windows.net/")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL LOCATION if not EXISTS ext_loc_gold
# MAGIC URL 'abfss://gold@finalprojstore.dfs.core.windows.net/'
# MAGIC WITH (STORAGE CREDENTIAL project_cred);

# COMMAND ----------

# DBTITLE 1,Log gold ext location
log_message("INFO", "External location 'ext_loc_gold' created at abfss://gold@finalprojstore.dfs.core.windows.net/")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG if not EXISTS retail_catalog;

# COMMAND ----------

# DBTITLE 1,Log catalog creation
log_message("INFO", "Catalog 'retail_catalog' created successfully")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA if not EXISTS retail_catalog.retail_schema;

# COMMAND ----------

# DBTITLE 1,Log schema creation
log_message("INFO", "Schema 'retail_catalog.retail_schema' created successfully")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE retail_catalog.retail_schema;

# COMMAND ----------

# DBTITLE 1,Log USE schema & notebook completion
log_message("INFO", "Switched to retail_catalog.retail_schema")
log_message("INFO", "ext_loc_catalog notebook completed successfully")