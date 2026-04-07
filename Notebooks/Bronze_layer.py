# Databricks notebook source
# MAGIC %run /Users/shreya.dani22@gmail.com/Final_project/Logging

# COMMAND ----------

dbutils.fs.ls("abfss://bronze@finalprojstore.dfs.core.windows.net/")

# COMMAND ----------

dbutils.fs.ls("abfss://bronze@finalprojstore.dfs.core.windows.net/API/")

# COMMAND ----------

try:
    log_message("INFO","Reading data from Bronze")

    base_path = "abfss://bronze@finalprojstore.dfs.core.windows.net/API/"

    file_list = dbutils.fs.ls(base_path)

    paths = [
        file.path for file in file_list 
        if file.isFile() and file.path.endswith(".json")
    ]

    df = spark.read \
        .option("multiline", "true") \
        .json(paths)

    log_message("INFO", "Data loaded successfully")

except Exception as e:
    log_message("ERROR",f"Ingestion failed: {str(e)}")
    raise


# COMMAND ----------

df.display()

log_message("INFO","API data ingestion successful (data available in Bronze)")