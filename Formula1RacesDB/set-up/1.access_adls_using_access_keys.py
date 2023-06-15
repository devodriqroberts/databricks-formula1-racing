# Databricks notebook source
# MAGIC %md
# MAGIC ## Access Azure Data Lake Using access keys
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuits.csv file

# COMMAND ----------

access_key = dbutils.secrets.get(scope = 'formula1-scope', key = 'access-key')

# COMMAND ----------

spark.conf.set("fs.azure.account.key.drobertsformula1.dfs.core.windows.net", access_key)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@drobertsformula1.dfs.core.windows.net"))

# COMMAND ----------


