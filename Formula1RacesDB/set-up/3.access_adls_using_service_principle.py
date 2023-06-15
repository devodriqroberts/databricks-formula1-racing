# Databricks notebook source
# MAGIC %md
# MAGIC ## Access Azure Data Lake Using Service Principle
# MAGIC
# MAGIC 1. Set the spark config for Service Principle
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuits.csv file

# COMMAND ----------

client_id       = dbutils.secrets.get(scope = 'formula1-scope', key = 'client-id')
tenant_id       = dbutils.secrets.get(scope = 'formula1-scope', key = 'tenant-id')
client_secret   = dbutils.secrets.get(scope = 'formula1-scope', key = 'client-secret')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.drobertsformula1.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.drobertsformula1.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.drobertsformula1.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.drobertsformula1.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.drobertsformula1.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------


