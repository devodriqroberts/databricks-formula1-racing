# Databricks notebook source
# MAGIC %md
# MAGIC ## Access Azure Data Lake Using Service Principle
# MAGIC
# MAGIC 1. Set the spark config for Service Principle
# MAGIC 2. List files from demo container
# MAGIC 3. Call fs utility to mount storage

# COMMAND ----------

def add_mounts(account, containers):
    client_id       = dbutils.secrets.get(scope = 'formula1-scope', key = 'client-id')
    tenant_id       = dbutils.secrets.get(scope = 'formula1-scope', key = 'tenant-id')
    client_secret   = dbutils.secrets.get(scope = 'formula1-scope', key = 'client-secret')

    configs = {
        "fs.azure.account.auth.type": "OAuth",
        "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        "fs.azure.account.oauth2.client.id": client_id,
        "fs.azure.account.oauth2.client.secret": client_secret,
        "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
    }

    for container in containers:
        newMountPoint = f"/mnt/{account}/{container}"

        if any(mount.mountPoint == newMountPoint for mount in dbutils.fs.mounts()):
            dbutils.fs.unmount(newMountPoint)


        dbutils.fs.mount(
            source = f"abfss://{container}@{account}.dfs.core.windows.net/",
            mount_point = newMountPoint,
            extra_configs = configs
        )

    print(display(dbutils.fs.mounts()))

# add_mounts("formula1", ["demo", "raw", "processed", "presentation"])

# COMMAND ----------

add_mounts("drobertsformula1", ["demo", "raw", "processed", "presentation"])

# COMMAND ----------


