# Databricks notebook source
# MAGIC %md
# MAGIC ### Mount Adls containers for the project

# COMMAND ----------

storage_account_name="f1dls"
client_id='72bc1c6c-7457-496b-be08-f5a8207cbc1a'
tenent_id='0fc445ec-453e-4458-8a3a-95412d9f108c'
client_secret='H3f8Q~qAhsk75NINx8eMUIPEZbzpbk_EZSNzZbH5'

# COMMAND ----------

def mount_adls(storage_account_name,container_name):
    #Set spark configuration
    configs={"fs.azure.account.auth.type":"OAuth",
        "fs.azure.account.oauth.provider.type":"org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        "fs.azure.account.oauth2.client.id":f"{client_id}",
        "fs.azure.account.oauth2.client.secret":f"{client_secret}",
        "fs.azure.account.oauth2.client.endpoint":f"https://login.microsoftonline.com/{tenent_id}/oauth2/token"}
    #unmount the mount point if it already mounted
    if any(mount.mountPoint==f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")
    #mount the storage account container
    dbutils.fs.mount(
  source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
  mount_point = f"/mnt/{storage_account_name}/{container_name}",
  extra_configs = configs)
    


# COMMAND ----------

mount_adls('f1dls','raw')

# COMMAND ----------

mount_adls('f1dls','presentation')

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.unmount('/mnt/formula1dladlsaccount/raw')

# COMMAND ----------

dbutils.fs.unmount('/mnt/formula11datalake/demo')

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

dbutils.fs.unmount('/mnt/formula11datalake/raw')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.f1dls.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.f1dls.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.f1dls.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.f1dls.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.f1dls.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenent_id}/oauth2/token")

# COMMAND ----------



# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@f1dls.dfs.core.windows.net/"))

# COMMAND ----------

display(spark.read.options(header='true').csv("dbfs:/mnt/f1dls/demo/circuits.csv"))

# COMMAND ----------


