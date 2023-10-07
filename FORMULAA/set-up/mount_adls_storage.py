# Databricks notebook source
storage_account_name="formula11datalake"
client_id           ="1e3e4529-9009-4991-88d7-cae87cfc49be"
tenent_id           ="b1a154ec-4b1f-4d7b-8a0c-86170ece63c7"
client_secret       ="TQm8Q~OE4qN.1RC-59EEFuKgri13pmy5OkRKScA5"

# COMMAND ----------

configs={"fs.azure.account.auth.type":"OAuth",
        "fs.azure.account.oauth.provider.type":"org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        "fs.azure.account.oauth2.client.id":f"{client_id}",
        "fs.azure.account.oauth2.client.secret":f"{client_secret}",
        "fs.azure.account.oauth2.client.endpoint":f"https://login.microsoftonline.com/{tenent_id}/oauth2/token"}

# COMMAND ----------

container_name="raw"
dbutils.fs.mount(
  source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
  mount_point = f"/mnt/{storage_account_name}/{container_name}",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.ls("/mnt/formula11datalake/raw")

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

def mount_adls(container_name):
    dbutils.fs.mount(
      source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
      mount_point = f"/mnt/{storage_account_name}/{container_name}",
      extra_configs = configs)
    

# COMMAND ----------

mount_adls("processed")

# COMMAND ----------

mount_adls("demo")

# COMMAND ----------

mount_adls("presentation")

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------


