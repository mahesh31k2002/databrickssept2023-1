# Databricks notebook source
# MAGIC %md
# MAGIC ###Access Azure data lake using access key
# MAGIC 1.set the spark config sof sas token
# MAGIC
# MAGIC 2.list files from demo container
# MAGIC 3.read the data from circutis.csv files

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1trainingdatalake.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1trainingdatalake.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1trainingdatalake.dfs.core.windows.net", "sp=rl&st=2023-08-01T11:41:39Z&se=2023-08-01T19:41:39Z&spr=https&sv=2022-11-02&sr=c&sig=lNHitkYryn%2B12YzL4M%2B2xY5%2BYEU21S4IA8wRxC90%2Bf0%3D")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1trainingdatalake.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.options(header='true').csv("abfss://demo@formula1trainingdatalake.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


