# Databricks notebook source
# MAGIC %md
# MAGIC ###Access Azure data lake using access key
# MAGIC 1.set the spark config fs.azure.account.key
# MAGIC 2.list files from demo container
# MAGIC 3.read the data from circutis.csv files

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.formula1trainingdatalake.dfs.core.windows.net",
    "xu+WGkeziXZhPdiDDqs3H2z/pFPua2rtzGrUlK8LUC39EN+eduf5W+Cg1QXTi09tnTGPE906ulyq+AStsYwucg=="
)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1trainingdatalake.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.options(header='true').csv("abfss://demo@formula1trainingdatalake.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


