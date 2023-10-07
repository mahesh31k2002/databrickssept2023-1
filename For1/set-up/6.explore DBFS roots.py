# Databricks notebook source
# MAGIC %md
# MAGIC #### Explore DBFS roots
# MAGIC 1.List all the folder in DBFS
# MAGIC 2.interact with DBFS file browser
# MAGIC 3.Upload the files to DBFS Root

# COMMAND ----------


display(dbutils.fs.ls('/'))

# COMMAND ----------

display(dbutils.fs.ls('/FileStore'))

# COMMAND ----------

display(spark.read.csv('dbfs:/FileStore/circuits.csv'))

# COMMAND ----------

display(spark.read.options(header='true').csv("dbfs:/FileStore/circuits.csv"))

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


