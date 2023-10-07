# Databricks notebook source
dbutils.fs.mounts()

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/f1dls/processed

# COMMAND ----------

races_df=spark.read.parquet('dbfs:/mnt/f1dls/processed/races')

# COMMAND ----------

races_filter_df=races_df.filter('race_year=2019 and round<5')

# COMMAND ----------

races_filter_df=races_df.where((races_df.race_year==2019) & (races_df.round<=5))

# COMMAND ----------

display(races_filter_df)

# COMMAND ----------


