# Databricks notebook source
# MAGIC %md
# MAGIC ##### Access the datFame using SQL
# MAGIC ######Objective
# MAGIC ###### 1.Create temporary view on dataframe
# MAGIC ###### 2.Access the view from SQL cell
# MAGIC ###### 3.Access the view from Python Cell

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/f1dls/presentation

# COMMAND ----------

race_results_df=spark.read.parquet('dbfs:/mnt/f1dls/presentation/race_results')

# COMMAND ----------

race_results_df.createOrReplaceTempView('v_race_results')

# COMMAND ----------

# MAGIC %sql
# MAGIC select *from v_race_results where race_year='2020'

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) from v_race_results where race_year=2020

# COMMAND ----------

race_year=2019

# COMMAND ----------

race_results=spark.sql(f"select *from v_race_results where race_year={race_year} ")

# COMMAND ----------

display(race_results)

# COMMAND ----------

# DBTITLE 0, 
# MAGIC
# MAGIC %md
# MAGIC ### Global Temporary Veiw
# MAGIC ##### 1.Create a global temporary view on dataFrame
# MAGIC ##### 2.Access the view from SQL cell
# MAGIC ##### 3.Access the view from python cell
# MAGIC ##### 4.Access the view from another notebook

# COMMAND ----------

race_results.createOrReplaceGlobalTempView('gv_race_results')


# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in global_temp

# COMMAND ----------

# MAGIC %sql
# MAGIC select *from global_temp.gv_race_results

# COMMAND ----------

spark.sql("select *from global_temp.gv_race_results").show()

# COMMAND ----------


