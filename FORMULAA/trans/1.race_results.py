# Databricks notebook source
# MAGIC %md
# MAGIC ### Read all the data as Required

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

driver_df=spark.read.parquet(f"{processed_folder_path}/drivers")\
.withColumnRenamed("number","driver_number")\
.withColumnRenamed("name","driver_name")\
.withColumnRenamed("nationality","driver_nationality")

# COMMAND ----------

display(driver_df)

# COMMAND ----------

constructors_df=spark.read.parquet(f"{processed_folder_path}/constructor")\
.withColumnRenamed("name","team")

# COMMAND ----------

circuits_df=spark.read.parquet(f"{processed_folder_path}/circuits")\
.withColumnRenamed("location","circuit_location")

# COMMAND ----------

races_df=spark.read.parquet(f"{processed_folder_path}/races")\
.withColumnRenamed("name","race_name")\
.withColumnRenamed("race_timestamp","race_date")

# COMMAND ----------

results_df=spark.read.parquet(f"{processed_folder_path}/results")\
.withColumnRenamed("time","race_time")

# COMMAND ----------

race_circuit_df=races_df.join(circuits_df,races_df.circuit_id==circuits_df.circuit_id,"inner")\
.select(races_df.race_id,races_df.race_year,races_df.race_name,races_df.race_date,circuits_df.circuit_location)

# COMMAND ----------

display(race_circuit_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join the results to all other dimention table

# COMMAND ----------

race_results_df=results_df.join(race_circuit_df,results_df.race_id==race_circuit_df.race_id,"inner")\
                          .join(driver_df,results_df.driver_id==driver_df.driver_id,"inner")\
                          .join(constructors_df,results_df.constructor_id==constructors_df.constructor_id,"inner")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df=race_results_df.select("race_year","race_name","race_date","circuit_location","driver_name","driver_number","driver_nationality","team",
                                "grid","fastest_lap","race_time","points","position")\
                         .withColumn("current_date",current_timestamp())
                           

# COMMAND ----------

display(final_df.filter("race_year== 2020 and race_name like '%Abu%'").orderBy(final_df.points.desc()))

# COMMAND ----------

final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC select *from f1_presentation.race_results

# COMMAND ----------


