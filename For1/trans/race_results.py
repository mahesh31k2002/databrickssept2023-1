# Databricks notebook source
dbutils.fs.mounts()

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/f1dls/processed

# COMMAND ----------

driver_df=spark.read.parquet('dbfs:/mnt/f1dls/processed/drivers')\
    .withColumnRenamed('number','driver_number')\
    .withColumnRenamed('name','driver_name')\
    .withColumnRenamed('nationality','driver_nationality')

# COMMAND ----------

constructor_df=spark.read.parquet('dbfs:/mnt/f1dls/processed/constructors')\
    .withColumnRenamed('name','team')

# COMMAND ----------

circuits_df=spark.read.parquet('dbfs:/mnt/f1dls/processed/circuits')\
    .withColumnRenamed('location','circuit_location')

# COMMAND ----------

races_df=spark.read.parquet('dbfs:/mnt/f1dls/processed/races')\
    .withColumnRenamed('name','race_name')\
    .withColumnRenamed('race_timestamp','race_date')

# COMMAND ----------

results_df=spark.read.parquet('dbfs:/mnt/f1dls/processed/results')\
    .withColumnRenamed('time','race_time')

# COMMAND ----------

# MAGIC %md
# MAGIC ####join circuits to races

# COMMAND ----------

race_circuits_df=races_df.join(circuits_df,races_df.circuit_id==circuits_df.circuit_id,'inner')\
    .select(races_df.race_id,races_df.race_year,races_df.race_name,races_df.race_date,circuits_df.circuit_location)

# COMMAND ----------

race_results_df=results_df.join(race_circuits_df,results_df.race_id==race_circuits_df.race_id,'inner')\
                          .join(driver_df,results_df.driver_id==driver_df.driver_id,'inner')\
                          .join(constructor_df,results_df.constructior_id==constructor_df.constructor_id,'inner')

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df=race_results_df.select('race_year','race_name','race_date','circuit_location','driver_name','driver_number','driver_nationality','team','grid','fastest_lap','race_time','points','position')\
    .withColumn('created_date',current_timestamp())

# COMMAND ----------

display(final_df)

# COMMAND ----------

display(final_df.filter("race_year==2020 and race_name=='Abu Dhabi Grand Prix'").orderBy(final_df.points.desc()))

# COMMAND ----------

# MAGIC %fs
# MAGIC ls 

# COMMAND ----------

final_df.write.mode('overwrite').parquet('/mnt/f1dls/presentation/race_results')

# COMMAND ----------


