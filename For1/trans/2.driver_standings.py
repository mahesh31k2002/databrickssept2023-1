# Databricks notebook source
# MAGIC %md
# MAGIC #### Produce Driver Standings

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/f1dls/presentation

# COMMAND ----------

race_results_df=spark.read.parquet('dbfs:/mnt/f1dls/presentation/race_results')

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

from pyspark.sql.functions import * 
driver_standing_df=race_results_df.groupBy('race_year','driver_name','driver_nationality','team')\
    .agg(sum('points').alias('total_points') ,count(when(col('position')==1,True)).alias('wins'))

# COMMAND ----------

display(driver_standing_df.filter("race_year=2020"))

# COMMAND ----------

from pyspark.sql.window import Window 

driver_rank_spec=Window.partitionBy('race_year').orderBy(desc('total_points'),desc('wins'))
final_df=driver_standing_df.withColumn('rank',rank().over(driver_rank_spec))

# COMMAND ----------

display(final_df)

# COMMAND ----------

final_df.write.mode('overwrite').parquet('dbfs:/mnt/f1dls/presentation/driver_standings')

# COMMAND ----------


