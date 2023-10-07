# Databricks notebook source
dbutils.fs.mounts()

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/f1dls/presentation

# COMMAND ----------

race_results_df=spark.read.parquet('dbfs:/mnt/f1dls/presentation/race_results')

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

constructior_standing_df=race_results_df\
    .groupBy('race_year','team')\
    .agg(sum('points').alias('total_points'),count(when(col('position')==1,True)).alias('wins'))

# COMMAND ----------

constructior_standing_df.filter("race_year=2020").show()


# COMMAND ----------

from pyspark.sql.window import Window 

# COMMAND ----------

construct_rank_spec=Window.partitionBy('race_year').orderBy(desc('total_points'),desc('wins'))
final_df=constructior_standing_df.withColumn('rank',rank().over(construct_rank_spec))

# COMMAND ----------

final_df.filter("race_year=2020").show()

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/f1dls/presentation

# COMMAND ----------

final_df.write.mode('overwrite').parquet('dbfs:/mnt/f1dls/presentation/constructor_standings')

# COMMAND ----------


