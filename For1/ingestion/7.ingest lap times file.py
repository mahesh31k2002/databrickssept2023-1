# Databricks notebook source
# MAGIC %md
# MAGIC #### ingest lap_time folder

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,IntegerType

# COMMAND ----------

lap_time_schema=StructType(fields=[StructField('raceId',IntegerType(),False),
                                   StructField('driverId',IntegerType(),True),
                                   StructField('lap',IntegerType(),True),
                                   StructField('position',IntegerType(),True),
                                   StructField('time',StringType(),True),
                                   StructField('milliseconds',IntegerType(),True)
                                   ])

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls /mnt/f1dls/raw

# COMMAND ----------

laps_time_df=spark.read.schema(lap_time_schema).csv('dbfs:/mnt/f1dls/raw/lap_times')

# COMMAND ----------

display(laps_time_df)

# COMMAND ----------

laps_time_df.count()

# COMMAND ----------

display(laps_time_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df=laps_time_df.withColumnRenamed('driverId','driver_id')\
                     .withColumnRenamed('raceId','race_id')\
                     .withColumn('ingestion_date',current_timestamp())


# COMMAND ----------

final_df.write.mode('overwrite').parquet('dbfs:/mnt/f1dls/processed/lap_times')

# COMMAND ----------

display(spark.read.parquet('dbfs:/mnt/f1dls/processed/lap_times'))

# COMMAND ----------


