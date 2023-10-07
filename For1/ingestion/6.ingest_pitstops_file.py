# Databricks notebook source
# MAGIC %md
# MAGIC ####ingest pitstops.json file

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,IntegerType

# COMMAND ----------

pitstops_schema=StructType(fields=[StructField('raceId',IntegerType(),False),
                                   StructField('driverId',IntegerType(),True),
                                   StructField('stop',StringType(),True),
                                   StructField('lap',IntegerType(),True),
                                   StructField('time',StringType(),True),
                                   StructField('duration',StringType(),True),
                                   StructField('milliseconds',IntegerType(),True)
                                                                      ])

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/f1dls/raw

# COMMAND ----------

pit_stop_df=spark.read.schema(pitstops_schema).option('multiline',True).json('dbfs:/mnt/f1dls/raw/pit_stops.json')

# COMMAND ----------

display(pit_stop_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df=pit_stop_df.withColumnRenamed('driverId','driver_id')\
                    .withColumnRenamed('racedId','race_id')\
                    .withColumn('ingestion_date',current_timestamp())

# COMMAND ----------

final_df.write.mode('overwrite').parquet('dbfs:/mnt/f1dls/processed/pit_stops')

# COMMAND ----------

display(spark.read.parquet('dbfs:/mnt/f1dls/processed/pit_stops'))

# COMMAND ----------


