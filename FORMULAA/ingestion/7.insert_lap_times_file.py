# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest the laps time folder

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1- Read the csv file using the spark dataframe reader Api

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType

# COMMAND ----------

lap_time_schema=StructType(fields=[StructField("raceId",IntegerType(),False),
                                   StructField("driverId",IntegerType(),True),
                                   StructField("lap",StringType(),True),
                                   StructField("position",IntegerType(),True),
                                   StructField("time",StringType(),True),
                                   StructField("milliseconds",IntegerType(),True)
                                                                      
                                               
                                              ])

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula11datalake/raw

# COMMAND ----------

lap_time_df=spark.read\
.schema(lap_time_schema)\
.csv(f"{raw_folder_path}/lap_times")

# COMMAND ----------

display(lap_time_df)

# COMMAND ----------

lap_time_df.count()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

final_df=lap_time_df.withColumnRenamed("raceId","race_id")\
                     .withColumnRenamed("driverId","driver_id")\
                     .withColumn("ingestion_date",current_timestamp())\
.withColumn("source",lit(v_data_source))

# COMMAND ----------

final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.lap_times")

# COMMAND ----------

display(spark.read.parquet('/mnt/formula11datalake/processed/lap_times'))

# COMMAND ----------


