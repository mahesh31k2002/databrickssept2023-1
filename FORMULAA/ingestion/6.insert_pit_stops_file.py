# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1-Read the json file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType

# COMMAND ----------

pit_stops_schema=StructType(fields=[StructField("raceId",IntegerType(),False),
                                   StructField("driverId",IntegerType(),True),
                                   StructField("stop",StringType(),True),
                                   StructField("lap",StringType(),True),
                                   StructField("time",StringType(),True),
                                   StructField("duration",StringType(),True),
                                   StructField("milliseconds",IntegerType(),True)
                                                                      
                                               
                                              ])

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula11datalake/raw

# COMMAND ----------

pit_stops_df=spark.read\
.schema(pit_stops_schema)\
.option("multiline",True)\
.json(f"{raw_folder_path}/pit_stops.json")

# COMMAND ----------

display(pit_stops_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

final_df=pit_stops_df.withColumnRenamed("raceId","race_id")\
                     .withColumnRenamed("driverId","driver_id")\
                     .withColumn("ingestion_date",current_timestamp())\
.withColumn("source",lit(v_data_source))

# COMMAND ----------

final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.pit_stops")

# COMMAND ----------

display(spark.read.parquet("/mnt/formula11datalake/processed/pit_stops"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select *from f1_processed.pit_stops

# COMMAND ----------


