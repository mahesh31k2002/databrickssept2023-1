# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest Results json file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### step 1- create the schema for results file

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DateType,FloatType

# COMMAND ----------

results_schema=StructType(fields=[StructField("resultId",IntegerType(),False),
                                 StructField("raceId",IntegerType(),True),
                                 StructField("driverId",IntegerType(),True),
                                 StructField("constructorId",IntegerType(),True),
                                 StructField("number",IntegerType(),True),
                                 StructField("grid",IntegerType(),True),
                                 StructField("position",IntegerType(),True),
                                 StructField("positionText",StringType(),True),
                                 StructField("positionOrder",IntegerType(),True),
                                 StructField("points",FloatType(),True),
                                  StructField("laps",IntegerType(),True),
                                  StructField("time",StringType(),True),
                                  StructField("milliseconds",IntegerType(),True),
                                  StructField("fastestLap",IntegerType(),True),
                                  StructField("rank",IntegerType(),True),
                                  StructField("fastestLapTime",StringType(),True),
                                  StructField("fastestLapSpeed",FloatType(),True),
                                  StructField("statusId",StringType(),True) ])

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula11datalake/raw/

# COMMAND ----------

results_df=spark.read\
.schema(results_schema)\
.json(f"{raw_folder_path}/results.json")

# COMMAND ----------

display(results_df)

# COMMAND ----------

from pyspark.sql.functions import current_date,lit

# COMMAND ----------

results_rename_df=results_df.withColumnRenamed("resultId","result_id")\
                         .withColumnRenamed("raceId","race_id")\
                         .withColumnRenamed("driverId","driver_id")\
                         .withColumnRenamed("constructorId","constructor_id")\
                         .withColumnRenamed("positionText","position_text")\
                         .withColumnRenamed("positionOrder","position_order")\
                         .withColumnRenamed("fastestLap","fastest_lap")\
                         .withColumnRenamed("fastestLapTime","fastest_lap_time")\
                         .withColumnRenamed("fastestLapSpeed","fastest_lap_speed")\
                         .withColumn("ingestion_date",current_date())\
.withColumn("source",lit(v_data_source))

# COMMAND ----------

display(results_rename_df)

# COMMAND ----------

results_final_df=results_rename_df.drop("statusId")

# COMMAND ----------

display(results_final_df)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula11datalake/processed

# COMMAND ----------

results_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

display(spark.read.parquet("/mnt/formula11datalake/processed/results"))

# COMMAND ----------


