# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest Qualifying Json Folder

# COMMAND ----------

from pyspark.sql.types import StructField,StructType,StringType,IntegerType

# COMMAND ----------

qualify_schema=StructType(fields=[StructField("qualifyId",IntegerType(),False),
                                 StructField("raceId",IntegerType(),True),
                                 StructField("driverId",IntegerType(),True),
                                 StructField("constructorId",IntegerType(),True),
                                 StructField("number",IntegerType(),True),
                                 StructField("position",IntegerType(),True),
                                 StructField("q1",StringType(),True),
                                 StructField("q2",StringType(),True),
                                 StructField("q3",StringType(),True)
                                ])

# COMMAND ----------

# MAGIC %fs
# MAGIC ls mnt/formula11datalake/raw

# COMMAND ----------

qualify_df=spark.read\
.schema(qualify_schema)\
.option("multiline",True)\
.json(f"{raw_folder_path}/qualifying/")

# COMMAND ----------

display(qualify_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

final_df=qualify_df.withColumnRenamed("qualifyId","qualify_id")\
.withColumnRenamed("raceId","race_id")\
.withColumnRenamed("driverId","driver_id")\
.withColumnRenamed("constructorId","constructor_id")\
.withColumn("Ingestion_date",current_timestamp())\
.withColumn("source",lit(v_data_source))

# COMMAND ----------

display(final_df)

# COMMAND ----------

final_df.write\
.mode("overwrite")\
.format("parquet")\
.saveAsTable("f1_processed.qualifying")

# COMMAND ----------

display(spark.read.parquet("/mnt/formula11datalake/processed/qualifying"))

# COMMAND ----------


