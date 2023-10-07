# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest Reces.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1-read the csv file using dataframe reader api

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DateType

# COMMAND ----------

reace_schema=StructType(fields=[StructField("raceId",IntegerType(),False),
                                StructField("year",IntegerType(),True),
                                StructField("round",IntegerType(),True),
                                StructField("circuitId",IntegerType(),True),
                                StructField("name",StringType(),True),
                                StructField("date",DateType(),True),
                                StructField("time",StringType(),True),
                                StructField("url",StringType(),True)
    
])

# COMMAND ----------


display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula11datalake/raw

# COMMAND ----------

races_df=spark.read\
.option("header",True)\
.schema(reace_schema)\
.csv(f"{raw_folder_path}/races.csv")

# COMMAND ----------

display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step -2 Add ingestion date and rece timestamp to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,to_timestamp,concat,col,lit

# COMMAND ----------

race_timestamp_df=races_df.withColumn("race_timestamp",to_timestamp(concat(col('date'),lit(' '),col('time')),'yyyy-MM-dd HH:mm:ss'))\
.withColumn("source",lit(v_data_source))

# COMMAND ----------

races_with_time_stamp_df=add_ingestion_date(race_timestamp_df)

# COMMAND ----------

display(races_with_time_stamp_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step-3 Selact only the required columns and rename as required

# COMMAND ----------

reces_select_df=races_with_time_stamp_df.select(col('raceId').alias('race_id'),col('name'),col('year').alias('race_year'),col('round'),col('circuitId').alias('circuit_id'),col('ingestion_date'),col('race_timestamp'),col('source'))

# COMMAND ----------

display(reces_select_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### wite the output to processed container in parquet format

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula11datalake/processed

# COMMAND ----------

reces_select_df.write.mode('overwrite').format("parquet").saveAsTable(f"f1_processed.races")

# COMMAND ----------

# MAGIC %sql
# MAGIC select *from f1_processed.races

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula11datalake/processed/races

# COMMAND ----------

display(spark.read.parquet('/mnt/formula11datalake/processed/races'))

# COMMAND ----------

reces_select_df.write.mode('overwrite').partitionBy('race_year').parquet('/mnt/formula11datalake/processed/races')

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula11datalake/processed/races

# COMMAND ----------

display(spark.read.parquet('/mnt/formula11datalake/processed/races'))

# COMMAND ----------


