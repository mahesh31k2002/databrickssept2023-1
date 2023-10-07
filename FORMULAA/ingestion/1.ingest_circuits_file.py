# Databricks notebook source
dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

v_data_source

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest circuits.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1-Read the CSV file using the spark dataframe reader

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls mnt/formula11datalake/raw

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType

# COMMAND ----------

circuits_schema=StructType(fields=[StructField("circuitId",IntegerType(),False),
                                   StructField("circuitRef",StringType(),True),
                                   StructField("name",StringType(),True),
                                   StructField("location",StringType(),True),
                                   StructField("country",StringType(),True),
                                   StructField("lat",DoubleType(),True),
                                   StructField("lng",DoubleType(),True),
                                   StructField("alt",IntegerType(),True),
                                   StructField("url",StringType(),True)
                                                                        
])

# COMMAND ----------

circuits_df=spark.read\
.option("header",True)\
.schema(circuits_schema)\
.csv(f"{raw_folder_path}/circuits.csv")

# COMMAND ----------

circuits_df.show()

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

circuits_df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select only required columns

# COMMAND ----------

circuits_select_df=circuits_df.select("circuitId","circuitRef","name","location","country","lat","lng","alt")

# COMMAND ----------

display(circuits_select_df)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuits_select_df=circuits_df.select(col("circuitId"),col("circuitRef"),col("name"),col("location")          ,col("country").alias("rece_country"),col("lat"),col("lng"),col("alt"))

# COMMAND ----------

display(circuits_select_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3-Rename the column as required

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

circuits_renamed_df=circuits_select_df.withColumnRenamed("circuitId","circuit_id")\
.withColumnRenamed("circuitRef","circuit_ref")\
.withColumnRenamed("lat","latitude")\
.withColumnRenamed("lng","longitude")\
.withColumnRenamed("alt","altitude")\
.withColumn("source",lit(v_data_source))


# COMMAND ----------

display(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4-Add ingestion date to the Dataframe

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

circuits_final_df=add_ingestion_date(circuits_renamed_df)


# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### write the data to datalake as a parquet

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %sql
# MAGIC create database f1_processed

# COMMAND ----------

circuits_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.circuits")

# COMMAND ----------

# MAGIC %sql
# MAGIC select *from f1_processed.circuits

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula11datalake/processed/circuits

# COMMAND ----------


