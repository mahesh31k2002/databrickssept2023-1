# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest Constructor.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### read the json file using the spark dataframe reader api

# COMMAND ----------

constructor_schema="constructorId INT,constructorRef STRING,name STRING,nationality STRING,url STRING"

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------



# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/sazelartraining/raw

# COMMAND ----------

constructor_df=spark.read\
.schema(constructor_schema)\
.json("dbfs:/mnt/sazelartraining/raw/constructors.json")

# COMMAND ----------

display(constructor_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### step-2 drop unwanted colums from dataframe

# COMMAND ----------

constructor_dropped_df=constructor_df.drop('url')

# COMMAND ----------

constructor_dropped_df.show()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

constructor_final_df=constructor_dropped_df.withColumnRenamed('constructorId','constructor_id')\
                                           .withColumnRenamed('constructorRef','constructor_ref')\
                                           .withColumn('ingestion_date',current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### step-3 write output to parquet file

# COMMAND ----------

constructor_final_df.write.mode('overwrite').parquet('/mnt/sazelartraining/processed/constructors')

# COMMAND ----------

display(spark.read.parquet('/mnt/sazelartraining/processed/constructors'))

# COMMAND ----------


