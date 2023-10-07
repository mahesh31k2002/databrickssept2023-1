# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source","testing")
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest constructor json file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### step 1-Read the json file using dataframe reader API

# COMMAND ----------

constructor_schema="constructorId INT,constructorRef STRING,name STRING,nationality STRING,url STRING"

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula11datalake/raw

# COMMAND ----------

constructor_df=spark.read\
.schema(constructor_schema)\
.json(f"{raw_folder_path}/constructors.json")

# COMMAND ----------

display(constructor_df)

# COMMAND ----------

constructor_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2-Drop unwanted columns from the dataframe

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

constructor_dropped_df=constructor_df.drop('url')

# COMMAND ----------

display(constructor_dropped_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3- Rename columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

constructor_final_df=constructor_dropped_df.withColumnRenamed('constructorId','constructor_id')\
                                           .withColumnRenamed('constructorRef','constructor_ref')\
                                           .withColumn('ingestion_date',current_timestamp())\
.withColumn("source",lit(v_data_source))

# COMMAND ----------

display(constructor_final_df)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula11datalake/processed

# COMMAND ----------

# MAGIC %md
# MAGIC ##### step 4 - write the date into parquet file

# COMMAND ----------

constructor_final_df.write\
.mode("overwrite")\
.format("parquet")\
.saveAsTable("f1_processed.constructor")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula11datalake/processed/constructor

# COMMAND ----------

# MAGIC %sql
# MAGIC select *from f1_processed.constructor

# COMMAND ----------


