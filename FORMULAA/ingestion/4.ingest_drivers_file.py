# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest drivers.json File

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1- Read the JSON file using dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DateType

# COMMAND ----------

name_schema=StructType(fields=[StructField("forename",StringType(),True),
                               StructField("surname",StringType(),True)
    
])

# COMMAND ----------

driver_schema=StructType(fields=[StructField("driverId",IntegerType(),False),
                               StructField("driverRef",StringType(),True),
                               StructField("number",IntegerType(),True),
                               StructField("name",name_schema),
                               StructField("dob",DateType(),True),
                               StructField("nationality",StringType(),True),
                               StructField("url",StringType(),True)
                               
    
])

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula11datalake/raw/

# COMMAND ----------

driver_df=spark.read\
.schema(driver_schema)\
.json(f"{raw_folder_path}/drivers.json")

# COMMAND ----------

driver_df.printSchema()

# COMMAND ----------

display(driver_df)

# COMMAND ----------

from pyspark.sql.functions import lit,current_date,concat,col

# COMMAND ----------

drivers_with_columns_df=driver_df.withColumnRenamed("driverId","driver_id")\
                                  .withColumnRenamed("driverRef",'driver_ref')\
                                  .withColumn("ingestion_date",current_date())\
                                  .withColumn("name",concat(col("name.forename"),lit(" "),col("name.surname")))\
.withColumn("source",lit(v_data_source))

# COMMAND ----------

display(drivers_with_columns_df)

# COMMAND ----------

driver_final_df=drivers_with_columns_df.drop(col("url"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 5-write the output toprocess container in parquet format

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula11datalake/processed

# COMMAND ----------

driver_final_df.write\
.mode("overwrite")\
.format("parquet")\
.saveAsTable("f1_processed.drivers")

# COMMAND ----------

display(spark.read.parquet("/mnt/formula11datalake/processed/drivers"))

# COMMAND ----------


