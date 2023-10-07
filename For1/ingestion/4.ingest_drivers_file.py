# Databricks notebook source
# MAGIC %md
# MAGIC #### ingest Drivers.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ####step-1 Read the JSON file using spark dataframe readr API

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType

# COMMAND ----------

name_schema=StructType(fields=[StructField('forename',StringType(),True),
                               StructField('surname',StringType(),True)                             

])

# COMMAND ----------

driver_schema=StructType(fields=[StructField('driverId',StringType(),False),
                                 StructField('driverRef',StringType(),True),
                                 StructField('number',IntegerType(),True),
                                 StructField('code',StringType(),True),
                                 StructField('name',name_schema),
                                 StructField('dob',DateType(),True),
                                 StructField('nationality',StringType(),True),
                                 StructField('url',StringType(),True)

])

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/sazelartraining/raw

# COMMAND ----------

driver_df=spark.read.schema(driver_schema).json('dbfs:/mnt/sazelartraining/raw/drivers.json')

# COMMAND ----------

display(driver_df)

# COMMAND ----------

from pyspark.sql.functions import col,concat,current_timestamp,lit

# COMMAND ----------

drivers_with_column_df=driver_df.withColumnRenamed('driverId','driver_id')\
                                .withColumnRenamed('driverRef','driver_ref')\
                                .withColumn('ingestion_date',current_timestamp())\
                                .withColumn('name',concat(col('name.forename'),lit(' '),col('name.surname')))
                               

# COMMAND ----------

display(drivers_with_column_df)

# COMMAND ----------

drivers_final_df=drivers_with_column_df.drop(col('url'))

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls /mnt/sazelartraining/processed

# COMMAND ----------

drivers_final_df.write.mode('overwrite').parquet('dbfs:/mnt/sazelartraining/processed/drivers')

# COMMAND ----------

display(spark.read.parquet('dbfs:/mnt/sazelartraining/processed/drivers'))

# COMMAND ----------


