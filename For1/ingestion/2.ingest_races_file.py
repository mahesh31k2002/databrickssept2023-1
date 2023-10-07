# Databricks notebook source
# MAGIC %md
# MAGIC #### Step 1-read the CSV file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType

# COMMAND ----------

races_schema=StructType(fields=[StructField("raceId",IntegerType(),False),
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
# MAGIC ls /mnt/sazelartraining/raw

# COMMAND ----------

races_df=spark.read\
    .option("header","True")\
    .schema(races_schema)\
    .csv("dbfs:/mnt/sazelartraining/raw/races.csv")
    

# COMMAND ----------

display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2- Add ingestion date and race_timestamp to the dataframe

# COMMAND ----------

from pyspark.sql.functions import  current_timestamp,to_timestamp,concat,col,lit 

# COMMAND ----------

race_with_timestamp_df=races_df.withColumn("ingestion_date",current_timestamp())\
                               .withColumn("race_timestamp",to_timestamp(concat(col('date'),lit(' '),col('time')), 'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

display(race_with_timestamp_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step-3 Select only required columns

# COMMAND ----------

races_selected_df=race_with_timestamp_df.select(col('raceId').alias('race_id'),col('year').alias('race_year'),col('round'),
                                                col('circuitId').alias('circuit_id'),col('name'),col('ingestion_date'),col('race_timestamp'))

# COMMAND ----------

display(races_selected_df)

# COMMAND ----------

races_selected_df.write.mode('overwrite').partitionBy('race_year').parquet('/mnt/sazelartraining/processed/races')

# COMMAND ----------



# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/sazelartraining/processed/races

# COMMAND ----------

display(spark.read.parquet('/mnt/sazelartraining/processed/races'))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/sazelartraining/processed

# COMMAND ----------


