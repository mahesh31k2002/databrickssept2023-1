# Databricks notebook source
# MAGIC %md
# MAGIC ### ingest circuits.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 -Read the csv file using spark dataframe reader

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/sazelartraining/raw

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType

# COMMAND ----------

circuits_schema=StructType(fields=[StructField('circuitId',IntegerType(),False),
                                   StructField('circuitRef',StringType(),True),
                                   StructField('name',StringType(),True),
                                   StructField('location',StringType(),True),
                                   StructField('country',StringType(),True),
                                   StructField('lat',DoubleType(),True),
                                   StructField('lng',DoubleType(),True),
                                   StructField('alt',IntegerType(),True),
                                   StructField('url',StringType(),True)
                                   ] )

# COMMAND ----------

circuits_df=spark.read \
    .options(header=True) \
    .schema(circuits_schema) \
    .csv('dbfs:/mnt/sazelartraining/raw/circuits.csv')

# COMMAND ----------

type(circuits_df)

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

circuits_df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Select only required columns 

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

circuits_select_df=circuits_df.select("circuitId","circuitRef","name","location","country","lat","lng","alt")

# COMMAND ----------

display(circuits_select_df)

# COMMAND ----------

display(circuits_df.describe())

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuits_selct_df=circuits_df.select(col("circuitId"),col("circuitRef"),col("name"),col("location"),col("country"),col("lat"),col("lng"),col("alt"))

# COMMAND ----------

display(circuits_selct_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step-3 Rename the columns as required

# COMMAND ----------

circuit_renamed_df=circuits_selct_df.withColumnRenamed('circuitId','circuit_id')\
                 .withColumnRenamed('circuitRef','circuits_ref')\
                 .withColumnRenamed('lat','latitude')\
                 .withColumnRenamed('lng','longitude')\
                 .withColumnRenamed('alt','altitude')

                    

# COMMAND ----------

display(circuit_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### step-4 add ingestion date to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp ,lit


# COMMAND ----------

circuit_final_df=circuit_renamed_df.withColumn('ingestion_date',current_timestamp())\
    .withColumn('environment',lit('production'))

# COMMAND ----------

display(circuit_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### step-5 write the data to datalake as a parquet
# MAGIC

# COMMAND ----------

circuit_final_df.write.mode('overwrite').parquet('/mnt/sazelartraining/processed/circuits')

# COMMAND ----------

display(spark.read.parquet('/mnt/sazelartraining/processed/circuits'))

# COMMAND ----------


