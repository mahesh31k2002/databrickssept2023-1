# Databricks notebook source
# MAGIC %md
# MAGIC #### ingest qualifing json file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 Read the qualifing json file using dataframe reader Api

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,IntegerType

# COMMAND ----------

qualifing_schema=StructType(fields=[StructField('qualifyId',IntegerType(),False),
                                    StructField('raceId',IntegerType(),True),
                                    StructField('driverId',IntegerType(),True),
                                    StructField('constructorId',IntegerType()),
                                    StructField('number',IntegerType(),True),
                                    StructField('position',IntegerType(),True),
                                    StructField('q1',StringType(),True),
                                    StructField('q2',StringType(),True),
                                    StructField('q3',StringType(),True)
                                     
                                    ])

# COMMAND ----------

qualifier_df=spark.read.schema(qualifing_schema).option('multiLine',True).json('/mnt/f1dls/raw/qualifying')

# COMMAND ----------

display(qualifier_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df=qualifier_df.withColumnRenamed('qualifyId','qualify_id')\
                     .withColumnRenamed('driverId','driver_id')\
                     .withColumnRenamed('raceId','race_id')\
                     .withColumnRenamed('constructorId','constructor_id')\
                     .withColumn('ingestion_date',current_timestamp())

# COMMAND ----------

display(final_df)

# COMMAND ----------

final_df.write.mode('overwrite').parquet('/mnt/f1dls/processed/qualifying')

# COMMAND ----------

display(spark.read.parquet('/mnt/f1dls/processed/qualifying'))

# COMMAND ----------


