# Databricks notebook source
# MAGIC %md
# MAGIC #### Spark Join Trnasformation

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/f1dls/processed

# COMMAND ----------

circuits_df=spark.read.parquet('dbfs:/mnt/f1dls/processed/circuits').filter('circuit_id<70')

# COMMAND ----------

races_df=spark.read.parquet('dbfs:/mnt/f1dls/processed/races').filter('race_year=2019')

# COMMAND ----------

race_circuits_df=circuits_df.join(races_df,circuits_df.circuit_id==races_df.circuit_id,'inner')\
    .select(circuits_df.name.alias('circuits_name'),circuits_df.location,circuits_df.country,races_df.name.alias('races_name'),races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

#left outer join

# COMMAND ----------

race_circuit_left_join=circuits_df.join(races_df,circuits_df.circuit_id==races_df.circuit_id,'left')\
    .select(circuits_df.name.alias('circuit_name'),circuits_df.location,circuits_df.country,races_df.name.alias('races_name'),races_df.round)

# COMMAND ----------

display(race_circuit_left_join)

# COMMAND ----------

race_circuit_left_join=circuits_df.join(races_df,circuits_df.circuit_id==races_df.circuit_id,'left')\
    .select(circuits_df.name.alias('circuit_name'),circuits_df.location,circuits_df.country,races_df.name.alias('races_name'),races_df.round)

# COMMAND ----------

#right outer join
race_circuit_right_join=circuits_df.join(races_df,circuits_df.circuit_id==races_df.circuit_id,'right')\
    .select(circuits_df.name.alias('circuits_name'),circuits_df.location,circuits_df.country,races_df.name.alias('races_name'),races_df.round)

# COMMAND ----------

display(race_circuit_right_join)

# COMMAND ----------

race_circuit_full_join_df=circuits_df.join(races_df,circuits_df.circuit_id==races_df.circuit_id,'full')\
    .select(circuits_df.name.alias('circuit_name'),circuits_df.location,circuits_df.country,races_df.name.alias('races_name'),races_df.round)

# COMMAND ----------

display(race_circuit_full_join_df)

# COMMAND ----------

# semi joins
circuit_race_right_join_df=circuits_df.join(races_df,circuits_df.circuit_id==races_df.circuit_id,'semi')
                                     

# COMMAND ----------

display(circuit_race_right_join_df)

# COMMAND ----------

#anti join
race_circuit_anti_join_df=races_df.join(circuits_df,['circuit_id'],'anti')

# COMMAND ----------

display(race_circuit_anti_join_df)

# COMMAND ----------

#cross join

race_circuit_cross_join_df=circuits_df.crossJoin(races_df)

# COMMAND ----------

display(race_circuit_cross_join_df)

# COMMAND ----------


