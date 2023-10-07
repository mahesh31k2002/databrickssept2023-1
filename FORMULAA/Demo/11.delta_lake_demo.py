# Databricks notebook source
# MAGIC %md
# MAGIC 1. wirte data to delta lake(managed table)
# MAGIC 2. write date to delta lake (external table)
# MAGIC 3. read data from delta lake(table)
# MAGIC 4. reade data from delta lake(file)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS TestDB
# MAGIC

# COMMAND ----------

order_df=spark.read\
.option("header",True)\
.csv("/FileStore/tables/orders-2")

# COMMAND ----------

order_df.write.format("delta").mode("overwrite").saveAsTable("TestDB.Orders1")

# COMMAND ----------

# MAGIC %sql
# MAGIC select *from TestDB.Orders1

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").save("/mnt/formula11datalake/demo/results_external")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE TestDB.Orders_delta
# MAGIC USING DELTA
# MAGIC select *from TestDB.Orders1

# COMMAND ----------

# MAGIC %sql
# MAGIC select *from TestDB.Orders_delta

# COMMAND ----------

results_external_df=spark.read.format('delta').load("/mnt/formula11datalake/demo/results_external")

# COMMAND ----------

display(results_external_df)

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").partitionBy("constructorId").saveAsTable("f1_demo.results_partitioned")

# COMMAND ----------

# MAGIC %sql
# MAGIC show partitions f1_demo.results_partitioned

# COMMAND ----------

# MAGIC %sql
# MAGIC select *from f1_demo.results_managed 

# COMMAND ----------

# MAGIC %sql
# MAGIC update f1_demo.results_managed
# MAGIC set position=11-position
# MAGIC where position<=10

# COMMAND ----------

# MAGIC %sql
# MAGIC select *from  f1_demo.results_managed

# COMMAND ----------

from delta.tables import DeltaTable
deltaTable=DeltaTable.forPath(spark,"/mnt/formula11datalake/demo/results_managed")
deltaTable.update("position<=10",{"position":"11+position"})

# COMMAND ----------

# MAGIC %sql
# MAGIC select *from  f1_demo.results_managed

# COMMAND ----------

from delta.tables import DeltaTable
deltaTable=DeltaTable.forPath(spark,"/mnt/formula11datalake/demo/results_managed")
deltaTable.update("position<=10",{"points":"11-position"})

# COMMAND ----------

# MAGIC %sql
# MAGIC select *from f1_demo.results_managed

# COMMAND ----------

from delta.tables import DeltaTable
deltaTable=DeltaTable.forPath(spark,"/mnt/formula11datalake/demo/results_managed")
deltaTable.update("position<=10",{"points":"21-position"})

# COMMAND ----------

# MAGIC %sql
# MAGIC select *from f1_demo.results_managed

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from f1_demo.results_managed
# MAGIC where position>10;

# COMMAND ----------

# MAGIC %sql
# MAGIC select *from f1_demo.results_managed

# COMMAND ----------

deltaTable=DeltaTable.forPath(spark,"/mnt/formula11datalake/demo/results_managed")
deltaTable.delete("points=0")

# COMMAND ----------

# MAGIC %sql
# MAGIC select *from f1_demo.results_managed

# COMMAND ----------

# MAGIC %md
# MAGIC ### Upsert using merge

# COMMAND ----------

driver_day1_df=spark.read\
.option("inferSchema",True)\
.json("/mnt/formula11datalake/raw/drivers.json")\
.filter("driverId<=10")\
.select("driverId","dob","name.forename","name.surname")

# COMMAND ----------

display(driver_day1_df)

# COMMAND ----------

from pyspark.sql.functions import upper

driver_day2_df=spark.read\
.option("inferSchema",True)\
.json("/mnt/formula11datalake/raw/drivers.json")\
.filter("driverId between 6 and 15")\
.select("driverId","dob",upper("name.forename").alias("forename"),upper("name.surname").alias("surname"))

# COMMAND ----------

display(driver_day2_df)

# COMMAND ----------

from pyspark.sql.functions import upper
driver_day3_df=spark.read\
.option("inferSchema",True)\
.json("/mnt/formula11datalake/raw/drivers.json")\
.filter("driverId between 1 and 5 or driverId between 16 and 20")\
.select("driverId","dob",upper("name.forename").alias("forename"),upper("name.surname").alias("surname"))

# COMMAND ----------

display(driver_day3_df)

# COMMAND ----------

driver_day1_df.createOrReplaceTempView("driver_day1")

# COMMAND ----------

driver_day2_df.createOrReplaceTempView("driver_day2")

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists f1_demo.drivers_merge
# MAGIC (
# MAGIC driverId INT,
# MAGIC dob DATE,
# MAGIC forename STRING,
# MAGIC surname STRING,
# MAGIC createdate DATE,
# MAGIC updatedate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %md Day 1

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING driver_day1 upd
# MAGIC ON tgt.driverId=upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET tgt.dob=upd.dob
# MAGIC ,tgt.forename=upd.forename
# MAGIC ,tgt.surname=upd.surname
# MAGIC ,tgt.updatedate=current_timestamp()
# MAGIC WHEN NOT MATCHED 
# MAGIC THEN INSERT (driverId,dob,forename,surname,createdate) VALUES(upd.driverId,upd.dob,upd.forename,upd.surname,current_timestamp())
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select *From f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %md Day2

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING driver_day2 upd
# MAGIC ON tgt.driverId=upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET tgt.dob=upd.dob
# MAGIC ,tgt.forename=upd.forename
# MAGIC ,tgt.surname=upd.surname
# MAGIC ,tgt.updatedate=current_timestamp()
# MAGIC WHEN NOT MATCHED 
# MAGIC THEN INSERT (driverId,dob,forename,surname,createdate) VALUES(upd.driverId,upd.dob,upd.forename,upd.surname,current_timestamp())
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select *From f1_demo.drivers_merge

# COMMAND ----------


from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable
deltaTable=DeltaTable.forPath(spark,"/mnt/formula11datalake/demo/drivers_merge")
deltaTable.alias("tgt").merge( driver_day3_df.alias("upd"),"tgt.driverId=upd.driverId")\
    .whenMatchedUpdate(set={"dob":"upd.dob","forename":"upd.forename","surname":"upd.surname","updatedate":"current_timestamp()"})\
    .whenNotMatchedInsert(values ={"driverId":"upd.driverId",
                                  "dob":"upd.dob",
                                  "forename":"upd.forename",
                                 "surname":"upd.surname",
                                 "createdate":"current_timestamp()"})\
.execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC select *From f1_demo.drivers_merge order by 1

# COMMAND ----------

# MAGIC %md
# MAGIC 1.History &Versioning
# MAGIC 1.Time Travel
# MAGIC 1.Vaccum

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_merge

# COMMAND ----------

# MAGIC  %sql
# MAGIC select *From f1_demo.drivers_merge VERSION AS OF 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC select *from f1_demo.drivers_merge VERSION AS OF 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *FROM f1_demo.drivers_merge VERSION AS OF 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC select *from f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM f1_demo.drivers_merge WHERE driverId=1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *FROM f1_demo.drivers_merge version as of 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING f1_demo.drivers_merge version as of 3 as src 
# MAGIC ON (tgt.driverId=src.driverId)
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT *
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC select *from f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_merge
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select max(version) from f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.driver_txn
# MAGIC (
# MAGIC driverId int,
# MAGIC dob date,
# MAGIC forename STRING,
# MAGIC surname STRING,
# MAGIC createdDate DATE,
# MAGIC updatedDate DATE
# MAGIC )USING DELTA 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.driver_txn

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo.driver_txn
# MAGIC select *from f1_demo.drivers_merge where driverId=1

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into f1_demo.driver_txn
# MAGIC select *from f1_demo.drivers_merge where driverId=2

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from f1_demo.driver_txn where driverId=1

# COMMAND ----------

for driver_id in range(3,20):
    spark.sql(f"""insert into f1_demo.driver_txn
    select *from f1_demo.drivers_merge 
    where driverId={driver_id}""")

# COMMAND ----------

# MAGIC %sql
# MAGIC select *from f1_demo.drivers_merge order by 1

# COMMAND ----------

# MAGIC %md
# MAGIC Convert from parquet to dela

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_convert_to_delta
# MAGIC (
# MAGIC driverId int,
# MAGIC dob date,
# MAGIC forename STRING,
# MAGIC surname STRING,
# MAGIC createdDate DATE,
# MAGIC updatedDate DATE
# MAGIC ) USING PARQUET
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into f1_demo.drivers_convert_to_delta
# MAGIC select *From f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC convert to delta f1_demo.drivers_convert_to_delta

# COMMAND ----------

df=spark.table("f1_demo.drivers_convert_to_delta")

# COMMAND ----------

df.write.format('parquet').save("/mnt/formula11datalake/demo/drivers_convert_to_delta_new")

# COMMAND ----------

# MAGIC %sql
# MAGIC convert to delta parquet.`/mnt/formula11datalake/demo/drivers_convert_to_delta_new`

# COMMAND ----------


