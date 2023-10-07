-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##### Lesson Objectives
-- MAGIC 1. Spark sql Documentation
-- MAGIC 1. Create database Demo
-- MAGIC 1. Data tab in the UI
-- MAGIC 1. SHOW command
-- MAGIC 1. DESCRIBE Command
-- MAGIC 1. Find the current Database

-- COMMAND ----------

CREATE DATABASE demo;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS demo;

-- COMMAND ----------

show databases;


-- COMMAND ----------

describe database demo;

-- COMMAND ----------

describe database extended demo;

-- COMMAND ----------

select current_database();

-- COMMAND ----------

show tables;

-- COMMAND ----------

show tables in demo;

-- COMMAND ----------

use demo;

-- COMMAND ----------



-- COMMAND ----------

select current_database();

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Lerning Objectives
-- MAGIC 1. Create Managed Table using python
-- MAGIC 1. Create Managed Table using sql
-- MAGIC 1. Effect of Dropping Managed Table
-- MAGIC 1. Describe Table

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df=spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").saveAsTable("demo.race_results_python")

-- COMMAND ----------

USE demo;
Show tables;

-- COMMAND ----------

describe race_results_python;

-- COMMAND ----------

describe extended race_results_python;

-- COMMAND ----------

select *from demo.race_results_python where race_year=2020;

-- COMMAND ----------

CREATE TABLE race_results_sql
AS
select *from demo.race_results_python;

-- COMMAND ----------

select current_database();

-- COMMAND ----------

describe extended demo.race_results_sql;

-- COMMAND ----------

drop table  demo.race_results_sql;

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").option("path",f"{presentation_folder_path}/race_results_ext_py").saveAsTable("demo.race_results_ext_py")

-- COMMAND ----------

desc extended demo.race_results_ext_py

-- COMMAND ----------

CREATE TABLE demo.race_results_ext_sql
(
race_year INT,
race_name STRING,
race_date TIMESTAMP,
circuit_loction STRING,
driver_name STRING,
driver_number INT,
driver_nationality STRING,
team STRING,
grid INT,
fastest_lap INT,
race_time STRING,
points FLOAT,
position INT,
created_date TIMESTAMP
)
USING parquet
LOCATION "/mnt/formula11datalake/presentation/race_results_ext_sql"

-- COMMAND ----------

SHOW TABLES in demo;

-- COMMAND ----------

INSERT INTO demo.race_results_ext_sql
select *from demo.race_results_ext_py where race_year=2020;

-- COMMAND ----------

select count(1) from demo.race_results_ext_sql;

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

DROP TABLE demo.race_results_ext_sql

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_race_results
AS
SELECT *from demo.race_results_ext_py
WHERE race_year=2018

-- COMMAND ----------

select *from v_race_results

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW gv_race_results
AS
SELECT *from demo.race_results_ext_py
WHERE race_year=2018

-- COMMAND ----------

select *from global_temp.gv_race_results

-- COMMAND ----------

CREATE OR REPLACE VIEW pv_race_results
AS
SELECT *from demo.race_results_ext_py
WHERE race_year=2000

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

CREATE OR REPLACE VIEW demo.pv_race_results
AS
SELECT *from demo.race_results_ext_py
WHERE race_year=2000

-- COMMAND ----------

drop view pv_race_results

-- COMMAND ----------

show tables in demo;

-- COMMAND ----------

select *from demo.pv_race_results

-- COMMAND ----------


