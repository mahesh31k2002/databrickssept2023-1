-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create circuits table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.circuits;
CREATE TABLE IF NOT EXISTS f1_raw.circuits
(
circuitId INT,
circuitRef STRING,
name STRING,
location STRING,
country STRING,
lat DOUBLE,
lan DOUBLE,
alt INT,
url STRING
)
USING csv
OPTIONS(path "/mnt/formula11datalake/raw/circuits.csv",header true)

-- COMMAND ----------

select *from f1_raw.circuits

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create Races Table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.races;
CREATE TABLE IF NOT EXISTS f1_raw.races
(
raceId INT,
year INT,
round INT,
circuitId INT,
name STRING,
date DATE,
time STRING,
url STRING
)
USING csv
OPTIONS(path "/mnt/formula11datalake/raw/races.csv",header true)

-- COMMAND ----------

select *from  f1_raw.races;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create tables for JSON files 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create constructor file
-- MAGIC 1. Single Line JSON
-- MAGIC 1. Simple Structre

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;
CREATE TABLE IF NOT EXISTS f1_raw.constructors
(
constructorId INT,
constructorRef STRING,
name STRING,
nationality STRING,
url STRING
)
USING json
OPTIONS (path "/mnt/formula11datalake/raw/constructors.json")

-- COMMAND ----------

select *from f1_raw.constructors;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.driver;
CREATE TABLE IF NOT EXISTS f1_raw.driver(
driverId INT,
driverRef STRING,
number INT,
code STRING,
name STRUCT<forename STRING,surname STRING>,
dob DATE,
nationality STRING,
url STRING
) using json
options(path "/mnt/formula11datalake/raw/drivers.json")

-- COMMAND ----------

select *from f1_raw.driver

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results;
CREATE TABLE IF NOT EXISTS f1_raw.results
(
resultId INT,
raceId INT,
driverId INT,
constructorId INT,
number INT,
grid INT,
position INT,
positionText STRING,
positionOrder INT,
points INT,
laps INT,
time STRING,
milliseconds INT,
fastestLap INT,
rank int,
fastestLapTime STRING,
fastestLapSpeed float,
statusId string)
using json
options(path "/mnt/formula11datalake/raw/results.json")



-- COMMAND ----------

select *from f1_raw.results

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pit_stops;
CREATE TABLE IF NOT EXISTS f1_raw.pit_stops
(
driverId int,
duration string,
lap int,
milliseconds int,
raceId int,
stop int,
time STRING
)
USING JSON
OPTIONS(path "/mnt/formula11datalake/raw/pit_stops.json",multiLine true)

-- COMMAND ----------

select *from f1_raw.pit_stops

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times;
CREATE TABLE IF NOT EXISTS f1_raw.lap_times
(
raceId int,
driverId int,
lap int,
position int,
time string,
milliseconds int
) using csv
options (path "/mnt/formula11datalake/raw/lap_times")


-- COMMAND ----------

select *from f1_raw.lap_times

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;
CREATE TABLE IF NOT EXISTS f1_raw.qualifying
(
constructorId int,
driverId int,
number int,
position int,
q1 string,
q2 string,
q3 string,
qualifyId int,
raceId int
)using jsOn
options(path "/mnt/formula11datalake/raw/qualifying" ,multiLine true)

-- COMMAND ----------

select *from f1_raw.qualifying;

-- COMMAND ----------

describe extended f1_raw.qualifying;

-- COMMAND ----------


