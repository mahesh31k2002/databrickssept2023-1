-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "/mnt/formula11datalake/processed"

-- COMMAND ----------

DESCRIBE  DATABASE f1_processed;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation
LOCATION "/mnt/formula11datalake/presentation"

-- COMMAND ----------

desc database f1_presentation

-- COMMAND ----------


