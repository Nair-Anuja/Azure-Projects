-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##### Create data base

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_raw
LOCATION "/mnt/dldataengineeringdata/raw"

-- COMMAND ----------



CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "/mnt/dldataengineeringdata/processed"






-- COMMAND ----------

--drop database f1_processed cascade

-- COMMAND ----------


DESC DATABASE f1_processed;



-- COMMAND ----------


CREATE DATABASE IF NOT EXISTS f1_presentation
LOCATION "/mnt/dldataengineeringdata/presentation"

-- COMMAND ----------

