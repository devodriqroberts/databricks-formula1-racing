-- Databricks notebook source
-- DROP SCHEMA f1_processed CASCADE;
CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "/mnt/drobertsformula1/processed"

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED f1_processed;

-- COMMAND ----------


