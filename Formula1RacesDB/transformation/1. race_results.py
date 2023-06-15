# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-04-18")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

dbutils.widgets.text("p_report_race_name", "Abu Dhabi Grand Prix")
v_report_race_name = dbutils.widgets.get("p_report_race_name")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results data from 2020 Adu Dhabi Grand Prix race

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. Read races Data

# COMMAND ----------

races_df = spark.read.format("delta").load(f"{processed_folder_path}/races") \
    .withColumnRenamed("name", "race_name") \
    .withColumnRenamed("race_timestamp", "race_date") \
    .withColumnRenamed("time", "race_time")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Read circuit Data

# COMMAND ----------

circuits_df = spark.read.format("delta").load(f"{processed_folder_path}/circuits") \
    .withColumnRenamed("name", "circuit_name") \
    .withColumnRenamed("location", "circuit_location")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. Read drivers Data

# COMMAND ----------

drivers_df = spark.read.format("delta").load(f"{processed_folder_path}/drivers") \
    .withColumnRenamed("name", "driver_name") \
    .withColumnRenamed("number", "driver_number") \
    .withColumnRenamed("nationality", "driver_nationality")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4. Read constructors Data

# COMMAND ----------

constructors_df = spark.read.format("delta").load(f"{processed_folder_path}/constructors") \
    .withColumnRenamed("name", "team") \
    .withColumnRenamed("nationality", "constructor_nationality")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5. Read results Data

# COMMAND ----------

results_df = spark.read.format("delta").load(f"{processed_folder_path}/results") \
    .filter(f"file_date = '{v_file_date}'") \
    .withColumnRenamed("number", "results_number") \
    .withColumnRenamed("race_id", "results_race_id") \
    .withColumnRenamed("file_date", "results_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join tables

# COMMAND ----------

race_circuits_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id) \
    .select(races_df.race_id, races_df.race_year, races_df.race_name, races_df.race_date, races_df.race_time, circuits_df.circuit_location)

# COMMAND ----------

race_results_df = results_df.join(race_circuits_df, results_df.results_race_id == race_circuits_df.race_id) \
    .join(drivers_df, results_df.driver_id == drivers_df.driver_id) \
    .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id)


# COMMAND ----------

from pyspark.sql.functions import current_timestamp

final_df = race_results_df.select("race_id", "race_year", "race_name", "race_date", "circuit_location", "driver_name", "driver_number", "driver_nationality", "team", "grid", "fastest_lap", "race_time", "points", "position", "results_file_date") \
    .withColumn("created_date", current_timestamp()) \
    .withColumnRenamed("results_file_date", "file_date")

# COMMAND ----------

# %sql
# DROP TABLE IF EXISTS f1_presentation.race_results;

# COMMAND ----------

merge_condition = "tgt.driver_name = src.driver_name"
incremental_load_delta(df=final_df, database="f1_presentation", table="race_results", path=presentation_folder_path, partition_column="race_id", merge_condition=merge_condition)

# COMMAND ----------

# %sql
# SELECT * FROM f1_presentation.race_results

# COMMAND ----------

# %sql
# SELECT race_id, count(1)
# FROM f1_presentation.race_results
# GROUP BY race_id
# ORDER BY race_id DESC;

# COMMAND ----------


