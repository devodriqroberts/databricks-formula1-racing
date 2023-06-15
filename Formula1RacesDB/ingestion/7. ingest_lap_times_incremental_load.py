# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest lap times file

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. Read the CSV file using the spark dataframe reader.

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DoubleType, TimestampType

# COMMAND ----------

lap_times_schema = StructType([
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), False),
    StructField("lap", IntegerType(), False),
    StructField("position", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

lap_times_df = spark.read.schema(lap_times_schema).csv(f"{raw_folder_path}/{v_file_date}/lap_times/*.csv", header= True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Rename Columns

# COMMAND ----------

from pyspark.sql.functions import lit

lap_times_renamed_df = lap_times_df \
    .withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumn("data_source", lit(dbutils.widgets.get("p_data_source"))) \
    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. Add ingested time column

# COMMAND ----------

lap_times_final_df = add_ingestion_date(lap_times_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4. Write result to datalake in parquet format

# COMMAND ----------

# %sql
# DROP TABLE f1_processed.lap_times

# COMMAND ----------

merge_condition = "tgt.driver_id = src.driver_id and tgt.lap = src.lap"
incremental_load_delta(df=lap_times_final_df, database="f1_processed", table="lap_times", path=processed_folder_path, partition_column="race_id", merge_condition=merge_condition)

# COMMAND ----------

dbutils.notebook.exit("Success")
