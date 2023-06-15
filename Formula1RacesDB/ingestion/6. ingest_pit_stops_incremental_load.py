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
# MAGIC ## Ingest pitstops file

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. Read the JSON file using the spark dataframe reader.

# COMMAND ----------

from pyspark.sql.types import StringType, IntegerType, DateType, StructType, StructField

# COMMAND ----------

pit_stops_schema = StructType([
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), False),
    StructField("stop", IntegerType(), False),
    StructField("lap", IntegerType(), False),
    StructField("time", StringType(), False),
    StructField("duration", StringType(), True),
    StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

pit_stops_df = spark.read.schema(pit_stops_schema).json(f"{raw_folder_path}/{v_file_date}/pit_stops.json", multiLine=True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Rename Columns

# COMMAND ----------

from pyspark.sql.functions import lit

pit_stops_renamed_df = pit_stops_df \
    .withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumn("data_source", lit(dbutils.widgets.get("p_data_source"))) \
    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. Add ingested time column

# COMMAND ----------

pit_stops_final_df = add_ingestion_date(pit_stops_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4. Write result to datalake in parquet format

# COMMAND ----------

# %sql
# DROP TABLE f1_processed.pit_stops

# COMMAND ----------

merge_condition = "tgt.driver_id = src.driver_id and tgt.stop = src.stop"
incremental_load_delta(df=pit_stops_final_df, database="f1_processed", table="pit_stops", path=processed_folder_path, partition_column="race_id", merge_condition=merge_condition)

# COMMAND ----------

dbutils.notebook.exit("Success")
