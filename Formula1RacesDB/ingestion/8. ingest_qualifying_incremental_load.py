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
# MAGIC ## Ingest qualifying file

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. Read the CSV file using the spark dataframe reader.

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DoubleType, TimestampType

# COMMAND ----------

qualifying_schema = StructType([
    StructField("qualifyId", IntegerType(), False),
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), False),
    StructField("constructorId", IntegerType(), False),
    StructField("number", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("q1", StringType(), True),
    StructField("q2", StringType(), True),
    StructField("q3", StringType(), True)
])

# COMMAND ----------

qualifying_df = spark.read.schema(qualifying_schema).json(f"{raw_folder_path}/{v_file_date}/qualifying/*.json", multiLine=True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Rename Columns

# COMMAND ----------

from pyspark.sql.functions import lit

qualifying_renamed_df = qualifying_df \
    .withColumnRenamed("qualifyId", "qualify_id") \
    .withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("constructorId", "constructor_id") \
    .withColumn("data_source", lit(dbutils.widgets.get("p_data_source"))) \
    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. Add ingested time column

# COMMAND ----------

qualifying_final_df = add_ingestion_date(qualifying_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4. Write result to datalake in parquet format

# COMMAND ----------

# %sql
# DROP TABLE f1_processed.qualifying

# COMMAND ----------

merge_condition = "tgt.qualify_id = src.qualify_id"
incremental_load_delta(df=qualifying_final_df, database="f1_processed", table="qualifying", path=processed_folder_path, partition_column="race_id", merge_condition=merge_condition)

# COMMAND ----------

dbutils.notebook.exit("Success")
