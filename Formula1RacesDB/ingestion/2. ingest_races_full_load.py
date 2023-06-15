# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest races file

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. Read the CSV file using the spark dataframe reader.

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DoubleType, TimestampType

# COMMAND ----------

field_types = {
    "raceId" : IntegerType(),
    "year" : IntegerType(),
    "round" : IntegerType(),
    "circuitId" : IntegerType(),
    "name" : StringType(),
    "date" : StringType(),
    "time" : StringType()
}
races_schema = StructType(fields=[StructField(field, field_type, False) for field, field_type in field_types.items()])

# COMMAND ----------

races_df = spark.read.schema(races_schema).csv(f"{raw_folder_path}/{v_file_date}/races.csv", header= True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Rename Columns

# COMMAND ----------

races_renamed_df = races_df \
    .withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("year", "race_year") \
    .withColumnRenamed("circuitId", "circuit_id")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Concat date and time column

# COMMAND ----------

from pyspark.sql.functions import col, concat, lit, to_timestamp

races_combined_date_time_df = races_renamed_df \
    .withColumn("race_timestamp", to_timestamp(concat(col("date"), lit(" "), col("time")))) \
    .withColumn("data_source", lit(dbutils.widgets.get("p_data_source"))) \
    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. Add ingested time column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
races_final_df = races_combined_date_time_df
races_final_df = add_ingestion_date(races_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4. Write result to datalake in parquet format

# COMMAND ----------

# %sql
# DROP TABLE f1_processed.races

# COMMAND ----------

races_final_df.write.partitionBy("race_year").mode("overwrite").format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

dbutils.notebook.exit("Success")
