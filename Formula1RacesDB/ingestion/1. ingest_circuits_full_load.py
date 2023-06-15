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
# MAGIC ## Ingest Circuits File

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. Read the CSV file using the spark dataframe reader.

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DoubleType

# COMMAND ----------

field_types = {
    "circuitId" : IntegerType(),
    "circuitRef" : StringType(),
    "name" : StringType(),
    "location" : StringType(),
    "country" : StringType(),
    "lat" : DoubleType(),
    "lng" : DoubleType(),
    "alt" : IntegerType(),
    "url" : StringType()
}

circuits_schema = StructType(fields=[StructField(field, field_type, False) for field, field_type in field_types.items()])

# COMMAND ----------

circuits_df = spark.read.option("header", True).schema(circuits_schema).csv(f"{raw_folder_path}/{v_file_date}/circuits.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Drop unused columns

# COMMAND ----------

from pyspark.sql.functions import col

circuits_selected_df = circuits_df.select(
    col('circuitId'),
    col('circuitRef'), 
    col('name'), 
    col('location'), 
    col('country'), 
    col('lat'), 
    col('lng'), 
    col('alt')
    )

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. Rename the columns as required.

# COMMAND ----------

from pyspark.sql.functions import lit

circuits_renamed_df = circuits_selected_df \
    .withColumnRenamed("circuitId", "circuit_id") \
    .withColumnRenamed("circuitRef", "circuit_ref") \
    .withColumnRenamed("lat", "latitude") \
    .withColumnRenamed("lng", "longitude") \
    .withColumnRenamed("alt", "altitude") \
    .withColumn("data_source", lit(dbutils.widgets.get("p_data_source"))) \
    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4. Add ingested date.

# COMMAND ----------

circuits_df_with_timestamp = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5. Write data to datalake in parquet format.

# COMMAND ----------

# %sql
# DROP TABLE f1_processed.circuits

# COMMAND ----------

circuits_df_with_timestamp.write.mode("overwrite").format("delta").saveAsTable("f1_processed.circuits")

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------


