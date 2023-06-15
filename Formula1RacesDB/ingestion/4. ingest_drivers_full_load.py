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
# MAGIC ## Ingest drivers file

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. Read the JSON file using the spark dataframe reader.

# COMMAND ----------

from pyspark.sql.types import StringType, IntegerType, DateType, StructType, StructField

# COMMAND ----------

name_schema = StructType([
    StructField("forename", StringType(), True),
    StructField("surname", StringType(), True)
])

drivers_schema = StructType([
    StructField("driverId", StringType(), False),
    StructField("driverRef", StringType(), True),
    StructField("number", IntegerType(), True),
    StructField("code", StringType(), True),
    StructField("name", name_schema),
    StructField("dob", DateType(), True),
    StructField("nationality", StringType(), True)
])

# COMMAND ----------

drivers_df = spark.read.schema(drivers_schema).json(f"{raw_folder_path}/{v_file_date}/drivers.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Rename Columns

# COMMAND ----------

drivers_renamed_df = drivers_df \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("driverRef", "driver_ref")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. Combine forename/surname and add ingested time column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col, concat, lit

drivers_final_df = drivers_renamed_df \
    .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname"))) \
    .withColumn("data_source", lit(dbutils.widgets.get("p_data_source"))) \
    .withColumn("file_date", lit(v_file_date))
    
drivers_final_df = add_ingestion_date(drivers_final_df)
    

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4. Write result to datalake in parquet format

# COMMAND ----------

# %sql
# DROP TABLE f1_processed.drivers

# COMMAND ----------

drivers_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.drivers")

# COMMAND ----------

dbutils.notebook.exit("Success")
