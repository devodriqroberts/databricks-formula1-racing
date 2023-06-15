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
# MAGIC ## Ingest constructors file

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. Read the JSON file using the spark dataframe reader.

# COMMAND ----------

field_types = {
    "constructorId" : "INT",
    "constructorRef" : "STRING",
    "name" : "STRING",
    "nationality" : "STRING"
    }
constructors_schema = ", ".join([f"{key} {val}" for key, val in field_types.items()])

# COMMAND ----------

constructors_df = spark.read.schema(constructors_schema).json(f"{raw_folder_path}/{v_file_date}/constructors.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Rename Columns

# COMMAND ----------

constructors_renamed_df = constructors_df \
    .withColumnRenamed("constructorId", "constructor_id") \
    .withColumnRenamed("constructorRef", "constructor_ref")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. Add ingested time column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

constructors_final_df = constructors_renamed_df \
    .withColumn("data_source", lit(dbutils.widgets.get("p_data_source"))) \
    .withColumn("file_date", lit(v_file_date))

constructors_final_df = add_ingestion_date(constructors_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4. Write result to datalake in parquet format

# COMMAND ----------

# %sql
# DROP TABLE f1_processed.constructors

# COMMAND ----------

constructors_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.constructors")

# COMMAND ----------

dbutils.notebook.exit("Success")
