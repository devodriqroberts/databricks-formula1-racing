# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-04-18")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Produce constructor standings

# COMMAND ----------

race_year_list = df_col_to_list(presentation_folder_path, "race_results", "race_year", v_file_date)

# COMMAND ----------

from pyspark.sql.functions import col

race_results = spark.read.format("delta").load(f"{presentation_folder_path}/race_results") \
    .filter(col("race_year").isin(race_year_list))

# COMMAND ----------

from pyspark.sql.functions import sum, count, countDistinct, when, col, desc, asc, rank
from pyspark.sql.window import Window

constructor_rank_window_spec  = Window.partitionBy("race_year").orderBy(desc("total_points"))

constuctor_standings_df = race_results \
    .groupBy("race_year", "team") \
    .agg(sum("points").alias("total_points"),
         count(when(col("position") == 1, True)).alias("wins")) \
    .withColumn("rank", rank().over(constructor_rank_window_spec)) \
    .orderBy(col("race_year").desc(), col("rank").asc())
    

# COMMAND ----------

merge_condition = "tgt.team = src.team"
incremental_load_delta(df=constuctor_standings_df, database="f1_presentation", table="constuctor_standings", path=presentation_folder_path, partition_column="race_year", merge_condition=merge_condition)
