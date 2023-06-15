# Databricks notebook source
from pyspark.sql.functions import current_date

def add_ingestion_date(input_df):
    return input_df.withColumn("ingested_date", current_date())

# COMMAND ----------


def incremental_load_data_partition(database, table, df, partition_column):
    # Move partition col to last position
    column_list = [col for col in df.schema.names if col != partition_column]
    column_list_with_partition = column_list + [partition_column]
    reorded_df = df.select(*column_list_with_partition)

    # Set Spark Conf
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    # Add data to table
    if (spark._jsparkSession.catalog().tableExists(f"{database}.{table}")):
        reorded_df.write.mode("overwrite").insertInto(f"{database}.{table}")
    else:
        reorded_df.write.mode("overwrite").partitionBy(partition_column).format("parquet").saveAsTable(f"{database}.{table}")

# COMMAND ----------

def incremental_load_delta(df, database, table, path, partition_column, merge_condition):
    from delta.tables import DeltaTable

    # Set Spark Conf
    spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning", "true")

    # Add data to table
    if (spark._jsparkSession.catalog().tableExists(f"{database}.{table}")):
        deltaTable = DeltaTable.forPath(spark, f"{path}/{table}")
        deltaTable.alias("tgt").merge(
            df.alias("src"),
            f"{merge_condition} and tgt.{partition_column} = src.{partition_column}"
        ) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()
    else:
        df.write.mode("overwrite").partitionBy(partition_column).format("delta").saveAsTable(f"{database}.{table}")

# COMMAND ----------

def df_col_to_list(path, table, col, date):
    row_list = spark.read.format("delta").load(f"{path}/{table}") \
        .filter(f"file_date = '{date}'") \
        .select(col) \
        .distinct() \
        .collect()

    return [row[col] for row in row_list]
