# Databricks notebook source
dbutils.widgets.text("p_data_source", "Ergast API")
dbutils.widgets.text("p_file_date", "2021-04-18")

# COMMAND ----------

v_data_source = dbutils.widgets.get("p_data_source")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

params = {"p_data_source" : v_data_source, "p_file_date" : v_file_date}

# COMMAND ----------

result = dbutils.notebook.run("1. ingest_circuits_full_load", 0, params)

# COMMAND ----------

result

# COMMAND ----------

result = dbutils.notebook.run("2. ingest_races_full_load", 0, params)

# COMMAND ----------

result

# COMMAND ----------

result = dbutils.notebook.run("3. ingest_constructors_full_load", 0, params)

# COMMAND ----------

result

# COMMAND ----------

result = dbutils.notebook.run("4. ingest_drivers_full_load", 0, params)

# COMMAND ----------

result

# COMMAND ----------

result = dbutils.notebook.run("5. ingest_results_incremental_load", 0, params)

# COMMAND ----------

result

# COMMAND ----------

result = dbutils.notebook.run("6. ingest_pit_stops_incremental_load", 0, params)

# COMMAND ----------

result

# COMMAND ----------

result = dbutils.notebook.run("7. ingest_lap_times_incremental_load", 0, params)

# COMMAND ----------

result

# COMMAND ----------

result = dbutils.notebook.run("8. ingest_qualifying_incremental_load", 0, params)

# COMMAND ----------

result
