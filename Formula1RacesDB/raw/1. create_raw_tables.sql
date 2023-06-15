-- Databricks notebook source
-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create circuits Raw Table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.circuits;
CREATE TABLE IF NOT EXISTS f1_raw.circuits(
  circuitId INT,
  circuitRef STRING,
  name STRING,
  location STRING,
  country STRING,
  lat DOUBLE,
  lng DOUBLE,
  alt INT,
  url STRING
)
USING csv
OPTIONS(path "/mnt/drobertsformula1/raw/circuits.csv", header true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create races Raw Table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.races;
CREATE TABLE IF NOT EXISTS f1_raw.races(
  raceId INT,
  year INT,
  round INT,
  circuitId INT,
  name STRING,
  date STRING,
  time STRING
)
USING csv
OPTIONS(path "/mnt/drobertsformula1/raw/races.csv", header true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create constructors Raw Table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;
CREATE TABLE IF NOT EXISTS f1_raw.constructors(
  constructorId INT,
  constructorRef STRING,
  name STRING,
  nationality STRING
)
USING json
OPTIONS(path "/mnt/drobertsformula1/raw/constructors.json")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create drivers Raw Table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;
CREATE TABLE IF NOT EXISTS f1_raw.drivers(
  driverId STRING,
  driverRef STRING,
  number INT,
  code STRING,
  name STRUCT<forename STRING, surname STRING>,
  dob DATE,
  nationality STRING
)
USING json
OPTIONS(path "/mnt/drobertsformula1/raw/drivers.json")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create results Raw Table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results;
CREATE TABLE IF NOT EXISTS f1_raw.results(
  resultId INT,
  raceId INT,
  driverId INT,
  constructorId INT,
  number INT,
  grid INT,
  position INT,
  positionText STRING,
  positionOrder INT,
  points DOUBLE,
  laps INT,
  time STRING,
  milliseconds INT,
  fastestLap INT,
  rank INT,
  fastestLapTime STRING,
  fastestLapSpeed STRING,
  statusId INT
)
USING json
OPTIONS(path "/mnt/drobertsformula1/raw/results.json")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create pit_stops Raw Table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pit_stops;
CREATE TABLE IF NOT EXISTS f1_raw.pit_stops(
  raceId INT,
  driverId INT,
  stop INT,
  lap INT,
  time STRING,
  duration STRING,
  milliseconds INT
)
USING json
OPTIONS(path "/mnt/drobertsformula1/raw/pit_stops.json", multiline true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create lap_times Raw Table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times;
CREATE TABLE IF NOT EXISTS f1_raw.lap_times(
  raceId INT,
  driverId INT,
  lap INT,
  position INT,
  time STRING,
  milliseconds INT
)
USING csv
OPTIONS(path "/mnt/drobertsformula1/raw/lap_times/*.csv", header true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create qualifying Raw Table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;
CREATE TABLE IF NOT EXISTS f1_raw.qualifying(
  qualifyId INT,
  raceId INT,
  driverId INT,
  constructorId INT,
  number INT,
  position INT,
  q1 STRING,
  q2 STRING,
  q3 STRING
)
USING json
OPTIONS(path "/mnt/drobertsformula1/raw/qualifying/*.json", multiline true)

-- COMMAND ----------

SELECT * FROM f1_raw.qualifying;

-- COMMAND ----------

DESCRIBE EXTENDED f1_raw.qualifying

-- COMMAND ----------


