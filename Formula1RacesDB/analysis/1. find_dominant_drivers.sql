-- Databricks notebook source
SELECT * FROM f1_presentation.calculated_race_results;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Total Points by Driver

-- COMMAND ----------

SELECT 
  driver_name,
  count(1) as total_races,
  sum(calculated_points) as total_points,
  AVG(calculated_points) as avg_points,
  RANK() OVER(ORDER BY AVG(calculated_points) DESC) as driver_rank
FROM f1_presentation.calculated_race_results
GROUP BY driver_name
HAVING total_races > 50
ORDER BY avg_points desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Total Points by Driver Last Decade

-- COMMAND ----------

SELECT 
  driver_name,
  count(1) as total_races,
  sum(calculated_points) as total_points,
  AVG(calculated_points) as avg_points
FROM f1_presentation.calculated_race_results
WHERE race_year BETWEEN 2012 AND 2022
GROUP BY driver_name
HAVING total_races > 50
ORDER BY avg_points desc

-- COMMAND ----------

SELECT 
  driver_name,
  count(1) as total_races,
  sum(calculated_points) as total_points,
  AVG(calculated_points) as avg_points
FROM f1_presentation.calculated_race_results
WHERE race_year BETWEEN 2002 AND 2012
GROUP BY driver_name
HAVING total_races > 50
ORDER BY avg_points desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Total Points by Driver by Year

-- COMMAND ----------

SELECT 
  race_year,
  driver_name,
  sum(calculated_points) as total_points,
  AVG(calculated_points) as avg_points
FROM f1_presentation.calculated_race_results
GROUP BY driver_name, race_year
ORDER BY race_year desc, avg_points desc


-- COMMAND ----------


