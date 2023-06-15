-- Databricks notebook source
SELECT * FROM f1_presentation.calculated_race_results;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Total Points by Team

-- COMMAND ----------

SELECT 
  team,
  count(1) as total_races,
  sum(calculated_points) as total_points,
  AVG(calculated_points) as avg_points
FROM f1_presentation.calculated_race_results
GROUP BY team
HAVING total_races > 100
ORDER BY avg_points desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Total Points by Team Last Decade

-- COMMAND ----------

SELECT 
  team,
  count(1) as total_races,
  sum(calculated_points) as total_points,
  AVG(calculated_points) as avg_points
FROM f1_presentation.calculated_race_results
WHERE race_year BETWEEN 2012 AND 2022
GROUP BY team
HAVING total_races > 100
ORDER BY avg_points desc

-- COMMAND ----------

SELECT 
  team,
  count(1) as total_races,
  sum(calculated_points) as total_points,
  AVG(calculated_points) as avg_points
FROM f1_presentation.calculated_race_results
WHERE race_year BETWEEN 2002 AND 2012
GROUP BY team
HAVING total_races > 100
ORDER BY avg_points desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Total Points by Team by Year

-- COMMAND ----------

SELECT 
  race_year,
  team,
  sum(calculated_points) as total_points
FROM f1_presentation.calculated_race_results
GROUP BY team, race_year
ORDER BY race_year desc, total_points desc


-- COMMAND ----------


