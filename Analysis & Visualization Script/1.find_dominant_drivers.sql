-- Databricks notebook source
SELECT * FROM f1_presentation.calculated_race_results;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Let us calculate the total number of points for each driver and rank them based on their accumulated points

-- COMMAND ----------

SELECT driver_name,
      COUNT(1) AS total_races,
      SUM(calculated_points) AS total_points,
      ROUND(AVG(calculated_points),2) AS average_points
  FROM f1_presentation.calculated_race_results
  WHERE race_year BETWEEN 2011 AND 2020
GROUP BY driver_name
HAVING COUNT(1) >= 50 
ORDER BY average_points DESC;

-- COMMAND ----------

SELECT driver_name,
      COUNT(1) AS total_races,
      SUM(calculated_points) AS total_points,
      ROUND(AVG(calculated_points),2) AS average_points
  FROM f1_presentation.calculated_race_results
  WHERE race_year BETWEEN 2001 AND 2010
GROUP BY driver_name
HAVING COUNT(1) >= 50 
ORDER BY average_points DESC;

-- COMMAND ----------

