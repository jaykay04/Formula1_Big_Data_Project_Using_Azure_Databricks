-- Databricks notebook source
SELECT team_name,
      COUNT(1) AS total_races,
      SUM(calculated_points) AS total_points,
      ROUND(AVG(calculated_points),2) AS average_points
  FROM f1_presentation.calculated_race_results
GROUP BY team_name
HAVING COUNT(1) >= 100 
ORDER BY average_points DESC;

-- COMMAND ----------

SELECT team_name,
      COUNT(1) AS total_races,
      SUM(calculated_points) AS total_points,
      ROUND(AVG(calculated_points),2) AS average_points
  FROM f1_presentation.calculated_race_results
  WHERE race_year BETWEEN 2011 AND 2020
GROUP BY team_name
HAVING COUNT(1) >= 100 
ORDER BY average_points DESC;

-- COMMAND ----------

SELECT team_name,
      COUNT(1) AS total_races,
      SUM(calculated_points) AS total_points,
      ROUND(AVG(calculated_points),2) AS average_points
  FROM f1_presentation.calculated_race_results
  WHERE race_year BETWEEN 2001 AND 2011
GROUP BY team_name
HAVING COUNT(1) >= 100 
ORDER BY average_points DESC;

-- COMMAND ----------

