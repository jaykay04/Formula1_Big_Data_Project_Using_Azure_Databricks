-- Databricks notebook source
SELECT driver_name,
      COUNT(1) AS total_races,
      SUM(calculated_points) AS total_points,
      ROUND(AVG(calculated_points),2) AS average_points,
      rank() OVER (ORDER BY ROUND(AVG(calculated_points),2) DESC) AS driver_rank
  FROM f1_presentation.calculated_race_results
GROUP BY driver_name
HAVING COUNT(1) >= 50 
ORDER BY average_points DESC;

-- COMMAND ----------

SELECT race_year,driver_name,
      COUNT(1) AS total_races,
      SUM(calculated_points) AS total_points,
      ROUND(AVG(calculated_points),2) AS average_points,
      rank() OVER (ORDER BY ROUND(AVG(calculated_points),2) DESC) AS driver_rank
  FROM f1_presentation.calculated_race_results
GROUP BY race_year,driver_name
ORDER BY race_year,average_points DESC;

-- COMMAND ----------

WITH dominant_drivers
AS 
(SELECT race_year,driver_name,
      COUNT(1) AS total_races,
      SUM(calculated_points) AS total_points,
      ROUND(AVG(calculated_points),2) AS average_points,
      rank() OVER (ORDER BY ROUND(AVG(calculated_points),2) DESC) AS driver_rank
  FROM f1_presentation.calculated_race_results
GROUP BY race_year,driver_name
ORDER BY race_year,average_points DESC)

SELECT * FROM dominant_drivers WHERE driver_rank <= 10;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC html = """<h1 style="color:Black;text-align:center;font-family:Ariel">Report On Dominant Formula 1 Drivers </h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_dominant_drivers
AS
SELECT driver_name,
      COUNT(1) AS total_races,
      SUM(calculated_points) AS total_points,
      ROUND(AVG(calculated_points),2) AS average_points,
      rank() OVER (ORDER BY ROUND(AVG(calculated_points),2) DESC) AS driver_rank
  FROM f1_presentation.calculated_race_results
GROUP BY driver_name
HAVING COUNT(1) >= 50 
ORDER BY average_points DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Drivers Performance per year based on their average points

-- COMMAND ----------

SELECT race_year,driver_name,
      COUNT(1) AS total_races,
      SUM(calculated_points) AS total_points,
      ROUND(AVG(calculated_points),2) AS average_points,
      RANK() OVER (ORDER BY ROUND(AVG(calculated_points),2) DESC) AS driver_rank
  FROM f1_presentation.calculated_race_results
  WHERE driver_name IN (SELECT driver_name FROM v_dominant_drivers WHERE driver_rank <= 10)
GROUP BY race_year,driver_name
ORDER BY race_year,average_points DESC;

-- COMMAND ----------

SELECT race_year,driver_name,
      COUNT(1) AS total_races,
      SUM(calculated_points) AS total_points,
      ROUND(AVG(calculated_points),2) AS average_points,
      RANK() OVER (ORDER BY ROUND(AVG(calculated_points),2) DESC) AS driver_rank
  FROM f1_presentation.calculated_race_results
  WHERE driver_name IN (SELECT driver_name FROM v_dominant_drivers WHERE driver_rank <= 10)
GROUP BY race_year,driver_name
ORDER BY race_year,average_points DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC We want to see total races and total points by driver name

-- COMMAND ----------

SELECT race_year,driver_name,
      COUNT(1) AS total_races,
      SUM(calculated_points) AS total_points,
      ROUND(AVG(calculated_points),2) AS average_points,
      RANK() OVER (ORDER BY ROUND(AVG(calculated_points),2) DESC) AS driver_rank
  FROM f1_presentation.calculated_race_results
  WHERE driver_name IN (SELECT driver_name FROM v_dominant_drivers WHERE driver_rank <= 10)
GROUP BY race_year,driver_name
ORDER BY race_year,average_points DESC;

-- COMMAND ----------

