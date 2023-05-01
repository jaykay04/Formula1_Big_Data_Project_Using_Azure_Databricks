-- Databricks notebook source
SELECT team_name,
      COUNT(1) AS total_races,
      SUM(calculated_points) AS total_points,
      ROUND(AVG(calculated_points),2) AS average_points,
      rank() OVER (ORDER BY ROUND(AVG(calculated_points),2) DESC) AS driver_rank
  FROM f1_presentation.calculated_race_results
GROUP BY team_name
HAVING COUNT(1) >= 100 
ORDER BY average_points DESC;

-- COMMAND ----------

SELECT race_year,team_name,
      COUNT(1) AS total_races,
      SUM(calculated_points) AS total_points,
      ROUND(AVG(calculated_points),2) AS average_points,
      RANK() OVER (ORDER BY ROUND(AVG(calculated_points),2) DESC) AS driver_rank
  FROM f1_presentation.calculated_race_results
GROUP BY race_year,team_name
ORDER BY race_year,average_points DESC;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC html = """<h1 style="color:Black;text-align:center;font-family:Ariel">Report on Dominant Formula 1 Teams </h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_dominant_teams
AS
SELECT team_name,
      COUNT(1) AS total_races,
      SUM(calculated_points) AS total_points,
      ROUND(AVG(calculated_points),2) AS average_points,
      rank() OVER (ORDER BY ROUND(AVG(calculated_points),2) DESC) AS team_rank
  FROM f1_presentation.calculated_race_results
GROUP BY team_name
HAVING COUNT(1) >= 100 
ORDER BY average_points DESC;

-- COMMAND ----------

SELECT race_year,team_name,
      COUNT(1) AS total_races,
      SUM(calculated_points) AS total_points,
      ROUND(AVG(calculated_points),2) AS average_points,
      RANK() OVER (ORDER BY ROUND(AVG(calculated_points),2) DESC) AS team_rank
  FROM f1_presentation.calculated_race_results
  WHERE team_name IN (SELECT team_name FROM v_dominant_teams WHERE team_rank <= 5)
GROUP BY race_year,team_name
ORDER BY race_year,average_points DESC;

-- COMMAND ----------

SELECT race_year,team_name,
      COUNT(1) AS total_races,
      SUM(calculated_points) AS total_points,
      ROUND(AVG(calculated_points),2) AS average_points,
      RANK() OVER (ORDER BY ROUND(AVG(calculated_points),2) DESC) AS team_rank
  FROM f1_presentation.calculated_race_results
  WHERE team_name IN (SELECT team_name FROM v_dominant_teams WHERE team_rank <= 5)
GROUP BY race_year,team_name
ORDER BY race_year,average_points DESC;

-- COMMAND ----------

