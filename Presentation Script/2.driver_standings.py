# Databricks notebook source
# MAGIC %md
# MAGIC #### Produce Driver Standing

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %run "../includes/configurations"

# COMMAND ----------

# MAGIC %md
# MAGIC Find race years for which data is to be processed

# COMMAND ----------

race_result_list = spark.read.format("delta").load(f"{presentation_folder_path}/race_results")\
.filter(f"file_date = '{v_file_date}'")\
.select("race_year").distinct().collect()

# COMMAND ----------

race_year_list = []
for race_year in race_result_list:
    race_year_list.append(race_year.race_year)

# COMMAND ----------

from pyspark.sql.functions import *

race_result_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results")\
                .filter(col("race_year").isin(race_year_list))

# COMMAND ----------



driver_standing_df = race_result_df.groupBy("race_year", "driver_name", "driver_nationality")\
                                .agg(sum("points").alias("total_points"),
                                count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))

final_driver_standing = driver_standing_df.withColumn("rank", rank().over(driver_rank_spec))

# COMMAND ----------

# final_driver_standing.write.mode("overwrite").parquet(f"{presentation_folder_path}/driver_standings")

# COMMAND ----------

# overwrite_partition(final_driver_standing, "f1_presentation", "driver_standings", "race_year")

# COMMAND ----------

merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_year = src.race_year"
merge_delta_data(final_driver_standing, "f1_presentation", "driver_standings", presentation_folder_path, merge_condition, "race_year")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.driver_standings;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_year, COUNT(1)
# MAGIC   FROM f1_presentation.driver_standings
# MAGIC GROUP BY race_year
# MAGIC ORDER BY race_year DESC;

# COMMAND ----------

