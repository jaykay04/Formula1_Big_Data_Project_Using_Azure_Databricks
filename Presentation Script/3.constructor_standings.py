# Databricks notebook source
# MAGIC %md
# MAGIC #### Produce Constructor Standing

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %run "../includes/configurations"

# COMMAND ----------

race_result_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results")\
    .filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

race_year_list = df_column_to_list(race_result_df, "race_year")

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

race_result_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results")\
    .filter(col("race_year").isin(race_year_list))

# COMMAND ----------



constructor_standing_df = race_result_df.groupBy("race_year", "team")\
                                .agg(sum("points").alias("total_points"),
                                count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

constructor_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))

final_constructor_standing = constructor_standing_df.withColumn("rank", rank().over(constructor_rank_spec))

# COMMAND ----------

# final_constructor_standing.write.mode("overwrite").parquet(f"{presentation_folder_path}/constructor_standings")

# COMMAND ----------

# overwrite_partition(final_constructor_standing, "f1_presentation", "constructor_standings", "race_year")

# COMMAND ----------

merge_condition = "tgt.team = src.team AND tgt.race_year = src.race_year"
merge_delta_data(final_constructor_standing, "f1_presentation", "constructor_standings", presentation_folder_path, merge_condition, "race_year")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.constructor_standings;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_year, COUNT(1)
# MAGIC   FROM f1_presentation.driver_standings
# MAGIC GROUP BY race_year
# MAGIC ORDER BY race_year DESC;