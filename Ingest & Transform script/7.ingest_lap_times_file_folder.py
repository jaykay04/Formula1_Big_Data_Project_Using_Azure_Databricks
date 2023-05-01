# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest the lap_times folder

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the CSV files in the lap_times folder

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configurations"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

lap_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), True),
    StructField("lap", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

lap_df = spark.read\
    .schema(lap_schema)\
    .csv(f"{raw_folder_path}/{v_file_date}/lap_times")

display(lap_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename the columns and add the ingestion date column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp


lap_renamed_df = lap_df.withColumnRenamed("raceId", "race_id")\
    .withColumnRenamed("driverId", "driver_id")

display(lap_renamed_df)

# COMMAND ----------

lap_renamed_df = add_ingestion_date(lap_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - write output to datalake in parquet

# COMMAND ----------

# lap_renamed_df.write.mode("overwrite").parquet("/mnt/databricksprojectdl/processed/lap_times")

# COMMAND ----------

# lap_renamed_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.lap_times")

# COMMAND ----------

# overwrite_partition(lap_renamed_df, "f1_processed", "lap_times", "race_id")

# COMMAND ----------

merge_condition = "tgt.driver_id = src.driver_id AND tgt.race_id = src.race_id AND tgt.lap = src.lap"
merge_delta_data(lap_renamed_df, "f1_processed", "lap_times", processed_folder_path, merge_condition, "race_id")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.lap_times;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(1) FROM f1_processed.lap_times
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC;

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP TABLE f1_processed.lap_times;