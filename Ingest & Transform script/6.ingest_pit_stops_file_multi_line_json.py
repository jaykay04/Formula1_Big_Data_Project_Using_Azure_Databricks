# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest Pitsops file which is a multi line json

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

# MAGIC %md
# MAGIC ##### Step 1 - Read the multi line json file using the spark dataframe API reader

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------



pitstops_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), True),
    StructField("stop", StringType(), True),
    StructField("lap", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("duration", StringType(), True),
    StructField("milliseconds", IntegerType(), True),
])

# COMMAND ----------

pitstops_df = spark.read\
                   .schema(pitstops_schema)\
                   .option("multiLine", True)\
                   .json(f"{raw_folder_path}/{v_file_date}/pit_stops.json")

display(pitstops_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add ingestion date column

# COMMAND ----------

from pyspark.sql.functions import *

final_df = pitstops_df.withColumnRenamed("raceId", "race_id")\
                    .withColumnRenamed("driverId", "driver_id")\
                    .withColumn("data_source", lit(v_data_source))\
                    .withColumn("file_date", lit(v_file_date))

display(final_df)

# COMMAND ----------

final_df = add_ingestion_date(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - write the output file to datalake as parquet file

# COMMAND ----------

# final_df.write.mode("overwrite").parquet("/mnt/databricksprojectdl/processed/pit_stops")

# COMMAND ----------

# final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.pit_stops")

# COMMAND ----------

#overwrite_partition(final_df, "f1_processed", "pit_stops", "race_id")

# COMMAND ----------

merge_condition = "tgt.driver_id = src.driver_id AND tgt.race_id = src.race_id AND tgt.stop = src.stop"
merge_delta_data(final_df, "f1_processed", "pit_stops", processed_folder_path, merge_condition, "race_id")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.pit_stops;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(1) FROM f1_processed.pit_stops
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC;

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP TABLE f1_processed.pit_stops;

# COMMAND ----------

