# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest the Result file which is a json format

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
# MAGIC ##### Step 1 - Read the file using the spark dataframe reader API

# COMMAND ----------

# Lets define the schema first
from pyspark.sql.types import *

result_schema = StructType(fields=[
    StructField("resultId", IntegerType(), False),
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), False),
    StructField("constructorId", IntegerType(), False),
    StructField("number", IntegerType(), True),
    StructField("grid", IntegerType(), False),
    StructField("position", IntegerType(), True),
    StructField("positionText", StringType(), False),
    StructField("positionOrder", IntegerType(), False),
    StructField("points", FloatType(), False),
    StructField("laps", IntegerType(), False),
    StructField("time", StringType(), True),
    StructField("milliseconds", IntegerType(), True),
    StructField("fastestLap", IntegerType(), True),
    StructField("rank", IntegerType(), True),
    StructField("fastestLapTime", StringType(), True),
    StructField("fastestLapSpeed", StringType(), True),
    StructField("statusId", IntegerType(), False)

])

# COMMAND ----------

results_df = spark.read\
                  .schema(result_schema)\
                  .json(f"{raw_folder_path}/{v_file_date}/results.json")

display(results_df)

# COMMAND ----------

results_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename the columns, add ingestion date and drop statusId

# COMMAND ----------

from pyspark.sql.functions import *

results_final_df = results_df.withColumnRenamed("resultId", "result_id")\
                             .withColumnRenamed("raceId", "race_id")\
                             .withColumnRenamed("driverId", "driver_id")\
                             .withColumnRenamed("constructorId", "constructor_id")\
                             .withColumnRenamed("positionText", "position_text")\
                             .withColumnRenamed("positionOrder", "position_order")\
                             .withColumnRenamed("fastestLap", "fastest_lap")\
                             .withColumnRenamed("fastestLapTime", "fastest_lap_time")\
                             .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed")\
                             .withColumn("data_source", lit(v_data_source))\
                             .withColumn("file_date", lit(v_file_date))\
                             .drop(col("statusId"))

display(results_final_df)

# COMMAND ----------

results_final_df = add_ingestion_date(results_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC De-dupe the Dataframe

# COMMAND ----------

results_final_df = results_final_df.dropDuplicates(["race_id", "driver_id"])

# COMMAND ----------

#.withColumn("ingestion_date", current_timestamp())\

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - write output to data lake and partition by race_id

# COMMAND ----------

# results_final_df.write.mode("overwrite").partitionBy("race_id").parquet("/mnt/databricksprojectdl/processed/results")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Method 1
# MAGIC ##### ALTER TABLE delete partitons
# MAGIC - First we need to get the distinct partitions and put it in a list by using the collect method
# MAGIC - Then we loop through the list and drop the partition if exists in a for loop.
# MAGIC - Also, we need to also know if the table itself exist

# COMMAND ----------

# for race_id_list in results_final_df.select("race_id").distinct().collect():
#     if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#         spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION(race_id = {race_id_list.race_id})")

# COMMAND ----------

# results_final_df.write.mode("append").partitionBy("race_id").format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Method 2
# MAGIC - Here, instead of an append, we do an overwrite
# MAGIC - First we make sure the race_id partition key is the last column
# MAGIC - We also set the overwrite mode in sparksession to dynamic

# COMMAND ----------

#overwrite_partition(results_final_df, "f1_processed", "results", "race_id")

# COMMAND ----------


merge_condition = "tgt.result_id = src.result_id AND tgt.race_id = src.race_id"
merge_delta_data(results_final_df, "f1_processed", "results", processed_folder_path, merge_condition, "race_id")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.results;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(1) FROM f1_processed.results
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC From what we can see above, we have run thge race_id 1053 twice resulting in duplicates.
# MAGIC If there is an issue where we need to delete a particular partition, we can always use the ALTER syntax DROP partition provided in Spark SQL
# MAGIC As shown above

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, driver_id, COUNT(1)
# MAGIC   FROM f1_processed.results
# MAGIC GROUP BY race_id, driver_id
# MAGIC   HAVING COUNT(1) > 1
# MAGIC ORDER BY race_id, driver_id DESC;

# COMMAND ----------

