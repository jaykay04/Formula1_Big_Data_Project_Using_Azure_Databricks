# Databricks notebook source
# MAGIC %md
# MAGIC ##### Step 1 - Read the File using Spark Dataframe Reader

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configurations"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/databricksprojectdl/raw"

# COMMAND ----------

# Define the Schema
from pyspark.sql.types import *
races_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("year", IntegerType(), True),
    StructField("round", IntegerType(), True),
    StructField("circuitId", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("date", StringType(), True),
    StructField("time", StringType(), True),
    StructField("url", StringType(), True)
    ])

# COMMAND ----------

races_df = spark.read\
        .option("header", True)\
        .schema(races_schema)\
        .csv(f"{raw_folder_path}/{v_file_date}/races.csv")

races_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Select Only required columns

# COMMAND ----------

from pyspark.sql.functions import *

races_selected_df = races_df.select(col("raceId"), col("year"), col("round"), col("circuitId"), col("name"), col("date"), col("time"))
    
display(races_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Rename required columns

# COMMAND ----------

races_renamed_df = races_selected_df.withColumnRenamed("raceId","race_id")\
                                    .withColumnRenamed("year","race_year")\
                                    .withColumnRenamed("circuitId","circuit_id")\

display(races_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Add race timestamp and ingestion date

# COMMAND ----------

races_final_df = races_renamed_df.withColumn("race_timestamp", to_timestamp(concat(col("date"),lit(" "),col("time")),"yyyy-MM-dd HH:mm:ss"))\
                                .withColumn("ingestion_date", current_timestamp())\
                                .withColumn("data_source", lit(v_data_source))\
                                .withColumn("file_date", lit(v_file_date))


display(races_final_df)

# COMMAND ----------

races_final_df2 = races_final_df.select(col("race_id"), col("race_year"), col("round"), col("circuit_id"), col("name"), col("race_timestamp"), col("ingestion_date"), col("data_source"), col("file_date"))

display(races_final_df2)

# COMMAND ----------

races_final_df2.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 5 - Write the data to data lake as parquet

# COMMAND ----------

#races_final_df2.write.mode("overwrite").partitionBy("race_year").parquet("/mnt/databricksprojectdl/processed/races")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Lets write it to datalake and create table at the same time

# COMMAND ----------

races_final_df2.write.mode("overwrite").format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC   FROM f1_processed.races;

# COMMAND ----------

