# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest the qualifying file which is a folder that conatins multi line json

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

# MAGIC %md
# MAGIC #### Step 1 - Read the data using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

qual_schema = StructType(fields=[
    StructField("qualifyId", IntegerType(), False),
    StructField("raceId", IntegerType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("constructorId", IntegerType(), True),
    StructField("number", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("q1", StringType(), True),
    StructField("q2", StringType(), True),
    StructField("q3", StringType(), True)
])

# COMMAND ----------

qual_df = spark.read\
        .schema(qual_schema)\
        .option("multiLine", True)\
        .json(f"{raw_folder_path}/{v_file_date}/qualifying")

display(qual_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - rename columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import *

qual_renamed_df = qual_df.withColumnRenamed("qualifyId", "qualify_id")\
                        .withColumnRenamed("raceId", "race_id")\
                        .withColumnRenamed("driverId", "driver_id")\
                        .withColumnRenamed("constructorId", "constructor_id")

display(qual_renamed_df)

# COMMAND ----------

qual_renamed_df = add_ingestion_date(qual_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - write output file to datalake in parquet format

# COMMAND ----------

# qual_renamed_df.write.mode("overwrite").parquet("/mnt/databricksprojectdl/processed/qualifying")

# COMMAND ----------

# qual_renamed_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.qualifying")

# COMMAND ----------

# overwrite_partition(qual_renamed_df, "f1_processed", "qualifying", "race_id")

# COMMAND ----------

merge_condition = " tgt.qualify_id = src.qualify_id AND tgt.race_id = src.race_id"
merge_delta_data(qual_renamed_df, "f1_processed", "qualifying", processed_folder_path, merge_condition, "race_id")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.qualifying;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(1) FROM f1_processed.qualifying
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC;

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP TABLE f1_processed.qualifying;

# COMMAND ----------

