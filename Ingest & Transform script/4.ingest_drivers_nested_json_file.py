# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest drivers.json file which is ina nested json format

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the json file using the spark dataframe reader API

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

from pyspark.sql.types import *

# COMMAND ----------

# Because it is a nested json, we define two schemas as seen in the file

name_schema = StructType(fields=[
    StructField("forename", StringType(), True),
    StructField("surname", StringType(), True)
])


drivers_schema = StructType(fields=[
    StructField("driverId", IntegerType(), False),
    StructField("driverRef", StringType(), True),
    StructField("number", IntegerType(), True),
    StructField("code", StringType(), True),
    StructField("name", name_schema),
    StructField("dob", DateType(), True),
    StructField("nationality", StringType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

drivers_df = spark.read\
                .schema(drivers_schema)\
                .json(f"{raw_folder_path}/{v_file_date}/drivers.json")

display(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add new columns
# MAGIC 1. driverId to driver_id
# MAGIC 2. driverRef to driver_ref
# MAGIC 3. add ingestion_date
# MAGIC 4. concatenate forename and surname
# MAGIC 5. drop unwanted column

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

drivers_renamed_df = drivers_df.withColumnRenamed("driverId", "driver_id") \
                               .withColumnRenamed("driverRef", "driver_ref") \
                               .withColumn("ingestion_date", current_timestamp()) \
                               .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname")))\
                                .withColumn("data_source", lit(v_data_source))\
                                .withColumn("file_date", lit(v_file_date))\
                               .drop(col("url"))

display(drivers_renamed_df)

# COMMAND ----------

# drivers_renamed_df.write.mode("overwrite").parquet("/mnt/databricksprojectdl/processed/drivers")

# COMMAND ----------

drivers_renamed_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.drivers")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.drivers