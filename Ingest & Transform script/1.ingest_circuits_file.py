# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest circuit.csv file

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
# MAGIC ##### Step 1 - Read the CSV file using the spark dataframe reader

# COMMAND ----------

# See all our file paths
display(dbutils.fs.mounts())

# COMMAND ----------

# Let us define our schema ourselves without using the inferSchema parameter

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

circuits_schema = StructType(fields=[
    StructField("circuitId", IntegerType(), False),
    StructField("circuitRef", StringType(), True),
    StructField("name", StringType(), True),
    StructField("location", StringType(), True),
    StructField("country", StringType(), False),
    StructField("lat", DoubleType(), True),
    StructField("lng", DoubleType(), True),
    StructField("alt", IntegerType(), False),
    StructField("url", StringType(), False)
])

# COMMAND ----------

circuits_df = spark.read\
    .option("header", True)\
    .schema(circuits_schema)\
    .csv(f"{raw_folder_path}/{v_file_date}/circuits.csv")

circuits_df.printSchema()

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Select only the required columns

# COMMAND ----------

# First Method

circiuts_selected_df = circuits_df.select("circuitId","circuitRef","name","location","country","lat","lng","alt")
display(circiuts_selected_df)

# COMMAND ----------

# Second Method
circuits_selected_df2 = circuits_df.select(circuits_df.circuitId, circuits_df.circuitRef,circuits_df.name,circuits_df.location,circuits_df.country,circuits_df.lat,circuits_df.lng,circuits_df.alt)

display(circuits_selected_df2)

# COMMAND ----------

# Third Method

circuits_selected_df3 = circuits_df.select(circuits_df["circuitId"], circuits_df["circuitRef"],circuits_df["name"],circuits_df["location"],circuits_df["country"],circuits_df["lat"],circuits_df["lng"],circuits_df["alt"])

display(circuits_selected_df3)

# COMMAND ----------

# Fourth Method

from pyspark.sql.functions import col, lit

circuits_selected_df4 = circuits_df.select(col("circuitId"), col("circuitRef"),col("name"),col("location"),col("country"),col("lat"),col("lng"),col("alt"))

display(circuits_selected_df4)

# COMMAND ----------

# Note that the first method doesn't allow you rename the column while the other three methods allow you rename the columns via the alias method

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Rename columns

# COMMAND ----------

circuits_renamed_df = circuits_selected_df4.withColumnRenamed("circuitId","circuit_id")\
                                .withColumnRenamed("CircuitRef","circuit_ref")\
                                .withColumnRenamed("lat","latitude")\
                                .withColumnRenamed("lng","longitude")\
                                .withColumnRenamed("alt","altitude")\
                                .withColumn("data_source", lit(v_data_source))\
                                .withColumn("file_date", lit(v_file_date))

display(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Add Ingestion date to dataframe

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_renamed_df)

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 5 - Write data to data lake as parquet

# COMMAND ----------

# circuits_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/circuits")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### We will replace step 5 above because we want write the data to the files and also create managed tables on top of it

# COMMAND ----------

circuits_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.circuits")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.circuits;

# COMMAND ----------

