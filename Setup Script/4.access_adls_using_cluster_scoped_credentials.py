# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using cluster scoped credentials
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuit.csv file

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@databricksprojectdl.dfs.core.windows.net"))

# COMMAND ----------

demo=spark.read.csv("abfss://demo@databricksprojectdl.dfs.core.windows.net/circuits.csv")
demo.show()

# COMMAND ----------

display(demo)

# COMMAND ----------

