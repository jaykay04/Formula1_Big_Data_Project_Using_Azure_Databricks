# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using Access Keys
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuit.csv file

# COMMAND ----------

access_key = dbutils.secrets.get(scope = "databricks-project-scope", key = "databricksprojectdl-account-key")

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.databricksprojectdl.dfs.core.windows.net",
    access_key
)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@databricksprojectdl.dfs.core.windows.net"))

# COMMAND ----------

demo=spark.read.csv("abfss://demo@databricksprojectdl.dfs.core.windows.net/circuits.csv")
demo.show()

# COMMAND ----------

display(demo)

# COMMAND ----------

