# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake Using SAS token.
# MAGIC 1. Set the spark config for SAS token
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuit.csv file

# COMMAND ----------

sas_token = dbutils.secrets.get(scope = "databricks-project-scope", key = "databricksprojectdl-sas-token")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.databricksprojectdl.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.databricksprojectdl.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.databricksprojectdl.dfs.core.windows.net", sas_token)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@databricksprojectdl.dfs.core.windows.net"))

# COMMAND ----------

df= spark.read.csv("abfss://demo@databricksprojectdl.dfs.core.windows.net/circuits.csv")
display(df)

# COMMAND ----------

