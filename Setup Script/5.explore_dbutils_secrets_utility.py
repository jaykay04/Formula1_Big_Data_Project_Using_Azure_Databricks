# Databricks notebook source
dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope = "databricks-project-scope")

# COMMAND ----------

dbutils.secrets.get(scope = "databricks-project-scope", key = "databricksprojectdl-account-key")

# COMMAND ----------

