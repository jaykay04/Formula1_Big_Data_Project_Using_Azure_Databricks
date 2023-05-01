# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake Using Service Principal.
# MAGIC 1. Register Azure AD Application/Service Principal
# MAGIC 2. Generate a secret/password for the Application
# MAGIC 3. Set Spark Config with App/Client Id, Directory/Tenant Id & Secret
# MAGIC 4. Assign Role 'Storage Blob Contributor' to the Data Lake

# COMMAND ----------

client_id = dbutils.secrets.get(scope = "databricks-project-scope", key = "databricksprojectdl-client-id")
tenant_id = dbutils.secrets.get(scope = "databricks-project-scope", key = "databricksprojectdl-tenant-id")
client_secret = dbutils.secrets.get(scope = "databricks-project-scope", key = "databricksprojectdl-client-secret")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.databricksprojectdl.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.databricksprojectdl.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.databricksprojectdl.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.databricksprojectdl.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.databricksprojectdl.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@databricksprojectdl.dfs.core.windows.net"))

# COMMAND ----------

df= spark.read.csv("abfss://demo@databricksprojectdl.dfs.core.windows.net/circuits.csv")
display(df)

# COMMAND ----------

