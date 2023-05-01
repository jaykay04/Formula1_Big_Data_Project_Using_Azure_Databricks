# Databricks notebook source
# MAGIC %md
# MAGIC #### Mount Azure Data Lake Containers for Our Databricks Project

# COMMAND ----------

def mount_adls(sa_name, container_name):
    # Set secrets from key vaults
    client_id = dbutils.secrets.get(scope = "databricks-project-scope", key = "databricksprojectdl-client-id")
    tenant_id = dbutils.secrets.get(scope = "databricks-project-scope", key = "databricksprojectdl-tenant-id")
    client_secret = dbutils.secrets.get(scope = "databricks-project-scope", key = "databricksprojectdl-client-secret")

    # Set Spark Configurations
    configs = {"fs.azure.account.auth.type": "OAuth",
            "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
            "fs.azure.account.oauth2.client.id": client_id,
            "fs.azure.account.oauth2.client.secret": client_secret,
            "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

    # Unmount Mount Point if already exists
    if any(mount.mountPoint == f"/mnt/{sa_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{sa_name}/{container_name}")

    # Mount the Storage Account Containers
    dbutils.fs.mount(
        source = f"abfss://{container_name}@{sa_name}.dfs.core.windows.net/",
        mount_point = f"/mnt/{sa_name}/{container_name}",
        extra_configs = configs)

    display(dbutils.fs.mounts())

# COMMAND ----------

mount_adls("databricksprojectdl", "raw")

# COMMAND ----------

mount_adls("databricksprojectdl", "processed")

# COMMAND ----------

mount_adls("databricksprojectdl", "presentation")

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

mount_adls("databricksprojectdl", "demo")

# COMMAND ----------

## To Remove a mount
## dbutils.fs.unmount('/mnt/databricksprojectdl/demo')