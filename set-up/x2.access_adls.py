# Databricks notebook source
# MAGIC %md
# MAGIC #### Set Spark properties to configure Azure credentials to access Azure storage

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope='Formula1Scope')

# COMMAND ----------

clientid=dbutils.secrets.get(scope='Formula1Scope',key='formula1app-clientid')
tenantid=dbutils.secrets.get(scope='Formula1Scope',key='formula1app-tenantid')
secret=dbutils.secrets.get(scope='Formula1Scope',key='formula1app-secret')

# COMMAND ----------



configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": f"{clientid}",
           "fs.azure.account.oauth2.client.secret": f"{secret}",
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenantid}/oauth2/token"}


storageaccountname="dldataengineeringdata"
containername="raw"


# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://raw@dldataengineeringdata.dfs.core.windows.net/",
  mount_point = "/mnt/raw",
  extra_configs = configs)

# COMMAND ----------



# COMMAND ----------

display(dbutils.fs.ls("/mnt/raw"))

# COMMAND ----------

containername='lookup'

# COMMAND ----------

dbutils.fs.mount(
  source = f"abfss://{containername}@{storageaccountname}.dfs.core.windows.net/",
  mount_point = f"/mnt/{containername}",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls(f"/mnt/{containername}"))

# COMMAND ----------

dbutils.fs.unmount("/mnt/lookup")

# COMMAND ----------

