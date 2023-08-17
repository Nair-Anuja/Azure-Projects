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



spark.conf.set("fs.azure.account.auth.type.dldataengineeringdata.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.dldataengineeringdata.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.dldataengineeringdata.dfs.core.windows.net", f"{clientid}")
spark.conf.set(f"fs.azure.account.oauth2.client.secret.dldataengineeringdata.dfs.core.windows.net", f"{secret}")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.dldataengineeringdata.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenantid}/oauth2/token")

# COMMAND ----------



# COMMAND ----------

display(dbutils.fs.ls("abfss://raw@dldataengineeringdata.dfs.core.windows.net"))