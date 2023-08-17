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



# COMMAND ----------

def mount(containername,storageaccountname):
    
    mountpoint =f"/mnt/{storageaccountname}/{containername}"

    if not any (mountpoint == mount.mountPoint for mount in dbutils.fs.mounts()):
        dbutils.fs.mount(
        source = f"abfss://{containername}@{storageaccountname}.dfs.core.windows.net/",
        mount_point = mountpoint,
        extra_configs = configs)

    

# COMMAND ----------

containers = ['raw','processed','presentation','lookup']

for containername in containers:
    mount(containername,storageaccountname)

display(dbutils.fs.mounts())


# COMMAND ----------

