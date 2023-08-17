# Databricks notebook source
# MAGIC %md
# MAGIC ###### Run the configuration file 

# COMMAND ----------

# MAGIC %run "../common/configurations"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Import the required libraries

# COMMAND ----------

from pyspark.sql.functions import col ,lit,current_date

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Set the variables for source and load date

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")


# COMMAND ----------

# import requests module
import requests
import pandas as pd
 
# Making a get request
response = requests.get("https://ergast.com/api/f1/drivers.json?limit=1000")
 
 
#print json content
#print(response.json())



# COMMAND ----------

drivers = response.json()
drivers_df = pd.DataFrame(drivers["MRData"]["DriverTable"]['Drivers'])
display(drivers_df)


# COMMAND ----------

drivers_sdf = spark.createDataFrame(drivers_df)
display(drivers_sdf)

# COMMAND ----------


drivers_sdf = drivers_sdf .withColumnRenamed("driverid","driverref")\
                          .withColumnRenamed("givenName","firstname")\
                          .withColumnRenamed("familyName","lastname") \
                          .withColumn("source",lit(v_data_source) ) \
                          .withColumn("loaddate",current_date())      

drivers_final_df =drivers_sdf.drop(col("permanentNumber"),col("code"),col("url"))                         
display(drivers_final_df)




# COMMAND ----------

drivers_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_raw.drivers")  

# COMMAND ----------

