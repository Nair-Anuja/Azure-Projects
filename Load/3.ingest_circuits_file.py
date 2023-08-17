# Databricks notebook source
# MAGIC %md
# MAGIC ###### Run the configuration file 

# COMMAND ----------

# MAGIC %run "../common/configurations"

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
response = requests.get("https://ergast.com/api/f1/circuits.json?limit=200")
 
# print response
print(response.json())



# COMMAND ----------

circuits = response.json()
circuits_df = pd.DataFrame(circuits["MRData"]["CircuitTable"]['Circuits'])
#display(circuits_df)


# COMMAND ----------

location_df= pd.json_normalize(circuits_df['Location'], max_level=0)

# COMMAND ----------

circuits_df_concat = pd.concat([circuits_df.drop(['Location'], axis=1), location_df], axis=1)

# COMMAND ----------

circuits_sdf = spark.createDataFrame(circuits_df_concat)


# COMMAND ----------


circuits_final_df = circuits_sdf .withColumnRenamed("circuitid","circuitref")\
                          .withColumnRenamed("lat","latitude")\
                          .withColumnRenamed("long","longitude") \
                          .withColumn("source",lit(v_data_source) ) \
                          .withColumn("loaddate",current_date())       

                        
display(circuits_final_df )

#df.write.csv(f"{raw_folder_path}/driverstest.csv")


# COMMAND ----------

circuits_final_df .write.mode("overwrite").format("delta").saveAsTable("f1_raw.circuits")  