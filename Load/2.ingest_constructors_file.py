# Databricks notebook source
# MAGIC %md
# MAGIC ###### Run the configuration file 

# COMMAND ----------

# MAGIC %run "../common/configurations"

# COMMAND ----------

from pyspark.sql.functions import lit,current_date,col

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
response = requests.get("https://ergast.com/api/f1/constructors.json?limit=500")

print(response.json())



# COMMAND ----------

# MAGIC %md
# MAGIC ##### Convert the json response to a pandas dataframe
# MAGIC ##### The column names are case sensitive

# COMMAND ----------

constructors = response.json()
constructors_df = pd.DataFrame(constructors["MRData"]["ConstructorTable"]['Constructors']) 
display(constructors_df)


# COMMAND ----------

constructors_sdf = spark.createDataFrame(constructors_df)
display(constructors_sdf)

# COMMAND ----------


constructors_sdf = constructors_sdf .withColumnRenamed("constructorid","constructorref")\
                          .withColumnRenamed("name","constructorname")\
                          .withColumn("source",lit(v_data_source) ) \
                          .withColumn("loaddate",current_date())     

constructors_final_df =constructors_sdf.drop(col("url"))                         
display(constructors_final_df)

#df.write.csv(f"{raw_folder_path}/driverstest.csv")


# COMMAND ----------

constructors_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_raw.constructors")  

# COMMAND ----------

