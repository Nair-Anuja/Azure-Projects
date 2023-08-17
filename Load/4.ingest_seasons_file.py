# Databricks notebook source
# MAGIC %md
# MAGIC ###### Run the configuration file 

# COMMAND ----------

# MAGIC %run "../common/configurations"

# COMMAND ----------

from pyspark.sql.functions import  current_date,col,lit

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
response = requests.get("https://ergast.com/api/f1/seasons.json?limit=100")
 
# print response
#print(response)
 
# print json content
print(response.json())



# COMMAND ----------

seasons = response.json()
seasons_df = pd.DataFrame(seasons["MRData"]["SeasonTable"]['Seasons'])
#display(seasons_df)


# COMMAND ----------

seasons_sdf = spark.createDataFrame(seasons_df)


# COMMAND ----------

seasons_final_df= seasons_sdf.drop(col("url"))

# COMMAND ----------


seasons_final_df = seasons_final_df .withColumn("source",lit(v_data_source) ) \
                            .withColumn("loaddate",current_date())     

                        
display(seasons_final_df )

#df.write.csv(f"{raw_folder_path}/driverstest.csv")


# COMMAND ----------

seasons_final_df .write.mode("overwrite").format("delta").saveAsTable("f1_raw.seasons")  

# COMMAND ----------

# MAGIC %sql
# MAGIC select max(season) from f1_raw.seasons

# COMMAND ----------

