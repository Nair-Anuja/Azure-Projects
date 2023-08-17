# Databricks notebook source
# MAGIC %md
# MAGIC ###### Run the configuration file 

# COMMAND ----------

# MAGIC %run "../common/configurations"
# MAGIC

# COMMAND ----------

# MAGIC %run "../common/commonfunctions"

# COMMAND ----------

from pyspark.sql.functions import  lit,current_date,col
import requests
import pandas as pd

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Set the variables for source and load date

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")



# COMMAND ----------

df = spark.read.table("f1_raw.races")
seasons_df =  df.select(['race_year','round']).distinct()
seasons_df = seasons_df.filter((seasons_df.race_year >= "2012") )

# COMMAND ----------

from delta.tables import DeltaTable
spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

# COMMAND ----------

for row in seasons_df.collect():
    race_year =row["race_year"]
    round     =row["round"]
     

    response    = requests.get(f"https://ergast.com/api/f1/{race_year}/{round}/pitstops.json?limit=100")
    if response.status_code == 200:
        pitstops    = response.json()
        if len(pitstops["MRData"]["RaceTable"]["Races"]) != 0:
            pitstops_df = pd.DataFrame(pitstops["MRData"]["RaceTable"]["Races"][0]["PitStops"])


            pitstops_sdf = spark.createDataFrame(pitstops_df)

            pitstops_final_sdf = pitstops_sdf.withColumn("source",lit(v_data_source) ) \
                                  .withColumn("loaddate",current_date()) \
                                  .withColumn("race_year",lit(f"{race_year}"))\
                                  .withColumn("round",lit(f"{round}"))        


            pitstops_final_sdf.write.mode("overwrite").partitionBy('race_year','round').format  ("delta").saveAsTable("f1_raw.pitstops")   


# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC --drop table f1_processed.pitstops
# MAGIC select race_year,round,count(*) from f1_raw.pitstops group by race_year,round order by race_year,round

# COMMAND ----------

