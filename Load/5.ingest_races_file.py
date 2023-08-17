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

df = spark.read.table("f1_raw.seasons")
seasons_df =  df.select(['season']).distinct()
#seasons_df = seasons_df.filter("season=2022")

# COMMAND ----------

from delta.tables import DeltaTable
spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

# COMMAND ----------

for row in seasons_df.collect():
    season=row["season"]
    partition_column=season
    response = requests.get(f"https://ergast.com/api/f1/{season}/races.json")
    races    = response.json()
    races_df = pd.DataFrame(races["MRData"]["RaceTable"]["Races"])

    races_df.rename(columns={"season":"race_year","url": "race_url", "date": "race_date"},inplace=True,copy=True)
    #,"time":"race_time"

    races_df_circuit=unpack(races_df, 'Circuit', fillna=None)

    races_df_circuit.rename(columns={"url": "circuit_url"})
    
    #columns = ["season","round","raceName","race_date","race_time","circuitId","circuitName"]
    

    #races_df_circuit = races_df_circuit.reindex(columns=columns)
    #display(races_df_circuit)
    races_final_df = races_df_circuit[["race_year","round","raceName","race_date","circuitId","circuitName"]]

    races_sdf = spark.createDataFrame(races_final_df)

    races_final_sdf = races_sdf.withColumn("source",lit(v_data_source) ) \
                          .withColumn("loaddate",current_date()) 


    races_final_sdf.write.mode("overwrite").partitionBy('race_year').format("delta").saveAsTable("f1_raw.races")   


# COMMAND ----------

# MAGIC %sql
# MAGIC --drop table f1_processed.races
# MAGIC select race_year,count(*) from f1_raw.races group by race_year order by race_year

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from  f1_raw.races limit 10

# COMMAND ----------

