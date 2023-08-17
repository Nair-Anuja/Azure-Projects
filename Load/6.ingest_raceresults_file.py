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
#seasons_df = seasons_df.filter("season=2022")

# COMMAND ----------

from delta.tables import DeltaTable
spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

# COMMAND ----------

"""
response = requests.get(f"https://ergast.com/api/f1/2023/1/results.json?limit=1000")
race_results    = response.json()

display(race_results)
race_results_dict = race_results["MRData"]["RaceTable"]["Races"][0]["Results"]

"""

# COMMAND ----------

#display(type(race_results_dict))

# COMMAND ----------

#display(race_results_dict)

# COMMAND ----------

"""
data=[]

res = [ele for ele in ({key: val for key, val in sub.items() if val}
                       for sub in race_results_dict) if ele]

for row in res:
    for key,value in row.items():
        if key=='number':
            number = value
        if key=='position':
            position = value
        if key=='grid':
            grid = value
        if key=='laps':
            laps = value
        if key=='points':
            points = value    
        if key=='status':
            status = value
        if key=='Driver':
            driverref   =value['driverId']
            drivercode  =value['code']
            driverfirstname= value['givenName']
            driverlastname=value['familyName']
            driverdob=value['dateOfBirth']
            drivernationality =value['nationality']
        if key=='Constructor':
            constructorref   =value['constructorId']
            constructorname =value['name']                                           
        if key=='Time':
            laptime_time = value['time']
            laptime_millis = value['millis']
        if key=='FastestLap':
            fastestlap_avgspeed_kph = value['AverageSpeed']['speed']
            fastestlap_rank = value['rank'] 
            fastestlap_lap = value['lap'] 
            fastestlap_time = value['Time']['time'] 
       
             



    #print(row['FastestLap']['Time']['time'])
    #print(row['FastestLap']['lap'])
    data.append([number,position,grid,laps,status,points,driverref,drivercode,driverfirstname,driverlastname,driverdob,drivernationality,constructorref,constructorname,laptime_time,laptime_millis,fastestlap_avgspeed_kph,fastestlap_rank,fastestlap_lap,fastestlap_time])
#display(data)
race_results_df = pd.DataFrame (data,columns=['number', 'position', 'grid','laps','status','points' ,'driverref', 'drivercode', 'driverfirstname','driverlastname','drivernationality','driverdob', 'constructorref', 'constructorname', 'laptime_time','laptime_millis','fastestlap_avgspeed_kph','fastestlap_rank','fastestlap_lap','fastestlap_time'])   

display(race_results_df)
"""

# COMMAND ----------

for row in seasons_df.collect():
    race_year =row["race_year"]
    round     =row["round"]
    
    print(f"processing {race_year} and round {round}")

    response = requests.get(f"https://ergast.com/api/f1/{race_year}/{round}/results.json?limit=1000")
    if response.status_code == 200:
        race_results    = response.json()
        if len(race_results["MRData"]["RaceTable"]["Races"]) != 0:
            race_results_dict = race_results["MRData"]["RaceTable"]["Races"][0]["Results"]

            data=[]

            res = [ele for ele in ({key: val for key, val in sub.items() if val}
                       for sub in race_results_dict) if ele]

            for row in res:
                for key,value in row.items():
                    if key=='number':
                        number = value
                    if key=='position':
                        position = value
                    if key=='grid':
                        grid = value
                    if key=='laps':
                        laps = value
                    if key=='points':
                        points = value    
                    if key=='status':
                        status = value
                    if key=='Driver':
                        driverref   =value['driverId']
                        drivercode ='' #value['code']
                        driverfirstname= value['givenName']
                        driverlastname=value['familyName']
                        driverdob=value['dateOfBirth']
                        drivernationality =value['nationality']
                    if key=='Constructor':
                        constructorref   =value['constructorId']
                        constructorname =value['name']                                           
                    if key=='Time':
                        laptime_time = value['time']
                        laptime_millis = value['millis']
                    if key=='FastestLap':
                        fastestlap_avgspeed_kph = value['AverageSpeed']['speed']
                        fastestlap_rank = value['rank'] 
                        fastestlap_lap = value['lap'] 
                        fastestlap_time = value['Time']['time'] 
       
        
                data.append([number,position,grid,laps,status,points,driverref,drivercode,driverfirstname,driverlastname,driverdob,drivernationality,constructorref,constructorname,laptime_time,laptime_millis,fastestlap_avgspeed_kph,fastestlap_rank,fastestlap_lap,fastestlap_time])

            race_results_df = pd.DataFrame (data,columns=['number', 'position', 'grid','laps','status','points' ,'driverref', 'drivercode', 'driverfirstname','driverlastname','driverdob','drivernationality', 'constructorref', 'constructorname', 'laptime_time','laptime_millis','fastestlap_avgspeed_kph','fastestlap_rank','fastestlap_lap','fastestlap_time'])      

 

            race_results_sdf = spark.createDataFrame(race_results_df)

            races_results_final_sdf = race_results_sdf.withColumn("source",lit(v_data_source) ) \
                          .withColumn("loaddate",current_date()) \
                          .withColumn("race_year",lit(f"{race_year}"))\
                          .withColumn("round",lit(f"{round}")) 


            races_results_final_sdf.write.mode("overwrite").partitionBy('race_year','round').format("delta").saveAsTable("f1_raw.race_results")   


# COMMAND ----------

                           

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC --drop table f1_processed.race_results
# MAGIC select race_year,round,count(*) from f1_raw.race_results group by race_year,round order by race_year

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select race_year,count(distinct round) from f1_raw.race_results group by race_year order by race_year