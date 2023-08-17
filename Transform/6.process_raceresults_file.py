# Databricks notebook source
# MAGIC %md
# MAGIC ###### Run the configuration file 

# COMMAND ----------

# MAGIC %run "../common/configurations"
# MAGIC

# COMMAND ----------

# MAGIC %run "../common/commonfunctions"

# COMMAND ----------

from pyspark.sql.functions import current_date


# COMMAND ----------

from pyspark.sql.types import IntegerType,StringType,DateType,FloatType

# COMMAND ----------

from delta.tables import DeltaTable
spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

# COMMAND ----------

raceresults_df = spark.read.format("delta").load(f"{raw_folder_path}/race_results")\
    .select("number", "position", "grid","laps","status","points" ,"driverref",  "driverfirstname","driverlastname","driver_nationality","driver_dob", "constructorref", "constructorname", "laptime_time","laptime_millis","fastestlap_avgspeed_kph","fastestlap_rank","fastestlap_lap","fastestlap_time","race_year","round")\
    .withColumn("number",col("number").cast(IntegerType()))\
    .withColumn("position",col("position").cast(IntegerType()))\
    .withColumn("grid",col("grid").cast(IntegerType()))\
    .withColumn("laps",col("laps").cast(IntegerType()))\
    .withColumn("points",col("points").cast(IntegerType()))\
    .withColumn("fastestlap_avgspeed_kph",col("fastestlap_avgspeed_kph").cast(FloatType()))\
    .withColumn("fastestlap_rank",col("fastestlap_rank").cast(IntegerType()))\
    .withColumn("fastestlap_lap",col("fastestlap_lap").cast(IntegerType()))\
    .withColumn("laptime_millis",col("number").cast(IntegerType()))\
    .withColumn("driver_dob",col("driver_dob").cast(DateType()))\
    .withColumnRenamed("driver_nationality","drivernationality")\
    .withColumnRenamed("driver_dob","driverdob")\
    .withColumn("load_date",current_date()) 
                   
    
   

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Drop duplicates

# COMMAND ----------

raceresults_final_df = raceresults_df.dropDuplicates(["number", "position", "grid","laps","status","points" ,"driverref",  "driverfirstname","driverlastname","drivernationality","driverdob", "constructorref", "constructorname", "laptime_time","laptime_millis","fastestlap_avgspeed_kph","fastestlap_rank","fastestlap_lap","fastestlap_time","race_year","round"])

# COMMAND ----------

 raceresults_final_df.write.mode("overwrite").partitionBy('race_year','round').format("delta").saveAsTable("f1_processed.race_results")   


# COMMAND ----------

                           

# COMMAND ----------

# MAGIC %sql
# MAGIC --drop table f1_processed.race_results
# MAGIC select race_year,round,count(*) from f1_processed.race_results group by race_year,round order by race_year

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select race_year,count(distinct round) from f1_processed.race_results group by race_year order by race_year