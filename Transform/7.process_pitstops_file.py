# Databricks notebook source
# MAGIC %md
# MAGIC ###### Run the configuration file 

# COMMAND ----------

# MAGIC %run "../common/configurations"
# MAGIC

# COMMAND ----------

# MAGIC %run "../common/commonfunctions"

# COMMAND ----------

from pyspark.sql.functions import current_date,col


# COMMAND ----------

from pyspark.sql.types import IntegerType, StringType

# COMMAND ----------

pistops_df = spark.read.format("delta").load(f"{raw_folder_path}/pitstops")\
    .select("driverid", "lap", "stop","time","duration","race_year" ,"round",  "source")\
    .withColumn("lap",col("lap").cast(IntegerType()))\
    .withColumn("stop",col("stop").cast(IntegerType()))\
    .withColumnRenamed("driverid","driverref")\
    .withColumn("load_date",current_date())   

                   

# COMMAND ----------


   
pistops_df.filter("driverref IS NULL OR lap IS NULL OR duration is NULL").show()
#pistops_df.filter(df.state.isNull() & df.gender.isNull()).show()
 
     

# COMMAND ----------

# MAGIC %md
# MAGIC ##### drop duplicates

# COMMAND ----------

pitstops_final_df= pistops_df.dropDuplicates(["driverref", "lap", "stop","time","duration","race_year" ,"round",  "source"])

# COMMAND ----------

pitstops_final_df.write.mode("overwrite").partitionBy('race_year','round').format("delta").saveAsTable("f1_processed.pitstops")   


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.pitstops limit 10

# COMMAND ----------

