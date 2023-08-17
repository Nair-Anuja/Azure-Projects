# Databricks notebook source
# MAGIC %md
# MAGIC ###### Run the configuration file 

# COMMAND ----------

# MAGIC %run "../common/configurations"
# MAGIC

# COMMAND ----------

# MAGIC %run "../common/commonfunctions"

# COMMAND ----------

from pyspark.sql.types import  StructType,StructField,IntegerType,StringType,DateType

# COMMAND ----------

from pyspark.sql.functions import current_date,cast

# COMMAND ----------


races_df = spark.read.format("delta").load(f"{raw_folder_path}/races")\
     .select("race_year","round","raceName","race_date","circuitId","source")\
     .withColumn("race_year",col("race_year").cast(IntegerType()))\
     .withColumn("round",col("round").cast(IntegerType()))\
     .withColumn("race_date",col("race_date").cast(DateType()))\
     .withColumnRenamed("raceName","race_name")\
     .withColumn("load_date",current_date())
 


# COMMAND ----------

# MAGIC %md
# MAGIC ##### drop duplicates

# COMMAND ----------

races_final_df = races_df.dropDuplicates(["race_year","round","race_name","race_date","circuitId","source"])

# COMMAND ----------

races_final_df .write.mode("overwrite").format("delta").saveAsTable("f1_processed.races")  

# COMMAND ----------

# MAGIC %sql
# MAGIC --drop table f1_processed.races
# MAGIC select race_year,count(*) from f1_processed.races group by race_year order by race_year

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from  f1_processed.races limit 10

# COMMAND ----------

