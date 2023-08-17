# Databricks notebook source
# MAGIC %md
# MAGIC ###### Run the configuration file 

# COMMAND ----------

# MAGIC %run "../common/configurations"

# COMMAND ----------

from pyspark.sql.functions import col,lit,current_date,concat_ws

# COMMAND ----------

drivers_df = spark.read.format("delta").load(f"{raw_folder_path}/drivers")\
    .select("driverref",concat(col("firstname"),lit(' '),col("lastname")).alias("drivername"),"dateOfBirth","nationality","source")\
    .withColumn("load_date",current_date())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Verify of key columns have nulls

# COMMAND ----------


drivers_df.filter("driverref IS NULL OR drivername IS NULL ").show()

# COMMAND ----------

drivers_final_df = drivers_df.dropDuplicates(["driverref","drivername","dateOfBirth","nationality"])

# COMMAND ----------

drivers_final_df.write.mode("overwrite").format("delta").insertInto("f1_processed.drivers")  

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.drivers

# COMMAND ----------

# MAGIC %sql
# MAGIC select drivername,count(*)
# MAGIC from f1_processed.drivers
# MAGIC group by drivername
# MAGIC having count(*) >1

# COMMAND ----------

