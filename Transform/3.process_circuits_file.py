# Databricks notebook source
# MAGIC %md
# MAGIC ###### Run the configuration file 

# COMMAND ----------

# MAGIC %run "../common/configurations"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp ,lit,current_date

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------


circuits_df = spark.read.format("delta").load(f"{raw_folder_path}/circuits")\
    .select("circuitref","circuitName","latitude","longitude","locality","country","source")\
    .withColumn("load_date",current_date())



# COMMAND ----------

# MAGIC %md
# MAGIC ##### Verify if key columns have nulls

# COMMAND ----------

circuits_df.filter("circuitref IS NULL OR circuitName IS NULL ").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Drop duplicates

# COMMAND ----------


circuits_final_df =circuits_df.dropDuplicates(["circuitref","circuitName","latitude","longitude","locality","country"])  

# COMMAND ----------

circuits_final_df .write.mode("overwrite").format("delta").saveAsTable("f1_processed.circuits")  