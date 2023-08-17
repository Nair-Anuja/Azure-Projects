# Databricks notebook source
# MAGIC %md
# MAGIC ###### Run the configuration file 

# COMMAND ----------

# MAGIC %run "../common/configurations"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp 

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------


seasons_df = spark.read.format("delta").load(f"{raw_folder_path}/seasons")\
    .select("season","source")\
    .withColumn("load_date",current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Drop duplicates

# COMMAND ----------

seasons_final_df = seasons_df.dropDuplicates(["season"])

# COMMAND ----------

seasons_final_df .write.mode("overwrite").format("delta").saveAsTable("f1_processed.seasons")  

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.seasons

# COMMAND ----------

