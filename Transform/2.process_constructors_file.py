# Databricks notebook source
# MAGIC %md
# MAGIC ###### Run the configuration file 

# COMMAND ----------

# MAGIC %run "../common/configurations"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp ,lit,current_date


# COMMAND ----------

constructors_df = spark.read.format("delta").load(f"{raw_folder_path}/constructors")\
    .select("constructorref","constructorname","nationality","source")\
    .withColumn("load_date",current_date())


# COMMAND ----------

# MAGIC %md
# MAGIC ##### verify if the key columns have nulls

# COMMAND ----------

constructors_df.filter("constructorref IS NULL OR constructorname IS NULL ").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Drop duplicates
# MAGIC

# COMMAND ----------

   

constructors_final_df =constructors_df.dropDuplicates(["constructorref","constructorname","nationality"])  
                      



# COMMAND ----------

constructors_final_df.write.mode("overwrite").format("delta").insertInto("f1_processed.constructors")  

# COMMAND ----------

