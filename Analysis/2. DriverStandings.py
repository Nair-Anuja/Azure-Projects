# Databricks notebook source
# MAGIC %sql
# MAGIC --DROP TABLE f1_presentation.driver_standings;
# MAGIC CREATE TABLE  f1_presentation.driver_standings
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT rr.race_year,
# MAGIC        rr.round,   
# MAGIC        rr.driverref,
# MAGIC        dr.drivername,
# MAGIC        rr.position,
# MAGIC        (11- position) AS calculated_points ,
# MAGIC        rr.constructorname
# MAGIC FROM   f1_processed.race_results rr
# MAGIC JOIN   f1_processed.drivers      dr ON rr.driverref = dr.driverref
# MAGIC WHERE  position <= 10
# MAGIC ORDER BY rr.race_year,
# MAGIC        rr.round, 
# MAGIC        calculated_points
# MAGIC       
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH CTE AS ( 
# MAGIC SELECT   ds.race_year , 
# MAGIC          ds.drivername, 
# MAGIC          ds.constructorname,
# MAGIC          sum(ds.calculated_points) AS points,
# MAGIC          dense_rank() OVER (PARTITION BY ds.race_year  ORDER BY sum(ds.calculated_points)  DESC) AS RNK
# MAGIC FROM     f1_presentation.driver_standings ds 
# MAGIC GROUP BY ds.race_year, ds.drivername,ds.constructorname
# MAGIC ORDER BY ds.race_year DESC 
# MAGIC ,points DESC
# MAGIC )
# MAGIC SELECT * 
# MAGIC FROM  CTE
# MAGIC WHERE RNK <=10
# MAGIC AND   race_year ='2023'  
# MAGIC ORDER BY race_year,RNK

# COMMAND ----------

