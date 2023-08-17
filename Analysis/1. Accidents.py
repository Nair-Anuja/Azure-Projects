# Databricks notebook source
# MAGIC %md
# MAGIC #### Consolidate the accident results which includes information on circuit, season, driver

# COMMAND ----------

#%sql
#drop table f1_presentation.race_results_accidents

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE  f1_presentation.race_results_accidents
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT rr.race_year,
# MAGIC        rr.round,
# MAGIC        r.race_date,
# MAGIC        c.circuitref ,
# MAGIC        c.circuitName,
# MAGIC        c.locality,
# MAGIC        c.country,
# MAGIC        rr.driverref,
# MAGIC        dr.drivername,
# MAGIC        rr.drivernationality,
# MAGIC        floor(datediff(r.race_date,rr.driverdob)/365.2425) as age,
# MAGIC        rr.position AS driver_position,
# MAGIC        rr.grid,
# MAGIC        rr.status,
# MAGIC        rr.constructorref,
# MAGIC        rr.constructorname
# MAGIC FROM  f1_processed.race_results rr
# MAGIC JOIN  f1_processed.races        r ON  rr.race_year = r.race_year
# MAGIC                                   AND rr.round     = r.round
# MAGIC JOIN  f1_processed.circuits     c ON  r.circuitId  = c.circuitref
# MAGIC JOIN  f1_processed.drivers      dr ON rr.driverref = dr.driverref  
# MAGIC WHERE status      ='Accident'
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE f1_presentation.race_results_accidents
# MAGIC ZORDER BY (race_year,driverref,circuitref,constructorref)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  RA.race_year,COUNT(*) AS accident_count
# MAGIC FROM   f1_presentation.race_results_accidents RA
# MAGIC GROUP BY RA.race_year 
# MAGIC ORDER BY accident_count DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH CTE AS (
# MAGIC SELECT  RA.circuitref,RA.circuitName ,COUNT(RA.driverref) AS accident_count
# MAGIC ,dense_rank() OVER(ORDER BY COUNT(RA.driverref) DESC) AS RNK
# MAGIC FROM    f1_presentation.race_results_accidents RA
# MAGIC GROUP BY RA.circuitref ,RA.circuitName
# MAGIC )
# MAGIC SELECT *
# MAGIC FROM   CTE C  
# MAGIC WHERE  C.RNK <=10

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH CTE AS (
# MAGIC SELECT   RA.driverref
# MAGIC         ,RA.drivername 
# MAGIC         ,COUNT(*) AS accident_count
# MAGIC         ,dense_rank() OVER(ORDER BY COUNT(RA.driverref) DESC) AS RNK
# MAGIC FROM     f1_presentation.race_results_accidents RA
# MAGIC GROUP BY RA.driverref,RA.drivername
# MAGIC )
# MAGIC SELECT *
# MAGIC FROM    CTE C 
# MAGIC WHERE   RNK <=10
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM  f1_presentation.race_results_accidents

# COMMAND ----------

