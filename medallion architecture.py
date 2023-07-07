#In Bronze Layer:

#Load the data from ADLS/DBFS into databricks

#In the Silver Layer:

#Drop Null values
#Drop duplicate values
#Convert the PHONE_NUMBER column data to proper number format
#Store the data in SILVER_TABLE

#In the Gold Layer:

#Fetch the Highest salary as per the JOB_ID
#Fetch the total compensation per month per department
#Store the data of the two outputs in two seperate GOLD_TABLES

# Databricks notebook source
# DBTITLE 1,Import functions
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,Bronze_Layer
df = spark.read.format("csv").option("header", True).option("delimiter", ",").load("dbfs:/FileStore/employees.csv")
df.show()
df.count()

# COMMAND ----------

# DBTITLE 1,Silver_Layer
df_del_dups = df.dropDuplicates()
df_del_dups.show()
df_del_dups.count()

# COMMAND ----------

df_del_nulls = df_del_dups.dropna(subset=['EMPLOYEE_ID','MANAGER_ID','DEPARTMENT_ID'])
df_del_nulls.show()
df_del_nulls.count()

# COMMAND ----------

df_drop_hifen = df_del_nulls.withColumn("MANAGER_ID", col("MANAGER_ID").cast("INTEGER")).withColumn("DEPARTMENT_ID", col("MANAGER_ID").cast("INTEGER"))
df_drop_hifen = df_drop_hifen.filter(col("MANAGER_ID").isNotNull())
df_drop_hifen.display()

# COMMAND ----------

# DBTITLE 1,Clean the Phone_Number column
df_clean_PhoneNum = df_drop_hifen.withColumn("PHONE_NUMBER", translate("PHONE_NUMBER",".",""))
df_clean_PhoneNum = df_clean_PhoneNum.withColumn("PHONE_NUMBER", col("PHONE_NUMBER").cast("LONG"))
df_clean_PhoneNum.display()

# COMMAND ----------

# DBTITLE 1,Store the data in SILVER_TABLE
df_clean_PhoneNum.write.format("delta").mode("overwrite").saveAsTable("SILVER_TABLE")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM SILVER_TABLE

# COMMAND ----------

# DBTITLE 1,GOLD_LAYER
df_aggregate_total_dept_comp = df_clean_PhoneNum.select("*").groupBy("JOB_ID").agg(sum("SALARY").alias("GOLD_TABLE_DEPT_TOTAL_SALARY"))
df_aggregate_total_dept_comp.display()

# COMMAND ----------

# DBTITLE 1,Total_dept_compensation_per_Month
df_aggregate_total_dept_comp.write.format("delta").mode("overwrite").saveAsTable("GOLD_TABLE_DEPT_TOTAL_SALARY")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM GOLD_TABLE_DEPT_TOTAL_SALARY

# COMMAND ----------

# DBTITLE 1,Highest_Salary_DeptWise
from pyspark.sql.window import Window
windowSpec = Window.partitionBy("JOB_ID").orderBy(col("SALARY").desc())
df_dept_highest_sal = df_clean_PhoneNum.withColumn("DENSE_RANK", dense_rank().over(windowSpec))
df_dept_highest_sal = df_dept_highest_sal.select("*").filter(col("DENSE_RANK")==1)
df_dept_highest_sal.display()

# COMMAND ----------

df_dept_highest_sal.write.format("delta").mode("overwrite").saveAsTable("GOLD_TABLE_DEPT_HIGHEST_SALARY")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM GOLD_TABLE_DEPT_HIGHEST_SALARY

# COMMAND ----------


