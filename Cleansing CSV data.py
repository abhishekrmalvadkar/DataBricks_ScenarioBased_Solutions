"""
Question: -
Read a csv from dbfs and transform the data with the below two rules and show the output:
1. The output should have only single record of the user.
2. If the firstName or lastName is missing, the user should be skipped.

Sample input file:
id,ind,fname,lname,apartment,street,city,country,phone
001,FN,John
001,LN,Wick
001,AD,22,Continental Hotel, New York, USA
001,PH,9999999999
002,FN,Sherlock
002,LN,Homes
002,AD,44,Baker St, London, UK
002,PH,8888888888
003,FN,Ted
003,LN,Lasso
003,AD,AFC,Richmond, London, UK
003,PH,3336669990
004,FN,Star
004,LN,Lord
004,AD,
004,PH,
005,FN,Clark
005,LN,
005,AD,44,Daily Planet, Metropolis, USA
005,PH,1234567890
006,FN,Bruce
006,LN,Wayne
006,AD,1007,Mountain Drive, Gotham, USA
006,PH,1111111111
007,FN,Sachin
007,LN,Chavan
007,AD,2013,Mallesh Palya, Karnataka, India
007,PH,1111111111
008,FN,
008,LN,Chavan
008,AD,2013,Mallesh Palya, Karnataka, India
008,PH,
009,FN,Dundappa

Note: Here "ind" column is the indicator column which tells us which column is to be populated. Example, if "ind = FN"
then FirstName should be populated in the end output
"""
# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

df1 = spark.read.format("csv").option("header",True).load("dbfs:/FileStore/Cleansing.csv")

# COMMAND ----------

df1.display()

# COMMAND ----------

df2 = df1.select("id", when(col("ind")==lit("FN"),col("fname")).otherwise("null").alias("fname"),
            when(col("ind")==lit("LN"),col("fname")).otherwise("null").alias("lname"),
            when(col("ind")==lit("PH"),col("fname")).otherwise("null").alias("phone"),
            when(col("ind")==lit("AD"),concat_ws(",",col("fname"),col("lname"),col("apartment"),col("street"),col("city"),col("country"))).otherwise("null").alias("address"))

# COMMAND ----------

df2.display()

# COMMAND ----------

df3 = df2.groupBy("id").agg(min("fname").alias("fname"),min("lname").alias("lname"),min("phone").alias("phone"),min("address").alias("address"))
df3.display()

# COMMAND ----------

#df5 = df3.dropna("fname", how="all")

help('dropna')

# COMMAND ----------

df4 = df3.filter((col("fname")!=lit("null")) & (col("lname")!=lit("null")))
df4.display()

# COMMAND ----------

df5 = df4.withColumn("apartment", split("address",",").getItem(0))\
      .withColumn("street",split("address",",").getItem(1))\
        .withColumn("city",split("address",",").getItem(2))\
            .withColumn("country",split("address",",").getItem(3))

df5.select("id","fname","lname","apartment","street","city","country","phone").display()

# COMMAND ----------


