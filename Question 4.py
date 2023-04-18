# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC You are given a CSV file sales.csv with the following schema:
# MAGIC 
# MAGIC root
# MAGIC  |-- sales_id: integer (nullable = true)
# MAGIC  |-- product_name: string (nullable = true)
# MAGIC  |-- quantity: integer (nullable = true)
# MAGIC  |-- price: double (nullable = true)
# MAGIC 
# MAGIC Write a PySpark function that reads the sales.csv file, calculates the total revenue for each product (i.e., quantity multiplied by price), and returns a DataFrame with the following schema:
# MAGIC 
# MAGIC root
# MAGIC  |-- product_name: string (nullable = true)
# MAGIC  |-- total_revenue: double (nullable = true)
# MAGIC 
# MAGIC Your function should be able to handle large datasets and efficiently calculate the total revenue using Spark's DataFrame API and functions.

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

def read_file(path):
    df = spark.read.format("csv").option("header",True).option("inferSchema", True).load(path)
    return df

def total_revenue(df):
    df = df.withColumn("total_revenue", col("quantity")*col("price"))
    df = df.groupBy("product_name").agg(sum("total_revenue").alias("TOTAL_REV"))
    return df

def display_result(df):
    df.display()

path = "dbfs:/FileStore/sales_function.csv"
sales_df = read_file(path)
revenue_df = total_revenue(sales_df)
display_result(revenue_df)

# COMMAND ----------


