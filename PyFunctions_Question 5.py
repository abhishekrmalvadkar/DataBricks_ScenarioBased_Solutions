# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC Question:
# MAGIC 
# MAGIC You have a DataFrame called orders_df with the following schema:
# MAGIC 
# MAGIC root
# MAGIC  |-- order_id: integer (nullable = true)
# MAGIC  |-- customer_id: integer (nullable = true)
# MAGIC  |-- order_date: string (nullable = true)
# MAGIC  |-- total_amount: double (nullable = true)
# MAGIC 
# MAGIC You need to write a PySpark function that takes this DataFrame as input and performs the following tasks:
# MAGIC 
# MAGIC Adds a new column called year which contains the year extracted from the order_date column.
# MAGIC Filters the DataFrame to only include orders where the total_amount is greater than 100.
# MAGIC Groups the filtered DataFrame by year and calculates the total revenue (sum of total_amount) for each year.
# MAGIC Returns a new DataFrame with two columns: year and total_revenue, sorted by year in ascending order.
# MAGIC Write a function called calculate_yearly_revenue that implements the above tasks and takes orders_df as input. The function should return the resulting DataFrame.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, sum
from pyspark.sql.types import IntegerType

def read_file(file_path):
    df = spark.read.format("csv").option("inferSchema", True).option("header", True).load(file_path)
    df = df.withColumn("order_date", col("order_date").cast("date"))
    df = df.withColumn("year", year(col("order_date")).cast(IntegerType()))
    filtered_df = df.filter(col("total_amount") > 100)
    result_df = filtered_df.groupBy("year").agg(sum("total_amount").alias("total_revenue"))
    result_df = result_df.sort("year")
    result_df.display()
    return result_df

read_file("dbfs:/FileStore/Order_function.csv")


# COMMAND ----------


