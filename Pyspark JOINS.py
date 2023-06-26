# Databricks notebook source
# MAGIC %md
# MAGIC Scenario:
# MAGIC You have been given two datasets: "orders.csv" and "customers.csv". The "orders.csv" dataset contains information about customer orders, including the order ID, customer ID, order date, and total amount. The "customers.csv" dataset contains information about customers, including the customer ID, name, and email address. Your task is to join these two datasets based on the customer ID, filter orders placed in the year 2022, calculate the total order amount for each customer, and write the result to a new CSV file.
# MAGIC
# MAGIC Datasets:
# MAGIC
# MAGIC "orders.csv" columns: "order_id", "customer_id", "order_date", "total_amount"
# MAGIC "customers.csv" columns: "customer_id", "name", "email"
# MAGIC Question:
# MAGIC Write a PySpark code that performs the above tasks and saves the result in a new CSV file named "customer_order_amount.csv".

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

customers_path = "dbfs:/FileStore/Customer.csv"
orders_path = "dbfs:/FileStore/orders.csv"
output_path = "dbfs:/FileStore/Customer_Order_CSV"
year_filter = 2023

def process_orders(orders_path, customers_path, output_path, year_filter):
    
    orders_df = spark.read.csv(orders_path, header=True, inferSchema=True)
    customers_df = spark.read.csv(customers_path, header=True, inferSchema=True)
    
    orders_filtered = filter_orders_by_year(orders_df, year_filter)
    
    joined_df = join_orders_customers(orders_filtered, customers_df)
    
    order_amount_df = calculate_order_amount(joined_df)
    
    write_to_csv(order_amount_df, output_path)
    
    show_result(order_amount_df)


def filter_orders_by_year(orders_df, year_filter):
    return orders_df.filter(year(orders_df["order_date"]) == year_filter)


def join_orders_customers(orders_df, customers_df):
    return orders_df.join(customers_df, "customer_id")


def calculate_order_amount(joined_df):
    return joined_df.groupBy("customer_id", "name", "email") \
                    .agg(sum("total_amount").alias("total_order_amount"))


def write_to_csv(df, output_path):
    df.write.csv(output_path, header=True)


def show_result(df):
    df.show()
process_orders(orders_path, customers_path, output_path, year_filter)
