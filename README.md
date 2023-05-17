# DataBricks_ScenarioBased_Solutions

Please find the questions below along with the file name:

----------------------------------------------------------------------------------------------------------------------------------------------------------------
| FileName	                                          | Question                                                                                               |
----------------------------------------------------------------------------------------------------------------------------------------------------------------
| PyFunctions - Addind a column dynamically	          | Add a column to a dataFrame dynamically using functions                                                |
| PyFunctions - Adding multiple columns dynamically   |	Add multiple columns to a dataFrame dynamically using functions                                        |
| Cleansing CSV data	                                | Read a csv from dbfs and transform the data with the below two rules and show the output:              |
|                                                     | 1. The output should have only single record of the user.                                              |
|                                                     | 2. If the firstName or lastName is missing, the user should be skipped.                                |
|                                                     |                                                                                                        |
| Checking if all columns are present in a DF	        | Read the data from csv file and validate if the dataframe schema defined is similar to that present    |
|                                                     | in the source file                                                                                     |
----------------------------------------------------------------------------------------------------------------------------------------------------------------

![image](https://user-images.githubusercontent.com/48563516/230334942-ce601622-3e9f-489f-8ac6-40aa2c6256bb.png)
--------------------------------------------------------------------------------------------------------------------------------------------------------------------
Question 4 : 

You are given a CSV file sales.csv with the following schema:

root
 |-- sales_id: integer (nullable = true)
 |-- product_name: string (nullable = true)
 |-- quantity: integer (nullable = true)
 |-- price: double (nullable = true)

Write a PySpark function that reads the sales.csv file, calculates the total revenue for each product (i.e., quantity multiplied by price), and returns a DataFrame with the following schema:

root
 |-- product_name: string (nullable = true)
 |-- total_revenue: double (nullable = true)

Your function should be able to handle large datasets and efficiently calculate the total revenue using Spark's DataFrame API and functions.
--------------------------------------------------------------------------------------------------------------------------------------------------------------------
Question 5 :

You have a DataFrame called orders_df with the following schema:

root
 |-- order_id: integer (nullable = true)
 |-- customer_id: integer (nullable = true)
 |-- order_date: string (nullable = true)
 |-- total_amount: double (nullable = true)

You need to write a PySpark function that takes this DataFrame as input and performs the following tasks:

Adds a new column called year which contains the year extracted from the order_date column.
Filters the DataFrame to only include orders where the total_amount is greater than 100.
Groups the filtered DataFrame by year and calculates the total revenue (sum of total_amount) for each year.
Returns a new DataFrame with two columns: year and total_revenue, sorted by year in ascending order.
Write a function called calculate_yearly_revenue that implements the above tasks and takes orders_df as input. The function should return the resulting DataFrame.
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Employee_NoteBook.py :

Mount the blob storage from ADLS in databricks notebook and apply some basic transformations using pyspark, use dbutils widgets to categorise the data based on employee department and load the data in the form of csv file back to blob storage.
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
