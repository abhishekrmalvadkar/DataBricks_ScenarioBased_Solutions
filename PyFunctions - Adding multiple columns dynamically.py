# Databricks notebook source

"""
Add multiple columns to a dataFrame dynamically.

"""

from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql.functions import *

data2 = [("James","","Smith","36636","M",3000),
    ("Michael","Rose","","40288","M",4000),
    ("Robert","","Williams","42114","M",4000),
    ("Maria","Anne","Jones","39192","F",4000),
    ("Jen","Mary","Brown","","F",1000)
  ]

schema = StructType([ \
    StructField("firstname",StringType(),True), \
    StructField("middlename",StringType(),True), \
    StructField("lastname",StringType(),True), \
    StructField("id", StringType(), True), \
    StructField("gender", StringType(), True), \
    StructField("salary", IntegerType(), True) \
  ])
 
df = spark.createDataFrame(data=data2,schema=schema)
display(df)

# COMMAND ----------

columns = ['City','State','Country']
lit_values = ['Bengaluru','Karnataka','India']
def add_multiple_cols(col_names, lit_vals):
        df1 = df.withColumn(col_names, lit(lit_vals)) 
        df1.display()
        return()
add_multiple_cols(columns[0], lit_values[0])
add_multiple_cols(columns[1], lit_values[1])



# COMMAND ----------


