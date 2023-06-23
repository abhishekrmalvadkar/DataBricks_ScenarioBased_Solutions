# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

data  =  (["Abhishek",24],["malvadkar",30])
df = spark.createDataFrame(data, ["name","age"])

#Define the UDF

def uppercase_string(s):
    if s is not None:
        return s.upper()
    else:
        return None
    
#Register the UDF

uppercase_udf = udf(uppercase_string, StringType())
spark.udf.register("uppercase_udf", uppercase_udf)

#using the UDF 

df.withColumn ("upper_name", uppercase_udf("name")).show()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

data  =  (["Abhishek",24],["malvadkar",30])
df = spark.createDataFrame(data, ["name","age"])

#Define the UDF

def string_len(s):
    if s is not None:
        return len(s)
    else:
        return None
    
#Register the UDF

stringLength_udf = udf(string_len, IntegerType())
spark.udf.register("stringLength_udf", stringLength_udf)

#using the UDF 

df.withColumn ("string_Length", stringLength_udf("name")).show()

# COMMAND ----------

from pyspark.sql.functions import * 
from pyspark.sql.types import *

data = (["Abhishek","Malvadkar"],["Malvadkar","Abhishek"])
df = spark.createDataFrame(data, ["firstName","LastName"])
#df.show()

def full_name(col1, col2):
    if col1 is not None and col2 is not None:
        fullName = col1 + "_" + col2
        return fullName
    else:
        return None

fullName_udf = udf(full_name, StringType())
spark.udf.register("fullName_udf", fullName_udf)

df.withColumn("Full_Name",fullName_udf("firstName","lastName"))
df.show()

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def transform_data(col1, col2):
    if col1 is not None and col2 is not None:
        transformed_value = col1 + "_" + col2
        return transformed_value
    else:
        return None

transform_data_udf = udf(transform_data, StringType())
spark.udf.register("transform_data_udf", transform_data_udf)

data = [("Abhishek", "Malvadkar"), ("Malvadkar", "Abhishek")]
df = spark.createDataFrame(data, ["first_name", "last_name"])

df.withColumn("full_name", transform_data_udf("first_name", "last_name")).show()


# COMMAND ----------

									
from pyspark.sql.functions import *
from pyspark.sql.types import *
																				   
def categorize_text(text):
    if text is not None:
												
        patterns = {
            "fruits": ["apples","mango","banana"],
            "city":["bangalore","mysore","mumbai"],
            "colors":["red","black","blue"]
        }

												   
        for category, pattern_list in patterns.items():
            for pattern in pattern_list:
                if pattern in text.lower():
                    return category
		
        return 'other'
					  
    else:
        return None
    
categorize_udf = udf(categorize_text, StringType())
spark.udf.register("categorize_udf",categorize_udf)

data = [("Hello World",),("Abhishek R Malvadkar bangalore",),("Data Engineering apples",)]
																																			  
df = spark.createDataFrame(data,["text"])
														 
df.withColumn("category", categorize_udf("text"))
df.show(truncate = False)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

# Define a UDF function for text categorization
def categorize_text(text):
    if text is not None:
        # Predefined patterns for categorization
        patterns = {
            "fruits": ["apple", "banana", "orange"],
            "animals": ["dog", "cat", "elephant"],
            "colors": ["red", "blue", "green"]
        }
        
        # Check if text matches any of the patterns
        for category, pattern_list in patterns.items():
            for pattern in pattern_list:
                if pattern in text.lower():
                    return category
        
        # If no pattern match found, return "other"
        return "other"
    else:
        return None

# Register the UDF
categorize_text_udf = udf(categorize_text, StringType())
spark.udf.register("categorize_text_udf", categorize_text_udf)

# Create a sample DataFrame with text column
data = [("I love apples.",), ("My cat is cute.",), ("The sky is blue.",), ("Python is awesome.",)]
df = spark.createDataFrame(data, ["text"])

# Use the UDF in DataFrame operations for text categorization
df.withColumn("category", categorize_text_udf("text")).show()


# COMMAND ----------


