# Databricks notebook source
dbutils.fs.mount(source = 'wasbs://employeein@demostorageabhishek.blob.core.windows.net',
                mount_point = '/mnt/blobStorage',
                extra_configs = {'fs.azure.account.key.demostorageabhishek.blob.core.windows.net':'pcy7mEao6FT6zttzYIaEJQ9HFJ5g4fG2XIIY8tQcwj/TKya0HxBN92WO5iESUfESCT4xABfUcwlt+AStIlFBFQ=='} )

# COMMAND ----------

dbutils.fs.ls('/mnt/blobStorage')

# COMMAND ----------

dbutils.fs.ls('/mnt/blobStorage')

# COMMAND ----------

df = spark.read.csv('/mnt/blobStorage/employeeData.csv',inferSchema=True, header = True)
df.display()

# COMMAND ----------

df = spark.read.csv('/mnt/blobStorage/employeeData.csv',inferSchema=True, header = True)
df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import *
df1 = df.drop(col("Job Status"))

# COMMAND ----------

df1.printSchema()

# COMMAND ----------

df1.display()

# COMMAND ----------

df1 = df1.withColumnRenamed('Monthly Salary','Salary Per Month')

# COMMAND ----------

df1.display()

# COMMAND ----------

df1.createOrReplaceTempView('Employee')
df1.printSchema()

# COMMAND ----------

dbutils.widgets.text("category","")
category = dbutils.widgets.get("category")
print(category)

# COMMAND ----------

df2 = spark.sql("""select * from Employee where Department in ('{0}') """.format(category))

# COMMAND ----------

df2.display()

# COMMAND ----------



# COMMAND ----------

df2.write.csv("/mnt/blobStorage/out_Emp.csv", header = True, mode = "overwrite")


# COMMAND ----------


