# Databricks notebook source
dbutils.fs.mount(source = 'wasbs://movies-input@demostorageabhishek.blob.core.windows.net',
                mount_point = '/mnt/blobStorage2',
                extra_configs = {'fs.azure.sas.movies-input.demostorageabhishek.blob.core.windows.net':'sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupiytfx&se=2023-05-17T15:24:15Z&st=2023-05-17T07:24:15Z&spr=https&sig=eP2mzocmhQ6u4stx%2B0vQSM3xRZURUOMBqTTWLL1B8SQ%3D'} )

# COMMAND ----------

df = spark.read.options(header='True', inferSchema='True', delimiter=',') \
  .csv("/mnt/blobStorage2")

# COMMAND ----------

df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import split, substring
from pyspark.sql.types import IntegerType
df_split = split(df["title"], "\s+\(")
df = df.withColumn("movie_name", df_split.getItem(0))
df = df.withColumn("year", substring(df_split.getItem(1), 1, 4).cast(IntegerType()))

# COMMAND ----------

df_years = df.groupBy("year").count().orderBy("year")

# COMMAND ----------

df_years.display()

# COMMAND ----------

df_years.write.csv("/mnt/blobStorage2/MoviesReleasedEachYear", header = True, mode = "overwrite")

# COMMAND ----------


