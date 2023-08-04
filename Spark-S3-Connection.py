# Databricks notebook source
dbutils.fs.ls('/tmp/')

# COMMAND ----------

dbutils.fs.put("/tmp/sampleFile.txt", "Hello World!", True)

# COMMAND ----------

dbutils.fs.ls("s3://bkt-yashraj-bigdata/cities.parquet")

citiesDF = spark.read.format("parquet").load("s3://bkt-yashraj-bigdata/cities.parquet")

# COMMAND ----------

citiesDF.show(2)

# COMMAND ----------

# spark.sql("SELECT * FROM ")
spark.sql("SELECT * FROM parquet.`s3://bkt-yashraj-bigdata/cities.parquet`").show(10)
