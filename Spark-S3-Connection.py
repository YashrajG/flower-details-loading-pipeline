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

# COMMAND ----------

access_key = ""
secret_key = ""

#Mount bucket on databricks

encoded_secret_key = secret_key.replace("/", "%2F")

aws_bucket_name = "bkt-yashraj-bigdata"

mount_name = "bigdata-training"

dbutils.fs.mount("s3a://%s" % (aws_bucket_name), "/mnt/%s" % mount_name)

display(dbutils.fs.ls("/mnt/%s" % mount_name))

mount_name = "bigdata-training"

file_name="iris.csv"

df = spark.read.format("csv").load("/mnt/%s/%s" % (mount_name , file_name))

df.show()

#spark.read.format("csv").load("s3://bkt-nithin-04aug/iris.csv")

# COMMAND ----------

# spark.write.parquet()
strMountPointParquetFile="/mnt/%s/abc.parquet" % (mount_name)
df.write.parquet(strMountPointParquetFile)
