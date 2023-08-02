# Databricks notebook source
# MAGIC %python
# MAGIC print("Hello World!")

# COMMAND ----------

dfIrisDetails = (
    spark.read.format("sqlserver")
    .option("host", "sample-data-pipeline-input.database.windows.net")
    .option("port", "1433")
    .option("user", "admin-yashraj")
    .option("password", "SQLRoot12")
    .option("database", "sample-pipeline-input")
    .option("dbtable", "dbo.iris_data")
    .load()
)

# COMMAND ----------

dfIrisDetails.show(10)
