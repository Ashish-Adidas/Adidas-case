# Databricks notebook source
# MAGIC %md
# MAGIC This Notebook is used to Clean Raw Dataset and Apply Data profiling 

# COMMAND ----------

complete = spark.read.format("json").option("mode", "DROPMALFORMED").load("dbfs:/mnt/open-library-dmeo/Raw/ol_cdump.json")
complete = complete.select("authors","by_statement","genres","key","languages","last_modified","number_of_pages","publish_date","publishers","series","subjects","type","title","works")
display(complete)

# COMMAND ----------

complete = complete.filter(complete.authors.isNotNull()).filter(complete.number_of_pages.isNotNull()).filter(complete.title.isNotNull())
complete1 = complete.na.fill("")
#complete.filter(complete.genres.contains("null")).show()


# COMMAND ----------

complete.printSchema()

# COMMAND ----------

from pyspark.sql.functions import explode
complete2 = complete1.select("authors","by_statement",explode(complete1.genres).alias("genres"),"key","languages","last_modified","number_of_pages","publish_date","publishers","series","subjects","type","title","works")


# COMMAND ----------

complete3 = complete2.na.drop(subset=["title"]).filter(complete2.number_of_pages > 20).filter(complete2.publish_date > 1950)
display(complete3)

# COMMAND ----------

complete3.count()

# COMMAND ----------

complete3.coalesce(1).write.format("delta").mode("overwrite").save("/mnt/open-library-dmeo/Cleansed/Completedump")

# COMMAND ----------

authors = spark.read.format("csv").option("mode", "DROPMALFORMED").load("dbfs:/mnt/open-library-dmeo/Raw/ol_dump_authors_2021-11-30.txt.gz")
display(authors)
