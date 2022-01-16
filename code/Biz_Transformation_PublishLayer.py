# Databricks notebook source
# MAGIC %md
# MAGIC This Notebook reads Cleaned dataset from 2nd layer in Delta format and Provides Business use case Transformations .
# MAGIC Which Can be utilised for Reporting and Analytical Purposes.
# MAGIC Delta Format is used to provide ACID capabilitied for merging the incremental data load into main tables for future requirements.

# COMMAND ----------

delta = spark.read.format("delta").load("/mnt/open-library-dmeo/Cleansed/Completedump")

# COMMAND ----------

delta.createOrReplaceTempView("books")

# COMMAND ----------

display(spark.sql("""select * from books"""))

# COMMAND ----------

# MAGIC %md
# MAGIC Get the book with the most pages

# COMMAND ----------

spark.sql("""select title,number_of_pages from books where number_of_pages=(select max(number_of_pages) from books)""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC top 5 genres with most books

# COMMAND ----------

spark.sql("""select genres from (select count(title) as title ,genres from books group by genres) order by title desc limit 5""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC Per publish year, get the number of authors that published at least one book

# COMMAND ----------

spark.sql("""select count(authors),publish_date from books group by publish_date order by publish_date""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC Find the number of authors and number of books published per month for years between 1950 and 1970

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.functions import col
dates = (1950,  2013)
res = delta.select("authors","title","last_modified.value")
res1 = res.withColumn("ts",to_timestamp(col("value"))).withColumn("datetype",to_date(col("ts")))
res2 = res1.withColumn("month",month(col("datetype"))).withColumn("year",year(col("datetype"))).where((col('year').between(*dates)))


# COMMAND ----------

res2.createOrReplaceTempView("authorbymonth")

# COMMAND ----------

spark.sql("""select count(authors),count(title),month,year from authorbymonth group by month,year order by month""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC Retrieve the top 5 authors who (co-)authored the most books.

# COMMAND ----------

spark.sql("""SELECT title,count(authors) as authors
  FROM books
  GROUP BY title having count(authors) >1 order by authors desc""").show()

# COMMAND ----------

spark.sql("""select authors,title from books where title in (SELECT title
  FROM books
  GROUP BY title having count(authors) >1)""").show()
