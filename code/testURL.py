# Databricks notebook source
# MAGIC %md 
# MAGIC Raw Data Ingestion Notebook
# MAGIC 
# MAGIC Purpose of this Notebook is to Ingest raw Data from HTTP Endpoint and Put it in to AWS S3 Storage.

# COMMAND ----------

# MAGIC %sh
# MAGIC wget --continue https://s3-eu-west-1.amazonaws.com/csparkdata/ol_cdump.json -O /dbfs/mnt/open-library-dmeo/Raw/ol_cdump.json

# COMMAND ----------

import urllib 
urllib.request.urlretrieve("https://openlibrary.org/data/ol_dump_ratings_latest.txt.gz", "/dbfs/mnt/open-library-dmeo/Raw/ol_dump_ratings_latest.txt.gz")

# COMMAND ----------

#https://openlibrary.org/data/ol_dump_works_latest.txt.gz
urllib.request.urlretrieve("https://openlibrary.org/data/ol_dump_works_latest.txt.gz", "/dbfs/mnt/open-library-dmeo/Raw/ol_dump_works_latest.txt.gz")

# COMMAND ----------

import urllib
urllib.request.urlretrieve("https://openlibrary.org/data/ol_cdump_latest.txt.gz", "/dbfs/mnt/open-library-dmeo/Raw/ol_cdump_latest.txt.gz")

# COMMAND ----------

dbutils.fs.ls("/mnt/open-library-dmeo/Raw")
