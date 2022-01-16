# Databricks notebook source
# MAGIC %md
# MAGIC AWS S3 Bucket Mount into DBFS

# COMMAND ----------

from pyspark.sql.functions import *
import urllib

# COMMAND ----------

file_type = "csv"
first_row_is_header = "true"
delimiter = ","
# Read the CSV file to spark dataframe
aws_keys_df = spark.read.format(file_type)\
.option("header", first_row_is_header)\
.option("sep", delimiter)\
.load("/FileStore/tables/new_user_credentials.csv")

# COMMAND ----------

aws_keys_df.show()

# COMMAND ----------

# Get the AWS access key and secret key from the spark dataframe
ACCESS_KEY = aws_keys_df.where(col('User name')=='aws-databricks-s3-demo').select('Access key ID').collect()[0]['Access key ID']
SECRET_KEY = aws_keys_df.where(col('User name')=='aws-databricks-s3-demo').select('Secret access key').collect()[0]['Secret access key']
# Encode the secrete key
ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")

# COMMAND ----------

# AWS S3 bucket name
AWS_S3_BUCKET = "open-library-dmeo"
# Mount name for the bucket
MOUNT_NAME = "/mnt/open-library-dmeo"
# Source url
SOURCE_URL = "s3n://{0}:{1}@{2}".format(ACCESS_KEY, ENCODED_SECRET_KEY, AWS_S3_BUCKET)
# Mount the drive
dbutils.fs.mount(SOURCE_URL, MOUNT_NAME)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/open-library-dmeo/Raw"))
