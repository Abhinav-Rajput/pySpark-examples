import os

project_dir = os.path.dirname(os.path.abspath(__file__))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

spark = SparkSession.builder.appName(" withColumnRenamed festures").getOrCreate()

df =spark.read.options(header=True,inferSchema=True).csv(f"{project_dir}/Data/StudentData.csv")

df.show()

df =df.withColumnRenamed("gender","sex")

df.show()

df =df.withColumnRenamed("gender","sex")  # note although there is no 'gender' column available (re-named at line 14), still it does not throw any error, and simply executes with 'sex' as columns-name

df.select(col("name").alias("Full Name")).show()