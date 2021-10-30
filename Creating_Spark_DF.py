from pyspark.sql import SparkSession
import os

project_dir= os.path.dirname(os.path.abspath(__file__))


spark = SparkSession.builder.appName("Spark DataFrame").getOrCreate()

df =spark.read.options(inferSchema=True,header=True).csv(f"{project_dir}\Data\StudentData.csv")

print(df.printSchema())
