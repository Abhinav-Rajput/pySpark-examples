import os

project_dir = os.path.dirname(os.path.abspath(__file__))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

spark =  SparkSession.builder.appName("fileter_where").getOrCreate()

df = spark.read.options(header =True, inferSchema=True).csv(f"{project_dir}/Data/StudentData.csv")

#1
df.filter(df.course == "DB").show()


#2
df.filter(col("course")=="DB").show()

#3
df.filter((df.course == "DB") & (df.marks >50)).show()

df.select("gender").distinct().show()

df.dropDuplicates(["gender","course"]).show()