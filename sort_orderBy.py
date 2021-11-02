import os

project_dir = os.path.dirname(os.path.abspath(__file__))

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("sort_order").getOrCreate()

df = spark.read.options(header=True, inferschema=True).csv(f"{project_dir}/Data/StudentData.csv")

#df.show()
#df.sort("marks").show()

#df.sort(["marks","age"]).show()

df.sort("name").show()

df.sort(df.marks.desc()).show()