import os
project_dir = os.path.dirname(os.path.abspath(__file__))

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("quiz_sort_orderby").getOrCreate()

df = spark.read.options(header=True, inferSchema=True).csv(f"{project_dir}/Data/OfficeData.csv")

df.show()

df_bonus = df.sort("bonus")
df_bonus.show()

df_age_salary = df.sort(df.age.asc(),df.salary.asc())
df_age_salary.show()

df_age_bonus_salary = df.sort(df.age.desc(),df.bonus.desc(),df.salary.asc())
df_age_bonus_salary.show()

