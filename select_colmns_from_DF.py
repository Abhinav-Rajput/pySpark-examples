import os
project_dir = os.path.dirname( os.path.abspath(__file__))

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Select columns from Data frame").getOrCreate()

df= spark.read.options(header=True, inferSchema=True).csv(f"{project_dir}/Data/StudentData.csv")

# eays of selecting column

#1
#df.select("gender","name").show()    

#2
#df.select(df.name, df.email).show()



#3
from pyspark.sql.functions import col

#df.select(col("roll"),col("name")).show()

#4
#df.select("*").show()


#5
#df.columns[2]
#df.select(df.columns).show()
#df.select(df.columns[2:6]).show()
