import os

project_dir = os.path.dirname(os.path.abspath(__file__))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


spark = SparkSession.builder.appName("withColumn feature").getOrCreate()

df = spark.read.options(header=True, inferSchema=True ).csv(f"{project_dir}\Data\StudentData.csv")



#df.withColumn("roll",col("roll")).show()
df = df.withColumn("roll", col("roll").cast("String"))
df.printSchema()

df.show()  #before
df =df.withColumn("marks", col('marks')+10)
df.show()  #after


#creation of new-column
df = df.withColumn("new_updated_marks", col('marks')-10)
df.show()

from pyspark.sql.functions import col, lit
df = df.withColumn("Country", lit("USA"))
df.show()

#multiple withColumn in 1 go
df = spark.read.options(header=True, inferSchema=True ).csv(f"{project_dir}\Data\StudentData.csv")
df.show()

df.withColumn("marks",col("marks")-10).withColumn("updated_marks", col("marks")+20).withColumn("Country", lit("USA")).show()

