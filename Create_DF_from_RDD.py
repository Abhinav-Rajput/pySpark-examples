import os 
project_dir = os.path.dirname(os.path.abspath(__file__))

from pyspark.sql import SparkSession, column
spark = SparkSession.builder.appName("Create DF from RDD").getOrCreate()

from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("RDD")
sc =  SparkContext.getOrCreate(conf=conf)

rdd = sc.textFile(f"{project_dir}/Data/StudentData.csv")
#print(rdd.collect()) # Caution, resulting array is expected to be small


headers = rdd.first()
rdd= rdd.filter(lambda x: x!=headers).map(lambda x:x.split(','))

columns = headers.split(',')
dfRdd= rdd.toDF(columns)

print(rdd)