from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


"""
StryctType: provides complete schema
StructField : provides schema to single column

StringType,IntegerType : are actual datatype
"""
import os

project_dir = os.path.dirname(os.path.abspath(__file__))

# schema = StringType([
#                 StructField("age", IntegerType(),True),
#                 StructField("gender",StringType(), True),
#                 StructField("name",StringType(),True),
#                 StructField("course",StringType(),True),
#                 StructField("roll",StringType(),True),
#                 StructField("marks",IntegerType(),True),
#                 StructField("email",StringType(),True)
# ])

schema = StructType([
                    StructField("age", IntegerType(), True),
                    StructField("gender", StringType(), True),
                    StructField("name", StringType(), True),
                    StructField("course", StringType(), True),
                    StructField("roll", StringType(), True),
                    StructField("marks", IntegerType(), True),
                    StructField("email", StringType(), True)
])
spark = SparkSession.builder.appName("provideing_schema").getOrCreate()
df = spark.read.options(header=True).schema(schema=schema).csv(f"{project_dir}/Data/StudentData.csv")
df.show()
df.printSchema()