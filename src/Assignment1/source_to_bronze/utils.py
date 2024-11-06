from pyspark.shell import spark
from pyspark.sql import SparkSession
import re

def read_csv(location):
    df1 = spark.read.csv(location, header=True, inferSchema=True)
    return df1