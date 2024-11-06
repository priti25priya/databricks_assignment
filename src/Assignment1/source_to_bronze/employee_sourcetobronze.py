#1.
from pyspark.sql import SparkSession

from databricks_assignment.src.Assignment1.source_to_bronze import utils
spark = SparkSession.builder.appName("EmployeeSourceToBronze").getOrCreate()


employee_df = spark.read.csv("dbfs:/FileStore/shared_uploads/priti25priya@gmail.com/Employee_Q1.csv", header=True, inferSchema=True)
department_df = spark.read.csv("dbfs:/FileStore/shared_uploads/priti25priya@gmail.com/Department_Q1.csv", header=True,inferSchema=True)
country_df = spark.read.csv("dbfs:/FileStore/shared_uploads/priti25priya@gmail.com/Country_Q1.csv", header=True, inferSchema=True)


#2.


save_path = "/source_to_bronze/"


employee_df.write.mode("overwrite").csv(f"{save_path}employee.csv", header=True)
department_df.write.mode("overwrite").csv(f"{save_path}department.csv", header=True)
country_df.write.mode("overwrite").csv(f"{save_path}country.csv", header=True)

print("DataFrames successfully written to DBFS.")


