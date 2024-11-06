from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("EmployeeBronzeToSilver").getOrCreate()


employee_df = spark.read.csv("dbfs:/FileStore/shared_uploads/priti25priya@gmail.com/Employee_Q1.csv", header=True, inferSchema=True)
department_df = spark.read.csv("dbfs:/FileStore/shared_uploads/priti25priya@gmail.com/Department_Q1.csv", header=True,inferSchema=True)
country_df = spark.read.csv("dbfs:/FileStore/shared_uploads/priti25priya@gmail.com/Country_Q1.csv", header=True, inferSchema=True)


# 2.
from pyspark.sql import SparkSession
import re

spark = SparkSession.builder.appName("TransformCamelToSnake").getOrCreate()


country_data = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/priti25priya@gmail.com/Country_Q1.csv", header=True, inferSchema=True)
department_data = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/priti25priya@gmail.com/Department_Q1.csv", header=True,inferSchema=True)
employee_data = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/priti25priya@gmail.com/Employee_Q1.csv", header=True, inferSchema=True)


def camel_to_snake(df):
    new_column_names = [re.sub(r'(?<!^)(?=[A-Z])', '_', col).lower() for col in df.columns]
    return df.toDF(*new_column_names)


country_data_snake = camel_to_snake(country_data)
department_data_snake = camel_to_snake(department_data)
employee_data_snake = camel_to_snake(employee_data)


country_data_snake.write.format('csv').mode("overwrite").save("dbfs:/FileStore/bronze_to_silver/country_data")
department_data_snake.write.format('csv').mode("overwrite").save("dbfs:/FileStore/bronze_to_silver/department_data")
employee_data_snake.write.format('csv').mode("overwrite").save("dbfs:/FileStore/bronze_to_silver/employee_data")


def display(department_data_snake):
    pass


display(department_data_snake)
display(employee_data_snake)
display(country_data_snake)

# 3.
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date


spark = SparkSession.builder.appName("AddLoadDateAndSaveDelta").getOrCreate()


employee_data = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/bronze_to_silver/employee_data")


def camel_to_snake(df):
    import re
    new_column_names = [re.sub(r'(?<!^)(?=[A-Z])', '_', col).lower() for col in df.columns]
    return df.toDF(*new_column_names)

employee_data_snake = camel_to_snake(employee_data)

# Add the load_date column with the current date
employee_data_with_date = employee_data_snake.withColumn("load_date", current_date())

# Define the Delta table path and database/table names
delta_path = "dbfs:/FileStore/shared_uploads/priti25priya@gmail.com/my_namee"
database_name = "Employee_info"
table_name = "dim_employee"


employee_data_with_date.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(delta_path)

# Create the database if it doesn't exist
spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")

# Register the Delta table in the Metastore
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {database_name}.{table_name}
    USING DELTA
    LOCATION '{delta_path}'
""")


print("Delta table has been successfully created and saved.")

