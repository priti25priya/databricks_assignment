#1.

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("EmployeeSilverToGold").getOrCreate()

# Define the path to the Delta table in the silver layer
silver_table_path = "dbfs:/FileStore/shared_uploads/priti25priya@gmail.com/my_namee"

# Read the Delta table stored in the silver layer as a DataFrame
employee_silver_df = spark.read.format("delta").load(silver_table_path)


selected_columns = ["i_n", "james", "d101", "load_date"]
employee_gold_df = employee_silver_df.select(*selected_columns)


# Define the path to save the transformed data in the gold layer
gold_table_path = "dbfs:/FileStore/shared_uploads/priti25priya@gmail.com/gold"

# Write the selected DataFrame to the gold layer as a Delta table
employee_gold_df.write.format("delta").mode("overwrite").save(gold_table_path)

# Register the Delta table in the Databricks Metastore if needed
database_name = "Employee_info"
gold_table_name = "dim_employee_gold"

# Create the database if it does not exist
spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")

# Register the gold layer table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {database_name}.{gold_table_name}
    USING DELTA
    LOCATION '{gold_table_path}'
""")

print("Gold Delta table has been successfully created and saved.")

# 2.
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, avg, count

# Initialize Spark session if not already started
spark = SparkSession.builder.appName("EmployeeGoldTransformations").getOrCreate()

# Load the employee data from the silver layer
silver_table_path = "dbfs:/FileStore/shared_uploads/priti25priya@gmail.com/Employee_Q1.csv"
employee_df = spark.read.format("csv").load(silver_table_path)

# Requirement 1: Find the total salary of each department in descending order

department_salary_df = (employee_df
                        .groupBy("department_id")
                        .sum('salary')
                        .withColumnRenamed("sum(salary)", "total_salary")
                         .orderBy("total_salary", ascending=False))


# Requirement 2: Find the number of employees in each department located in each country
department_country_count_df = (employee_df
                               .groupBy("department_id", "country_id")
                               .agg(count("employee_id").alias("employee_count")))

# Requirement 3: List the department names along with their corresponding country names
department_df = spark.read.format("csv").load("dbfs:/FileStore/shared_uploads/priti25priya@gmail.com/Department_Q1.csv")
country_df = spark.read.format("csv").load("dbfs:/FileStore/shared_uploads/priti25priya@gmail.com/Country_Q1.csv")

# Join department and country tables to get names
department_country_names_df = (department_df
                               .join(country_df, "country_id", "inner")
                               .select("department_name", "country_name"))

# Requirement 4: Calculate the average age of employees in each department
department_avg_age_df = (employee_df
                         .groupBy("department_id")
                         .agg(avg("age").alias("average_age")))

# Requirement 5: Add the at_load_date column to data frames
# Adding `at_load_date` to each DataFrame for consistency
at_load_date = current_date()
employee_df = employee_df.withColumn("at_load_date", at_load_date)
department_salary_df = department_salary_df.withColumn("at_load_date", at_load_date)
department_country_count_df = department_country_count_df.withColumn("at_load_date", at_load_date)
department_country_names_df = department_country_names_df.withColumn("at_load_date", at_load_date)
department_avg_age_df = department_avg_age_df.withColumn("at_load_date", at_load_date)

# Requirement 6: Write the employee_df to DBFS with overwrite and replace on `at_load_date`
gold_table_path = "dbfs:/FileStore/shared_uploads/priti25priya@gmail.com/Employee_Q1.csv"

# Write employee_df as Delta table in the gold layer with overwrite mode
(employee_df
 .write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .save(gold_table_path))

# Register the Delta table in the Databricks Metastore if needed
database_name = "Employee_info"
table_name = "fact_employee"

# Create the database if it does not exist
spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")

# Register the Delta table in the Metastore
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {database_name}.{table_name}
    USING DELTA
    LOCATION '{gold_table_path}'
""")



