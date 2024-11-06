import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


spark = SparkSession.builder.appName("FetchAndProcessData").getOrCreate()

# Define the custom schema for the data we want to retrieve
custom_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("email", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("avatar", StringType(), True)
])

# Initialize variables
url = "https://reqres.in/api/users"
page = 1
all_data = []

# Fetch data from the API until it's empty
while True:
    response = requests.get(url, params={"page": page})
    data = response.json()

    # Check if 'data' is empty
    if not data.get("data"):
        break

    # Append only the user data
    all_data.extend(data["data"])
    page += 1

# Create a DataFrame from the collected data with the specified schema
raw_df = spark.createDataFrame(all_data, schema=custom_schema)

# Flatten the DataFrame (already flat in this case) and add new columns
final_df = (raw_df
            .withColumn("site_address", lit("reqres.in"))
            .withColumn("load_date", current_date()))

# Define the DBFS path and write the DataFrame in Delta format
dbfs_path = "/dbfs/site_info/person_info"

# Write to DBFS in Delta format with overwrite mode
final_df.write.format("delta").mode("overwrite").save(dbfs_path)

print("Data has been successfully written to DBFS as Delta table.")
