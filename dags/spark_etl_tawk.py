from pyspark.sql import SparkSession
import requests

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("ETL Job") \
    .getOrCreate()

# Download JSON data
response = requests.get("https://api.mfapi.in/mf/118550")
json_data = response.json()

# Transform JSON data to DataFrame
df = spark.createDataFrame(json_data)

# Save DataFrame to CSV
df.write.csv("dataframe.csv", header=True)

# Stop SparkSession
spark.stop()
