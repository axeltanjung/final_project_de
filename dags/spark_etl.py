import requests
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# build spark session for Spark dataframe API
spark = SparkSession \
    .builder \
    .appName("DataExtraction") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# Get data form url
response = requests.get("https://api.mfapi.in/mf/118550")
data = response.text

# Build rdd
sparkContext = spark.sparkContext
RDD = sparkContext.parallelize([data])
raw_json_dataframe = spark.read.json(RDD)

#print schema create temporary view
raw_json_dataframe.printSchema()
raw_json_dataframe.createOrReplaceTempView("Mutual_benefit")

# transform
dataframe = raw_json_dataframe.withColumn("data", F.explode(F.col("data"))) \
        .withColumn('meta', F.expr("meta")) \
        .select("data.*", "meta.*")

# Convert to pandas and save
dataframe.show(100, False)
dataframe.toPandas().to_csv("/opt/airflow/output/dataframe.csv")
