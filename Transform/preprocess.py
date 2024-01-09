from pyspark.sql import SparkSession
from hdfs import InsecureClient

# Create a Spark session
spark = SparkSession.builder \
    .appName("CSVToSparkExample") \
    .getOrCreate()

# Path to your CSV file
csv_file_path = "/home/dell/Processing_Online_Retail_Data/Data/output.csv"

# Read the CSV file into a DataFrame
df = spark.read.csv(csv_file_path, header=True, inferSchema=True)

# Show the DataFrame
df.show()

# Perform Spark operations on the DataFrame
# For example, you can filter, aggregate, or transform the data here

# Stop the Spark session
spark.stop()

def save_file_to_hdfs(tmp_hdfs_path):
  client = InsecureClient('http://localhost:9870', user='root')
  client.upload( '/data1/', tmp_hdfs_path)
save_file_to_hdfs('/home/dell/Processing_Online_Retail_Data/Transform/output.csv')