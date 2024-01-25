from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import col, avg, when, regexp_replace, regexp_extract


spark = SparkSession.\
        builder.\
        appName("Deme").\
        config("spark.executor.memory", "512m").\
        getOrCreate()
# hdfs_host = 'namenode'
# hdfs_port = 9000
# hdfs_directory = '/data'

# hadoop_conf = spark._jsc.hadoopConfiguration()
# hadoop_conf.set("fs.defaultFS", f"hdfs://{hdfs_host}:{hdfs_port}")
# fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)

# files = fs.listStatus(spark._jvm.org.apache.hadoop.fs.Path(hdfs_directory))
# # latest_file_status = max(files, key=lambda x: x[1])[0]
# # latest_file_path = latest_file_status.getPath().toString()
# latest_file_path = files[len(files)-1].getPath().toString()
# #print("Latest File Path:", files[len(files)-2].getPath().toString())
# from pyspark.sql.types import *
# schema = StructType([
#     StructField('NAME', StringType(), True),
#     StructField('productUrl', StringType(), True),
#     StructField('imageUrl', StringType(), True),
#     StructField('originalPrice', DoubleType(), True),
#     StructField('DiscountedPrice', DoubleType(),True),
#     StructField('Discount', StringType(), True),
#     StructField('ratingScore', DoubleType(),True),
#     StructField('review', IntegerType(),True),
#     StructField('description', StringType(), True),
#     StructField('categories', StringType(), True),
#     StructField('itemId', DoubleType(),True),
#     StructField('itemSoldCntShow', IntegerType(), True),
#     StructField('sellerName', StringType(), True),
#     StructField('sellerId', StringType(), True),
#     StructField('brandId', StringType(), True),
#     StructField('brandName', StringType(), True),
#     StructField('location', StringType(), True)

# ])

# df = (spark.read.format("com.databricks.spark.csv")
#         .option("header", "true")
#         #.option("inferSchema","true")
#         .schema(schema)
#         .load('hdfs://namenode:9000/data/lazada_2024-01-15_11-47-48.csv'))

# df.show()
data = [("Alice", 1), ("Bob", 2), ("Charlie", 3)]
columns = ["Name", "Value"]
df = spark.createDataFrame(data, columns)

    # Show the DataFrame
df.show()