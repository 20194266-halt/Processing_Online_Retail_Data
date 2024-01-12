
from hdfs import InsecureClient
def save_file_to_hdfs(tmp_hdfs_path):
  client = InsecureClient('http://localhost:9870')
  client.upload('/data/output1.csv', tmp_hdfs_path)
save_file_to_hdfs('/home/dell/Processing_Online_Retail_Data/Data/output1.csv')
# from pyspark.sql import SparkSession
# from pyspark.sql.types import *


# spark = SparkSession.\
#         builder.\
#         appName("Demo").\
#         master("spark://172.18.0.14:7077").\
#         config("spark.executor.memory", "512m").\
#         getOrCreate()
# print("lllllllllllllllllllllllllllllllllllllllllllllllllll")
# schema = StructType([
#     StructField('NAME', StringType(), True),
#     StructField('originalPrice', StringType(), True),
#     StructField('Discount', DoubleType(),True),
#     StructField('ratingScore', DoubleType(),True),
#     StructField('review', IntegerType(),True),
#     StructField('itemId', DoubleType(),True),
# ])

# # df = (spark.read.format("com.databricks.spark.csv")
# #         .option("header", "true")
# #         .option("inferSchema","true")
# #         .schema(schema)
# #         .load("hdfs://localhost:9000/data/output.csv"))

# # #df_list = df.collect()pys
# # df.show()
# # df = spark.read.csv("/home/dell/Processing_Online_Retail_Data/Data/output.csv")
# # df.show()
# data = [
#         ('{"a": "James"}',"OH","M"),
#         ('{"b": "John"}',"NY","F"),
#         ('{"c": "Jack"}',"OH","F"),
#         ]

# from pyspark.sql.types import StructType,StructField, StringType        
# schema = StructType([
#     StructField('name', StringType(), True),
#      StructField('state', StringType(), True),
#      StructField('gender', StringType(), True)
#      ])
# df = spark.createDataFrame(data = data, schema = schema)
# df.show()
# import psycopg2

# conn = psycopg2.connect(
#             host='localhost',
#             port=5432,
#             database='test',
#             user='ha',
#             password='123'
#         )

#         # Create a cursor object to interact with the database
# cursor = conn.cursor()
# sql_query = 'SELECT * FROM "Product";'
# cursor.execute(sql_query)

# # Fetch the results
# results = cursor.fetchall()

# for row in results:
#     print(row)

# # Commit changes and close the cursor and connection
# conn.commit()
# cursor.close()
# conn.close()