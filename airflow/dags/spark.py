from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import col, avg, when, regexp_replace, regexp_extract
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

spark = SparkSession.\
        builder.\
        appName("Deme").\
        config("spark.executor.memory", "512m").\
        getOrCreate()
hdfs_host = '172.21.0.5'
hdfs_port = 9000
hdfs_directory = '/data'
timestamp = datetime.now().strftime('%Y-%m-%d')
hdfs_path = f'hdfs://namenode:9000/data/lazada_{timestamp}.csv'

from pyspark.sql.types import *
schema = StructType([
    StructField('NAME', StringType(), True),
    StructField('originalPrice', DoubleType(), True),
    StructField('DiscountedPrice', DoubleType(),True),
    StructField('Discount', StringType(), True),
    StructField('ratingScore', DoubleType(),True),
    StructField('review', IntegerType(),True),
    StructField('categories', StringType(), True),
    StructField('itemSoldCntShow', StringType(), True),
    StructField('sellerName', StringType(), True),
    StructField('brandName', StringType(), True),
    StructField('location', StringType(), True)

])

df = (spark.read.format("com.databricks.spark.csv")
        .option("header", "true")
        #.option("inferSchema","true")
        .schema(schema)
        .load(hdfs_path))

df.show()
#
filtered_df = df.select(['NAME', 'originalPrice','DiscountedPrice','Discount', 'ratingScore', 'review', 'categories', 'itemSoldCntShow', 'sellerName','brandName','location'])

average_value = filtered_df.agg(avg(col('DiscountedPrice'))).collect()[0][0]
filtered_df = filtered_df.fillna(value=average_value, subset=['DiscountedPrice'])

columns_to_fill = ["originalPrice", "DiscountedPrice"]
filtered_df = filtered_df.withColumn("originalPrice", when(col("originalPrice").isNull(), col("DiscountedPrice")).otherwise(col("originalPrice")))

columns_to_fill_with_zero = ['Discount', 'ratingScore', 'review', 'itemSoldCntShow']
filtered_df = filtered_df.fillna(0, subset=columns_to_fill_with_zero)

pattern = r'\d+'
# Extract numbers from the text_column
filtered_df = filtered_df.withColumn(
    "Discount",
    when(col("Discount").rlike(pattern), regexp_extract(col("Discount"), pattern, 0)).otherwise(0)
)
filtered_df = filtered_df.withColumn("Discount", col("Discount").cast("int"))

filtered_df.select("Discount").show()
pattern_with_k = r'(\d+)k'
pattern_without_k = r'(\d+)'
filtered_df.select("itemSoldCntShow").show()
filtered_df = filtered_df.withColumn(
    "itemSoldCntShow",
    when(col("itemSoldCntShow").contains("k"), (regexp_extract(col("itemSoldCntShow"), pattern_with_k, 1).cast("int") * 1000))
    .when(col("itemSoldCntShow").rlike(pattern_without_k), regexp_extract(col("itemSoldCntShow"), pattern_without_k, 1).cast("int"))
    .otherwise(0)
)
filtered_df.select("itemSoldCntShow").show()

text_columns = ['NAME', 'sellerName', 'brandName', 'location']
pattern1 = r'\[.*?\]'
pattern2 = r'\(.*?\)'
for txt_col in text_columns:
    filtered_df = filtered_df.withColumn(txt_col, regexp_replace(col(txt_col), pattern1, ''))
    filtered_df = filtered_df.withColumn(txt_col, regexp_replace(col(txt_col), pattern2, ''))
    filtered_df = filtered_df.withColumn(txt_col, regexp_replace(regexp_replace(col(txt_col), ",", ""), "'", ""))
    filtered_df = filtered_df.withColumn(txt_col, initcap(trim(col(txt_col))))
filtered_df.select(['NAME', 'sellerName', 'brandName', 'location']).show()

#filtered_df.dropDuplicates()
filtered_df.show()

# Load processed data into final destination (postgresQL)


#pandas_df = df.toPandas()


table_dimcategory = 'DimCategory'
table_dimbrand = 'DimBrand'
table_dimseller = "DimSeller"

db_config = {
        'user': 'ha',
        'password': '123',
        'port': 5432,
        'host': '10.155.72.168',
        'database': 'test'
}
engine = create_engine(f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}")
pd_dimcategory_df = pd.read_sql_table(table_dimcategory, engine)
pd_dimbrand_df = pd.read_sql_table(table_dimbrand, engine)
pd_dimseller_df = pd.read_sql_table(table_dimseller, engine)

dim_category_df = spark.createDataFrame(pd_dimcategory_df)
dim_brand_df = spark.createDataFrame(pd_dimbrand_df)
dim_seller_df = spark.createDataFrame(pd_dimseller_df)

joined_df = filtered_df.join(dim_category_df, filtered_df.categories == dim_category_df.categoryname, "inner")
joined_df = joined_df.drop('categories', 'categoryname')  
joined_df.show(truncate=False)

joined_df1 = joined_df.join(dim_brand_df, joined_df.brandName == dim_brand_df.brandname, "inner")
joined_df1 = joined_df1.drop('brandName', 'brandname', 'yearofex', 'brandcountry') 
joined_df1.show(truncate=False)

joined_df2 = joined_df1.join(dim_seller_df, joined_df1.sellerName == dim_seller_df.sellername, 'inner')
joined_df2 = joined_df2.drop('sellerlocation', 'yearofex', 'sellername', 'location', 'sellerName')
joined_df2.show(truncate=False)

pandas_df = joined_df2.toPandas()



pandas_df.to_sql('tes', engine, if_exists='append', index=False)


engine.dispose()
spark.stop()

