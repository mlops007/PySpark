from pyspark.sql import SparkSession
import getpass
from pyspark.sql.functions import countDistinct
from pyspark.sql.types import StructType, StructField, TimestampType, IntegerType, StringType, DateType, FloatType
from pyspark.sql.functions import from_unixtime, expr
import datetime
from pyspark.sql.functions import *
from pyspark.sql.functions import year, month, dayofmonth, quarter

username = getpass.getuser()
spark = SparkSession. \
builder. \
config('spark.ui.port', '0'). \
appName("week13").\
config('spark.shuffle.useOldFetchProtocol', 'true'). \
config("spark.sql.warehouse.dir", f"/user/itv009033/warehouse"). \
enableHiveSupport(). \
master('yarn'). \
getOrCreate()

print("START.....")
customers_schema = StructType([StructField("o_id", IntegerType(), True)
                              ,StructField("date", DateType(), True)
                              ,StructField("c_id", IntegerType(), True)
                              ,StructField("amount", FloatType(), True)])

cust_transf_df = spark.read.format("csv").schema(customers_schema).load("landing/cust_transf.csv")
cust_transf_df = cust_transf_df.withColumn("day", dayofmonth("date").alias("day"))
cust_transf_df = cust_transf_df.withColumn("month", month("date").alias("month"))
cust_transf_df = cust_transf_df.cache()
print("caching.......")
cust_transf_df.show()
print("no of rows : ", cust_transf_df.count())
print("no of rows : ", cust_transf_df.count())
print("cached.....")
print("no of partitions : ", cust_transf_df.rdd.getNumPartitions())
cust_transf_df.printSchema()


print(".........END")

spark.stop()


