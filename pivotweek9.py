from pyspark.sql import SparkSession
import getpass
#creating schema for reading the json file
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
from pyspark.sql.functions import col,size
from pyspark.sql.functions import countDistinct

username = getpass.getuser()
spark= SparkSession. \
builder. \
appName("week_9"). \
config('spark.ui.port','0'). \
config("spark.sql.warehouse.dir", f"/user/itv009033/warehouse"). \
config("spark.shuffle.useOldFetchProtocol", "true").\
enableHiveSupport(). \
master('yarn'). \
getOrCreate()

users_schema = StructType([StructField("user_id", IntegerType(), nullable=False),
StructField("user_first_name", StringType(), nullable=False),
StructField("user_last_name", StringType(), nullable=False),
StructField("user_email", StringType(), nullable=False),
StructField("user_gender", StringType(), nullable=False),
StructField("user_phone_numbers", ArrayType(StringType()),
nullable=True),
StructField("user_address", StructType([
StructField("street", StringType(), nullable=False),
StructField("city", StringType(), nullable=False),
StructField("state", StringType(), nullable=False),
StructField("postal_code", StringType(), nullable=False),
]), nullable=False)])

df = spark.read.format("json").schema(users_schema).load("/public/sms/users/*")
print("No of initial Partitions : ",df.rdd.getNumPartitions())
print("Total records in the dataframe : ", df.count())

df_unnest = df.withColumn("user_street",col("user_Address.street")) \
.withColumn("user_city",col("user_address.city")) \
.withColumn("user_state", col("user_Address.state")) \
.withColumn("user_postal_code", col("user_address.postal_code")) \
.withColumn("num_phn_numbers",
size(col("user_phone_numbers")))

df_pvt = df_unnest.select("user_gender", "user_state").filter(df_unnest.num_phn_numbers>0)

df_count = df_pvt.groupBy("user_state", "user_gender").count().sort("count", ascending = False)
df_count = df_count.withColumnRenamed("count", "MF")

result = df_count.groupBy("user_state").pivot("user_gender").sum("MF")

result.write.format("csv").mode("overwrite").option("path", "/user/itv009033/pivot_assignment_result").save()

spark.stop()