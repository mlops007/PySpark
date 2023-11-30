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

df_unnest.show(truncate = False)

df_state = df_unnest.select("user_state")
print("No of united states zip : ",df_state.filter(df_state.user_state == "New York").count())

df_postal = df_unnest.select("user_state", "user_postal_code")

temp = df_postal.groupBy("user_state").agg(countDistinct("user_postal_code")).sort("count(user_postal_code)",ascending = False)
temp = temp.withColumnRenamed("count(user_postal_code)", "zip_codes")
temp.show()


users = df_unnest.select("user_id", "user_city")
userrs = users.groupBy("user_city").agg(countDistinct("user_id")).sort("count(user_id)", ascending = False)
userrs = userrs.withColumnRenamed("count(user_id)", "no_of_people_per_state")
userrs = userrs.filter(userrs.user_city != "null")
userrs.show()

email = df_unnest.select("user_id", "user_email").filter(df_unnest.user_email.like("%bizjournals.com%"))
email.show(truncate = False)

print("No of emails with bizjournal : ",email.count())

ph_num = df_unnest.select("user_id", "num_phn_numbers").filter(df_unnest.num_phn_numbers==4)
print("user with 4 ph_numbers : ",ph_num.count())

print("Saving ..")

df_unnest.write.format("parquet").mode("overwrite").partitionBy("user_state", "user_city").option("path", "/user/itv009033/customers_folder_write").save()

print("end ....")

spark.stop()




