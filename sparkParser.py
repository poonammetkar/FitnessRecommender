'''
Following script reads the JSON data from HDFS by establishing the connection
and runs the spark queries to dump the heart rate and speed data in the MongoDB
for the real time processing. 
'''

'''
Import require packages.
'''
import pyspark
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType
from pyspark.sql import SparkSession
from pyspark.sql.functions import count
import statistics

'''
Create SparkSession.
'''
spark = SparkSession.builder\
            .master("local")\
            .appName("HDFSToMongoDB")\
            .config('spark.jars.packages','org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
            .getOrCreate()

'''
Create data frame for fitness data.
'''
data_frame = spark.read.option("multiline","true").json("fitness_data_500_data_speed.json")

'''
udf : User defined function that is used to create a reusable function in Spark.
array mean will return the mean for time series data 
'''
array_mean = udf(lambda x: float(statistics.mean(x)), FloatType())

#_________Collect Sport Data based on Heart_rate range and Gender______________

'''
Following function performs folowing actions:
1. Calculate mean for Heart_rate time series data by calling array_mean function.
2. 'withColumn' data frame function will add new column in existing data frame.
3. Create new data frame for updating avg heart rate.
'''
updated_df_hr = data_frame.withColumn('mean_heart_rate', array_mean('heart_rate'))


'''
Drop heart_rate coloumn as it is no longer needed.
'''
updated_df_hr = updated_df_hr.drop('heart_rate')

'''
Calculate min max range for 'mean_heart_rate' column.
'''
updated_df_hr = updated_df_hr.withColumn("heart_rate_min", updated_df_hr.mean_heart_rate/10)
updated_df_hr = updated_df_hr.withColumn("heart_rate_min", updated_df_hr.heart_rate_min.cast('int'))
updated_df_hr = updated_df_hr.withColumn("heart_rate_min", updated_df_hr.heart_rate_min*10)
updated_df_hr = updated_df_hr.withColumn("heart_rate_max", updated_df_hr.heart_rate_min+10)

'''
Select only required fields and drop the rest of the fields from 'updated_df_hr'
data frame.
'''
updated_df = updated_df_hr.select("gender","heart_rate_min","heart_rate_max","sport")

'''
Generate count for each sport activity based on the gender and heart rate range.
'''
updated_df = updated_df.groupBy("gender","heart_rate_min","heart_rate_max","sport").agg(count("sport").alias("sportCount"))

'''
Cache data in MongoDB.
'''
updated_df.select("gender","heart_rate_min","heart_rate_max","sport","sportCount").write\
    .format('com.mongodb.spark.sql.DefaultSource')\
    .mode('overwrite')\
    .option( "uri", "mongodb://localhost:27017/local.Heart_Rate") \
    .save()

'''
Showcase the data frame with "gender", "min_speed_range", "max_speed_range" and "sport".
'''
updated_df.show()


#_________Collect Sport Data based on Speed range and Gender___________________

'''
Calculate mean for Speed time series data by calling array_mean function
withColumn data frame function will add new column in existing data frame
Create new data frame for updating avg speed
'''
df_speed = data_frame.withColumn('mean_speed', array_mean('speed'))

'''
Drop Speed coloumn as it is no longer needed.
'''
df_speed = df_speed.drop('speed')

'''
Calculate min and max range for Speed column.
'''
df_speed = df_speed.withColumn("min_speed_range", df_speed.mean_speed/10)
df_speed = df_speed.withColumn("min_speed_range", df_speed.min_speed_range.cast('int'))
df_speed = df_speed.withColumn("min_speed_range", df_speed.min_speed_range*10)
df_speed = df_speed.withColumn("max_speed_range", df_speed.min_speed_range+10)

'''
Select only required fields and drop the rest of the fields from 'df_speed'
data frame.
'''
df_speed = df_speed.select("gender","min_speed_range","max_speed_range","sport")

'''
Generate count for each sport activity based on the gender and speed range.
'''
df_speed = df_speed.groupBy("gender","min_speed_range","max_speed_range","sport").agg(count("sport").alias("sportCount"))


'''
Cache data in MongoDB.
'''
df_speed.select("gender","min_speed_range","max_speed_range","sport","sportCount").write\
    .format('com.mongodb.spark.sql.DefaultSource')\
    .mode('overwrite')\
    .option("uri","mongodb://localhost:27017/local.Speed") \
    .save()

'''
Showcase the data frame with "gender","min_speed_range","max_speed_range" and "sport".
'''
df_speed.show()
