from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split

# Create a SparkSession
spark = SparkSession.builder \
    .appName("S3TextStreamShowExample") \
    .getOrCreate()

# Read data from the S3 bucket as a streaming DataFrame
lines = spark.readStream \
    .format("text") \
    .load(f"s3a://<path_to_your_bucket_folder>")

# Split the lines into words
words = lines.select(
   explode(
       split(lines.value, " ")
   ).alias("word")
)

# Generate running word count
wordCounts = words.groupBy("word").count()

query = wordCounts.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()

spark.stop()
