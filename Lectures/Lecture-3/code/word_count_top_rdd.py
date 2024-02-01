import sys
from pyspark import SparkContext

# Check arguments
if len(sys.argv) != 3:
    print("Usage: word_count_rdd.py <input_file_path> <output_dir> ", file=sys.stderr)
    exit(-1)

# Initialize Spark context
sc = SparkContext(appName="WordCountTopRDD")

# Read text data and create an RDD
lines = sc.textFile(sys.argv[1])

# Tokenize into words
words = lines.flatMap(lambda line: line.split(" "))

# Perform word counting
word_counts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)    

# Take the top 20 used words
top_20_word_counts = word_counts.top(20, lambda word: word[1])

# Write to file on cluster
top_to_file = sc.parallelize(top_20_word_counts).coalesce(1)
top_to_file.saveAsTextFile(sys.argv[2])

# Stop Spark context
sc.stop()
