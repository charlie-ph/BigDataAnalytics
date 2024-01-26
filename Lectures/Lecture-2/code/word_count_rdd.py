from pyspark import SparkContext

# Initialize Spark context
sc = SparkContext("local", "WordCount")

# Read text data and create an RDD
lines = sc.textFile("path-to-text-file")

# Tokenize into words
words = lines.flatMap(lambda line: line.split(" "))

# Perform word counting
word_counts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)    

# Take the top 20 used words
top_20_word_counts = word_counts.top(20, lambda word: word[1])
print(top_20_word_counts)

# Take the bottom 20 used words
bottom_20_word_counts = word_counts.takeOrdered(20, lambda word: word[1])
print(bottom_20_word_counts)

# Stop Spark context
sc.stop()
