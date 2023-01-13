from pyspark.sql import SparkSession
from pyspark.sql import functions as func


spark = SparkSession.builder.appName('WordCount').getOrCreate()

inputDF = spark.read.text("Data/book.txt")

# split using a regular expression that extracts words
words = inputDF.select(func.explode(func.split(inputDF.value, "\\W+")).alias("word"))

words.filter(words.word != "")

# Normalize everything to lowercase
lowercaseWords = words.select(func.lower(words.word).alias("word"))

# Count up the occurrences of each word
wordCounts = lowercaseWords.groupBy("word").count()

# Sort by counts
wordCountsSorted = wordCounts.sort("count", ascending=False)

# Show the results.
wordCountsSorted.show(wordCountsSorted.count())