from pyspark import SparkConf, SparkContext


conf = SparkConf().setMaster("local").setAppName("WorldCount")
sc = SparkContext(conf=conf)

input = sc.textFile("Data/Book.txt")

words = input.flatMap(lambda x: x.split())
wordCounts = words.countByValue()

for word, count in wordCounts.items():
    cleanWorld = word.encode('ascii', 'ignore')
    if cleanWorld:
        print(cleanWorld, count)
