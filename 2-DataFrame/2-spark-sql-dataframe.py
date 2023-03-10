from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

people = spark.read.option("header", "true").option("inferSchema", "true").csv("data/fakefriends-header.csv")

print("Here is our inferred schema:")
people.printSchema()

print("Let's display the name column:")
people.select("name").show(10)

print("Filterout anyone over 21")
people.filter(people.age < 21).show()

print("Group by age")
people.groupBy("age").count().show()

print("Make everyone 10 year older:")
people.select(people.name, people.age + 10).show()

spark.stop()
