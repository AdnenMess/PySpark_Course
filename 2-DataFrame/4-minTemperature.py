from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType


sc = SparkContext()
spark = SparkSession(sc, jsparkSession=None).builder.appName("Temperature").getOrCreate()

schema = StructType([StructField("StationID", StringType()),
                    StructField("Date", IntegerType()),
                    StructField("mesure_type", StringType()),
                    StructField("temperature", FloatType())])

df = spark.read.schema(schema).csv("Data/temperatures.csv")

# First solution with SQL query

df.createOrReplaceTempView("data")

minTemps = spark.sql('''
                     Select stationID, 
                     round(min(temperature) * 0.1 * (9.0 / 5.0) + 32.0, 2) as MinTemperature
                     From data 
                     Where mesure_type Like "TMIN"
                     Group By stationID
                     ''')
for m in minTemps.collect():
    print(m)
    
# Second solution with Data Frame

minTemp = df.filter(df.mesure_type == "TMIN")

stationTemps = minTemp.select("stationID", "temperature")

minTempByStation = stationTemps.groupBy("stationID").min("temperature")

minTempsByStationF = minTempByStation.withColumn("temperature",
                                                  func.round(func.col("min(temperature)") * 0.1 * (9.0 / 5.0) + 32.0, 2))\
                                                  .select("stationID", "temperature").sort("temperature")
                                                  
results = minTempsByStationF.collect()

for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))
    
spark.stop()