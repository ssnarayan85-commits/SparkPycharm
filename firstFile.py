from pyspark.sql import SparkSession

#spark session
spark = SparkSession.builder\
        .master("local") \
        .appName("firstfile") \
        .getOrCreate()

data = [('k1', 'v1'), ('k2','v2'), ('k3','v')]
columns = ("key", "val")
df = spark.createDataFrame(data, columns)
df.show()
df.printSchema()
