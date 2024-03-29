from pyspark.sql import SparkSession
from pyspark.sql.functions import concat,lit,udf
import pyspark.sql.functions as F
import pytest # added a new module

#external commit by dev1
#external commit by dev2

#external commit by dev3
#external commit by dev4

#spark session
spark = SparkSession.builder\
        .master("local") \
        .appName("firstfile") \
        .getOrCreate()

data = [('k1', 'v1'), ('k2','v2'), ('k3','v')]
columns = ("key", "val")

def udf_add_fullname():
    fullnameUDF = udf(lambda x : add_fullname_udf(x), StringType())
    return fullnameUDF

def add_fullname_udf(str):
        print( concat(firstname, " ",  lastname))

data = [('sasi', 'rekha', 1000), ('vasantha','kumar',2000), ('shelby','zacharia',3000)]
columns = ("Firstname", "Lastname", "Salary")
df = spark.createDataFrame(data, columns)

df.select(df["Firstname"], df["Lastname"], concat(df.Firstname, lit(" "), df.Lastname).alias("Fullname"),\
          (df.Salary * 0.3).alias("Bonus")).show()

df.printSchema()
df.show(20)
#added show 1
#added show 2
df.show()


