from pyspark.sql import SparkSession
from pyspark.sql.types import *

if __name__ == '__main__':
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("Dataframe Intro") \
        .getOrCreate()

    # Create a dataframe using existing RDD
    inputdata = [("Ram", 100), ("Shyam", 101), ("Manoj", 102)]
    inputrdd = spark.sparkContext.parallelize(inputdata)

    # print(inputrdd.collect())

    inputdf = inputrdd.toDF(["firstname", "ids"])
    # inputdf.show()
    # inputdf.printSchema()

    inputdf1 = spark.createDataFrame(inputrdd).toDF(*["firstname", "ids"])
    # inputdf1.show()
    # inputdf1.printSchema()

    data = [(1, "Yesh Patil", 29, "M"),
            (2, "Ram Wagh", 30, "M"),
            (3, "Sita Patil", 29, "F")]

    dataschema = StructType([StructField(name="id", dataType=IntegerType()),
                             StructField(name="name", dataType=StringType()),
                             StructField("age", IntegerType()),
                             StructField("gender", StringType())])

    inputdf2 = spark.createDataFrame(data=data, schema=dataschema)
    # inputdf2.printSchema()
    # inputdf2.show()

    csvnoheaderdf = spark.read.csv(
        "C:\\Users\\tadit\\PycharmProjects\\pyspark_sessions\\input\\inputfile_withoutheader.csv")
    # csvnoheaderdf.printSchema()
    # csvnoheaderdf.show()

    csvheaderdf = spark.read.csv(
        r"C:\Users\tadit\PycharmProjects\pyspark_sessions\input\inputfile_withheader.csv", inferSchema=True,
        header=True)
    csvheaderdf.printSchema()
    csvheaderdf.show()

    dataschema1 = StructType([StructField(name="id", dataType=IntegerType()),
                              StructField(name="fname", dataType=StringType()),
                              StructField(name="lname", dataType=StringType()),
                              StructField("age", IntegerType()),
                              StructField("gender", StringType()),
                              StructField("deptno", IntegerType()),
                              StructField("salary", LongType())
                              ])

    csvwithschemadf = spark.read.csv(
        path=r"C:\Users\tadit\PycharmProjects\pyspark_sessions\input\inputfile_withheader.csv", schema=dataschema1,
        header=True)
    csvwithschemadf.printSchema()
    csvwithschemadf.show()
