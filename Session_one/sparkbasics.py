from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

if __name__ == '__main__':
    print("Hello Class...!")
    # sparkconf = SparkConf().setAppName("Spark Context Init").setMaster("local[*]")
    # sc = SparkContext(conf=sparkconf)
    # print(sc)

    spark = SparkSession.builder.appName("Spark Context Init") \
        .config("spark.driver.bindAddress", "localhost") \
        .config("spark.ui.port", "4050") \
        .master("local[*]").getOrCreate()
    print(spark)

    lst = [1, 2, 3, 4, 5]
    rdd1 = spark.sparkContext.parallelize(lst, 2)
    print(rdd1.getNumPartitions())
    print(rdd1.glom().collect())

    print("rdd1 count: " + str(rdd1.count()))
    print("rdd1 first element: " + str(rdd1.first()))

    # input("Enter any word")
    # spark.stop()
