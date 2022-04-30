# Session Third
# Date: 24/04/2022
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName("PYSPARK 2404") \
        .master("local[*]") \
        .config("spark.driver.bindAddress", "localhost") \
        .config("spark.ui.port", "4050") \
        .getOrCreate()

    dataschema1 = StructType([StructField(name="id", dataType=IntegerType()),
                              StructField(name="fname", dataType=StringType()),
                              StructField(name="lname", dataType=StringType()),
                              StructField("age", IntegerType()),
                              StructField("gender", StringType()),
                              StructField("deptno", IntegerType()),
                              StructField("salary", LongType())
                              ])

    empdf = spark.read.csv(path=r"D:\gitclone\pyspark_session\input\employee_details.csv",
                           schema=dataschema1,
                           header=True)

    empdf1 = spark.read.csv(path=r"D:\gitclone\pyspark_session\input\employee_details1.csv",
                            schema=dataschema1,
                            header=True)

    # empdf1.show()
    # empdf.show()

    deptdf = spark.read.csv(path=r"D:\gitclone\pyspark_session\input\department.csv", inferSchema=True)
    # deptdf.show()

    # 1. average salary per department
    empdf.groupby('deptno').agg(avg('salary').alias('avg_salary')).show()

    # retrieve employee_details with unique records from employee_detail.txt and employee_details1.txt and store them in different file.
    empdf.union(empdf1).distinct().show()
