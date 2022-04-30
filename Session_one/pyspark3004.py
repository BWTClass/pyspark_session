import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import *
from pyspark.sql.types import *

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").appName("Pyspark 3004").getOrCreate()

    schema = "id int,fname string,lname string,age int,gender string,deptno int,salary long"
    df = spark.read.csv(path=r"D:\gitclone\pyspark_session\input\employee_details2.csv", schema=schema)
    # df.printSchema()
    # df.show()

    windowspec = Window.partitionBy("deptno").orderBy(col("salary"))

    # cache() -- memory and disk
    # persist() -- StorageLevel
    df.persist(storageLevel=pyspark.StorageLevel.MEMORY_ONLY)

    # row_number()
    df.withColumn("row_number", row_number().over(windowspec)) \
        .select("deptno", "salary", "row_number").show()

    # rank() and dense_rank()
    df.select("deptno", "salary").withColumn("rank", rank()
                                             .over(Window.partitionBy("deptno")
                                                   .orderBy(col("salary")))
                                             ).show()

    df.select("deptno", "salary").withColumn("dense rank", dense_rank()
                                             .over(Window.partitionBy("deptno")
                                                   .orderBy(col("salary")))
                                             ).show()

    # lag
    df.withColumn("lag_Val", lag("salary", 1).over(windowspec)).show()

    # led
    df.withColumn("lag_Val", lead("salary", 1).over(windowspec)).show()

    # aggregate functions
    df.withColumn("avg", max("salary").over(Window.partitionBy("deptno"))).show()

    #
    df.withColumn("lag_Val", lead("salary", 1).over(windowspec)).withColumn("difference",
                                                                            col("lag_Val") - col("salary")).show()

    # Spark SQL
    df.createOrReplaceTempView("employee")
    spark.sql("select gender,count(*) as count_gender from employee group by gender").show()

    # spark UDFs
    def lowercasefunc(element):
        return str(element).lower()


    lowercaseUDF = udf(lambda x: lowercasefunc(x))

    df.select("gender", lowercaseUDF(col("gender"))).show()


    # M = Male and F = Female == new_val
    def fullformfunc(element):
        output = ""
        if str(element).upper() == "M":
            output = "Male"
        elif str(element).upper() == "F":
            output = "Female"
        else:
            output = "other"
        return output


    fullformUDF = udf(lambda x: fullformfunc(x))

    df.select("gender", fullformUDF(col("gender")).alias("gender_new")).drop("gender").show()

    # spark sql udf
    spark.udf.register("fullformfunc", fullformfunc, StringType())
    df.createOrReplaceTempView("employee")
    spark.sql("select gender,fullformfunc(gender) as new_val from employee").show()

    # datetime function
    inputdata = [
        (1, "2022-04-01"),
        (2, "2022-04-02"),
        (3, "2022-04-03"),
    ]

    df1 = spark.createDataFrame(inputdata, ["id", "input_dt"])
    # df1.printSchema()
    # df1.show()

    # current_date()
    df1.select(current_date()).show()

    # to_date()
    df1.select("id", to_date("input_dt")).printSchema()

    # date_format()
    df1.select("id", date_format("input_dt", "dd-MM-yyyy").alias("date_format")).show()

    from datetime import datetime

    inputdata1 = [
        (1, datetime.strptime("2022-04-01","%Y-%m-%d")),
        (2, datetime.strptime("2022-04-02","%Y-%m-%d")),
        (3, datetime.strptime("2022-04-03","%Y-%m-%d"))
    ]

    df2 = spark.createDataFrame(inputdata1, ["id", "input_dt"])
    df2.printSchema()

    #
    df1.select("id", "input_dt",
               datediff(lit(datetime.strptime("2022-04-05", "%Y-%m-%d")), to_date(col("input_dt")))).show()

    df1.select("id", "input_dt",
               lit(datetime.strptime("2022-04-05", "%Y-%m-%d")).alias("new_val")) \
        .select(
        datediff(col("new_val"), to_date(col("input_dt")))).show()

    df1.select(dayofmonth(to_date(col("input_dt")))).show()
    val = df1.select(dayofmonth(to_date(col("input_dt"))).alias("new_val")).take(1)[0][0]
    print(val)

    #
    from datetime import datetime

    df1.select("id", "input_dt",
               lit(datetime.strptime("2022-04-05", "%Y-%m-%d")).alias("new_val")) \
        .select(
        date_add(to_date(col("input_dt")), 1)).show()
