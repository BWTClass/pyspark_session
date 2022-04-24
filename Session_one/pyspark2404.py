from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, lit

if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName("PYSPARK 2404") \
        .master("local[*]")\
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

    csvwithschemadf = spark.read.csv(
        path=r"C:\Users\tadit\PycharmProjects\pyspark_sessions\input\inputfile_withheader.csv", schema=dataschema1,
        header=True)
    # csvwithschemadf.printSchema()
    # csvwithschemadf.show()

    # write dataframe in csv file
    # csvwithschemadf.write.csv(r"D:\gitclone\pyspark_session\output\csvoutput", sep="\\t")

    # # write dataframe in JSON file
    # csvwithschemadf.write.json(r"D:\gitclone\pyspark_session\output\jsonoutput", mode="overwrite")

    # # write dataframe in orc file
    # csvwithschemadf.write.orc(r"D:\gitclone\pyspark_session\output\orcoutput")
    #
    # # parquet file
    # csvwithschemadf.write.parquet(r"D:\gitclone\pyspark_session\output\parquetoutput")

    # # create a dataframe from json
    # jsondf = spark.read.json(r"D:\gitclone\pyspark_session\output\jsonoutput\*.json")
    # jsondf.printSchema()
    # jsondf.show()

    # # Parquet
    # parquetdf = spark.read.parquet(r"D:\gitclone\pyspark_session\output\parquetoutput\*")
    # parquetdf.printSchema()
    # parquetdf.show()

    # #create empty dataframe
    rdd1 = spark.sparkContext.parallelize([])
    # print(rdd1.collect())
    df1 = spark.createDataFrame(rdd1, schema=dataschema1)
    # df1.show()

    # # select function
    # csvwithschemadf.cache()
    csvwithschemadf.printSchema()
    # csvwithschemadf.show()
    # csvwithschemadf.cache()
    # csvwithschemadf.cache()
    # csvwithschemadf.select(csvwithschemadf.fname).show()
    # csvwithschemadf.select(csvwithschemadf.fname.alias("firstname")).show()
    # csvwithschemadf.select(col("fname"),col("lname")).show()
    # csvwithschemadf.select(["fname","lname"]).show()

    # # dealing with nested data
    # jsonnesteddf = spark.read.format("json").load(r"D:\gitclone\pyspark_session\input\nestedjson.json")
    # jsonnesteddf.printSchema()
    # jsonnesteddf.select(jsonnesteddf.address.city).show()
    # jsonnesteddf.select(["address.city", "address.state"]).show()
    # jsonnesteddf.select("*").show()

    # print(csvwithschemadf.columns)

    # withColumns()
    # csvwithschemadf.show()
    # # existing column value change
    # csvwithschemadf.withColumn("salary", col("salary") * 10).show()
    # # change datatype of existing column
    # csvwithschemadf.withColumn("deptno", col("deptno").cast("string")).printSchema()
    # # adding new column
    # csvwithschemadf.withColumn("state",lit(10)).printSchema()

    # withColumnRenamed
    # csvwithschemadf.withColumnRenamed("fname","firstname").show()

    # # filter
    # csvwithschemadf.filter(col("gender") == "M").show()
    # csvwithschemadf.filter((col("gender") == "M") & (col("salary") > 25000)).show()

    # drop(), dropDuplicate() distinct()
    from pyspark.sql import Row

    data = [Row(name='Ajay', age=20),
            Row(name="Satish", age=25),
            Row(name='Ajay', age=20),
            Row(name='Ajay', age=30)]
    df1 = spark.sparkContext.parallelize(data).toDF()
    # df1.show()
    # df1.printSchema()

    # # distinct()
    # df1.distinct().show()
    #
    # # droptDuplicate()
    # df1.dropDuplicates(['name']).show()

    # # drop
    # df1.drop('age').show()

    # # groupby() -- Aggregate function
    # csvwithschemadf.groupby('deptno').avg('salary').show()

    # Join
    data1 = [Row(deptno=11, deptname='HR'),
             Row(deptno=12, deptname='IT'),
             ]
    deptdf = spark.sparkContext.parallelize(data1).toDF()
    deptdf.printSchema()
    #
    # # inner, left , right, full , semi
    # csvwithschemadf.join(deptdf,
    #                      on=csvwithschemadf.deptno == deptdf.deptno,
    #                      how='inner').select(["id", "fname", "lname", csvwithschemadf.deptno, "deptname"]).show()
    #
    # # left join
    # csvwithschemadf.join(deptdf,
    #                      on=csvwithschemadf.deptno == deptdf.deptno,
    #                      how='left').show()
    #
    # # antijoin
    # csvwithschemadf.join(deptdf,
    #                      on=csvwithschemadf.deptno == deptdf.deptno,
    #                      how='left_anti').show()

    # union
    data2 = [Row(deptno=11, deptname='HR'),
             Row(deptno=14, deptname='IT'),
             Row(deptno=13, deptname='IT'),
             ]
    deptdf1 = spark.sparkContext.parallelize(data2).toDF()
    deptdf1.printSchema()

    deptdf.union(deptdf1).show()
    deptdf.unionAll(deptdf1).distinct().show()