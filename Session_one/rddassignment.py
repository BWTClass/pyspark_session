# Gender wise count
# sum of salary per department

from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").appName("Pratice").getOrCreate()

    # reading textfile as input rdd
    inputrdd = spark.sparkContext.textFile("C:\\Users\\tadit\\PycharmProjects\\pyspark_sessions\\input\\inputfile.txt")

    #
    outputrdd = (inputrdd.map(lambda x: x.split(","))
                 .map(lambda x: (x[4], 1))
                 .reduceByKey(lambda x, y: x + y))
    # outputrdd.collect()

    # sum of salary per department
    (inputrdd.map(lambda x: x.split(","))
     .map(lambda x: (x[5], int(x[6])))
     .reduceByKey(lambda x, y: x + y)
     # .collect()
     )

    inputdeptrdd = spark.sparkContext.textFile(
        "C:\\Users\\tadit\\PycharmProjects\\pyspark_sessions\\input\\department.txt")
    # skip header
    header = inputdeptrdd.first()
    inputdeptrdd = inputdeptrdd.filter(lambda line: line != header)

    """
    
    [001,Yesh,Patil,29,M,011,20000]
    (011,[001,Yesh,Patil,29,M,011,20000])
    
    [011,HR]
    (011,HR)
    
    [(011,[[001,Yesh,Patil,29,M,011,20000],HR])]
    
    ([[001,Yesh,Patil,29,M,011,20000],HR]) == element
    [001,Yesh,Patil,29,M,011,20000] == src
    """
    emprdd = inputrdd.map(lambda x: x.split(",")).map(lambda x: (x[5], x))

    deptrdd = inputdeptrdd.map(lambda x: x.split(",")).map(lambda x: (x[0], x[1]))

    joinrdd = emprdd.join(deptrdd).cache()

    def replacefun(element):
        src = element[0]
        src[5] = element[1]
        return src

    joinrdd.map(lambda x: x[1]).map(replacefun).saveAsTextFile("C:\\Users\\tadit\\PycharmProjects\\pyspark_sessions\\output\\rddassign")
