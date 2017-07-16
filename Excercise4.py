
## Launch pyspark shell from terminal one can use
# pyspark --master local[4] --conf "spark.app.name= testApp" --conf "spark.executor.memory= 2g"
## to check for pyspark setting type
## sc._conf.getAll()   ## all changes setting are listed inthe output
# important run time parameters
# spark.app.name, spark.driver.memory, spark.executor.memory,spark, spark.driver.extraClassPath (does not work inthe client mode), spark.executor.extraClassPath


# import os
# os.environ['SPARK_HOME'] = 'usr/lib/spark'
# spark_home = os.environ.get('SPARK_HOME', None)
# print spark_home
from pyspark import SparkConf , SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql.types import  *
from pyspark.sql import SQLContext, Row


conf = (SparkConf().setMaster("local[2]").setAppName("exercise4").set('spark.executor.memory', '2g'))

sc = SparkContext(conf = conf)
sqlc = SQLContext(sc)


### to read the csv file we are using csv package in python which does not support unicode so textfile is loaded withou unicode and then decoded to default utf-8
lcsrdd1 = sc.textFile("/user/cloudera/sqoop/LCA_FY2013.csv",  use_unicode=False).map(lambda x : split(x)).map(lambda x : [i.decode('utf-8') for i in x])

## best option is to use split command if that can parse the fields correctly
lcsrdd = sc.textFile("/user/cloudera/sqoop/LCA_FY2013.csv").map(lambda x : x.split('|'))


####### data source csv is not available in spark 1.6 for ap above 1.6 its available
lcscsv = sqlc.read.option("delimiter", "|").option("header","false").option("inferSchema", "true").csv("file:///home/cloudera/Desktop/LCA_FY2013.csv")


lcsrdd1.saveAsTextFile("file:///home/cloudera/Desktop/newfile", "org.apache.hadoop.io.compress.BZip2Codec")
# reading csv file in spark 1.6 is by defining a seperate function
import csv
from StringIO import StringIO
def split(line):
    reader = csv.reader(StringIO(line), delimiter= '|')

    return reader.next()


lcsrdd.map(lambda x: nullparse(x)).take(10)

def nullparse(list):
    list1 =[]
    for item in list:
        if (item ==""):
            item= None
        list1.append(item)
    return list1

StructType([StructField('LCA_CASE_NUMBER', StringType(), True),StructField('STATUS', StringType(), True),
            StructField('LCA_CASE_SUBMIT', StringType(), True),StructField('Decision_Date', StringType(), True),
            StructField('VISA_CLASS', StringType(), True),StructField('LCA_CASE_EMPLOYMENT_START_DATE', StringType(), True),
            StructField('LCA_CASE_EMPLOYMENT_END_DATE', StringType(), True),StructField('LCA_CASE_EMPLOYER_NAME', StringType(), True),
            StructField('LCA_CASE_EMPLOYMENT_END_DATE', StringType(), True),StructField('LCA_CASE_EMPLOYER_NAME', StringType(), True),





            ])