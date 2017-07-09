
### commands to be excecuted in console

import os
os.environ['SPARK_HOME']= '/usr/lib/spark'
import avro.schema
from avro.io import DatumWriter
from avro.datafile import DataFileReader, DataFileWriter
#from pyspark.sql import SQLContext , Row
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext , Row

#####Important### Install avro codex using linux shell
# $SPARK_HOME/bin/pyspark --packages com.databricks:spark-avro_2.11:3.2.0
# crete spark context and sql context to run sql operation onthe spark in distributed format
conf = (SparkConf().setMaster("local").setAppName("anoterApp1").set("spark.executor.memory", "1g"))
sc=SparkContext(conf= conf)
sqlc = SQLContext(sc)
sqlc.setConf("spark.sql.avro.compression.codec","snappy")
# schema_string ='''{"namespace": "example.avro",
#  "type": "record",
#  "name": "KeyValue",
#  "fields": [
#      {name :'auctionid', 'type' : 'int'}
#      {name :'bid', 'type' : 'int'}
#      {name :'bidtime', 'type': 'string'}df = sqlc.read.format("com.databricks.spark.avro").load("/user/cloudera/ebay_sk")
#      {name: 'bidder', 'type' : 'string'}
#      {name :'bidderrate', 'type' : 'int'}
#      {name : 'openbid', 'type': 'int'}
#      {name : 'price', 'type' : 'int'}
#  ]
# }'''
#
#
#
# schema = avro.schema.parse(schema_string)
#
# wrt = DataFileWriter(open("kv.avro", "w"), DatumWriter(), schema)
# wrt.append({"key": "foo", "value": -1})
# wrt.append({"key": "bar", "value": 1})
#########################################################################################################################################################
## convert from avro to dataframe
df1 = sqlc.read.format("com.databricks.spark.avro").load("/user/cloudera/ebay/ebay")

### reading xml schema is similar to other schema
# SPARK_HOME/bin/pyspark --packages com.databricks:spark-xml_2.10:0.3.3  ## define package only supports on spark 1.6
## any upgraded packages does not support on the spark vrsion of 1.6 so one has to set the given package fo format to work

df4 = sqlc.read.format("com.databricks.spark.xml").option("rowTag","<root row tag name usually the name of the file>").load("<path of the xml file >")

## Reading directly from jdbc connection
df = sqlc.read.format('jdbc').options(url='jdbc:mysql://localhost/sparktrain', dbtable='ebay').load()


## register table to implement sql operations
df1.registerTempTable("ebay")
sqlc.sql("select count(*) from ebay group by auctionid").show()



## show() shows the table in the formated form vs collect() shows collection of the rows
# function library in pyspark has to be imported in oreder to use aggregate function
## reading through api
##What’s the minimum, maximum, and average number of bids per item?
import pyspark.sql.functions as func
df1.groupby(df1.auctionid).count().agg(func.max("count"),func.min("count"), func.avg("count")).show()



## bids with price greater than 100
## by default all data while loading from avro to file to data frame is converted to string format
## convert string format to float use cast operator in the select function
df2= df1.select("auctionid", df1.price.cast("float").alias("price_num"))
df2
df2.filter(df2.price_num>100).show()

##### Using sql to find the bid >100

## dataframe df1 has already been temporarily registerd as ebay in previous commands
sqlc.sql("select auctionid , cast(price as int)  from ebay where price > 100").show()



####################Type casting date format in data frame###################

sqlc.sql("select auctionid, bid, TO_DATE(CAST(UNIX_TIMESTAMP(bidtime, 'DD.YYYYMM' ) as timestamp)) as biddate from ebay").show()


######### Another way of converting to data frame and type casting ######################
####### Rather than converting the table in avro format while importng using sqoop so that each coloumn is converted to string if type not specified
### when creating data frame
#### we can use named tuples for this load data into rdd in desired format and naming an then convert rdds to data frame
## using function createDataframes

##DataFrame[auctionid: string, bid: string, bidtime: string, bidder: string, bidderrate: string, openbid: string, price: string]

from collections import namedtuple
from datetime import datetime

field = ("auctionid", "bid","bidtime", "bidder", "bidderrate", "openbid", "price")

ebay = namedtuple("ebay", field, verbose = True)

def parse(row):
    row[0] = int(row[0])
    row[1] = int(row[1])
    #row[2] = datetime.strptime(row[2],'%d.%Y%m').date()  ## data in date is not correct
    row[4] = float(row[4])
    row[5] = int(row[5])
    row[6] = int(row[6])
    return ebay(*row[:7])

ebayrdd=sc.textFile("/user/cloudera/ebay_sk")
ebaydata= ebayrdd.map(lambda x : x.split(',')).map(parse)

for ebayd in ebaydata.collect():
  print(ebayd)

df3= sqlc.createDataFrame(ebaydata)

df3.select("auctionid", "price").filter(df3.price>100).show()


#### Sort data frames in descending based on price

df3.select("auctionid", "price").filter(df3.price>100).sort("price", ascending=False).show()

## writes by default to user direstory in ourcase to "hdfs://quickstart.cloudera:8020/user/cloudera/ebayselect.parquet"
df3.select("auctionid", "price").write.save("ebayselect.parquet", format= "parquet")

## write to specified location give the full path
df3.select("auctionid", "price").write.save("hdfs://quickstart.cloudera:8020/user/ebayselect.parquet", format= "parquet")


#
# Save Modes
# Save operations can optionally take a SaveMode, that specifies how to handle existing data if present. It is important to realize that these save modes do not utilize any locking and are not atomic. Additionally, when performing a Overwrite, the data will be deleted before writing out the new data.
#
# Scala/Java	Any Language	Meaning
# SaveMode.ErrorIfExists (default)	"error" (default)	When saving a DataFrame to a data source, if data already exists, an exception is expected to be thrown.
# SaveMode.Append	"append"	When saving a DataFrame to a data source, if data/table already exists, contents of the DataFrame are expected to be appended to existing data.
# SaveMode.Overwrite	"overwrite"	Overwrite mode means that when saving a DataFrame to a data source, if data/table already exists, existing data is expected to be overwritten by the contents of the DataFrame.
# SaveMode.Ignore	"ignore"	Ignore mode means that when saving a DataFrame to a data source, if data already exists, the save operation is expected to not save the contents of the DataFrame and to not change the existing data. This is similar to a CREATE TABLE IF NOT EXISTS in SQL.