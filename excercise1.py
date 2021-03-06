
import os
os.environ['SPARK_HOME'] = '/usr/lib/spark'

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql.types import *


## loading default data set in the in the mysql retail_db department in different formats to learn
## Sqoop command used to load to hdfs in avro , sequencefile and text file

# spark.driver.extraClassPath /path/to/my.jar ####.set("spark.driver.extraClassPath","<path of the jar file created using sqoop import>")
conf = (SparkConf().setMaster("local[2]").setAppName("exercise1").set('spark.executor.memory', '2g')) #.set("spark.driver.extraClassPath","/home/cloudera/Desktop/mysql/mysql-connector-java-5.1.42-bin.jar"))
sc = SparkContext(conf= conf)

sqlc = SQLContext(sc)

## Use of pyspark.sql.types to sat the schema of the imported file
## once the schema is set the imported file can be converted to the dataframes
usr_df = sc.textFile("/user/cloudera/intelli/departments").map(lambda x : x.split(","))

## work similar to namedtuples as discussed earlier it does not change the data type of the rdd
## most of the data type when loaded in default is converted to string format
## StructType gives schema names to them hence can be converted to data frames

## StructType takes an iterable data structure like list for all schemas names
## data type mentioned here should match the data type of the table loaded else it will give error hence casting cannot be done using this
schema = StructType([StructField('department_id', StringType(), True),StructField('department_name', StringType(), True) ] )

department_df = sqlc.createDataFrame(usr_df,schema)
department_df.show()


#####################################################################################################################
# data read and write in the sequencefile format
## read data from sequential file
## reading the sequential file format imported using the sqoop gives
# Error: that writable name not detected
## reason is that sqoop import schema is not detected
## one possible alternative is to try running pyspark shell again with jar file genrated during sqoop import
## have not tried the solution . First line of the sqoop imported file gives the file format of the import
text = "org.apache.hadoop.io.LongWritable"

dep_seq = sc.sequenceFile("file:///home/cloudera/departments/part-00001", text, text)

dep = sc.sequenceFile("/user/cloudera/intelli", "org.apache.hadoop.io.LongWritable","departments")



# $ SPARK_CLASSPATH=/path/to/elasticsearch-hadoop.jar ./bin/pyspark
# conf = {"es.resource" : "index/type"}   # assume Elasticsearch is running on localhost defaults
# rdd = sc.newAPIHadoopRDD("org.elasticsearch.hadoop.mr.EsInputFormat",\
#     "org.apache.hadoop.io.NullWritable", "org.elasticsearch.hadoop.mr.LinkedMapWritable", conf=conf)

rdd = sc.newAPIHadoopFile(path="/user/cloudera/intelli/part-m-00000",inputFormatClass="org.apache.hadoop.mapreduce.lib.input.SequenceFileAsBinaryInputFormat",keyClass="org.apache.hadoop.io.LongWritable", valueClass="org.apache.hadoop.io.Text")

## Above solution does not work
# SO ultimate inference is if the sqoop import does not give the writable type formats in the output sequence file format
# one has to built an converter class for this using attribute valueConverter="valueConverterClass" in the
# sc.sequenceFile or newAPIHadoopFile making converter class is not easy
# Writable Type	        Python Type
# Text	                unicode str
# IntWritable	        int
# FloatWritable	        float
# DoubleWritable	    float
# BooleanWritable	    bool
# BytesWritable	        bytearray
# NullWritable	        None
# MapWritable	        dict


## save sequence file format
retail = sc.textFile("/user/cloudera/intelli/departments")

## save using first coloumn as key to local system
retail.map(lambda x: tuple(x.split(',',1))).saveAsSequenceFile("file:///home/cloudera/departments") ##copy data to local system
## save with key as none to local system
retail.map(lambda x: (None,x)).saveAsSequenceFile("file:///home/cloudera/department1") ##copy data to local system

# Save using any file format

retail.map(lambda x: tuple(x.split(",", 1))).saveAsNewAPIHadoopFile("file:///home/cloudera/department3","org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat",keyClass="org.apache.hadoop.io.Text",valueClass="org.apache.hadoop.io.Text")

data = sc.sequenceFile("file:///home/cloudera/department3")
for rec in data.collect():
    print(rec)


data = sc.sequenceFile("file:///home/cloudera/department3", "org.apache.hadoop.io.Text","org.apache.hadoop.io.Text")
for rec in data.collect():
    print(rec)



# #********************************File format handlled by SQL Context Parquet, Avro, ORC , Json********************************************************#####

# sqoop import --connect jdbc:mysql://localhost/retail_db --username root --password cloudera --table departments --split-by department_id --warehouse-dir /user/cloudera/intelli --fields-terminated-by '|'  --as-textfile -m 1
# sqoop import --connect jdbc:mysql://localhost/retail_db --username root --password cloudera --table departments --split-by department_id --warehouse-dir /user/cloudera/intelli/dep --fields-terminated-by '|'  --as-parquetfile -m 1


###### Parquet file format ##########################
## alwa
data = sqlc.read.parquet('hdfs:///user/cloudera/intelli/dep/departments')
data.show()
data1 = sqlc.read.format('org.apache.spark.sql.parquet').load('hdfs:///user/cloudera/intelli/dep/departments')
data1.show()

## Read directly using spark sql
df = sqlc.sql("SELECT * FROM parquet.`intelli/dep/departments`")


# Write to parquet file
# org.apache.spark.parquet
data.write.format("org.apache.spark.sql.parquet").mode("error").option("header","true").save("hdfs:///user/cloudera/intelli/dep_wr")
data.write.parquet("hdfs:///user/cloudera/intelli/dep_wr1")

# sqoop import --connect jdbc:mysql://localhost/retail_db --username root --password cloudera --table departments --split-by department_id --warehouse-dir /user/cloudera/intelli/depsnappy --fields-terminated-by '|'  --as-parquetfile --compress --compression-codec org.apache.hadoop.io.compress.SnappyCodec  -m 1
# # sqoop import in snappy coded file
# gzip - org.apache.hadoop.io.compress.GzipCodec
# bzip2 - org.apache.hadoop.io.compress.BZip2Codec
# LZO - com.hadoop.compression.lzo.LzopCodec
# Snappy - org.apache.hadoop.io.compress.SnappyCodec
# Deflate - org.apache.hadoop.io.compress.DeflateCodec

## setting up codec api in spark sql shell
sqlc.setConf("spark.sql.parquet.compression.codec","snappy")

# Reading the compressed file from the hdfs
df_snappy = sqlc.read.parquet("hdfs:///user/cloudera/intelli/depsnappy/departments")
df_snappy.show()

#writing the compressed file to the local system
df_snappy.write.format("org.apache.spark.sql.parquet").mode("error").option("compression", None).option("header","true").save("file:///home/cloudera/intelli2/")

## write uncompressed file
sqlc.setConf("spark.sql.parquet.compression.codec","uncompressed")
df_snappy.write.format("org.apache.spark.sql.parquet").mode("error").option("header","true").save("file:///home/cloudera/intelli3/")

df_snappy.write.parquet("file:///home/cloudera/intelli3/")



######Read and write directly from data base ###############################################################

### by default the driver :com.mysql.jdbc.Driver is added to spark config
## if not added it can be added by setting the spark configration runtime parameters : set("spark.driver.extrClassPath", <location where mysql-connector-jar file is present>)
df_mysql= sqlc.read.format("jdbc").option("url", "jdbc:mysql://localhost/retail_db").option("driver", "com.mysql.jdbc.Driver").option("dbtable", "departments").option("user", "root").option("password", "cloudera").load()

df_mysql.show()


