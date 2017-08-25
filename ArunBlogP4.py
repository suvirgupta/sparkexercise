

# sqoop import --connect jdbc:mysql://localhost/retail_db --username root --password cloudera --table orders --warehouse-dir  /user/cloudera/problem5/text --as-textfile --fields-terminated-by '\t' --lines-terminated-by '\n';
# sqoop import --connect jdbc:mysql://localhost/retail_db --username root --password cloudera --table orders --warehouse-dir  /user/cloudera/problem5/avro --as-avrodatafile --fields-terminated-by '\t' --lines-terminated-by '\n';
# sqoop import --connect jdbc:mysql://localhost/retail_db --username root --password cloudera --table orders --warehouse-dir  /user/cloudera/problem5/parquet --as-parquetfile --fields-terminated-by '\t' --lines-terminated-by '\n';


from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext , Row


conf  = (SparkConf().setMaster('local[2]').setAppName('p4').set("spark.executor.memory","2g"))

sc = SparkContext(conf=conf)

sqlc = SQLContext(sc)



###############Transform from avro file ##############################################
df_orders = sqlc.read.format("com.databricks.spark.avro").load("/user/cloudera/problem5/avro/orders")

## save as parquet file

sqlc.setConf("spark.sql.parquet.compression.codec", "snappy")
df_orders.write.format("org.apache.spark.sql.parquet").mode("error").save("/user/cloudera/problem5/parquet-snappy-compress")


### Convert data frame to rdd to be used with textfile and sequence file
rdd_orders= df_orders.map(lambda x : (x.order_id, x.order_date, x.order_customer_id, x.order_status))
rdd_orders.take(10)
### compress as bzip codec
rdd_orders.saveAsTextFile("/user/cloudera/problem5/text-gzip-compress", "org.apache.hadoop.io.compress.BZip2Codec")
## compress as gzip codec
rdd_orders.saveAsTextFile("/user/cloudera/problem5/text-bzip-compress", "org.apache.hadoop.io.compress.GzipCodec")


### Save rdd as sequencial file formats
#  without any compression save has to be done with the key whether none or some element of the tuple
## Save sequence file with none key
rdd_orders.map(lambda x: (None, x)).saveAsSequenceFile("/user/cloudera/problem5/sequence")

## save sequenc filee with the first coloumn as key
rdd_orders.map(lambda x: (x[0],(x[1],x[2],x[3]))).saveAsSequenceFile("/user/cloudera/problem5/sequence_key1")

## Save the sequence file with first coloumn as key and compressed as snappy
## below code makes and arrayWritable valuClass that is difficult to read.
rdd_orders.map(lambda x: (x[0],(x[1],x[2],x[3]))).saveAsSequenceFile("/user/cloudera/problem5/sequence_key1-bzip", "org.apache.hadoop.io.compress.BZip2Codec")

## None key is NullWritable
## tuple of values is ArrayWritable
## save using hadoop new api
# dont use arrayWritable as valueClass as reading the sequential file with the arrayWritable will cause error
## seperate class has to be written for array writable rather save you value class as string and use Text as writable
rdd_orders.map(lambda x: (None, ''.join(str(x)))).saveAsNewAPIHadoopFile("/user/cloudera/problem5/sequence_newAPI4","org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat", keyClass = "org.apache.hadoop.io.NullWritable",valueClass = "org.apache.hadoop.io.Text" )
rdd_seq_ord = sc.sequenceFile("/user/cloudera/problem5/sequence_newAPI4","org.apache.hadoop.io.NullWritable", 'org.apache.hadoop.io.Text')
rdd_orders1.map(lambda x: (None, x[1])).take(10)
######################### Transform from parquet file################################

df_orders_par = sqlc.read.parquet("/user/cloudera/problem5/parquet-snappy-compress")
df_orders_par = sqlc.read.format("org.apache.spark.sql.parquet").load("/user/cloudera/problem5/parquet-snappy-compress")
df_orders_par_h = hqlc.read.format("org.apache.spark.sql.parquet").load("/user/cloudera/problem5/parquet-snappy-compress")
#### Save to hdfs without the compression
sqlc.setConf("spark.sql.parquet.compression.codec", "uncompressed")
df_orders_par.write.format("org.apache.spark.sql.parquet").save("/user/cloudera/problem5/parquet-no-compress")

## To save as snappy set codec to snappy
sqlc.setConf("spark.sql.avro.compression.codec", "snappy")
df_orders_par.write.format("com.databricks.spark.avro").save("/user/cloudera/problem5/avro-snappy")


#### Transformavro snappy

df_ord_av_sn = sqlc.read.format("com.databricks.spark.avro").load("/user/cloudera/problem5/avro-snappy")
df_ord_av_sn_h = hqlc.read.format("com.databricks.spark.avro").load("/user/cloudera/problem5/avro-snappy")
df_ord_av_sn_h.show()
# saving directly from dataframe but cannot find any library for the compression part
df_ord_av_sn.write.format("json").save("/user/cloudera/problem5/json-no-compress")
df_ord_av_sn.write.json("/user/cloudera/problem5/json-no-compress2")


## best way to compress and write json format is using sprk context

# convert dataframe to rdd
rdd_ord_json= df_ord_av_sn.toJSON()
rdd_ord_json.take(10)
### Save as gzip codec file

rdd_ord_json.saveAsTextFile("/user/cloudera/problem5/json-gzip1",compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec")

df_ord_json = sqlc.read.json("/user/cloudera/problem5/json-gzip1")
df_ord_json.map(lambda x: (x[0],x[1],x[2],x[3])).saveAsTextFile("/user/cloudera/problem5/csv-gzip", compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec")


### store orc file from the parquet file
from pyspark.sql import HiveContext
hqlc = HiveContext(sc)
df_orders_par_h.write.format("orc").save("/user/cloudera/problem5/orc")
df_orders_par_h = hqlc.read.format("org.apache.spark.sql.parquet").load("/user/cloudera/problem5/parquet-snappy-compress")








