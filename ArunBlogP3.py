
import os

os.environ['SPARK_HOME'] = 'usr/lib/spark'
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext , Row


conf  = (SparkConf().setMaster('local[2]').setAppName('p3').set("spark.executor.memory","2g"))

sc = SparkContext(conf=conf)


### sqoop import all tables
### no need to create the directory structure while importing throw sqoop
# sqoop import-all-tables --connect jdbc:mysql://quickstart.cloudera:3306/retail_db --username root --password cloudera --warehouse-dir /user/cloudera/retail.db --compress --compression-codec 'org.apache.hadoop.io.compress.SnappyCodec' --as-avrodatafile

# get avro imported file to the local
# hadoop fs -get /user/cloudera/retail.db/orders/part-m-00000.avro  /home/cloudera/Desktop
## change to directory where avro file is present
##get schema of the avro file
# avro-tools getschema part-m-00000.avro > orders.avsc

##copy schema from the local to the hadoop system
# make schema directory
# hadoop fs -mkdir /user/hive/schema/
# hadoop fs -copyFromLocal /home/cloudera/Desktop/orders.avsc /user/hive/schema

#### Create hive meta store ####
# create external table orders_sqoop
# stored as avro
# location '/user/hive/warehouse/retail.db/orders'
# tblproperties('avro.schema.url'= '/user/hive/schema/orders.avsc', 'skip.header.line.count'=0);


##Q3
# select * from orders_sqoop as X where X.order_date in (select inner.order_date from (select Y.order_date, count(1) as total_orders from orders_sqoop as Y group by Y.order_date order by total_orders desc, Y.order_date desc limit 1) inner);


#### Hive time conversion
# This function converts the number of seconds from unix epoch (1970-01-01 00:00:00 UTC) to a STRING that represents the TIMESTAMP of that moment in the current system time zone in the format of “1970-01-01 00:00:00”.
# The following example returns the current date including the time.
# hive> SELECT FROM_UNIXTIME(UNIX_TIMESTAMP());

# 2015-05-18 05:43:37


# unix_timestamp :
# This function converts the date to the specified date format and returns the number of seconds between the specified date and Unix epoch.
# If it fails, then it returns 0. The following example returns the value 1237487400

# hive> SELECT unix_timestamp ('2009-03-20', 'yyyy-MM-dd');

# 1237487400

# To_Date( string timestamp ) :
# The TO_DATE function returns the date part of the timestamp in the format ‘yyyy-MM-dd’
# hive> select TO_DATE('2000-01-01 10:20:30');

# 2000-01-01

#
# Step 5 and 6:
# create database retail;
#
# create table orders_avro
#     > (order_id int,
#     > order_date date,
#     > order_customer_id int,
#     > order_status string)
#     > partitioned by (order_month string)
#     > STORED AS AVRO;
#
#  insert overwrite table orders_avro partition (order_month)
# select order_id, to_date(from_unixtime(cast(order_date/1000 as int))), order_customer_id, order_status, substr(from_unixtime(cast(order_date/1000 as int)),1,7) as order_month from default.orders_sqoop;


## Alter table does not work onthe avro schema file one has to manually update the schema as linked to table to add new coloumn or modify the existing colounms 