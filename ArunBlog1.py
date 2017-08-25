import os
os.environ["SPARK_HOME"] = 'usr/lib/spark'

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, Row

conf = (SparkConf().setMaster("local[*]").setAppName("ArunBlog1").set("spark.executor.memory","5g"))

sc = SparkContext(conf = conf)

sqlc = SQLContext(sc)


from pyspark.sql import SQLContext, Row

from pyspark.sql.functions import *

sqlc =   SQLContext(sc)

df_orders= sqlc.read.avro("/user/cloudera/problem1/orders")

df_orders_items = sqlc.read.avro("/user/cloudera/problem1/order_items")

joinorder_item= df_orders.join(df_orders_items,df_orders.order_id==df_orders_items
.order_item_order_id)


# order_id|   order_date|order_customer_id|   order_status
#
# order_item_id, order_item_order_id, order_item_product_id, order_item_quantity, order_item_subtotal, order_item_product_price
#
# joinorder_item

joinorder_item.groupBy(to_date(from_unixtime(col("order_date")/1000)), col("order_status")).show()


###### Solving first problem throw RDD in python
orders_df = sqlc.read.format('com.databricks.spark.avro').load('/user/cloudera/problem1/orders')
order_items_df = sqlc.read.format('com.databricks.spark.avro').load('/user/cloudera/problem1/order-items')
orders_rdd = orders_df.map(lambda x : (x[0],(x[1],x[2],x[3])))
order_items_rdd = order_items_df.map(lambda x : (x[1],(x[0],x[2],x[3],x[4],x[5])))
orders_items_joined = orders_rdd.join(order_items_rdd)
ord_1 = orders_items_joined.map(lambda x : (x[0],x[1][0],x[1][1])).map(lambda x : (x[0],x[1][0],x[1][2],x[2][3])).map(lambda x : ((x[1],x[2]),(x[0],x[3])))
ord_1.combineByKey((lambda x : ((str(x[0]),), x[1])),(lambda x,y : (x[0]+ (str(y[0]),), x[1]+y[1])),(lambda x,y : (x[0]+y[0],x[1]+y[1]))).map(lambda x: (x[0][0],x[0][1],len(set(x[1][0])),x[1][1])).sortBy(lambda x : (-x[2],-x[3])).take(1)



######## Solving throw data frame


joinorder_item.groupBy(to_date(from_unixtime(col("order_date")/1000)).alias("order_formatted_date"),col("order_status")).agg(round(sum("order_item_subtotal"),2).alias("total_amount"),countDistinct("order_id").alias("total_orders")).orderBy(col("order_formatted_date").desc(),col("order_status"),col("total_amount").desc(),col("total_orders").desc()).show()




###### Solving throw sql

# df_join
sqlc.sql("select to_date(from_unixtime(order_date/1000)) as order_formatted_date,order_status ,round(sum(order_item_subtotal),2) as total_amount,count(Distinct(order_id)) as total_order from df_join group by to_date(from_unixtime(order_date/1000)), order_status  order by  order_formatted_date desc, order_status, total_amount desc, total_order desc").show()




 # joinorder_item.map(lambda x : ((float(x["order_date"]),str(x["order_status"])) , (float(x["order_item_subtotal"]),str(x["order_id"])) )).combineByKey((lambda x : (x[0],x[1])),(lambda x,y :(x[0]+y[0],x[1]+y[1]) ), (lambda acc1, acc2:(acc1[0]+acc2[0],acc1[1]+acc2[1]) ) ).take(10)


[(('1374735600000', 'CANCELED'), '129.990005493')]
(('1374735600000', 'CANCELED'), '129.990005493299.980010986')


sqlc.setConf("spark.sql.parquet.compression.codec","gzip")
df.write.format("parquet").option("header","true").mode("error")

