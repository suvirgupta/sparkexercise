
from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext,SQLContext, Row

conf = (SparkConf().setMaster('local[2]').setAppName('data_analysis').set("spark.executor.memory",'2g'))
sc = SparkContext(conf = conf)

hqlc = HiveContext(sc)

hqlc.sql('create database problem6')
hqlc.sql("drop database if exists problem6 cascade")
# sqoop import-all-tables --connect 'jdbc:mysql://localhost/retail_db' --username root --password cloudera --hive-import --hive-database problem6 --hive-overwrite
hqlc.sql("use problem6")
hqlc

df_rank=hqlc.sql("select d.department_name,c.category_name ,p.product_name,p.product_description,p.product_price, rank() over (partition by d.department_id order by p.product_price )  as prod_rank, "
         "dense_rank() over (partition by d.department_id order by p.product_price )  as prod_dense_rank from departments d inner join  categories c on d.department_id = c.category_department_id "
         " inner join products p on p.product_category_id = c.category_id order by d.department_id ,prod_rank desc  ")

hqlc.sql(
    "select   d.department_id,   p.product_id,   p.product_name,  p.product_price,  rank() over (partition by d.department_id order by p.product_price) as product_price_rank,  "
    " dense_rank() over (partition by d.department_id order by p.product_price) as product_dense_price_rank   from products p   inner join categories c on c.category_id = p.product_category_id  "
    "inner join departments d on c.category_department_id = d.department_id  order by d.department_id, product_price_rank desc, product_dense_price_rank ")


df_cust_10=hqlc.sql("select c.customer_id,c.customer_fname,   count(Distinct(oi.order_item_product_id)) as product_count from customers c inner join orders o on c.customer_id = o.order_customer_id inner join order_items oi  "
         "on o.order_id= oi.order_item_order_id group by c.customer_id, c.customer_fname  order by product_count Desc ,c.customer_id limit 10")

sqlc = SQLContext(sc)
df_cust = hqlc.read.format("jdbc").option("url", "jdbc:mysql://localhost/retail_db").option("user", "root").option("password","cloudera").option("dbtable","customers").option("driver","com.mysql.jdbc.Driver").load()
df_prod = hqlc.read.format("jdbc").option("url", "jdbc:mysql://localhost/retail_db").option("user", "root").option("password","cloudera").option("dbtable","products").option("driver","com.mysql.jdbc.Driver").load()
df_odr_itm = hqlc.read.format("jdbc").option("url", "jdbc:mysql://localhost/retail_db").option("user", "root").option("password","cloudera").option("dbtable","order_items").option("driver","com.mysql.jdbc.Driver").load()
df_odr = hqlc.read.format("jdbc").option("url", "jdbc:mysql://localhost/retail_db").option("user", "root").option("password","cloudera").option("dbtable","orders").option("driver","com.mysql.jdbc.Driver").load()

import pyspark.sql.functions as func
df_cust.join(df_odr,df_cust.customer_id==df_odr.order_customer_id).join(df_odr_itm, df_odr.order_id==df_odr_itm.order_item_order_id).select("customer_id","customer_fname", "order_item_product_id").groupBy("customer_id","customer_fname").agg(
func.countDistinct("order_item_product_id").alias("product_count")).orderBy(func.col("product_count").desc()).show()

prod_less_100= df_rank.filter(func.col("product_price")<100)
df_cust_10.registerTempTable("df_cust_10")

prod_less_100.registerTempTable("prod_less_100")

prod_10 = hqlc.sql(
    "select distinct p.* from products p inner join order_items oi on oi.order_item_product_id = p.product_id inner join orders o on o.order_id = oi.order_item_order_id "
    "inner join df_cust_10 tc on o.order_customer_id = tc.customer_id where p.product_price < 100")

hqlc.sql("create table prod_price_less_100 as select * from prod_less_100")

hqlc.sql(func.concat("create table prod_price_less_50 like", prod_less_100))

prod_less_100.write

prod_10.saveAsTable("/user/hive/warehouse/problem6.db")

