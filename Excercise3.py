
import os
os.environ["SPARK_HOME"]= "/usr/lib/spark"
from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext, SQLContext, Row


conf = (SparkConf().setMaster("local[2]").setAppName("excercise3").set("spark.executor.memory","2g"))

sc = SparkContext(conf = conf )

hqlc = HiveContext(sc)

hive_df =  hqlc.read.parquet("/user/hive/warehouse/hivelearn.db/nyse_buct1")

hive_df.show()

### Create hive schema inthe hive database
### to physically create table schema in hive and not just temporarily in for the session of HiveContext
## copy hive-site.xml file from hive conf folder to /usr/lib/spark/conf
# ## after doing that re run the spark session and the schema created will appear in the hive
hqlc.sql("use hivelearn")
hqlc.sql("create table nyse_buct3(exchang VARCHAR(20), symbo VARCHAR(10),date string, open float, high float,low float,close float,volume bigint, adj_close float) \
comment 'new bucketed table' \
row format delimited fields terminated by '\t' \
lines terminated by '\n' \
stored as textfile")

hqlc.sql("load data local inpath '/home/cloudera/Desktop/NYSE_daily.txt' into table nyse_buct3")
hqlc.sql("select * from nyse_buct3 limit 40").show()

hqlc.sql("describe formatted nyse_buct3").collect()
###'YYYY-MM-DD HH:MM:SS' or YYYYMMDDHHMMSS
hqlc.sql("select exchang, symbo, TO_DATE(CAST(UNIX_TIMESTAMP(date, 'YYYY-MM-DD' ) as timestamp)) as StockDate from nyse_buct3 ").show()



