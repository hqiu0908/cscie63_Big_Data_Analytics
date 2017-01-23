from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, Row
from pyspark.sql.functions import *
import re

conf = SparkConf().setMaster("local").setAppName("my_p3_part2") 
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)

# 1. Read data from Parquet file
parquetFile = sqlContext.read.parquet("auction.parquet")
parquetFileProcessed = sqlContext.read.parquet("auctionsPerItem.parquet")

print "\nMaximum, minimum and average bid (price) per item (Use Spark DataFrame API):"
parquetFile.groupBy("item").max("price").show()

parquetFile.groupBy("item").min("price").show()

parquetFile.groupBy("item").mean("price").show()

# 2. Create SQL like table
parquetFile.registerTempTable("AuctionParq")
parquetFileProcessed.registerTempTable("AuctionsPerItemParq")

print "\nTotal auctions were held (Use SQL):"
sqlContext.sql("SELECT count(DISTINCT auctionid) AS count FROM AuctionParq").show()
 
print "\nTotal number of bids were made per item (Use SQL):"
sqlContext.sql("SELECT item, count(*) AS count FROM AuctionParq GROUP BY item").show()

print "\nMaximum, minimum and average bid (price) per item (Use SQL):"
sqlContext.sql("SELECT item, max(price) AS max, min(price) AS min, avg(price) AS average FROM AuctionParq GROUP BY item").show()

print "\nMaximum, minimum and average number of bids per item (Use SQL):"
sqlContext.sql("SELECT item, max(bids) AS max, min(bids) AS min, avg(bids) AS average FROM AuctionsPerItemParq GROUP BY item").show()

print "\nShow 10 bids with price > 100 (Use SQL):"
sqlContext.sql("SELECT * FROM AuctionParq WHERE price > 100").show(10)
 
