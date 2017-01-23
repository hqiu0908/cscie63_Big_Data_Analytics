from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, Row
from pyspark.sql.functions import *
import re

conf = SparkConf().setMaster("local").setAppName("my_p3_part1") 
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)

lines = sc.textFile("input/ebay.csv").map(lambda line: str(line).split(","))
auctions = lines.map(lambda l: Row(auctionid = int(l[0]), bid = float(l[1]), bidtime = float(l[2]), bidder = l[3], bidderrate = float(l[4]), openbid = float(l[5]), price = float(l[6]), item = l[7], daystolive = int(l[8])))
print auctions.take(1)

Auction = sqlContext.createDataFrame(auctions)
Auction.printSchema()

print "\nShow 3 example records: "
Auction.show(3)

# 1. Use Spark DataFrame API
print "\nTotal auctions were held (Use Spark DataFrame API):"
print Auction.select("auctionid").distinct().count()

print "\nTotal number of bids were made per item (Use Spark DataFrame API):"
Auction.groupBy("item").count().show()

print "\nMaximum, minimum and average bid (price) per item (Use Spark DataFrame API):"
Auction.groupBy("item").max("price").show()

Auction.groupBy("item").min("price").show()

Auction.groupBy("item").mean("price").show()

print "\nMaximum, minimum and average number of bids per item (Use Spark DataFrame API):"
AuctionsPerItem = Auction.groupBy("auctionid", "item").count().orderBy(desc("count"))

AuctionsPerItem.groupBy("item").max("count").show()

AuctionsPerItem.groupBy("item").min("count").show()

AuctionsPerItem.groupBy("item").mean("count").show()

print "\nShow 10 bids with price > 100 (Use Spark DataFrame API):"
Auction.filter(Auction.price > 100).show(10)

# 2. Use regular SQL queries
Auction.registerTempTable("AuctionTbl")

print "\nTotal auctions were held (Use SQL):"
sqlContext.sql("SELECT count(DISTINCT auctionid) AS count FROM AuctionTbl").show()

print "\nTotal number of bids were made per item (Use SQL):"
sqlContext.sql("SELECT item, count(*) AS count FROM AuctionTbl GROUP BY item").show()

print "\nMaximum, minimum and average bid (price) per item (Use SQL):"
sqlContext.sql("SELECT item, max(price) AS max, min(price) AS min, avg(price) AS average FROM AuctionTbl GROUP BY item").show()

print "\nMaximum, minimum and average number of bids per item (Use SQL):"
AuctionsPerItem = sqlContext.sql("SELECT auctionid, item, count(bid) AS bids FROM AuctionTbl GROUP BY auctionid, item ORDER BY bids DESC")
AuctionsPerItem.registerTempTable("AuctionsPerItemTbl")
sqlContext.sql("SELECT item, max(bids) AS max, min(bids) AS min, avg(bids) AS average FROM AuctionsPerItemTbl GROUP BY item").show()

print "\nShow 10 bids with price > 100 (Use SQL):"
sqlContext.sql("SELECT * FROM AuctionTbl WHERE price > 100").show(10)

# 3. Write to persistent parquet file
Auction.write.save("auction.parquet", format = "parquet")
AuctionsPerItem.write.save("auctionsPerItem.parquet", format = "parquet")