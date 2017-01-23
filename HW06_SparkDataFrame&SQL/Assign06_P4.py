from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, HiveContext
from pyspark.sql.functions import *

conf = SparkConf().setMaster("local").setAppName("my_p4") 
sc = SparkContext(conf = conf)
sqlContext = HiveContext(sc)

# 1. Directly query from Hive table
print "\nOrders with the largest number of order item quantities (Directly query from Hive table)"
sqlContext.sql("SELECT order_item_order_id, sum(order_item_quantity) AS total_order_item_quantity FROM order_items GROUP BY order_item_order_id ORDER BY total_order_item_quantity DESC LIMIT 10").show()

print "\nOrders with the largest number of order items per order (Directly query from Hive table)"
sqlContext.sql("SELECT order_item_order_id, count(order_item_id) AS total_order_items FROM order_items GROUP BY order_item_order_id ORDER BY total_order_items DESC LIMIT 10").show()

# 2. Use Spark DataFrame API
orderItems = sqlContext.sql("select * from order_items")

print "\nOrders with the largest number of order item quantities (use Spark DataFrame API)"
orderItems.groupBy("order_item_order_id").sum("order_item_quantity").orderBy(desc("sum(order_item_quantity)")).show(10)

print "\nOrders with the largest number of order items per order (use Spark DataFrame API)"
orderItems.groupBy("order_item_order_id").count().orderBy(desc("count")).show(10)

orderItemsSorted = orderItems.groupBy("order_item_order_id").count().orderBy(desc("count"))
orderItemsSorted.filter("count = 5").show(10)

# 3. Use Spark Temporary Tables
orderItems.registerTempTable("OrderItems")
orderItemsSorted.registerTempTable("OrderItemsSortedTbl")

print "\nOrders with the largest number of order items per order (Use Spark Temporary Tables)"
sqlContext.sql("SELECT order_item_order_id, count(order_item_id) AS total_order_items FROM order_items GROUP BY order_item_order_id ORDER BY total_order_items DESC LIMIT 10").show()

sqlContext.sql("SELECT * FROM OrderItemsSortedTbl WHERE count = 5").show(10)

