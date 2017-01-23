from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, Row

conf = SparkConf().setMaster("local").setAppName("my_p2") 
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)

emps = sc.textFile("file:///mnt/hgfs/VM_shared/hw06/emps.txt")

fields = emps.map(lambda line: str(line).split(", "))
emps_fields = fields.map(lambda p: (p[0], p[1], p[2]))

print "\nShow the \'emps_fields\' tuple:"
print emps_fields.collect()

employee = emps_fields.map(lambda e : Row(name = e[0], age = int(e[1]), salary = float(e[2])))
print "\nShow the RDD \'employee\':"
print employee.collect()

employeeDf = sqlContext.createDataFrame(employee)
print "\nShow the data frame \'employeeDf\':"
employeeDf.show()

print "\nShow the schema of \'employeeDf\':"
employeeDf.printSchema()

print "\nSelect names of all employees whose salary > 3500:"
employeeDf.filter(employeeDf.salary > 3500).select("name").show()

employeeDf.registerTempTable("employeeTbl")
print "\nShow the results queried from the temporary table:"
sqlContext.sql("SELECT name FROM employeeTbl WHERE salary > 3500").show()

