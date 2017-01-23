from pyspark import SparkContext, SparkConf
import re

conf = SparkConf().setMaster("local").setAppName("my_p1")
sc = SparkContext(conf = conf)

paragraphA = sc.textFile("file:///mnt/hgfs/VM_shared/hw06/AlphaGoA.txt")
wordsA = paragraphA.flatMap(lambda line: re.sub(r"[^a-zA-Z\d]", " ", str(line)).lower().split())
wordsA.count()
print "\nTotal words in A: %d " % wordsA.count()
print wordsA.take(20)

paragraphB = sc.textFile("file:///mnt/hgfs/VM_shared/hw06/AlphaGoB.txt")
wordsB = paragraphB.flatMap(lambda line: re.sub(r"[^a-zA-Z\d]", " ", str(line)).lower().split())
wordsB.count()
print "\nTotal words in B: %d " % wordsB.count()
print wordsB.take(20)

print "\nList first 10 words in A:" 
print wordsA.take(10)
print "\nList first 10 words in B:" 
print wordsB.take(10)

distinctA = wordsA.distinct()
distinctA.count()
print "\nTotal unique words in A: %d " % distinctA.count()
print "\nList first 10 unique words in A:"
print distinctA.take(10)
distinctA.collect()

distinctB = wordsB.distinct()
distinctB.count()
print "\nTotal unique words in B: %d " % distinctB.count()
print "\nList first 10 unique words in B:"
print distinctB.take(10)
distinctB.collect()

wordsOnlyInA = distinctA.subtract(distinctB)
wordsOnlyInA.count()
print "\nTotal words only in A: %d " % wordsOnlyInA.count()
wordsOnlyInA.take(10)
print wordsOnlyInA.collect()

intersection = distinctA.intersection(distinctB)
intersection.count()
print "\nTotal words in both A and B: %d " % intersection.count()
print intersection.collect()