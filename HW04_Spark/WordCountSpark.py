from pyspark import SparkContext, SparkConf

import re


conf = SparkConf().setMaster("local").setAppName("wordCount") 

sc = SparkContext(conf = conf)

lines = sc.textFile("/user/cloudera/ulysis/4300.txt")

counts = lines.flatMap(lambda line: re.sub(r"[^a-zA-Z\d]", " ", str(line)).lower().split()) \
         .map(lambda word: (word, 1)) \
         .reduceByKey(lambda a, b: a + b)
# counts = lines.flatMap(lambda line: re.sub("[.,;:$%&'+/\\*\\#\\-\\?!\\\"\\[\\]\\(\\)\\{\\}_]", "", str(line)).lower().split(" ")) \
#          .map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

counts.sortByKey().saveAsTextFile("wordcounts_python")
