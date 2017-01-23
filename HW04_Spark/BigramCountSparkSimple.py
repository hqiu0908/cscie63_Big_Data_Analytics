from pyspark import SparkContext, SparkConf

import re


conf = SparkConf().setMaster("local").setAppName("wordCount") 

sc = SparkContext(conf = conf)

# lines = sc.textFile("/user/cloudera/ulysis/4300.txt")
lines = sc.textFile("file:///home/cloudera/Downloads/4300.txt")

# The read in lines are split by lines rather than sentences. So the sentences may be split
# over multiple lines. The glom() method is used to create a single entry for each document
# containing the list of all lines, we can then join the lines up, and then resplit them into
# sentences using "." or "!" or "?" as the separator. Using flatMap() so every object in our
# RDD is now a sentence.
# sentences = lines.glom() \
#            .map(lambda x: " ".join(x)) \
#            .flatMap(lambda x: re.split("[.?!]", x))

#bigrams = sentences.map(lambda x: re.sub(r"[^a-zA-Z\d]", " ", str(x)).lower().split()) \
#          .flatMap(lambda x: [ ((x[i], x[i + 1]), 1) for i in range(0, len(x) - 1) ])

# If we do not add the pairs in which the first word is the last word on the line and the second
# word is the first word on the subsequent line.
bigrams = lines.map(lambda x: re.split("[.?!]", str(x))) \
          .map(lambda x: re.sub(r"[^a-zA-Z\d]", " ", str(x)).lower().split()) \
          .flatMap(lambda x: [ ((x[i], x[i + 1]), 1) for i in range(0, len(x) - 1) ])

bigramsFreq = bigrams.reduceByKey(lambda a, b: a + b)

bigramsFreqSort = bigramsFreq.map(lambda x: (x[1], x[0])) \
                  .sortByKey(False) \
                  .map(lambda x: (x[1], x[0]))

print "Total bigrams: %d " % bigramsFreqSort.count()

# Take the first 20 pairs from the sorted list.
# print bigramsFreqSort.take(20)

# Another way for sort:
topTwentyBigrams = bigramsFreq.takeOrdered(20, lambda x : -x[1])

# Fliter out the bigrams contains "heaven"
bigramsWithHeaven = bigramsFreqSort.filter(lambda x: "heaven" in x[0])

bigramsFreqSort.saveAsTextFile("all_bigrams_sorted_simple")

file = open('top_twenty_bigrams_simple', 'w')

output = ""
for x in topTwentyBigrams:
	output += str(x[0]) + ", " + str(x[1]) + "\n"

file.write(output)
file.close()

bigramsWithHeaven.saveAsTextFile("bigrams_with_heaven_simple")



