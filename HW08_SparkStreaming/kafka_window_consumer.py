from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: direct_kafka_wordcount.py <broker_list> <topic>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="PythonStreamingDirectKafkaWordCount")
    ssc = StreamingContext(sc, 1)
    ssc.checkpoint("file:////home/cloudera/Documents/hw08/checkpoint")

    brokers, topic = sys.argv[1:]
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
    lines = kvs.map(lambda x: x[1].split("\t")[1] + " total")
    lines.pprint()
    
    counts = lines.flatMap(lambda line: str(line).split(" ")) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a+b) \
        .reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y: x - y, 30, 5)
    counts.pprint()

    ssc.start()
    ssc.awaitTermination()
