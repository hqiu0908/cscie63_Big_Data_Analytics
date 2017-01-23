from kafka import KafkaConsumer

consumer = KafkaConsumer('spark-topic')
for msg in consumer:
	print msg
