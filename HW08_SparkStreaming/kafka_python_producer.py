from kafka import KafkaProducer
from datetime import datetime
from random import randint
import time

producer = KafkaProducer(bootstrap_servers='localhost:9092')

while True:
	num = randint(1, 10)
	message = "message sent at " + str(datetime.now()) + ":\t" + str(num)
	print message
	producer.send('spark-topic', message)
	time.sleep(1)

print 'Done sending messages'
