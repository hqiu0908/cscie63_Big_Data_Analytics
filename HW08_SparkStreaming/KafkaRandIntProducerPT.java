package kafka.streaming;

import java.util.*;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaRandIntProducerPT {
	public static void main(String[] args) {
		if (args.length < 2) {
			System.err.println("Usage: KafkaRandIntProducer <brokers> <topic> [<events>]\n" +
					"  <brokers> is a list of one or more Kafka brokers\n" +
					"  <topic> is the kafka topic to produce to \n" +
					"  <events> is the total number of events to send \n");
			System.exit(1);
		}

		String brokers = args[0];
		String topic = args[1];

		/*
	    long events = 50000;
	    if (args.length == 3) {
	    	events = Long.parseLong(args[2]);
	    }
		 */

		Random rnd = new Random();

		Properties props = new Properties();
		props.put("metadata.broker.list", brokers);
		props.put("partitioner.class", "kafka.streaming.SimplePartitioner");
		props.put("serializer.class", "kafka.serializer.StringEncoder");

		ProducerConfig config = new ProducerConfig(props);

		Producer<String, String> producer = new Producer<String, String>(config);

		// for (long nEvents = 0; nEvents < events; nEvents++) {
		while (true) {
			// Generate a random number between 1 to 10
			int number = rnd.nextInt(10) + 1;

			// Get current time stamp			
			Date time = new Date(System.currentTimeMillis());

			// Compose the message
			String msg = "Message sent at " + time.toString() + ":\t" + number;

			// Send the message to topic
			KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, Integer.toString(number), msg);
			producer.send(data);

			System.out.println(msg);

			try {
				// Sleep for 1 second
				Thread.sleep(1000);
			} catch(InterruptedException ex) {
				Thread.currentThread().interrupt();
			}
		}

		/*
		if (producer != null) {
			producer.close();
		}
		 */
	}
}