package kafka.streaming;

import java.util.*;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public final class KafkaDirectRandIntConsumer {

	public static void main(String[] args) {
		if (args.length < 1) {
			System.err.println("Usage: KafkaDirectRandIntConsumer <topic>\n" +
					"  <topic> is the kafka topic to consume from\n\n");
			System.exit(1);
		}

		String topic = args[0];

		Properties props = new Properties();
		props.put("zookeeper.connect", "localhost:2181");
		props.put("group.id", "spark-app");

		ConsumerConnector consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));

		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();   
		topicCountMap.put(topic, 1);

		Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreams = consumer.createMessageStreams(topicCountMap);

		List<KafkaStream<byte[], byte[]>> streams = consumerStreams.get(topic);
		for (final KafkaStream stream : streams) {
			ConsumerIterator<byte[], byte[]> it = stream.iterator();
			while (it.hasNext()) {
				System.out.println("Message from Topic '" + topic + "': " + new String(it.next().message()));
			}
		}

		if (consumer != null) {
			consumer.shutdown();
		} 

	}
}