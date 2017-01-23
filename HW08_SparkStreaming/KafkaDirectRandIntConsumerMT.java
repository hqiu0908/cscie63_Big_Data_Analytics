package kafka.streaming;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class KafkaDirectRandIntConsumerMT {

	private final ConsumerConnector consumer;
	private final String topic;
	private ExecutorService executor;

	public KafkaDirectRandIntConsumerMT (String topic) {

		Properties props = new Properties();
		props.put("zookeeper.connect", "localhost:2181");
		props.put("group.id", "spark-app");
		props.put("zookeeper.session.timeout.ms", "400");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("auto.commit.interval.ms", "1000");

		consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));

		this.topic = topic;
	}

	public void run(int numThreads) {
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, new Integer(numThreads));

		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
		List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

		// now launch all the threads
		executor = Executors.newFixedThreadPool(numThreads);

		// now create an object to consume the messages
		int threadNumber = 0;
		for (final KafkaStream stream : streams) {
			System.out.println("Create thread " + threadNumber);
			executor.submit(new ConsumerThread(stream, threadNumber));
			threadNumber++;
		}
	}

	public static void main(String[] args) {
		String topic = args[0];
		int threads = Integer.parseInt(args[1]);

		KafkaDirectRandIntConsumerMT example = new KafkaDirectRandIntConsumerMT(topic);
		example.run(threads);
	}
}
