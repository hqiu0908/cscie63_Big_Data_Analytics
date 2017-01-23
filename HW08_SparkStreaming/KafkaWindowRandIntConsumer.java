package kafka.streaming;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Arrays;
import java.util.regex.Pattern;

import scala.Tuple2;

import com.google.common.collect.Lists;
import kafka.serializer.StringDecoder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.Durations;

public final class KafkaWindowRandIntConsumer {
	private static final Pattern TAB = Pattern.compile("\t");

	public static void main(String[] args) {
		if (args.length < 2) {
			System.err.println("Usage: KafkaWindowRandIntConsumer <brokers> <topics> [<batch_interval> <window_duration> <sliding_window_duration>]\n" +
					"  <brokers> is a list of one or more Kafka brokers\n" +
					"  <topics> is a list of one or more kafka topics to consume from\n\n");
			System.exit(1);
		}

		String brokers = args[0];
		String topics = args[1];

		int batchIntervalInSeconds = 1;
		int windowDurationInSeconds = 30;
		int slidingWindowDurationInSeconds = 5;

		if (args.length == 5) {
			batchIntervalInSeconds = Integer.parseInt(args[2]);
			windowDurationInSeconds = Integer.parseInt(args[3]);
			slidingWindowDurationInSeconds = Integer.parseInt(args[4]);
		}

		// Create context with a 1 second batch interval
		JavaSparkContext sparkConf = new JavaSparkContext("local[5]", "KafkaWindowRandIntConsumer");
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(batchIntervalInSeconds));

		HashSet<String> topicsSet = new HashSet<String>(Arrays.asList(topics.split(",")));
		HashMap<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put("metadata.broker.list", brokers);
		kafkaParams.put("zookeeper.connect", "localhost:2181");
		kafkaParams.put("group.id", "spark-app");
		System.out.println("Kafka parameters: " + kafkaParams);
		System.out.println("KafkaWindowLogCount parameters: topics=" + topics + 
				"; batchIntervalInSeconds=" + batchIntervalInSeconds + 
				"; windowDurationInSeconds=" + windowDurationInSeconds +
				"; slidingWindowDurationInSeconds=" + slidingWindowDurationInSeconds);

		// Create direct kafka stream with brokers and topics
		JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(
				jssc,
				String.class,
				String.class,
				StringDecoder.class,
				StringDecoder.class,
				kafkaParams,
				topicsSet
				);

		// Get the lines, split them into words
		JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
			@Override
			public String call(Tuple2<String, String> tuple2) {
				System.out.println("processing lines: " + tuple2._2());
				return tuple2._2();
			}
		});

		JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterable<String> call(String x) {
				return Lists.newArrayList(TAB.split(x)[1]);
			}
		});

		// Count each IP in each batch
		JavaPairDStream<String, Integer> pairs = words.mapToPair(
				new PairFunction<String, String, Integer>() {
					@Override public Tuple2<String, Integer> call(String s) {
						return new Tuple2<String, Integer>(s, 1);
					}
				});

		// Reduce function adding two integers, defined separately for clarity
		Function2<Integer, Integer, Integer> reduceFunc = new Function2<Integer, Integer, Integer>() {
			@Override public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		};

		// Reduce last windowDurationInSeconds seconds of data, every slidingWindowDurationInSeconds seconds
		JavaPairDStream<String, Integer> windowedWordCounts = pairs.reduceByKeyAndWindow(
				reduceFunc, Durations.seconds(windowDurationInSeconds), Durations.seconds(slidingWindowDurationInSeconds));

		windowedWordCounts.print();

		// Start the computation
		jssc.start();
		jssc.awaitTermination();
	}
}