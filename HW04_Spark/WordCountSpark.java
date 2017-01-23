package edu.hu.examples;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class WordCountSpark {
	public static void main(String[] args) throws Exception {

		String inputFile = args[0];
		String outputFile = args[1];

		// Create a Java Spark Context.
		// SparkConf conf = new SparkConf().setAppName("wordCount");
		// JavaSparkContext sc = new JavaSparkContext(conf);
		JavaSparkContext sc = new JavaSparkContext("local", "WordCountJavaApp");

		// Load our input data.
		JavaRDD<String> input = sc.textFile(inputFile);
		
		// Split up into words.
		JavaRDD<String> words = input.flatMap(new FlatMapFunction<String, String>() {
			public Iterable<String> call(String x) {	
				
				// First remove punctuation and transform all words into lower case before split.			
				return Arrays.asList(x.replaceAll("[^a-zA-Z\\d]", " ").toLowerCase().trim().split("\\s+"));

				// Another way to remove punctuations:
				// return Arrays.asList(x.replaceAll("[.,;:$%&'+/\\*\\#\\-\\?!\\\"\\[\\]\\(\\)\\{\\}_]", "").toLowerCase().trim().split(" "));
			
				// Since the count of digits / numbers isn't quite meaningful, we can exclude the digits too.	
				// We can only remove punctuation by using:  x.replaceAll("[^a-zA-Z]", " ")
				
				// We can also extract this step out and put it before flatMap() like this:
				// input = input.map(s -> s.replaceAll("[^a-zA-Z\\d]", " ").toLowerCase());
			}
		});

		// Transform into word and count.
		JavaPairRDD<String, Integer> counts = words.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String x) {
				return new Tuple2(x, 1);
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer x, Integer y) {
				return x + y;
			}
		});

		// Save the word count back out to a text file, causing evaluation.
		counts.sortByKey().saveAsTextFile(outputFile);
	}

}
