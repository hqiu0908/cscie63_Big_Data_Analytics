/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount2 {

	static HashSet<String> stopwords = new HashSet<String>(Arrays.asList("i", "a", "an", "and", "or", "of", "to",
			"about", "above", "after", "all", "are", "be", "but", "he", "she", "by", "can't", "for", "do", "i",
			"has", "don't", "her", "is", "in", "our", "his", "with", "that", "you", "it", "was", "on", "him",
			"as", "at", "com", "from", "how", "on", "the", "this", "what", "when", "where", "who", "will", "www",
			"not", "only", "other", "off", "me", "your", "my", "we", "so", "no", "up", "until", "hasn't", "which"));

	public static class TokenizerMapper 
	extends Mapper<Object, Text, Text, IntWritable>{

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				
				// Exclude the stop words and special characters
				word = processWord(word);
				
				// Don't write "stopwords" and "unknown" into results.
				if (word.toString().equals("stopwords") || word.toString().equals("unknown")) {
					continue;
				}
				
				context.write(word, one);
			}
		}
	}

	public static Text processWord(Text word) {
		Text newWord;

		String str = word.toString().toLowerCase();
		
		String delimiters = "[.,;:$%&'+/\\*\\#\\-\\?!\\\"\\[\\]\\(\\)\\{\\}\\d_]+";
		String[] args = str.split(delimiters);
		
		String newStr = null;
		if (args.length > 0) {
			if (! args[0].isEmpty()) {
				// Get the first valid word
				newStr = args[0];
			} else {
				// If a word starts with a special character, the first word is empty
				// after parsing. We should choose the second word.
				newStr = args[1];
			}
			// System.out.println(str + "\t" + newStr);
			
			// Exclude the stop words.
			if (! stopwords.contains(newStr)) {
				newWord = new Text(newStr);
			} else {
				newWord = new Text("stopwords");
			}
		} else {
			newWord = new Text("unknown");
		}

		return newWord;
	}

	public static class IntSumReducer 
	extends Reducer<Text,IntWritable,Text,IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, 
				Context context
				) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: wordcount2 <in> [<in>...] <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "word count");
		job.setJarByClass(WordCount2.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job,
				new Path(otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
