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
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCount5 {

	static HashSet<String> stopwords = new HashSet<String>(Arrays.asList("i", "a", "an", "and", "or", "of", "to",
			"about", "above", "after", "all", "are", "be", "but", "he", "she", "by", "can't", "for", "do", "i",
			"has", "don't", "her", "is", "in", "our", "his", "with", "that", "you", "it", "was", "on", "him",
			"as", "at", "com", "from", "how", "on", "the", "this", "what", "when", "where", "who", "will", "www",
			"not", "only", "other", "off", "me", "your", "my", "we", "so", "no", "up", "until", "hasn't", "which"));

	public static class TokenizerMapper1 
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

	public static class IntSumReducer1 
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

	public static class TokenizerMapper2 
	extends Mapper<Object, Text, IntWritable, Text>{

		private Text word = new Text();
		private Text count = new Text();

		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				count.set(itr.nextToken());	
				int intCount = Integer.parseInt(count.toString());
				context.write(new IntWritable(intCount), word);
			}
		}
	}

	public static class IntSumReducer2 
	extends Reducer<IntWritable, Text, IntWritable, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(IntWritable key, Iterable<Text> values, 
				Context context
				) throws IOException, InterruptedException {	
			int sum = 0;
			for (Text val : values) {
                sum++;
            }
			result.set(sum);
			context.write(key, result);
		}
	}

	public static class ChainJobs extends Configured implements Tool {
		public int run(String[] args) throws Exception {
			Configuration conf = getConf();
			
			// Job 1
			Job job1 = new Job(conf, "word count 1");
			job1.setJarByClass(WordCount5.class);
			job1.setMapperClass(TokenizerMapper1.class);
			job1.setCombinerClass(IntSumReducer1.class);
			job1.setReducerClass(IntSumReducer1.class);
			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(IntWritable.class);
			
			FileInputFormat.addInputPath(job1, new Path(args[0]));
			FileOutputFormat.setOutputPath(job1, new Path(args[1]));
					  
			job1.waitForCompletion(true);
			
			// Job 2
			Job job2 = new Job(conf, "word count 2");
			job2.setJarByClass(WordCount5.class);
			job2.setMapperClass(TokenizerMapper2.class);
			job2.setReducerClass(IntSumReducer2.class);
			job2.setMapOutputKeyClass(IntWritable.class);
			job2.setMapOutputValueClass(Text.class);
			job2.setOutputKeyClass(IntWritable.class);
			job2.setOutputValueClass(IntWritable.class);
			
			FileInputFormat.addInputPath(job2, new Path(args[2]));
			FileOutputFormat.setOutputPath(job2, new Path(args[3]));
			
			return job2.waitForCompletion(true) ? 0 : 1;
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 4) {
			System.err.println("Usage: wordcount5 <in> [<in>...] <out>");
			System.exit(2);
		}

		ToolRunner.run(conf, new ChainJobs(), otherArgs);
	}
}
