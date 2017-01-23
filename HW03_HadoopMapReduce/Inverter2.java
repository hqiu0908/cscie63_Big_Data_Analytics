package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Inverter2 {

	public static class TokenizerMapper 
	extends Mapper<Object, Text, Text, Text> {

		private Text str = new Text();
		private Text citing = new Text();
		private Text cited = new Text();

		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				str.set(itr.nextToken());
				String[] args = str.toString().split(",");			
				citing.set(args[0]);
				cited.set(args[1]);
				context.write(cited, citing);
			}
		}
	}

	public static class IntSumReducer 
	extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, 
				Context context
				) throws IOException, InterruptedException {
			String csv = "";
			for (Text value : values) {
				if (csv.length() > 0) csv += ",";
				csv += value.toString();
			}
			context.write(key, new Text(csv));
		}
	}

	public static void main(String[] args) throws Exception { 
	    Configuration conf = new Configuration();
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length < 2) {
	      System.err.println("Usage: Inverter2 <in> [<in>...] <out>");
	      System.exit(2);
	    }
	    Job job = new Job(conf, "Inverter 2");
	    job.setJarByClass(Inverter2.class);
	    job.setMapperClass(TokenizerMapper.class);
	    job.setReducerClass(IntSumReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    for (int i = 0; i < otherArgs.length - 1; ++i) {
	      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
	    }
	    FileOutputFormat.setOutputPath(job,
	      new Path(otherArgs[otherArgs.length - 1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
