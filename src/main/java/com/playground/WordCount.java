package com.playground;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

		public static class WordCountMap extends Mapper<LongWritable,Text,Text,IntWritable> {
			
			private Text word = new Text();
			private static final IntWritable one = new IntWritable(1);
			
			@Override
			public void map(LongWritable byteOffset,Text line,Context context) throws IOException, InterruptedException {
				
				StringTokenizer tokenizer = new StringTokenizer(line.toString());
				while(tokenizer.hasMoreTokens()) {
					word.set(tokenizer.nextToken());
					context.write(word,	one);
				}				
			}		
		}
		
		public static class WordCountReduce extends Reducer<Text,IntWritable,Text,IntWritable> {
			
			@Override
			public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
				int count = 0;
				Iterator<IntWritable> val = values.iterator();
				while(val.hasNext()) {
					count = count + val.next().get();
				}
				context.write(key, new IntWritable(count));
			}
		}
		
		public static void main(String[] args) throws Exception {
			
			if(args.length != 2) {
				throw new Exception("Ussage hadoop jar pro-hadoop.jar com.playground.WordCount <input-path> <output-path>");
			}
			
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, "word count");
			job.setJarByClass(WordCount.class);
			
			// input
			job.setInputFormatClass(TextInputFormat.class);
			TextInputFormat.addInputPath(job, new Path(args[0]));			
			// mapper
			job.setMapperClass(WordCountMap.class);			
			// reducer
			job.setReducerClass(WordCountReduce.class);
			// output
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			
			boolean returnVal = job.waitForCompletion(true);
			System.exit(returnVal?0:1);			
		}
}
