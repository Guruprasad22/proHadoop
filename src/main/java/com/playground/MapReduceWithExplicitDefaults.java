package com.playground;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;


/*
 * Program to illustrate the default values used for running a map reduce job based on explicitly settting the default values into
 * job. Should work just like MapReduceBareBone.java  
 * 
 * 
 */

public class MapReduceWithExplicitDefaults {

	public static void main(String[] args) throws Exception {
		
		if(args.length < 2) {
			throw new Exception("Ussage hadoop jar pro-hadoop.jar com.playground.MapReduceWithExplicitDefaults <input-path> <output-path>");
		}
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "MapReduceWithExplicitDefaults");
		job.setJarByClass(MapReduceWithExplicitDefaults.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setMapperClass(Mapper.class);
		job.setPartitionerClass(HashPartitioner.class);
		job.setReducerClass(Reducer.class);
		
		job.setNumReduceTasks(1);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		
		System.exit(job.waitForCompletion(true)?0:1);
		
		
	}
}
