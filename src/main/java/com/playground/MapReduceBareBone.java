package com.playground;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * this program is to illustrate what happens when we run a job with no custom mapper, reducer and configuration parameters set
 * executed using >> hadoop jar pro-hadoop.jar org.myorg.defaultsettings.MapReduceBareBone /datasets/wordcount.txt /output/mapreducebarebone
 * the framework invokes IdentityMapper which is the default mapper that gets called and writes the 
 * LonWritale byte offset as key and the line intepreted in Text format as value to output file
 * Refer the output from reducer to analyze in detail..
 */

public class MapReduceBareBone {

	public static void main(String[] args) throws Exception {
		
		if(args.length < 2) {
			throw new Exception("Ussage hadoop jar pro-hadoop.jar com.playground.MapReduceBareBone <input-path> <output-path>");
		}
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "MapReduceBareBone");
		job.setJarByClass(MapReduceBareBone.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0])); 
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true)?0:1);		
	}
	
}
