package com.playground;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
 * 
 * @author Guruprasad Bobbi
 * Testing the NLineinput format
 * Using the default mapper and reducer so given a text file input with 500 lines we will end up with 500/5 = 100 splits 
 * executed across mappers
 *
 */
public class WordCountNLine {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "N Line input job..");
		
		job.setJarByClass(WordCountNLine.class);
		
		job.setInputFormatClass(NLineInputFormat.class); // this is important
		
		NLineInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap",5);
		
		job.setMapperClass(Mapper.class); 
		job.setReducerClass(Reducer.class);
		
		int returnCode = job.waitForCompletion(true)?0:1;
		System.exit(returnCode);
	
	}
}
