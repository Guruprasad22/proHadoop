package com.playground;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;


public class FileOperations {
	
	public static void main(String[] args) throws IOException {
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		/*
		 * create a file
		 */
		if(!fs.exists(new Path("testFile.txt"))) {
			boolean created = fs.createNewFile(new Path("testFile.txt"));
			if(created) {
				System.out.println("File got created");
			} else {
				System.out.println("Could not create file.. ");
			}
		}  
		
		/*
		 * list all files 
		 */
		RemoteIterator<LocatedFileStatus> fileIterator = fs.listFiles(new Path("temp"), true);
		System.out.println("Files under the location are .. ");
		while(fileIterator.hasNext()) {
			System.out.println(fileIterator.next().getPath());
		}
	}
}
