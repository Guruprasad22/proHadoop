package com.playground;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
/**
 * @author Guruprasad Bobbi
 * class to print all the properties of hadoop stack
 * run as >> hadoop jar pro-hadoop.jar com.playground.ConfigurationImpl
 */
public class ConfigurationImpl extends Configured implements Tool {
	
		public static void main(String[] args) throws Exception {			
			ToolRunner.run(new ConfigurationImpl(), args);
		}

		public int run(String[] arg0) throws Exception {
			Configuration config = new Configuration();
			for(Map.Entry<String, String> record : config) {
				System.out.println(record);
			}
			return 0;
		}	
}