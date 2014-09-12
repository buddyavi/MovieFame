package com.usc.avi.main;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.usc.avi.mapper.MovieFameMapper;
import com.usc.avi.reducer.MovieFameReducer;

/**
 * Main Class for setting up the map reduce job
 * 
 * @author avi sanadhya
 * 
 */
public class MovieFameMain {
	public static void main(String args[]) throws Exception {

		Configuration conf = new Configuration();
		conf.set("mapred.textoutputformat.separator", "--");
		// Setting location for hdfs file system
		conf.set("fs.default.name", "hdfs://localhost:9000");
		// Defining location for Job tracker
		conf.set("mapreduce.job.tracker", "localhost:9001");

		Job job = new Job(conf);
		job.setJarByClass(MovieFameMain.class);
		// Input file location which contains all ratings given by different
		// users
		FileInputFormat
				.addInputPaths(job, "/user/avisanadhya/datasets/ratings");
		// Location where final output will be stored
		FileOutputFormat.setOutputPath(job, new Path("/MovieLens/output1/"));

		// setting mapper and reducer class in job object
		job.setMapperClass(MovieFameMapper.class);
		job.setReducerClass(MovieFameReducer.class);
		// setting map output Key Value class
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}