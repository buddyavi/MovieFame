package com.usc.avi.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * Mapper class which extracts the userId and ratings from the text file
 * @author avi sanadhya
 *
 */
public class MovieFameMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	String str[] = null;

	public void map(
			LongWritable key,
			Text value,
			org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
        //File separator is :
		str = value.toString().split(":");

		context.write(new Text(str[1]),
				new IntWritable(Integer.parseInt(str[2])));
	}
}