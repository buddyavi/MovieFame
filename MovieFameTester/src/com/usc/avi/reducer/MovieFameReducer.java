package com.usc.avi.reducer;

import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer Class which calculates the Maximum , Minimum and Average Rating for a
 * particular movie
 * 
 * @author avi sanadhya
 * 
 */
public class MovieFameReducer extends Reducer<Text, IntWritable, Text, Text> {

	public void reduce(
			Text key,
			Iterable<IntWritable> value,
			org.apache.hadoop.mapreduce.Reducer<Text, IntWritable, Text, Text>.Context out)
			throws java.io.IOException, InterruptedException {
		int count = 1;
		float sum,max,min,temp,avg = 0;
		Iterator<IntWritable> rating = value.iterator();
		sum = max = min = rating.next().get();
		// Calculating Max,Min and Avg for a particular movie
		while (rating.hasNext()) {
			temp = rating.next().get();
			count++;
			sum += temp;
			if (temp > max) {
				max = temp;
			}
			if (temp < min) {
				min = temp;
			}
		}
		avg = (sum / count);
		Text t = new Text("Max=" + max + " Min=" + min + " Avg=" + avg);

		out.write(key, t);

	}

}