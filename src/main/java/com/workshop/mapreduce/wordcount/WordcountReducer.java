package com.workshop.mapreduce.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * This class implements the reduce phase from the wordcount job
 * 
 * @author tudor
 *
 */
public class WordcountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	/**
	 * Count the frequency of each work
	 */
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		int sum = 0;
		for (IntWritable value : values) {
			sum += value.get();
		}
		context.write(key, new IntWritable(sum));
	}
}
