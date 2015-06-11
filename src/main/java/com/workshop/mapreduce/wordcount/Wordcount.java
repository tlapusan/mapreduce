package com.workshop.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * This class is responsible for job configuration
 * 
 * @author tudor
 *
 */
public class Wordcount extends Configured implements Tool {

	public static String JOB_NAME = Wordcount.class.getSimpleName() + "_Tudor";

	public static void main(String[] args) {
		try {
			ToolRunner.run(new Wordcount(), args);
		} catch (Exception e) {
			System.err.println("Exception for job " + JOB_NAME + " " + e.getMessage());
			e.printStackTrace();
		}
	}

	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Invalid number of arguments");
			return 1;
		}
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, JOB_NAME);
		job.setJarByClass(Wordcount.class);
		
		// set the mapper class for the job
		job.setMapperClass(WordcountMapper.class);
		
		// set the reducer class for the job
		job.setReducerClass(WordcountReducer.class);
		
		// set the types of key and value for the reduce output
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		// set the input path for the job
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		// set the output path for the job
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// start the job execution
		return job.waitForCompletion(true) ? 0 : 1;
	}
}
