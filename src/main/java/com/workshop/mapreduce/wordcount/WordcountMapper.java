package com.workshop.mapreduce.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.simple.parser.ParseException;

import com.workshop.mapreduce.wordcount.parser.JsonParser;
import com.workshop.mapreduce.wordcount.parser.WCJsonParser;

/**
 * This class implements the map phase from the wordcount job
 * 
 * @author tudor
 *
 */
public class WordcountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private Text newKey;
	private IntWritable newValue;
	private JsonParser parser;

	@Override
	protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		newKey = new Text();
		newValue = new IntWritable(1);
		parser = new WCJsonParser();
	}

	/**
	 * Split the value into words after space delimiter and prepare intermediary
	 * keys make of (key, value), where the key is the word and value is 1
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		if (line.isEmpty()) {
			return;
		}
		try {
			String text = (String) parser.getValue("text", line);
			if (text != null) {
				String[] words = text.split(" ");
				for (String word : words) {
					if (word.startsWith("#")) {
						newKey.set(word);
						context.write(newKey, newValue);
					}
				}
			}
		} catch (ParseException e) {
			System.err.println("Error parsing e, for line : " + e + ", " + line);
			e.printStackTrace();
		}
	}
}
