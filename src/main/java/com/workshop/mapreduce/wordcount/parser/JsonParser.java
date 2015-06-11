package com.workshop.mapreduce.wordcount.parser;

import org.json.simple.parser.ParseException;

public abstract class JsonParser {

	public abstract boolean isValid();

	public abstract Object getValue(String key, String line) throws ParseException;
}
