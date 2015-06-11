package com.workshop.mapreduce.wordcount.parser;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class WCJsonParser extends JsonParser {

	private JSONParser jsonParser = new JSONParser();

	@Override
	public boolean isValid() {
		return false;
	}

	@Override
	public Object getValue(String key, String line) throws ParseException {
		JSONObject jsonObj = (JSONObject) jsonParser.parse(line);
		if (jsonObj != null && jsonObj.containsKey(key)) {
			return jsonObj.get(key);
		} else {
			return null;
		}
	}

}
