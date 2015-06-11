package com.workshop.mapreduce.wordcount.sample;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class DecodeText {
	public static void main(String[] args) throws ParseException {
		JSONParser jsonParser = new JSONParser();
		String line = "{\"delete\":{\"status\":{\"id\":389513671082516481,\"id_str\":\"389513671082516481\",\"user_id\":709683929,\"user_id_str\":\"709683929\"},\"timestamp_ms\":\"1432537357422\"}}";
		Object obj = jsonParser.parse(line);
		JSONObject jsonObj = (JSONObject) obj;
		System.out.println(((JSONObject)jsonObj.get("delete")).get("status"));
	}
}
