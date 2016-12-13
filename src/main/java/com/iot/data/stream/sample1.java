package com.iot.data.stream;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class sample1 {

	public static void main(String[] args) throws ParseException {
		String str = "{type=walk, distance=100, step=20, lat=28.4867876, longt=77.1050471}";
		/*String str1 = str.replaceAll("\\{", "").replaceAll("\\}", "").replaceAll(",", "");
		System.out.println(str1);
		System.out.println("***********");
		String[] arr = str1.split("=");
		for(int i=0; i<arr.length; i++) {
			System.out.println(arr[i]);
		}*/
		
		System.out.println(str.replaceAll("=", "\":\"").replaceAll("[^\\[\\],]+", "\"$0\"").replaceAll(" ", "").replaceAll("\"\\{", "\\{\"").replaceAll("\\}\"", "\\\"}"));
		JSONParser parser = new JSONParser();
		Object confObj = parser.parse(str.replaceAll("=", "\":\"").replaceAll("[^\\[\\],]+", "\"$0\"").replaceAll(" ", "").replaceAll("\"\\{", "\\{\"").replaceAll("\\}\"", "\\\"}"));
		JSONObject jsonObj = (JSONObject) confObj;
		String latitude = (String) jsonObj.get("lat");
		String longitude = (String) jsonObj.get("longt");
		System.out.println(latitude);
		System.out.println(longitude);
		
	}
}
