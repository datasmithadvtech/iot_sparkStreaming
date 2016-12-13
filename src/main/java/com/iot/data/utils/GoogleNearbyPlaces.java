package com.iot.data.utils;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.Charset;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class GoogleNearbyPlaces {

	//private static String googlePlacesAPI;
	//private static String latitude, longitude;
	//private static String radius;
	//private static String advertiser;
	private static String apiKey;
	private static String url;
	
	public String makeGooglePlaceURL(String radius, String advertiser, String devLat, String devLongt) {
		String googlePlacesAPI = "https://maps.googleapis.com/maps/api/place/nearbysearch/json?";
		//latitude = "-33.8670522";
		//longitude = "151.1957362";
		//in metres
		//radius = "5000";
		//advertiser = "Starbucks";
		//api key for sahil tyagi's google developer account
		apiKey = "AIzaSyBqiX4MzJ_xBc2SaVRNqVT-R6Cxg75Db8E";
		
		url = googlePlacesAPI + "location=" + devLat + "," + devLongt + "&radius=" + radius + "&name=" + advertiser + "&key=" + apiKey;
		return url;
	}
	
	public String callGooglePlacesAPI(String myURL) {
		System.out.println("Requeted URL:" + myURL);
		StringBuilder sb = new StringBuilder();
		URLConnection urlConn = null;
		InputStreamReader in = null;
		try {
			URL url = new URL(myURL);
			urlConn = url.openConnection();
			if (urlConn != null)
				urlConn.setReadTimeout(60 * 1000);
			if (urlConn != null && urlConn.getInputStream() != null) {
				in = new InputStreamReader(urlConn.getInputStream(), Charset.defaultCharset());
				BufferedReader bufferedReader = new BufferedReader(in);
				if (bufferedReader != null) {
					int cp;
					while ((cp = bufferedReader.read()) != -1) {
						sb.append((char) cp);
					}
					bufferedReader.close();
				}
			}
			in.close();
		} catch (Exception e) {
			throw new RuntimeException("Exception while calling URL:"+ myURL, e);
		}
		
		return sb.toString();
	}
	
	private StringBuilder parseGoogleResponse(String response) throws ParseException {
		System.out.println("*************************************");
		StringBuilder sb = new StringBuilder("[");
		
		JSONParser parser = new JSONParser();
		Object obj = parser.parse(response);
		JSONObject conf = (JSONObject) obj;
		
		obj = parser.parse(conf.get("results").toString());
		JSONArray conf2 = (JSONArray) obj;
		
		for(int i=0; i<conf2.size(); i++) {
			obj = parser.parse(conf2.get(i).toString());
			JSONObject conf3 = (JSONObject) obj;
			
			obj = parser.parse(conf3.get("geometry").toString());
			JSONObject conf4 = (JSONObject) obj;
			System.out.println(conf4.get("location"));
			sb.append(conf4.get("location") + ",");
			
		}
		
		sb = new StringBuilder(sb.substring(0, sb.length() - 1).trim().toString() + "]");
		return sb;
	}
	
	public String parseGoogleResponseObject(String response) throws ParseException {
		System.out.println("*************************************");
		//StringBuilder sb = new StringBuilder("[");
		StringBuilder sb = new StringBuilder();
		
		JSONParser parser = new JSONParser();
		Object obj = parser.parse(response);
		JSONObject conf = (JSONObject) obj;
		
		obj = parser.parse(conf.get("results").toString());
		JSONArray conf2 = (JSONArray) obj;
		
		//for(int i=0; i<conf2.size(); i++) {
			obj = parser.parse(conf2.get(0).toString());
			JSONObject conf3 = (JSONObject) obj;
			
			obj = parser.parse(conf3.get("geometry").toString());
			JSONObject conf4 = (JSONObject) obj;
			//System.out.println(conf4.get("location"));
			sb.append(conf4.get("location"));
			
		//}
		
		//sb = new StringBuilder(sb.substring(0, sb.length() - 1).trim().toString() + "]");
		return sb.toString();
	}
	
	private String sendToPushServer(String campaignname, String message, String appsecret, String deviceid, String location) {
		StringBuilder sb = new StringBuilder();
		sb.append("{\"message\":" + "\"" + message+ "\"," + "\"title\":" + "\"" + campaignname + "\"," + "\"mod\":\"self\"," + "\"rq_param\":" + 
					location + "," + "\"app_secret\":" + "\"" + appsecret + "\"," + "\"device\":" + "\"" + deviceid + "\"}");
		
		return sb.toString();
	}
	
	private void sendToPushServer1(String campaignname, String message, String appsecret, String deviceid, String location) throws ParseException, IOException {
		StringBuilder sb = new StringBuilder();
		
		sb.append("{\"message\":" + "\"" + message+ "\"," + "\"title\":" + "\"" + campaignname + "\"," + "\"mod\":\"self\"," + "\"rq_param\":" + 
					location + "," + "\"app_secret\":" + "\"" + appsecret + "\"," + "\"device\":" + "\"" + deviceid + "\"}");
		
		JSONParser parser = new JSONParser();
		Object obj1 = parser.parse(location);
		JSONObject jsonConfObj = (JSONObject) obj1;
		String latitude = jsonConfObj.get("lat").toString();
		String longitude = jsonConfObj.get("lng").toString();
		
		org.json.simple.JSONObject jsonObj=new org.json.simple.JSONObject();
		org.json.simple.JSONObject rq_param=new org.json.simple.JSONObject();

		rq_param.put("lat", latitude);
		rq_param.put("lng", longitude);
		jsonObj.put("message", message);
		jsonObj.put("title", campaignname);
		jsonObj.put("mod", "self");
		jsonObj.put("rq_param", rq_param);
		jsonObj.put("app_secret", appsecret);
		jsonObj.put("device", deviceid);
		
		String url = "http://"+"138.201.83.148:5074"+"/push-api";
		
		URL pushurl = new URL(url);
		String urlParameters = jsonObj.toString();
		HttpURLConnection con = (HttpURLConnection) pushurl.openConnection();
		con.setRequestMethod("POST");
		System.out.println(jsonObj.toString());

		con.setDoOutput(true);
		DataOutputStream wr = new DataOutputStream(con.getOutputStream());
		wr.writeBytes(urlParameters);
		wr.flush();
		wr.close();
		int responseCode = con.getResponseCode();
		BufferedReader in;
		if(responseCode==200)
			in = new BufferedReader(new InputStreamReader(con.getInputStream()));
		else 
			in = new BufferedReader(new InputStreamReader(con.getErrorStream()));

		String inputLine;
		StringBuffer response=new StringBuffer();
		while ((inputLine = in.readLine()) != null) {
			response.append(inputLine);
		}
		in.close();

		jsonObj.put("responseCode", responseCode);
		jsonObj.put("response", response.toString());
		System.out.println(responseCode +" &" + response.toString());
		
	}
	
	public static void main(String[] args) throws ParseException, IOException {
		/*GoogleNearbyPlaces places = new GoogleNearbyPlaces();
		String url = places.makeGooglePlaceURL();
		String response = places.callGooglePlacesAPI(url);
		String location = places.parseGoogleResponseObject(response);
		
		JSONParser parser = new JSONParser();
		Object obj = parser.parse(location);
		JSONObject jsonConfObj = (JSONObject) obj;
		String latitude = jsonConfObj.get("lat").toString();
		String longitude = jsonConfObj.get("lng").toString();
		System.out.println(location);
		System.out.println(latitude);
		System.out.println(longitude);
		
		//String pushData = places.sendToPushServer("inthings1", "sample campaign", "stepCounter", "dev1234", location);
		places.sendToPushServer1("inthings1", "sample campaign", "stepCounter", "dev1234", location);*/
		
		GoogleNearbyPlaces places = new GoogleNearbyPlaces();
		String googlePlacesURL = places.makeGooglePlaceURL("5000", "Starbucks", "28.4866432", "77.1051734");
		String googlePlacesResponse = places.callGooglePlacesAPI(googlePlacesURL);
		String location = places.parseGoogleResponseObject(googlePlacesResponse);
		
		JSONParser parser = new JSONParser();
		Object obj = parser.parse(location);
		JSONObject jsonConfObj = (JSONObject) obj;
		String ADlatitude = jsonConfObj.get("lat").toString();
		String ADlongitude = jsonConfObj.get("lng").toString();
		System.out.println(ADlatitude + " -- " + ADlongitude);
		
	}
	
}
