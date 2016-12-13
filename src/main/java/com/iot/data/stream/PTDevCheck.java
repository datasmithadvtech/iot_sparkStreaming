package com.iot.data.stream;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.iot.data.databeans.PTDeviceBean;
import com.iot.data.utils.GoogleNearbyPlaces;
import com.iot.data.utils.HbaseUtils;

public class PTDevCheck {
	
	private static HbaseUtils dbUtils;
	private static GoogleNearbyPlaces places;
	
	public PTDevCheck() throws IOException, ParseException {
		dbUtils = new HbaseUtils("production_conf.json");
	}
	
	public static void processTPDevs() throws Exception{
		

		System.out.println("------------searching for new devices-------------!");
		List<PTDeviceBean> ptDeviceData = dbUtils.queryTPNewDevs();
		for (PTDeviceBean currPTDev : ptDeviceData) {
			// irrespective of what happens, update is_new_device to false
			boolean checkSetDevNewFalse = dbUtils.setNewPTDevFalse(currPTDev);
			if (checkSetDevNewFalse){
				System.out.println("Succesfully set New device to false!");
			}else{
				System.out.println("FAILED TO SETTTTT New device to false!");
			}
			
			String currDevId = currPTDev.getdeviceId();
			String currCampaignId = currPTDev.getcampaignId();
			String currAppSec = currPTDev.getappSecret();
			String currCampaignMessage = currPTDev.getcampaignMessage();
			//String currEventName = currPTDev.geteventName();
			String currCampaignTitle = currPTDev.getCampaignTitle();
			String currEventProps = currPTDev.geteventProps();
			System.out.println("TP appsecret & campaign-->" + currAppSec +", " + currCampaignId);
			System.out.println("TP deviceId-->" + currDevId);
			System.out.println("TP device rowkey-->" + currPTDev.getdevInpRowKey());
			// check the freq of campaign
			// if it is set to zero always send push and update respective columns
			System.out.println("curr event props string-->" + currEventProps);
			
			JSONParser parser = new JSONParser();
			
			Object confObj = parser.parse(currEventProps.replaceAll("=", "\":\"").replaceAll("[^\\[\\],]+", "\"$0\"").replaceAll(" ", "")
											.replaceAll("\"\\{", "\\{\"").replaceAll("\\}\"", "\\\"}"));
			
			//Object confObj = parser.parse(currEventProps);
			JSONObject jsonObj = (JSONObject) confObj;
			String latitude = (String) jsonObj.get("lat");
			String longitude = (String) jsonObj.get("longt");
			
			if (currPTDev.getcampaignFreq() == 0L){
				System.out.println("Campaign freq is 0, unlimited pushes");
				
				String googlePlacesURL = places.makeGooglePlaceURL("5000", "Starbucks", latitude, longitude);
				String googlePlacesResponse = places.callGooglePlacesAPI(googlePlacesURL);
				String location = places.parseGoogleResponseObject(googlePlacesResponse);
				
				parser = new JSONParser();
				Object obj = parser.parse(location);
				JSONObject jsonConfObj = (JSONObject) obj;
				String ADlatitude = jsonConfObj.get("lat").toString();
				String ADlongitude = jsonConfObj.get("lng").toString();
				
				System.out.println("$$$$$$$$$$$$ data sent to push server: " + currCampaignId + "--" + currCampaignMessage + "--" + currAppSec + "--" + 
									currDevId + "--" + ADlatitude + "--" + ADlongitude);
				
				sendToPushServer(currCampaignTitle, currCampaignMessage, currAppSec, currDevId, ADlatitude, ADlongitude);
				// function to send push
				// update last push sent time and num push sent
				System.out.println("rowkey of device selected-->" + currPTDev.getdevInpRowKey());
				boolean lastPushTimeUpdateCheck = dbUtils.updateLastPushSentTime(currPTDev);
				System.out.println("last push sent update succes-->" + lastPushTimeUpdateCheck);
				boolean chkIncrementNumPushSent = dbUtils.incrementNumPushSent(currPTDev);
				System.out.println("num push sent increment succes-->" + chkIncrementNumPushSent);
			}
			// if campaign freq is limited and num of pushes for this device is less than
			// than the campaign freq set
			if (currPTDev.getcampaignFreq() != 0L && 
					currPTDev.getnumSentPush() < currPTDev.getcampaignFreq() ){
				
				System.out.println("Campaign freq is limited-->" + currPTDev.getcampaignFreq());
				System.out.println("push sent for device until now-->" + currPTDev.getnumSentPush());
				
				String googlePlacesURL = places.makeGooglePlaceURL("5000", "Starbucks", latitude, longitude);
				String googlePlacesResponse = places.callGooglePlacesAPI(googlePlacesURL);
				String location = places.parseGoogleResponseObject(googlePlacesResponse);
				
				parser = new JSONParser();
				Object obj = parser.parse(location);
				JSONObject jsonConfObj = (JSONObject) obj;
				String ADlatitude = jsonConfObj.get("lat").toString();
				String ADlongitude = jsonConfObj.get("lng").toString();
				
				System.out.println("$$$$$$$$$$$ data sent to push server: " + currCampaignId + "--" + currCampaignMessage + "--" + currAppSec + "--" + 
									currDevId + "--" + ADlatitude + "--" + ADlongitude);
				// function to send push
				// goes here
				sendToPushServer(currCampaignTitle, currCampaignMessage, currAppSec, currDevId, ADlatitude, ADlongitude);
				// function to send push
				// update last push sent time and num push sent
				System.out.println("rowkey of device selected-->" + currPTDev.getdevInpRowKey());
				boolean lastPushTimeUpdateCheck = dbUtils.updateLastPushSentTime(currPTDev);
				System.out.println("last push sent update succes-->" + lastPushTimeUpdateCheck);
				boolean chkIncrementNumPushSent = dbUtils.incrementNumPushSent(currPTDev);
				System.out.println("num push sent increment succes-->" + chkIncrementNumPushSent);
			}
			// if campaign freq is limited and num of pushes for this device is greater than
			// than the campaign freq set. In this case, check if the last updated push is more than
			// 24 hours back. In such a case, reset the number to zero!
			if ( currPTDev.getcampaignFreq() != 0L && 
					currPTDev.getnumSentPush() >= currPTDev.getcampaignFreq() ){
				
				System.out.println("Campaign freq is limited-->" + currPTDev.getcampaignFreq());
				System.out.println("push sent for device until now-->" + currPTDev.getnumSentPush());
				
				
				long timeDiff = (System.currentTimeMillis() - 
						currPTDev.getlastPushSentTime())/(60*60 * 1000);
				
				if (timeDiff >= 24L){
					System.out.println("More than 24 hours since last push! resetting!");
					boolean checkResetPush = dbUtils.resetNumPushCnt(currPTDev);
					System.out.println("num push reset succes-->" + checkResetPush);
					
					String googlePlacesURL = places.makeGooglePlaceURL("5000", "Starbucks", latitude, longitude);
					String googlePlacesResponse = places.callGooglePlacesAPI(googlePlacesURL);
					String location = places.parseGoogleResponseObject(googlePlacesResponse);
					
					parser = new JSONParser();
					Object obj = parser.parse(location);
					JSONObject jsonConfObj = (JSONObject) obj;
					String ADlatitude = jsonConfObj.get("lat").toString();
					String ADlongitude = jsonConfObj.get("lng").toString();
					
					System.out.println("$$$$$$$$$$$ data sent to push server: " + currCampaignId + "--" + currCampaignMessage + "--" + currAppSec + "--" + 
										currDevId + "--" + ADlatitude + "--" + ADlongitude);
					// function to send push
					// goes here
					sendToPushServer(currCampaignTitle, currCampaignMessage, currAppSec, currDevId, ADlatitude, ADlongitude);
					// function to send push
					// update last push sent time and num push sent
					System.out.println("rowkey of device selected-->" + currPTDev.getdevInpRowKey());
					boolean lastPushTimeUpdateCheck = dbUtils.updateLastPushSentTime(currPTDev);
					System.out.println("last push sent update succes-->" + lastPushTimeUpdateCheck);
					boolean chkIncrementNumPushSent = dbUtils.incrementNumPushSent(currPTDev);
					System.out.println("num push sent increment succes-->" + chkIncrementNumPushSent);
				}else{
					System.out.println("PUSH LIMIT REACHED! WAITING FOR A RESET");
				}
				
			}
			
		}
	}
	
	private static void sendToPushServer(String campaigntitle, String message, String appsecret, String deviceid, String latitude, String longitude) 
										throws ParseException, IOException {
		
		org.json.simple.JSONObject jsonObj=new org.json.simple.JSONObject();
		org.json.simple.JSONObject rq_param=new org.json.simple.JSONObject();

		rq_param.put("lat", latitude);
		rq_param.put("lng", longitude);
		jsonObj.put("message", message);
		jsonObj.put("title", campaigntitle);
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
	
	public static void main(String[] args) throws Exception {
		PTDevCheck ptObj = new PTDevCheck();
		places = new GoogleNearbyPlaces();
		
		// run the update function in a while loop forever
		while(true){
			try {
				
				ptObj.processTPDevs();
				Thread.sleep(10000);
			}catch (Exception e) {  
	            e.printStackTrace();  
	        }
		}
	}

}
