package com.iot.data.databeans;

import java.io.Serializable;

public class PTCampaignBean implements Serializable{
	
	private String appSecret;
	private String campaignId;
	private String campaignName;
	private String campaignTitle;
	private long startTime;
	private long endTime;
	private String message;
	private long frequency;
	private String eventName;
	private String eventProperty;
	private String eventPropValue;
	private String compareOp;
	
	//campaign title set get methods
	public String getCampaignTitle() {
		return campaignTitle;
	}
	public void setCampaignTitle(String campaignTitle) {
		this.campaignTitle = campaignTitle;
	}
	
	// appSecret get set methods
	public String getappSecret() {
		return appSecret;
	}
	public void setappSecret(String appSecret) {
		this.appSecret = appSecret;
	}
	
	// campaignId get set methods
	public String getcampaignId() {
		return campaignId;
	}
	public void setcampaignId(String campaignId) {
		this.campaignId = campaignId;
	}
	
	// campaignName get set methods
	public String getcampaignName() {
		return campaignName;
	}
	public void setcampaignName(String campaignName) {
		this.campaignName = campaignName;
	}
	
	// startTime get set methods
	public long getstartTime() {
		return startTime;
	}
	public void setstartTime(long startTime) {
		this.startTime = startTime;
	}
	
	// endTime get set methods
	public long getendTime() {
		return endTime;
	}
	public void setendTime(long endTime) {
		this.endTime = endTime;
	}
	
	// frequency get set methods
	public long getfrequency() {
		return frequency;
	}
	public void setfrequency(long frequency) {
		this.frequency = frequency;
	}
	
	// eventPropValue get set methods
	public String geteventPropValue() {
		return eventPropValue;
	}
	public void seteventPropValue(String eventPropValue) {
		this.eventPropValue = eventPropValue;
	}
	
	// message get set methods
	public String getmessage() {
		return message;
	}
	public void setmessage(String message) {
		this.message = message;
	}
	
	// eventName get set methods
	public String geteventName() {
		return eventName;
	}
	public void seteventName(String eventName) {
		this.eventName = eventName;
	}
	
	// eventProperty get set methods
	public String geteventProperty() {
		return eventProperty;
	}
	public void seteventProperty(String eventProperty) {
		this.eventProperty = eventProperty;
	}
	
	// compareOp get set methods
	public String getcompareOp() {
		return compareOp;
	}
	public void setcompareOp(String compareOp) {
		this.compareOp = compareOp;
	}

}
