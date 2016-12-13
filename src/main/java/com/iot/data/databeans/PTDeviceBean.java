package com.iot.data.databeans;

import java.io.Serializable;

public class PTDeviceBean implements Serializable {
	
	private String appSecret;
	private String devInpRowKey;
	private String campaignId;
	private String campaignName;
	private String campaignTitle;
	private String campaignMessage;
	private String deviceId;
	private String eventName;
	private String eventProps;
	private String eventTime;
	private String serverTime;
	private long lastPushSentTime;
	private long numSentPush;
	private long campaignFreq;
	private long campaignEndTime;
	
	public String getCampaignTitle() {
		return campaignTitle;
	}
	public void setCampaignTitle(String campaignTitle) {
		this.campaignTitle = campaignTitle;
	}
	
	public String getappSecret() {
		return appSecret;
	}
	public void setappSecret(String appSecret) {
		this.appSecret = appSecret;
	}
	
	public String getdevInpRowKey() {
		return devInpRowKey;
	}
	public void setdevInpRowKey(String devInpRowKey) {
		this.devInpRowKey = devInpRowKey;
	}
	
	public String getcampaignId() {
		return campaignId;
	}
	public void setcampaignId(String campaignId) {
		this.campaignId = campaignId;
	}
	
	public String getcampaignName() {
		return campaignName;
	}
	public void setcampaignName(String campaignName) {
		this.campaignName = campaignName;
	}
	
	public String getcampaignMessage() {
		return campaignMessage;
	}
	public void setcampaignMessage(String campaignMessage) {
		this.campaignMessage = campaignMessage;
	}
	
	public String getdeviceId() {
		return deviceId;
	}
	public void setdeviceId(String deviceId) {
		this.deviceId = deviceId;
	}
	
	
	public String geteventName() {
		return eventName;
	}
	public void seteventName(String eventName) {
		this.eventName = eventName;
	}
	
	
	public String geteventProps() {
		return eventProps;
	}
	public void seteventProps(String eventProps) {
		this.eventProps = eventProps;
	}
	
	public String geteventTime() {
		return eventTime;
	}
	public void seteventTime(String eventTime) {
		this.eventTime = eventTime;
	}	
	
	public String getserverTime() {
		return serverTime;
	}
	public void setserverTime(String serverTime) {
		this.serverTime = serverTime;
	}		
	
	
	public long getlastPushSentTime() {
		return lastPushSentTime;
	}
	public void setlastPushSentTime(long lastPushSentTime) {
		this.lastPushSentTime = lastPushSentTime;
	}
	
	public long getnumSentPush() {
		return numSentPush;
	}
	public void setnumSentPush(long numSentPush) {
		this.numSentPush = numSentPush;
	}
	
	public long getcampaignFreq() {
		return campaignFreq;
	}
	public void setcampaignFreq(long campaignFreq) {
		this.campaignFreq = campaignFreq;
	}
	
	public long getcampaignEndTime() {
		return campaignEndTime;
	}
	public void setcampaignEndTime(long campaignEndTime) {
		this.campaignEndTime = campaignEndTime;
	}

}
