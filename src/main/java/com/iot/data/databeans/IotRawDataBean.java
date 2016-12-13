package com.iot.data.databeans;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class IotRawDataBean implements Serializable{
	// uuid for packet
	private String iotRawDataUUID;
	private String iotRawDataAppSecret;
	private String iotRawDataDeviceId;
	private String iotRawDataLibrary;
	private String iotRawDataLibraryVersion;
	private String iotRawDataServerIp;
	private String iotRawDataServerTime;
	private Map<String,String> iotRawDataUniqueIdAction;
//	private List iotRawDataPacket;
	// install ref data
	private boolean hasInstallReferrerData;
	private String iotRawDataInsRefAction;
	private String iotRawDataInsRefAppSessionId;
	private String iotRawDataInsRefRefString;
	private String iotRawDataInsRefTimeStamp;
	private String iotRawDataInsRefUserId;
	// stop session data
	private boolean hasStopSessionData;
	private String iotRawDataStopSessionUserId;
	private String iotRawDataStopSessionAppSessionId;
	private String iotRawDataStopSessionDuration;
	private String iotRawDataStopSessionSessionId;
	private String iotRawDataStopSessionTimeStamp;
	private String iotRawDataStopSessionAction;
	// push action data
	private boolean hasPUSHACTIONData;
	private String iotRawDataPUSHACTIONUserId;
	private String iotRawDataPUSHACTIONAppSessionId;
	private String iotRawDataPUSHACTIONSessionId;
	private String iotRawDataPUSHACTIONTimeStamp;
	private String iotRawDataPUSHACTIONAction;
	private String iotRawDataPUSHACTIONPUSHKEY;
	// advertising action data
	private boolean hasAdvertisingIdActionData;
	private String iotRawDataAdvertisingIdActionAppSessionId;
	private boolean iotRawDataAdvertisingIdActionADVERTISINGIDOPTOUT;
	private String iotRawDataAdvertisingIdActionSessionId;
	private String iotRawDataAdvertisingIdActionTimeStamp;
	private String iotRawDataAdvertisingIdActionAction;
	private String iotRawDataAdvertisingIdActionADVERTISINGIDKEY;
	// start session data
	private boolean hasStartSessionData;
	private String iotRawDataStartSessionScreenName;
	private String iotRawDataStartSessionAppSessionId;
	private String iotRawDataStartSessionSessionId;
	private String iotRawDataStartSessionTimeStamp;
	private String iotRawDataStartSessionAction;
	
	// iotRawDataUUID get set methods
	public String getiotRawDataUUID() {
		return iotRawDataUUID;
	}
	public void setiotRawDataUUID(String iotRawDataUUID) {
		this.iotRawDataUUID = iotRawDataUUID;
	}
	
	// iotRawDataAppSecret get set methods
	public String getiotRawDataAppSecret() {
		return iotRawDataAppSecret;
	}
	public void setiotRawDataAppSecret(String iotRawDataAppSecret) {
		this.iotRawDataAppSecret = iotRawDataAppSecret;
	}
	
	// iotRawDataDeviceId get set methods
	public String getiotRawDataDeviceId() {
		return iotRawDataDeviceId;
	}
	public void setiotRawDataDeviceId(String iotRawDataDeviceId) {
		this.iotRawDataDeviceId = iotRawDataDeviceId;
	}
	
	// iotRawDataLibrary get set methods
	public String getiotRawDataLibrary() {
		return iotRawDataLibrary;
	}
	public void setiotRawDataLibrary(String iotRawDataLibrary) {
		this.iotRawDataLibrary = iotRawDataLibrary;
	}
	
	// iotRawDataLibraryVersion get set methods
	public String getiotRawDataLibraryVersion() {
		return iotRawDataLibraryVersion;
	}
	public void setiotRawDataLibraryVersion(String iotRawDataLibraryVersion) {
		this.iotRawDataLibraryVersion = iotRawDataLibraryVersion;
	}
	
	// iotRawDataServerIp get set methods
	public String getiotRawDataServerIp() {
		return iotRawDataServerIp;
	}
	public void setiotRawDataServerIp(String iotRawDataServerIp) {
		this.iotRawDataServerIp = iotRawDataServerIp;
	}
	
	// iotRawDataServerTime get set methods
	public String getiotRawDataServerTime() {
		return iotRawDataServerTime;
	}
	public void setiotRawDataServerTime(String iotRawDataServerTime) {
		this.iotRawDataServerTime = iotRawDataServerTime;
	}
	
	// iotRawDataUniqueIdAction get set methods
	public Map<String,String> getiotRawDataUniqueIdAction() {
		return iotRawDataUniqueIdAction;
	}
	public void setiotRawDataUniqueIdAction(Map<String,String> iotRawDataUniqueIdAction) {
		this.iotRawDataUniqueIdAction = iotRawDataUniqueIdAction;
	}
	
	// iotRawDataPacket get set methods
//	public List getiotRawDataPacket() {
//		return iotRawDataPacket;
//	}
//	public void setiotRawDataPacket(List iotRawDataPacket) {
//		this.iotRawDataPacket = iotRawDataPacket;
//	}
	
	// hasInstallReferrerData get set methods
	public boolean gethasInstallReferrerData() {
		return hasInstallReferrerData;
	}
	public void sethasInstallReferrerData(boolean hasInstallReferrerData) {
		this.hasInstallReferrerData = hasInstallReferrerData;
	}
	
	// iotRawDataInsRefAction get set methods
	public String getiotRawDataInsRefAction() {
		return iotRawDataInsRefAction;
	}
	public void setiotRawDataInsRefAction(String iotRawDataInsRefAction) {
		this.iotRawDataInsRefAction = iotRawDataInsRefAction;
	}
	
	// iotRawDataInsRefAppSessionId get set methods
	public String getiotRawDataInsRefAppSessionId() {
		return iotRawDataInsRefAppSessionId;
	}
	public void setiotRawDataInsRefAppSessionId(String iotRawDataInsRefAppSessionId) {
		this.iotRawDataInsRefAppSessionId = iotRawDataInsRefAppSessionId;
	}
	
	// iotRawDataInsRefRefString get set methods
	public String getiotRawDataInsRefRefString() {
		return iotRawDataInsRefRefString;
	}
	public void setiotRawDataInsRefRefString(String iotRawDataInsRefRefString) {
		this.iotRawDataInsRefRefString = iotRawDataInsRefRefString;
	}
	
	// iotRawDataInsRefTimeStamp get set methods
	public String getiotRawDataInsRefTimeStamp() {
		return iotRawDataInsRefTimeStamp;
	}
	public void setiotRawDataInsRefTimeStamp(String iotRawDataInsRefTimeStamp) {
		this.iotRawDataInsRefTimeStamp = iotRawDataInsRefTimeStamp;
	}
	
	// iotRawDataInsRefUserId get set methods
	public String getiotRawDataInsRefUserId() {
		return iotRawDataInsRefUserId;
	}
	public void setiotRawDataInsRefUserId(String iotRawDataInsRefUserId) {
		this.iotRawDataInsRefUserId = iotRawDataInsRefUserId;
	}
	
	// hasStopSessionData get set methods
	public boolean hasStopSessionData() {
		return hasStopSessionData;
	}
	public void sethasStopSessionData(boolean hasStopSessionData) {
		this.hasStopSessionData = hasStopSessionData;
	}
	
	// iotRawDataStopSessionUserId get set methods
	public String getiotRawDataStopSessionUserId() {
		return iotRawDataStopSessionUserId;
	}
	public void setiotRawDataStopSessionUserId(String iotRawDataStopSessionUserId) {
		this.iotRawDataStopSessionUserId = iotRawDataStopSessionUserId;
	}
	
	// iotRawDataStopSessionAppSessionId get set methods
	public String getiotRawDataStopSessionAppSessionId() {
		return iotRawDataStopSessionAppSessionId;
	}
	public void setiotRawDataStopSessionAppSessionId(String iotRawDataStopSessionAppSessionId) {
		this.iotRawDataStopSessionAppSessionId = iotRawDataStopSessionAppSessionId;
	}
	
	// iotRawDataStopSessionDuration get set methods
	public String getiotRawDataStopSessionDuration() {
		return iotRawDataStopSessionDuration;
	}
	public void setiotRawDataStopSessionDuration(String iotRawDataStopSessionDuration) {
		this.iotRawDataStopSessionDuration = iotRawDataStopSessionDuration;
	}
	
	// iotRawDataStopSessionSessionId get set methods
	public String getiotRawDataStopSessionSessionId() {
		return iotRawDataStopSessionSessionId;
	}
	public void setiotRawDataStopSessionSessionId(String iotRawDataStopSessionSessionId) {
		this.iotRawDataStopSessionSessionId = iotRawDataStopSessionSessionId;
	}
	
	// iotRawDataStopSessionTimeStamp get set methods
	public String getiotRawDataStopSessionTimeStamp() {
		return iotRawDataStopSessionTimeStamp;
	}
	public void setiotRawDataStopSessionTimeStamp(String iotRawDataStopSessionTimeStamp) {
		this.iotRawDataStopSessionTimeStamp = iotRawDataStopSessionTimeStamp;
	}
	
	// iotRawDataStopSessionAction get set methods
	public String getiotRawDataStopSessionAction() {
		return iotRawDataStopSessionAction;
	}
	public void setiotRawDataStopSessionAction(String iotRawDataStopSessionAction) {
		this.iotRawDataStopSessionAction = iotRawDataStopSessionAction;
	}
	
	// hasPUSHACTIONData get set methods
	public boolean hasPUSHACTIONData() {
		return hasPUSHACTIONData;
	}
	public void sethasPUSHACTIONData(boolean hasPUSHACTIONData) {
		this.hasPUSHACTIONData = hasPUSHACTIONData;
	}
	
	// iotRawDataPUSHACTIONUserId get set methods
	public String getiotRawDataPUSHACTIONUserId() {
		return iotRawDataPUSHACTIONUserId;
	}
	public void setiotRawDataPUSHACTIONUserId(String iotRawDataPUSHACTIONUserId) {
		this.iotRawDataPUSHACTIONUserId = iotRawDataPUSHACTIONUserId;
	}
	
	// iotRawDataPUSHACTIONAppSessionId get set methods
	public String getiotRawDataPUSHACTIONAppSessionId() {
		return iotRawDataPUSHACTIONAppSessionId;
	}
	public void setiotRawDataPUSHACTIONAppSessionId(String iotRawDataPUSHACTIONAppSessionId) {
		this.iotRawDataPUSHACTIONAppSessionId = iotRawDataPUSHACTIONAppSessionId;
	}
	
	
	// iotRawDataPUSHACTIONSessionId get set methods
	public String getiotRawDataPUSHACTIONSessionId() {
		return iotRawDataPUSHACTIONSessionId;
	}
	public void setiotRawDataPUSHACTIONSessionId(String iotRawDataPUSHACTIONSessionId) {
		this.iotRawDataPUSHACTIONSessionId = iotRawDataPUSHACTIONSessionId;
	}
	
	// iotRawDataPUSHACTIONTimeStamp get set methods
	public String getiotRawDataPUSHACTIONTimeStamp() {
		return iotRawDataPUSHACTIONTimeStamp;
	}
	public void setiotRawDataPUSHACTIONTimeStamp(String iotRawDataPUSHACTIONTimeStamp) {
		this.iotRawDataPUSHACTIONTimeStamp = iotRawDataPUSHACTIONTimeStamp;
	}
	
	// iotRawDataPUSHACTIONAction get set methods
	public String getiotRawDataPUSHACTIONAction() {
		return iotRawDataPUSHACTIONAction;
	}
	public void setiotRawDataPUSHACTIONAction(String iotRawDataPUSHACTIONAction) {
		this.iotRawDataPUSHACTIONAction = iotRawDataPUSHACTIONAction;
	}

	// iotRawDataPUSHACTIONPUSHKEY get set methods
	public String getiotRawDataPUSHACTIONPUSHKEY() {
		return iotRawDataPUSHACTIONPUSHKEY;
	}
	public void setiotRawDataPUSHACTIONPUSHKEY(String iotRawDataPUSHACTIONPUSHKEY) {
		this.iotRawDataPUSHACTIONPUSHKEY = iotRawDataPUSHACTIONPUSHKEY;
	}
	
	// hasAdvertisingIdActionData get set methods
	public boolean hasAdvertisingIdActionData() {
		return hasAdvertisingIdActionData;
	}
	public void sethasAdvertisingIdActionData(boolean hasAdvertisingIdActionData) {
		this.hasAdvertisingIdActionData = hasAdvertisingIdActionData;
	}
	
	// iotRawDataAdvertisingIdActionAppSessionId get set methods
	public String getiotRawDataAdvertisingIdActionAppSessionId() {
		return iotRawDataAdvertisingIdActionAppSessionId;
	}
	public void setiotRawDataAdvertisingIdActionAppSessionId(String iotRawDataAdvertisingIdActionAppSessionId) {
		this.iotRawDataAdvertisingIdActionAppSessionId = iotRawDataAdvertisingIdActionAppSessionId;
	}
	
	// iotRawDataAdvertisingIdActionADVERTISINGIDOPTOUT get set methods
	public boolean getiotRawDataAdvertisingIdActionADVERTISINGIDOPTOUT() {
		return iotRawDataAdvertisingIdActionADVERTISINGIDOPTOUT;
	}
	public void setiotRawDataAdvertisingIdActionADVERTISINGIDOPTOUT(boolean iotRawDataAdvertisingIdActionADVERTISINGIDOPTOUT) {
		this.iotRawDataAdvertisingIdActionADVERTISINGIDOPTOUT = iotRawDataAdvertisingIdActionADVERTISINGIDOPTOUT;
	}
	
	// iotRawDataAdvertisingIdActionSessionId get set methods
	public String getiotRawDataAdvertisingIdActionSessionId() {
		return iotRawDataAdvertisingIdActionSessionId;
	}
	public void setiotRawDataAdvertisingIdActionSessionId(String iotRawDataAdvertisingIdActionSessionId) {
		this.iotRawDataAdvertisingIdActionSessionId = iotRawDataAdvertisingIdActionSessionId;
	}
	
	// iotRawDataAdvertisingIdActionTimeStamp get set methods
	public String getiotRawDataAdvertisingIdActionTimeStamp() {
		return iotRawDataAdvertisingIdActionTimeStamp;
	}
	public void setiotRawDataAdvertisingIdActionTimeStamp(String iotRawDataAdvertisingIdActionTimeStamp) {
		this.iotRawDataAdvertisingIdActionTimeStamp = iotRawDataAdvertisingIdActionTimeStamp;
	}
	
	// iotRawDataAdvertisingIdActionAction get set methods
	public String getiotRawDataAdvertisingIdActionAction() {
		return iotRawDataAdvertisingIdActionAction;
	}
	public void setiotRawDataAdvertisingIdActionAction(String iotRawDataAdvertisingIdActionAction) {
		this.iotRawDataAdvertisingIdActionAction = iotRawDataAdvertisingIdActionAction;
	}

	// iotRawDataAdvertisingIdActionADVERTISINGIDKEY get set methods
	public String getiotRawDataAdvertisingIdActionADVERTISINGIDKEY() {
		return iotRawDataAdvertisingIdActionADVERTISINGIDKEY;
	}
	public void setiotRawDataAdvertisingIdActionADVERTISINGIDKEY(String iotRawDataAdvertisingIdActionADVERTISINGIDKEY) {
		this.iotRawDataAdvertisingIdActionADVERTISINGIDKEY = iotRawDataAdvertisingIdActionADVERTISINGIDKEY;
	}
	
	// hasStartSessionData get set methods
	public boolean hasStartSessionData() {
		return hasStartSessionData;
	}
	public void sethasStartSessionData(boolean hasStartSessionData) {
		this.hasStartSessionData = hasStartSessionData;
	}
	
	// iotRawDataStartSessionScreenName get set methods
	public String getiotRawDataStartSessionScreenName() {
		return iotRawDataStartSessionScreenName;
	}
	public void setiotRawDataStartSessionScreenName(String iotRawDataStartSessionScreenName) {
		this.iotRawDataStartSessionScreenName = iotRawDataStartSessionScreenName;
	}
	
	// iotRawDataStartSessionAppSessionId get set methods
	public String getiotRawDataStartSessionAppSessionId() {
		return iotRawDataStartSessionAppSessionId;
	}
	public void setiotRawDataStartSessionAppSessionId(String iotRawDataStartSessionAppSessionId) {
		this.iotRawDataStartSessionAppSessionId = iotRawDataStartSessionAppSessionId;
	}
	
	// iotRawDataStartSessionSessionId get set methods
	public String getiotRawDataStartSessionSessionId() {
		return iotRawDataStartSessionSessionId;
	}
	public void setiotRawDataStartSessionSessionId(String iotRawDataStartSessionSessionId) {
		this.iotRawDataStartSessionSessionId = iotRawDataStartSessionSessionId;
	}
	
	// iotRawDataStartSessionTimeStamp get set methods
	public String getiotRawDataStartSessionTimeStamp() {
		return iotRawDataStartSessionTimeStamp;
	}
	public void setiotRawDataStartSessionTimeStamp(String iotRawDataStartSessionTimeStamp) {
		this.iotRawDataStartSessionTimeStamp = iotRawDataStartSessionTimeStamp;
	}
	
	// iotRawDataStartSessionAction get set methods
	public String getiotRawDataStartSessionAction() {
		return iotRawDataStartSessionAction;
	}
	public void setiotRawDataStartSessionAction(String iotRawDataStartSessionAction) {
		this.iotRawDataStartSessionAction = iotRawDataStartSessionAction;
	}

}
