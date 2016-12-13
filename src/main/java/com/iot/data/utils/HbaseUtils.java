package com.iot.data.utils;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

import com.iot.data.databeans.PTCampaignBean;
import com.iot.data.databeans.PTDeviceBean;
import com.iot.data.schema.PacketData;
import com.iot.data.schema.SerializableIotData;

public class HbaseUtils {

	private static File dirNamefile;
	private static File currDirParentPath;
	private static String confFileName;
	private static JSONParser confParser = new JSONParser();
	private static Object confObj;
	private static JSONObject confJsonObject;
	// hbase params
	private static String raw_iot_tab_name;
	private static String raw_iot_tab_colfam;
	private static String secondary_iot_tab_name;
	private static String secondary_iot_tab_colfam;
	private static String push_trigger_campaign_tab_name;
	private static String push_trigger_campaign_tab_colfam;
	private static String push_trigger_deviceInput_tab_name;
	private static String push_trigger_deviceInput_tab_colfam;
	private static String push_trigger_deviceStats_tab_name;
	private static String push_trigger_deviceStats_tab_colfam;
	private static String hbase_master_ip;
	private static String hbase_master_port;
	private static String hbase_zookeeper_port;
	
	private static Configuration hbaseIotConf;
	
	private final static Logger logger = LoggerFactory
			.getLogger(HbaseUtils.class);
	
	public HbaseUtils(String confTypeFile) throws IOException, ParseException {
		// read conf file and corresponding params
		
		dirNamefile = new File(System.getProperty("user.dir"));
		currDirParentPath = new File(dirNamefile.getParent());
		//confFileName= currDirParentPath.toString() + "/conf/" + confTypeFile;
		
		//confFileName = "/home/iot/fsociety/dal/conf/" + confTypeFile;
		confFileName = "/Users/sahil/Desktop/random/conf/" + confTypeFile;
		
		// read the json file and create a map of the parameters
		confObj = confParser.parse(new FileReader(confFileName));
        confJsonObject = (JSONObject) confObj;
        // read parameters from conf file
        // hbase params
        raw_iot_tab_name = (String) confJsonObject.get("hbase_table_primary");
        raw_iot_tab_colfam = (String) confJsonObject.get("hbase_raw_data_tab_colfam");
        secondary_iot_tab_name = (String) confJsonObject.get("hbase_table_secondary");
        secondary_iot_tab_colfam = (String) confJsonObject.get("hbase_table_secondary_colfam");
        push_trigger_campaign_tab_name = "push_trigger_campaign";
        push_trigger_campaign_tab_colfam = "push_trigger_campaign_cf";
        push_trigger_deviceInput_tab_name = "push_trigger_device_input_tab";
        push_trigger_deviceInput_tab_colfam = "push_trigger_device_input_cf";
        push_trigger_deviceStats_tab_name = "push_trigger_device_stats_tab";
        push_trigger_deviceStats_tab_colfam = "push_trigger_device_stats_cf";
        hbase_master_ip = (String) confJsonObject.get("server_ip");
        hbase_master_port = (String) confJsonObject.get("hbase_master_port");
        hbase_zookeeper_port = (String) confJsonObject.get("hbase_zookeeper_port");
        // set up hbase conn
        hbaseIotConf = HBaseConfiguration.create();
        hbaseIotConf.set("hbase.master",hbase_master_ip + ":" + hbase_master_port);
        hbaseIotConf.set("hbase.zookeeper.quorum", hbase_master_ip);
        hbaseIotConf.set("hbase.zookeeper.property.clientPort", hbase_zookeeper_port);	
	}
	
	@SuppressWarnings("deprecation")
	public Tuple2<ImmutableBytesWritable, Put> cnvrtIotRawDataToPut(SerializableIotData iotRawPacket) throws IOException {  
			
			try {
				HTable hbaseTabName = new HTable(hbaseIotConf, raw_iot_tab_name);
				// put the data
				String currIotRowKey = iotRawPacket.getAppSecret() + "__" + iotRawPacket.getPacketId();
				Put p = new Put(Bytes.toBytes(currIotRowKey));
				// add basic data to corresponding columns
				p.add(Bytes.toBytes(raw_iot_tab_colfam),
						Bytes.toBytes("packet_id"), Bytes.toBytes(iotRawPacket.getPacketId()));
				p.add(Bytes.toBytes(raw_iot_tab_colfam),
						Bytes.toBytes("app_secret"), Bytes.toBytes(iotRawPacket.getAppSecret()));
				p.add(Bytes.toBytes(raw_iot_tab_colfam),
						Bytes.toBytes("device_id"), Bytes.toBytes(iotRawPacket.getDeviceId()));
				p.add(Bytes.toBytes(raw_iot_tab_colfam),
						Bytes.toBytes("library"), Bytes.toBytes(iotRawPacket.getLibrary()));
				p.add(Bytes.toBytes(raw_iot_tab_colfam),
						Bytes.toBytes("library_version"), Bytes.toBytes(iotRawPacket.getLibraryVersion()));
				p.add(Bytes.toBytes(raw_iot_tab_colfam),
						Bytes.toBytes("server_ip"), Bytes.toBytes(iotRawPacket.getServerIp()));
				p.add(Bytes.toBytes(raw_iot_tab_colfam),
						Bytes.toBytes("server_time"), Bytes.toBytes(iotRawPacket.getServerTime()));
				// get unique_id_action in a hashmap and put the data
	        	Map<String,String> currUidDataMap = new HashMap<String,String>();
	        	currUidDataMap = iotRawPacket.getUNIQUEIDACTION();
	        	for (Map.Entry<String, String> entry : currUidDataMap.entrySet())
	        	{
	        		p.add(Bytes.toBytes(raw_iot_tab_colfam),
							Bytes.toBytes("UID__"+entry.getKey()), Bytes.toBytes(entry.getValue()));
	        	}
	        	// Loop through "packet" and put the data
	        	// setup a few counts
	        	int countScreens = 1;
	        	int countEvents = 1;
	        	for (PacketData currPd : iotRawPacket.getPacket()) {
	        		// Install Referrer Data
	        		if ( currPd.getInstallReferrer() != null ){
	        			p.add(Bytes.toBytes(raw_iot_tab_colfam),
								Bytes.toBytes("hasIRdata"), Bytes.toBytes(true));
	        			if ( currPd.getInstallReferrer().getAction() != null ){
		        			p.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("IRAction"), Bytes.toBytes(currPd.getInstallReferrer().getAction()));
	        			}
	        			if ( currPd.getInstallReferrer().getAppSessionId() != null ){
		        			p.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("IRAppSessionID"), Bytes.toBytes(currPd.getInstallReferrer().getAppSessionId()));
	        			}
	        			if ( currPd.getInstallReferrer().getReferrerString() != null ){
		        			p.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("IRRefString"), Bytes.toBytes(currPd.getInstallReferrer().getReferrerString()));
	        			}
	        			if ( currPd.getInstallReferrer().getTimestamp() != null ){
		        			p.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("IRTimestamp"), Bytes.toBytes(currPd.getInstallReferrer().getTimestamp()));
	        			}
	        			if ( currPd.getInstallReferrer().getUserId() != null ){
		        			p.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("IRUserId"), Bytes.toBytes(currPd.getInstallReferrer().getUserId()));
	        			}
	    			}
	        		// start session data
	        		if (currPd.getStartSession() != null){
	        			p.add(Bytes.toBytes(raw_iot_tab_colfam),
								Bytes.toBytes("hasStartSessn"), Bytes.toBytes(true));
	        			if ( currPd.getStartSession().getAction() != null ){
		        			p.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("StartSessnAction"), Bytes.toBytes(currPd.getStartSession().getAction()));
	        			}
	        			if ( currPd.getStartSession().getAppSessionId() != null ){
		        			p.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("StartSessnAppSessionID"), Bytes.toBytes(currPd.getStartSession().getAppSessionId()));
	        			}
	        			if ( currPd.getStartSession().getScreenName() != null ){
		        			p.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("StartSessnScreenName"), Bytes.toBytes(currPd.getStartSession().getScreenName()));
	        			}
	        			if ( currPd.getStartSession().getTimestamp() != null ){
		        			p.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("StartSessnTimestamp"), Bytes.toBytes(currPd.getStartSession().getTimestamp()));
	        			}
	        			if ( currPd.getStartSession().getSessionId() != null ){
		        			p.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("StartSessnSessionID"), Bytes.toBytes(currPd.getStartSession().getSessionId()));
	        			}
	    			}
	        		// stop session data
	        		if ( currPd.getStopSession() != null ){
	        			p.add(Bytes.toBytes(raw_iot_tab_colfam),
								Bytes.toBytes("hasStopSessn"), Bytes.toBytes(true));
	        			if ( currPd.getStopSession().getAction() != null ){
		        			p.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("StopSessnAction"), Bytes.toBytes(currPd.getStopSession().getAction()));
	        			}
	        			if ( currPd.getStopSession().getAppSessionId() != null ){
		        			p.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("StopSessnAppSessionID"), Bytes.toBytes(currPd.getStopSession().getAppSessionId()));
	        			}
	        			if ( currPd.getStopSession().getDuration() != null ){
		        			p.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("StopSessnDuration"), Bytes.toBytes(currPd.getStopSession().getDuration()));
	        			}
	        			if ( currPd.getStopSession().getTimestamp() != null ){
		        			p.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("StopSessnTimestamp"), Bytes.toBytes(currPd.getStopSession().getTimestamp()));
	        			}
	        			if ( currPd.getStopSession().getSessionId() != null ){
		        			p.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("StopSessnSessionID"), Bytes.toBytes(currPd.getStopSession().getSessionId()));
	        			}
	        			if ( currPd.getStopSession().getUserId() != null ){
		        			p.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("StopSessnUserID"), Bytes.toBytes(currPd.getStopSession().getUserId()));
	        			}
	    			}
	        		// screen data
                    if (currPd.getScreen() != null){
                    	// increment screen count and set up hbase cols according to count
                    	countScreens += 1;
                    	String curScreenCntStr = "__" + Integer.toString(countScreens);
                        p.add(Bytes.toBytes(raw_iot_tab_colfam),
                                Bytes.toBytes("hasScreen"), Bytes.toBytes(true));
                        if ( currPd.getScreen().getAction() != null ){
                            p.add(Bytes.toBytes(raw_iot_tab_colfam),
                                    Bytes.toBytes("ScreenAction" + curScreenCntStr), Bytes.toBytes(currPd.getScreen().getAction()));
                        }
                        if ( currPd.getScreen().getAppSessionId() != null ){
                            p.add(Bytes.toBytes(raw_iot_tab_colfam),
                                    Bytes.toBytes("ScreenAppSessionID" + curScreenCntStr), Bytes.toBytes(currPd.getScreen().getAppSessionId()));
                        }
                        if ( currPd.getScreen().getTimestamp() != null ){
                            p.add(Bytes.toBytes(raw_iot_tab_colfam),
                                    Bytes.toBytes("ScreenTimeStamp" + curScreenCntStr), Bytes.toBytes(currPd.getScreen().getTimestamp()));
                        }
                        if ( currPd.getScreen().getSessionId() != null ){
                            p.add(Bytes.toBytes(raw_iot_tab_colfam),
                                    Bytes.toBytes("ScreenSessionID" + curScreenCntStr), Bytes.toBytes(currPd.getScreen().getSessionId()));
                        }
                        if ( currPd.getScreen().getScreenId() != null ){
                            p.add(Bytes.toBytes(raw_iot_tab_colfam),
                                    Bytes.toBytes("ScreenScreenID" + curScreenCntStr), Bytes.toBytes(currPd.getScreen().getScreenId()));
                        }
                        // get properties in a hashmap and put the data
                        if ( currPd.getScreen().getProperties() != null ){
                            Map<String,String> currScreenMap = new HashMap<String,String>();
                            currScreenMap = currPd.getScreen().getProperties();
                            p.add(Bytes.toBytes(raw_iot_tab_colfam),
                                    Bytes.toBytes("hasScreenProps"), Bytes.toBytes(true));
                            for (Map.Entry<String, String> entry : currScreenMap.entrySet())
                            {
                                p.add(Bytes.toBytes(raw_iot_tab_colfam),
                                        Bytes.toBytes("ScreenProp__" + curScreenCntStr + "__" + entry.getKey()),
                                        	Bytes.toBytes(entry.getValue()));
                            }
                        }
                    }
                    // identity data
                    if (currPd.getIdentity() != null){
                        p.add(Bytes.toBytes(raw_iot_tab_colfam),
                                Bytes.toBytes("hasID"), Bytes.toBytes(true));
                        if ( currPd.getIdentity().getAction() != null ){
                            p.add(Bytes.toBytes(raw_iot_tab_colfam),
                                    Bytes.toBytes("IDAction"), Bytes.toBytes(currPd.getIdentity().getAction()));
                        }
                        if ( currPd.getIdentity().getAppSessionId() != null ){
                            p.add(Bytes.toBytes(raw_iot_tab_colfam),
                                    Bytes.toBytes("IDAppSessionID"), Bytes.toBytes(currPd.getIdentity().getAppSessionId()));
                        }
                        if ( currPd.getIdentity().getTimestamp() != null ){
                            p.add(Bytes.toBytes(raw_iot_tab_colfam),
                                    Bytes.toBytes("IDTimeStamp"), Bytes.toBytes(currPd.getIdentity().getTimestamp()));
                        }
                        if ( currPd.getIdentity().getSessionId() != null ){
                            p.add(Bytes.toBytes(raw_iot_tab_colfam),
                                    Bytes.toBytes("IDSessionID"), Bytes.toBytes(currPd.getIdentity().getSessionId()));
                        }
                        if ( currPd.getIdentity().getUserId() != null ){
                            p.add(Bytes.toBytes(raw_iot_tab_colfam),
                                    Bytes.toBytes("IDUserID"), Bytes.toBytes(currPd.getIdentity().getUserId()));
                        }
                        // get properties in a hashmap and put the data
                        if ( currPd.getIdentity().getProperties() != null ){
                            Map<String,String> currIdentityMap = new HashMap<String,String>();
                            currIdentityMap = currPd.getIdentity().getProperties();
                            p.add(Bytes.toBytes(raw_iot_tab_colfam),
                                    Bytes.toBytes("hasIDProps"), Bytes.toBytes(true));
                            for (Map.Entry<String, String> entry : currIdentityMap.entrySet())
                            {
                                p.add(Bytes.toBytes(raw_iot_tab_colfam),
                                        Bytes.toBytes("IDProp__"+entry.getKey()), Bytes.toBytes(entry.getValue()));
                            }
                        }
                    }
                    // events data
                    if (currPd.getEvents() != null){
                    	// increment events count and set up hbase cols according to count
                    	countEvents += 1;
                    	String curEventCntStr = "__" + Integer.toString(countEvents);
                        p.add(Bytes.toBytes(raw_iot_tab_colfam),
                                Bytes.toBytes("hasEvents"), Bytes.toBytes(true));
                        if ( currPd.getEvents().getAction() != null ){
                            p.add(Bytes.toBytes(raw_iot_tab_colfam),
                                    Bytes.toBytes("EventsAction" + curEventCntStr), Bytes.toBytes(currPd.getEvents().getAction()));
                        }
                        if ( currPd.getEvents().getAppSessionId() != null ){
                            p.add(Bytes.toBytes(raw_iot_tab_colfam),
                                    Bytes.toBytes("EventsAppSessionID" + curEventCntStr), Bytes.toBytes(currPd.getEvents().getAppSessionId()));
                        }
                        if ( currPd.getEvents().getTimestamp() != null ){
                            p.add(Bytes.toBytes(raw_iot_tab_colfam),
                                    Bytes.toBytes("EventsTimeStamp" + curEventCntStr), Bytes.toBytes(currPd.getEvents().getTimestamp()));
                        }
                        if ( currPd.getEvents().getSessionId() != null ){
                            p.add(Bytes.toBytes(raw_iot_tab_colfam),
                                    Bytes.toBytes("EventsSessionID" + curEventCntStr), Bytes.toBytes(currPd.getEvents().getSessionId()));
                        }
                        if ( currPd.getEvents().getUserId() != null ){
                            p.add(Bytes.toBytes(raw_iot_tab_colfam),
                                    Bytes.toBytes("EventsUserID" + curEventCntStr), Bytes.toBytes(currPd.getEvents().getUserId()));
                        }
                        if ( currPd.getEvents().getEvent() != null ){
                            p.add(Bytes.toBytes(raw_iot_tab_colfam),
                                    Bytes.toBytes("Events" + curEventCntStr),
                                    Bytes.toBytes(currPd.getEvents().getEvent()));
                        }
                        if ( currPd.getEvents().getPosition() != null ){
                            p.add(Bytes.toBytes(raw_iot_tab_colfam),
                                    Bytes.toBytes("EventsPosition"), Bytes.toBytes(currPd.getEvents().getPosition()));
                        }
                        // get properties in a hashmap and put the data
                        if ( currPd.getEvents().getProperties() != null ){
                            Map<String,String> currEventsMap = new HashMap<String,String>();
                            currEventsMap = currPd.getEvents().getProperties();
                            p.add(Bytes.toBytes(raw_iot_tab_colfam),
                                    Bytes.toBytes("hasEventsProps"), Bytes.toBytes(true));
                            for (Map.Entry<String, String> entry : currEventsMap.entrySet())
                            {
                                p.add(Bytes.toBytes(raw_iot_tab_colfam),
                                        Bytes.toBytes("EventsProp"+ curEventCntStr + "__" + entry.getKey()), Bytes.toBytes(entry.getValue()));
                            }
                        }
                    }
	        		// push action data
	        		if (currPd.getPUSHACTION() != null){
	        			p.add(Bytes.toBytes(raw_iot_tab_colfam),
								Bytes.toBytes("hasPushActn"), Bytes.toBytes(true));
	        			if ( currPd.getPUSHACTION().getAction() != null ){
		        			p.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("PushActnActionName"), Bytes.toBytes(currPd.getPUSHACTION().getAction()));
	        			}
	        			if ( currPd.getPUSHACTION().getAppSessionId() != null ){
		        			p.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("PushActnAppSessionID"), Bytes.toBytes(currPd.getPUSHACTION().getAppSessionId()));
	        			}
	        			if ( currPd.getPUSHACTION().getPUSHKEY() != null ){
		        			p.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("PushActnPushKey"), Bytes.toBytes(currPd.getPUSHACTION().getPUSHKEY()));
	        			}
	        			if ( currPd.getPUSHACTION().getTimestamp() != null ){
		        			p.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("PushActnTimestamp"), Bytes.toBytes(currPd.getPUSHACTION().getTimestamp()));
	        			}
	        			if ( currPd.getPUSHACTION().getSessionId() != null ){
		        			p.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("PushActnSessionID"), Bytes.toBytes(currPd.getPUSHACTION().getSessionId()));
	        			}
	        			if ( currPd.getPUSHACTION().getUserId() != null ){
		        			p.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("PushActnUserID"), Bytes.toBytes(currPd.getPUSHACTION().getUserId()));
	        			}
	    			}
	        		// adv_id action data
	        		if (currPd.getADVERTISINGIDACTION() != null){
	        			p.add(Bytes.toBytes(raw_iot_tab_colfam),
								Bytes.toBytes("hasAdIDAction"), Bytes.toBytes(true));
	        			if ( currPd.getADVERTISINGIDACTION().getAction() != null ){
		        			p.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("AdIDActionActionName"), Bytes.toBytes(currPd.getADVERTISINGIDACTION().getAction()));
	        			}
	        			if ( currPd.getADVERTISINGIDACTION().getAppSessionId() != null ){
		        			p.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("AdIDActionAppSessionID"), Bytes.toBytes(currPd.getADVERTISINGIDACTION().getAppSessionId()));
	        			}
	        			if ( currPd.getADVERTISINGIDACTION().getADVERTISINGIDKEY() != null ){
		        			p.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("AdIDActionAdIDKey"), Bytes.toBytes(currPd.getADVERTISINGIDACTION().getADVERTISINGIDKEY()));
	        			}
	        			if ( currPd.getADVERTISINGIDACTION().getTimestamp() != null ){
		        			p.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("AdIDActionTimestamp"), Bytes.toBytes(currPd.getADVERTISINGIDACTION().getTimestamp()));
	        			}
	        			if ( currPd.getADVERTISINGIDACTION().getSessionId() != null ){
		        			p.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("AdIDActionSessionID"), Bytes.toBytes(currPd.getADVERTISINGIDACTION().getSessionId()));
	        			}
	        			if ( currPd.getADVERTISINGIDACTION().getADVERTISINGIDOPTOUT() != null ){
		        			p.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("AdIDActionAdIDOptOut"), Bytes.toBytes(currPd.getADVERTISINGIDACTION().getADVERTISINGIDOPTOUT()));
	        			}
	    			}
	        		// new device data
	        		if (currPd.getNewDevice() != null){
	        			p.add(Bytes.toBytes(raw_iot_tab_colfam),
								Bytes.toBytes("hasNewDev"), Bytes.toBytes(true));
	        			if ( currPd.getNewDevice().getAction() != null ){
		        			p.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("NewDevAction"), Bytes.toBytes(currPd.getNewDevice().getAction()));
	        			}
	        			if ( currPd.getNewDevice().getTimestamp() != null ){
		        			p.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("NewDevTimeStamp"), Bytes.toBytes(currPd.getNewDevice().getTimestamp()));
	        			}
	        			if ( currPd.getNewDevice().getContext() != null ){
	        				p.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("hasNewDevCxt"), Bytes.toBytes(true));
	        				// context features data
	        				if ( currPd.getNewDevice().getContext().getFeatures() != null ){
	        					p.add(Bytes.toBytes(raw_iot_tab_colfam),
										Bytes.toBytes("hasNewDevCxtFeatures"), Bytes.toBytes(true));
	        					if ( currPd.getNewDevice().getContext().getFeatures().getHasNFC() != null ){
	        						p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("NewDevCxtFeaturesNFC"), 
											Bytes.toBytes(currPd.getNewDevice().getContext().getFeatures().getHasNFC()));
	        					}
	        					if ( currPd.getNewDevice().getContext().getFeatures().getHasTelephony() != null ){
	        						p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("NewDevCxtFeaturesTelephony"), 
											Bytes.toBytes(currPd.getNewDevice().getContext().getFeatures().getHasTelephony()));
	        					}
	        					if ( currPd.getNewDevice().getContext().getFeatures().getHasGPS() != null ){
	        						p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("hasNewDevCxtFeaturesGPS"), 
											Bytes.toBytes(currPd.getNewDevice().getContext().getFeatures().getHasGPS()));
	        					}
	        					if ( currPd.getNewDevice().getContext().getFeatures().getHasAccelerometer() != null ){
	        						p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("hasNewDevCxtFeaturesAcclroMtr"), 
											Bytes.toBytes(currPd.getNewDevice().getContext().getFeatures().getHasAccelerometer()));
	        					}
	        					if ( currPd.getNewDevice().getContext().getFeatures().getHasBarometer() != null ){
	        						p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("hasNewDevCxtFeaturesBaromtr"), 
											Bytes.toBytes(currPd.getNewDevice().getContext().getFeatures().getHasBarometer()));
	        					}
	        					if ( currPd.getNewDevice().getContext().getFeatures().getHasCompass() != null ){
	        						p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("hasNewDevCxtFeaturesCompass"), 
											Bytes.toBytes(currPd.getNewDevice().getContext().getFeatures().getHasCompass()));
	        					}
	        					if ( currPd.getNewDevice().getContext().getFeatures().getHasGyroscope() != null ){
	        						p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("hasNewDevCxtFeaturesGyro"), 
											Bytes.toBytes(currPd.getNewDevice().getContext().getFeatures().getHasGyroscope()));
	        					}
	        					if ( currPd.getNewDevice().getContext().getFeatures().getHasLightsensor() != null ){
	        						p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("hasNewDevCxtFeaturesLightSensr"), 
											Bytes.toBytes(currPd.getNewDevice().getContext().getFeatures().getHasLightsensor()));
	        					}
	        					if ( currPd.getNewDevice().getContext().getFeatures().getHasProximity() != null ){
	        						p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("hasNewDevCxtFeaturesProxmty"), 
											Bytes.toBytes(currPd.getNewDevice().getContext().getFeatures().getHasProximity()));
	        					}
	        					if ( currPd.getNewDevice().getContext().getFeatures().getBluetoothVersion() != null ){
	        						p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("hasNewDevCxtFeaturesBTVrsn"), 
											Bytes.toBytes(currPd.getNewDevice().getContext().getFeatures().getBluetoothVersion()));
	        					}
	        					
	        				}
	        				// context display data
	        				if ( currPd.getNewDevice().getContext().getDisplay() != null ){
	        					p.add(Bytes.toBytes(raw_iot_tab_colfam),
										Bytes.toBytes("hasNewDevCxtDisplay"), Bytes.toBytes(true));
	        					if ( currPd.getNewDevice().getContext().getDisplay().getDisplayHeight() != null ){
	        						p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("NewDevCxtDisplayHeight"), 
											Bytes.toBytes(currPd.getNewDevice().getContext().getDisplay().getDisplayHeight()));
	        					}
	        					if ( currPd.getNewDevice().getContext().getDisplay().getDisplayWidth() != null ){
	        						p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("NewDevCxtDisplayWidth"), 
											Bytes.toBytes(currPd.getNewDevice().getContext().getDisplay().getDisplayWidth()));
	        					}
	        					if ( currPd.getNewDevice().getContext().getDisplay().getDisplayDensity() != null ){
	        						p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("NewDevCxtDisplayDensity"), 
											Bytes.toBytes(currPd.getNewDevice().getContext().getDisplay().getDisplayDensity()));
	        					}
	        				}
	        				// context total mem info data
	        				if ( currPd.getNewDevice().getContext().getTotalMemoryInfo() != null ){
	        					p.add(Bytes.toBytes(raw_iot_tab_colfam),
										Bytes.toBytes("hasNewDevCxtTotalMemry"), Bytes.toBytes(true));
	        					if ( currPd.getNewDevice().getContext().getTotalMemoryInfo().getTotalRAM() != null ){
	        						p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("NewDevCxtTotalMemryRAM"), 
											Bytes.toBytes(currPd.getNewDevice().getContext().getTotalMemoryInfo().getTotalRAM()));
	        					}
	        					if ( currPd.getNewDevice().getContext().getTotalMemoryInfo().getTotalStorage() != null ){
	        						p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("NewDevCxtTotalMemryStorage"), 
											Bytes.toBytes(currPd.getNewDevice().getContext().getTotalMemoryInfo().getTotalStorage()));
	        					}
	        				}
	        			}
	        			
	        		}
	        		// device info data
	        		if (currPd.getDeviceInfo() != null){
	        			p.add(Bytes.toBytes(raw_iot_tab_colfam),
								Bytes.toBytes("hasDevInfo"), Bytes.toBytes(true));
	        			if (currPd.getDeviceInfo().getAction() != null){
	        				p.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("DevInfoAction"), 
									Bytes.toBytes(currPd.getDeviceInfo().getAction()));
	        			}
	        			if (currPd.getDeviceInfo().getTimestamp() != null){
	        				p.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("DevInfoTimestamp"), 
									Bytes.toBytes(currPd.getDeviceInfo().getTimestamp()));
	        			}
	        			if (currPd.getDeviceInfo().getAppSessionId() != null){
	        				p.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("DevInfoAppSessionID"), 
									Bytes.toBytes(currPd.getDeviceInfo().getAppSessionId()));
	        			}
	        			if (currPd.getDeviceInfo().getSessionId() != null){
	        				p.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("DevInfoSessionID"), 
									Bytes.toBytes(currPd.getDeviceInfo().getSessionId()));
	        			}
	        			if (currPd.getDeviceInfo().getContext() != null){
	        				p.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("hasDevInfoCxt"), Bytes.toBytes(true));
	        				if (currPd.getDeviceInfo().getContext().getAppBuild() != null){
	        					p.add(Bytes.toBytes(raw_iot_tab_colfam),
										Bytes.toBytes("hasDevInfoCxtAppBuild"), Bytes.toBytes(true));
	        					if (currPd.getDeviceInfo().getContext().getAppBuild().getPackageName() != null){
	        						p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("DevInfoCxtAppBuildPackageName"),
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getAppBuild().getPackageName()));
	        					}
	        					if (currPd.getDeviceInfo().getContext().getAppBuild().getVersionCode() != null){
	        						p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("DevInfoCxtAppBuildVrsnCode"),
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getAppBuild().getVersionCode()));
	        					}
	        					if (currPd.getDeviceInfo().getContext().getAppBuild().getVersionName() != null){
	        						p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("DevInfoCxtAppBuildVrsnName"),
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getAppBuild().getVersionName()));
	        					}
	        				}
	        				// device info context device
	        				if (currPd.getDeviceInfo().getContext().getDevice() != null){
	        					p.add(Bytes.toBytes(raw_iot_tab_colfam),
										Bytes.toBytes("hasDevInfoCxtDevice"), Bytes.toBytes(true));
	        					if (currPd.getDeviceInfo().getContext().getDevice().getSdkVersion() != null){
	        						p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("DevInfoCxtDeviceSDKVrsn"),
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getDevice().getSdkVersion()));
	        					}
	        					if (currPd.getDeviceInfo().getContext().getDevice().getReleaseVersion() != null){
	        						p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("DevInfoCxtDeviceReleaseVrsn"),
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getDevice().getReleaseVersion()));
	        					}
	        					if (currPd.getDeviceInfo().getContext().getDevice().getDeviceBrand() != null){
	        						p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("DevInfoCxtDeviceBrand"),
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getDevice().getDeviceBrand()));
	        					}
	        					if (currPd.getDeviceInfo().getContext().getDevice().getDeviceManufacturer() != null){
	        						p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("DevInfoCxtDeviceManfactrer"),
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getDevice().getDeviceManufacturer()));
	        					}
	        					if (currPd.getDeviceInfo().getContext().getDevice().getDeviceModel() != null){
	        						p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("DevInfoCxtDeviceModel"),
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getDevice().getDeviceModel()));
	        					}
	        					if (currPd.getDeviceInfo().getContext().getDevice().getDeviceBoard() != null){
	        						p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("DevInfoCxtDeviceBoard"),
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getDevice().getDeviceBoard()));
	        					}
	        					if (currPd.getDeviceInfo().getContext().getDevice().getDeviceProduct() != null){
	        						p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("DevInfoCxtDeviceProduct"),
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getDevice().getDeviceProduct()));
	        					}
	        				}
	        				// device info context locale
	        				if (currPd.getDeviceInfo().getContext().getLocale() != null){
	        					p.add(Bytes.toBytes(raw_iot_tab_colfam),
										Bytes.toBytes("hasDevInfoCxtLocale"), Bytes.toBytes(true));
	        					if (currPd.getDeviceInfo().getContext().getLocale().getDeviceCountry() != null){
	        						p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("DevInfoCxtLocaleDevCountry"),
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getLocale().getDeviceCountry()));
	        					}
	        					if (currPd.getDeviceInfo().getContext().getLocale().getDeviceLanguage() != null){
	        						p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("DevInfoCxtLocaleDevLang"),
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getLocale().getDeviceLanguage()));
	        					}
	        				}
	        				// device info context location
	        				if (currPd.getDeviceInfo().getContext().getLocation() != null){
	        					p.add(Bytes.toBytes(raw_iot_tab_colfam),
										Bytes.toBytes("hasDevInfoCxtLocation"), Bytes.toBytes(true));
	        					if (currPd.getDeviceInfo().getContext().getLocation().getLatitude() != null){
	        						p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("DevInfoCxtLocationLat"),
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getLocation().getLatitude()));
	        					}
	        					if (currPd.getDeviceInfo().getContext().getLocation().getLongitude() != null){
	        						p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("DevInfoCxtLocationLong"),
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getLocation().getLongitude()));
	        					}
	        					if (currPd.getDeviceInfo().getContext().getLocation().getSpeed() != null){
	        						p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("DevInfoCxtLocationSpeed"),
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getLocation().getSpeed()));
	        					}
	        				}
	        				// device info context telephone
	        				if (currPd.getDeviceInfo().getContext().getTelephone() != null){
	        					p.add(Bytes.toBytes(raw_iot_tab_colfam),
										Bytes.toBytes("hasDevInfoCxtTelephone"), Bytes.toBytes(true));
	        					if (currPd.getDeviceInfo().getContext().getTelephone().getPhoneCarrier() != null){
	        						p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("DevInfoCxtTelephonePhnCarrier"),
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getTelephone().getPhoneCarrier()));
	        					}
	        					if (currPd.getDeviceInfo().getContext().getTelephone().getPhoneRadio() != null){
	        						p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("DevInfoCxtTelephonePhnRadio"),
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getTelephone().getPhoneRadio()));
	        					}
	        					if (currPd.getDeviceInfo().getContext().getTelephone().getInRoaming() != null){
	        						p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("DevInfoCxtTelephoneInRoaming"),
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getTelephone().getInRoaming()));
	        					}
	        				}
	        				// device info context wifi
	        				if (currPd.getDeviceInfo().getContext().getWifi() != null){
	        					p.add(Bytes.toBytes(raw_iot_tab_colfam),
										Bytes.toBytes("hasDevInfoCxtWifi"), Bytes.toBytes(true));
	        					p.add(Bytes.toBytes(raw_iot_tab_colfam),
										Bytes.toBytes("DevInfoCxtWifi"), 
										Bytes.toBytes(currPd.getDeviceInfo().getContext().getWifi().toString()));
	        				}
	        				// device info context bluetoothInfo
	        				if (currPd.getDeviceInfo().getContext().getBluetoothInfo() != null){
	        					p.add(Bytes.toBytes(raw_iot_tab_colfam),
										Bytes.toBytes("hasDevInfoCxtBTInfo"), Bytes.toBytes(true));
	        					if ( currPd.getDeviceInfo().getContext().getBluetoothInfo().getBluetoothStatus() != null ){
		        					p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("DevInfoCxtBTInfoBTStatus"), 
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getBluetoothInfo().getBluetoothStatus()));
	        					}
	        				}
	        				// device info context availableMemoryInfo
	        				if (currPd.getDeviceInfo().getContext().getAvailableMemoryInfo() != null){
	        					p.add(Bytes.toBytes(raw_iot_tab_colfam),
										Bytes.toBytes("hasDevInfoCxtAvailbleMemryInfo"), Bytes.toBytes(true));
	        					if ( currPd.getDeviceInfo().getContext().getAvailableMemoryInfo().getAvailableRAM() != null ){
		        					p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("DevInfoCxtAvailbleMemryInfoAvailRAM"), 
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getAvailableMemoryInfo().getAvailableRAM()));
	        					}
	        					if ( currPd.getDeviceInfo().getContext().getAvailableMemoryInfo().getAvailableStorage() != null ){
		        					p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("DevInfoCxtAvailbleMemryInfoAvailStorage"), 
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getAvailableMemoryInfo().getAvailableStorage()));
	        					}
	        				}
	        				// device info context cpuInfo
	        				if (currPd.getDeviceInfo().getContext().getCpuInfo() != null){
	        					p.add(Bytes.toBytes(raw_iot_tab_colfam),
										Bytes.toBytes("hasDevInfoCxtCPUInfo"), Bytes.toBytes(true));
	        					if ( currPd.getDeviceInfo().getContext().getCpuInfo().getCpuTotal() != null ){
		        					p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("DevInfoCxtCPUInfoTotal"), 
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getCpuInfo().getCpuTotal()));
	        					}
	        					if ( currPd.getDeviceInfo().getContext().getCpuInfo().getCpuIdle() != null ){
		        					p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("DevInfoCxtCPUInfoIdle"), 
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getCpuInfo().getCpuIdle()));
	        					}
	        					if ( currPd.getDeviceInfo().getContext().getCpuInfo().getCpuUsage() != null ){
		        					p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("DevInfoCxtCPUInfoUsage"), 
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getCpuInfo().getCpuUsage()));
	        					}
	        				}
	        				// device info context USER_AGENT_ACTION
	        				if (currPd.getDeviceInfo().getContext().getUSERAGENTACTION() != null){
	        					p.add(Bytes.toBytes(raw_iot_tab_colfam),
										Bytes.toBytes("hasDevInfoCxtUsrAgntActn"), Bytes.toBytes(true));
	        					if ( currPd.getDeviceInfo().getContext().getUSERAGENTACTION().getUserAgent() != null ){
	        						p.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("DevInfoCxtUsrAgntActnUsrAgnt"),
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getUSERAGENTACTION().getUserAgent()));
	        					}
	        				}
	        			}
	        		}
	        		
	        	}
	        	
	        	// return the put object
	        	return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(Bytes.toBytes(currIotRowKey)), p);
	        	
			}catch (IOException e) {  
	            e.printStackTrace();  
	        }
			// if something fails!!
			return new Tuple2<ImmutableBytesWritable, Put>(
					new ImmutableBytesWritable(Bytes.toBytes("PUT-FAILED")), 
					new Put(Bytes.toBytes("PUT-FAILED")));
	}
	
	
	
	public Tuple2<ImmutableBytesWritable, Put> cnvrtIotSecondaryDataToPut(SerializableIotData iotRawPacket) throws IOException {  
		
		try {
			HTable hbaseTabName = new HTable(hbaseIotConf, secondary_iot_tab_name);
			// put the data
			String currIotRowKey = iotRawPacket.getAppSecret() + "__" + iotRawPacket.getPacketId();
			Put p = new Put(Bytes.toBytes(currIotRowKey));
			// add basic data to corresponding columns
			p.add(Bytes.toBytes(secondary_iot_tab_colfam),
					Bytes.toBytes("packet_id"), Bytes.toBytes(iotRawPacket.getPacketId()));
			p.add(Bytes.toBytes(secondary_iot_tab_colfam),
					Bytes.toBytes("app_secret"), Bytes.toBytes(iotRawPacket.getAppSecret()));
			p.add(Bytes.toBytes(secondary_iot_tab_colfam),
					Bytes.toBytes("device_id"), Bytes.toBytes(iotRawPacket.getDeviceId()));
			p.add(Bytes.toBytes(secondary_iot_tab_colfam),
					Bytes.toBytes("library"), Bytes.toBytes(iotRawPacket.getLibrary()));
			p.add(Bytes.toBytes(secondary_iot_tab_colfam),
					Bytes.toBytes("library_version"), Bytes.toBytes(iotRawPacket.getLibraryVersion()));
			p.add(Bytes.toBytes(secondary_iot_tab_colfam),
					Bytes.toBytes("server_ip"), Bytes.toBytes(iotRawPacket.getServerIp()));
			p.add(Bytes.toBytes(secondary_iot_tab_colfam),
					Bytes.toBytes("server_time"), Bytes.toBytes(iotRawPacket.getServerTime()));
			// get unique_id_action in a hashmap and put the data
        	Map<String,String> currUidDataMap = new HashMap<String,String>();
        	currUidDataMap = iotRawPacket.getUNIQUEIDACTION();
        	for (Map.Entry<String, String> entry : currUidDataMap.entrySet())
        	{
        		p.add(Bytes.toBytes(secondary_iot_tab_colfam),
						Bytes.toBytes("UID__"+entry.getKey()), Bytes.toBytes(entry.getValue()));
        	}
        	// Loop through "packet" and put the data
        	// setup a few counts
        	int countScreens = 1;
        	int countEvents = 1;
        	for (PacketData currPd : iotRawPacket.getPacket()) {
        		// Install Referrer Data
        		if ( currPd.getInstallReferrer() != null ){
        			p.add(Bytes.toBytes(secondary_iot_tab_colfam),
							Bytes.toBytes("hasIRdata"), Bytes.toBytes(true));
        			if ( currPd.getInstallReferrer().getAction() != null ){
	        			p.add(Bytes.toBytes(secondary_iot_tab_colfam),
								Bytes.toBytes("IRAction"), Bytes.toBytes(currPd.getInstallReferrer().getAction()));
        			}
        			if ( currPd.getInstallReferrer().getAppSessionId() != null ){
	        			p.add(Bytes.toBytes(secondary_iot_tab_colfam),
								Bytes.toBytes("IRAppSessionID"), Bytes.toBytes(currPd.getInstallReferrer().getAppSessionId()));
        			}
        			if ( currPd.getInstallReferrer().getReferrerString() != null ){
	        			p.add(Bytes.toBytes(secondary_iot_tab_colfam),
								Bytes.toBytes("IRRefString"), Bytes.toBytes(currPd.getInstallReferrer().getReferrerString()));
        			}
        			if ( currPd.getInstallReferrer().getTimestamp() != null ){
	        			p.add(Bytes.toBytes(secondary_iot_tab_colfam),
								Bytes.toBytes("IRTimestamp"), Bytes.toBytes(currPd.getInstallReferrer().getTimestamp()));
        			}
        			if ( currPd.getInstallReferrer().getUserId() != null ){
	        			p.add(Bytes.toBytes(secondary_iot_tab_colfam),
								Bytes.toBytes("IRUserId"), Bytes.toBytes(currPd.getInstallReferrer().getUserId()));
        			}
    			}
        		// start session data
        		if (currPd.getStartSession() != null){
        			p.add(Bytes.toBytes(secondary_iot_tab_colfam),
							Bytes.toBytes("hasStartSessn"), Bytes.toBytes(true));
        			if ( currPd.getStartSession().getAction() != null ){
	        			p.add(Bytes.toBytes(secondary_iot_tab_colfam),
								Bytes.toBytes("StartSessnAction"), Bytes.toBytes(currPd.getStartSession().getAction()));
        			}
        			if ( currPd.getStartSession().getAppSessionId() != null ){
	        			p.add(Bytes.toBytes(secondary_iot_tab_colfam),
								Bytes.toBytes("StartSessnAppSessionID"), Bytes.toBytes(currPd.getStartSession().getAppSessionId()));
        			}
        			if ( currPd.getStartSession().getScreenName() != null ){
	        			p.add(Bytes.toBytes(secondary_iot_tab_colfam),
								Bytes.toBytes("StartSessnScreenName"), Bytes.toBytes(currPd.getStartSession().getScreenName()));
        			}
        			if ( currPd.getStartSession().getTimestamp() != null ){
	        			p.add(Bytes.toBytes(secondary_iot_tab_colfam),
								Bytes.toBytes("StartSessnTimestamp"), Bytes.toBytes(currPd.getStartSession().getTimestamp()));
        			}
        			if ( currPd.getStartSession().getSessionId() != null ){
	        			p.add(Bytes.toBytes(secondary_iot_tab_colfam),
								Bytes.toBytes("StartSessnSessionID"), Bytes.toBytes(currPd.getStartSession().getSessionId()));
        			}
    			}
        		// stop session data
        		if ( currPd.getStopSession() != null ){
        			p.add(Bytes.toBytes(secondary_iot_tab_colfam),
							Bytes.toBytes("hasStopSessn"), Bytes.toBytes(true));
        			if ( currPd.getStopSession().getAction() != null ){
	        			p.add(Bytes.toBytes(secondary_iot_tab_colfam),
								Bytes.toBytes("StopSessnAction"), Bytes.toBytes(currPd.getStopSession().getAction()));
        			}
        			if ( currPd.getStopSession().getAppSessionId() != null ){
	        			p.add(Bytes.toBytes(secondary_iot_tab_colfam),
								Bytes.toBytes("StopSessnAppSessionID"), Bytes.toBytes(currPd.getStopSession().getAppSessionId()));
        			}
        			if ( currPd.getStopSession().getDuration() != null ){
	        			p.add(Bytes.toBytes(secondary_iot_tab_colfam),
								Bytes.toBytes("StopSessnDuration"), Bytes.toBytes(currPd.getStopSession().getDuration()));
        			}
        			if ( currPd.getStopSession().getTimestamp() != null ){
	        			p.add(Bytes.toBytes(secondary_iot_tab_colfam),
								Bytes.toBytes("StopSessnTimestamp"), Bytes.toBytes(currPd.getStopSession().getTimestamp()));
        			}
        			if ( currPd.getStopSession().getSessionId() != null ){
	        			p.add(Bytes.toBytes(secondary_iot_tab_colfam),
								Bytes.toBytes("StopSessnSessionID"), Bytes.toBytes(currPd.getStopSession().getSessionId()));
        			}
        			if ( currPd.getStopSession().getUserId() != null ){
	        			p.add(Bytes.toBytes(secondary_iot_tab_colfam),
								Bytes.toBytes("StopSessnUserID"), Bytes.toBytes(currPd.getStopSession().getUserId()));
        			}
    			}
        		// screen data
                if (currPd.getScreen() != null){
                	// increment screen count and set up hbase cols according to count
                	countScreens += 1;
                	String curScreenCntStr = "__" + Integer.toString(countScreens);
                    p.add(Bytes.toBytes(secondary_iot_tab_colfam),
                            Bytes.toBytes("hasScreen"), Bytes.toBytes(true));
                    if ( currPd.getScreen().getAction() != null ){
                        p.add(Bytes.toBytes(secondary_iot_tab_colfam),
                                Bytes.toBytes("ScreenAction" + curScreenCntStr), Bytes.toBytes(currPd.getScreen().getAction()));
                    }
                    if ( currPd.getScreen().getAppSessionId() != null ){
                        p.add(Bytes.toBytes(secondary_iot_tab_colfam),
                                Bytes.toBytes("ScreenAppSessionID" + curScreenCntStr), Bytes.toBytes(currPd.getScreen().getAppSessionId()));
                    }
                    if ( currPd.getScreen().getTimestamp() != null ){
                        p.add(Bytes.toBytes(secondary_iot_tab_colfam),
                                Bytes.toBytes("ScreenTimeStamp" + curScreenCntStr), Bytes.toBytes(currPd.getScreen().getTimestamp()));
                    }
                    if ( currPd.getScreen().getSessionId() != null ){
                        p.add(Bytes.toBytes(secondary_iot_tab_colfam),
                                Bytes.toBytes("ScreenSessionID" + curScreenCntStr), Bytes.toBytes(currPd.getScreen().getSessionId()));
                    }
                    if ( currPd.getScreen().getScreenId() != null ){
                        p.add(Bytes.toBytes(secondary_iot_tab_colfam),
                                Bytes.toBytes("ScreenScreenID" + curScreenCntStr), Bytes.toBytes(currPd.getScreen().getScreenId()));
                    }
                    // get properties in a hashmap and put the data
                    if ( currPd.getScreen().getProperties() != null ){
                        Map<String,String> currScreenMap = new HashMap<String,String>();
                        currScreenMap = currPd.getScreen().getProperties();
                        p.add(Bytes.toBytes(secondary_iot_tab_colfam),
                                Bytes.toBytes("hasScreenProps"), Bytes.toBytes(true));
                        for (Map.Entry<String, String> entry : currScreenMap.entrySet())
                        {
                            p.add(Bytes.toBytes(secondary_iot_tab_colfam),
                                    Bytes.toBytes("ScreenProp__" + curScreenCntStr + "__" + entry.getKey()),
                                    	Bytes.toBytes(entry.getValue()));
                        }
                    }
                }
                // identity data
                if (currPd.getIdentity() != null){
                    p.add(Bytes.toBytes(secondary_iot_tab_colfam),
                            Bytes.toBytes("hasID"), Bytes.toBytes(true));
                    if ( currPd.getIdentity().getAction() != null ){
                        p.add(Bytes.toBytes(secondary_iot_tab_colfam),
                                Bytes.toBytes("IDAction"), Bytes.toBytes(currPd.getIdentity().getAction()));
                    }
                    if ( currPd.getIdentity().getAppSessionId() != null ){
                        p.add(Bytes.toBytes(secondary_iot_tab_colfam),
                                Bytes.toBytes("IDAppSessionID"), Bytes.toBytes(currPd.getIdentity().getAppSessionId()));
                    }
                    if ( currPd.getIdentity().getTimestamp() != null ){
                        p.add(Bytes.toBytes(secondary_iot_tab_colfam),
                                Bytes.toBytes("IDTimeStamp"), Bytes.toBytes(currPd.getIdentity().getTimestamp()));
                    }
                    if ( currPd.getIdentity().getSessionId() != null ){
                        p.add(Bytes.toBytes(secondary_iot_tab_colfam),
                                Bytes.toBytes("IDSessionID"), Bytes.toBytes(currPd.getIdentity().getSessionId()));
                    }
                    if ( currPd.getIdentity().getUserId() != null ){
                        p.add(Bytes.toBytes(secondary_iot_tab_colfam),
                                Bytes.toBytes("IDUserID"), Bytes.toBytes(currPd.getIdentity().getUserId()));
                    }
                    // get properties in a hashmap and put the data
                    if ( currPd.getIdentity().getProperties() != null ){
                        Map<String,String> currIdentityMap = new HashMap<String,String>();
                        currIdentityMap = currPd.getIdentity().getProperties();
                        p.add(Bytes.toBytes(secondary_iot_tab_colfam),
                                Bytes.toBytes("hasIDProps"), Bytes.toBytes(true));
                        for (Map.Entry<String, String> entry : currIdentityMap.entrySet())
                        {
                            p.add(Bytes.toBytes(secondary_iot_tab_colfam),
                                    Bytes.toBytes("IDProp__"+entry.getKey()), Bytes.toBytes(entry.getValue()));
                        }
                    }
                }
                // events data
                if (currPd.getEvents() != null){
                	// increment events count and set up hbase cols according to count
                	countEvents += 1;
                	String curEventCntStr = "__" + Integer.toString(countEvents);
                    p.add(Bytes.toBytes(secondary_iot_tab_colfam),
                            Bytes.toBytes("hasEvents"), Bytes.toBytes(true));
                    if ( currPd.getEvents().getAction() != null ){
                        p.add(Bytes.toBytes(secondary_iot_tab_colfam),
                                Bytes.toBytes("EventsAction" + curEventCntStr), Bytes.toBytes(currPd.getEvents().getAction()));
                    }
                    if ( currPd.getEvents().getAppSessionId() != null ){
                        p.add(Bytes.toBytes(secondary_iot_tab_colfam),
                                Bytes.toBytes("EventsAppSessionID" + curEventCntStr), Bytes.toBytes(currPd.getEvents().getAppSessionId()));
                    }
                    if ( currPd.getEvents().getTimestamp() != null ){
                        p.add(Bytes.toBytes(secondary_iot_tab_colfam),
                                Bytes.toBytes("EventsTimeStamp" + curEventCntStr), Bytes.toBytes(currPd.getEvents().getTimestamp()));
                    }
                    if ( currPd.getEvents().getSessionId() != null ){
                        p.add(Bytes.toBytes(secondary_iot_tab_colfam),
                                Bytes.toBytes("EventsSessionID" + curEventCntStr), Bytes.toBytes(currPd.getEvents().getSessionId()));
                    }
                    if ( currPd.getEvents().getUserId() != null ){
                        p.add(Bytes.toBytes(secondary_iot_tab_colfam),
                                Bytes.toBytes("EventsUserID" + curEventCntStr), Bytes.toBytes(currPd.getEvents().getUserId()));
                    }
                    if ( currPd.getEvents().getEvent() != null ){
                        p.add(Bytes.toBytes(secondary_iot_tab_colfam),
                                Bytes.toBytes("Events" + curEventCntStr),
                                Bytes.toBytes(currPd.getEvents().getEvent()));
                    }
                    if ( currPd.getEvents().getPosition() != null ){
                        p.add(Bytes.toBytes(secondary_iot_tab_colfam),
                                Bytes.toBytes("EventsPosition"), Bytes.toBytes(currPd.getEvents().getPosition()));
                    }
                    // get properties in a hashmap and put the data
                    if ( currPd.getEvents().getProperties() != null ){
                        Map<String,String> currEventsMap = new HashMap<String,String>();
                        currEventsMap = currPd.getEvents().getProperties();
                        p.add(Bytes.toBytes(secondary_iot_tab_colfam),
                                Bytes.toBytes("hasEventsProps"), Bytes.toBytes(true));
                        for (Map.Entry<String, String> entry : currEventsMap.entrySet())
                        {
                            p.add(Bytes.toBytes(secondary_iot_tab_colfam),
                                    Bytes.toBytes("EventsProp"+ curEventCntStr + "__" + entry.getKey()), Bytes.toBytes(entry.getValue()));
                        }
                    }
                }
        		// push action data
        		if (currPd.getPUSHACTION() != null){
        			p.add(Bytes.toBytes(secondary_iot_tab_colfam),
							Bytes.toBytes("hasPushActn"), Bytes.toBytes(true));
        			if ( currPd.getPUSHACTION().getAction() != null ){
	        			p.add(Bytes.toBytes(secondary_iot_tab_colfam),
								Bytes.toBytes("PushActnActionName"), Bytes.toBytes(currPd.getPUSHACTION().getAction()));
        			}
        			if ( currPd.getPUSHACTION().getAppSessionId() != null ){
	        			p.add(Bytes.toBytes(secondary_iot_tab_colfam),
								Bytes.toBytes("PushActnAppSessionID"), Bytes.toBytes(currPd.getPUSHACTION().getAppSessionId()));
        			}
        			if ( currPd.getPUSHACTION().getPUSHKEY() != null ){
	        			p.add(Bytes.toBytes(secondary_iot_tab_colfam),
								Bytes.toBytes("PushActnPushKey"), Bytes.toBytes(currPd.getPUSHACTION().getPUSHKEY()));
        			}
        			if ( currPd.getPUSHACTION().getTimestamp() != null ){
	        			p.add(Bytes.toBytes(secondary_iot_tab_colfam),
								Bytes.toBytes("PushActnTimestamp"), Bytes.toBytes(currPd.getPUSHACTION().getTimestamp()));
        			}
        			if ( currPd.getPUSHACTION().getSessionId() != null ){
	        			p.add(Bytes.toBytes(secondary_iot_tab_colfam),
								Bytes.toBytes("PushActnSessionID"), Bytes.toBytes(currPd.getPUSHACTION().getSessionId()));
        			}
        			if ( currPd.getPUSHACTION().getUserId() != null ){
	        			p.add(Bytes.toBytes(secondary_iot_tab_colfam),
								Bytes.toBytes("PushActnUserID"), Bytes.toBytes(currPd.getPUSHACTION().getUserId()));
        			}
    			}
        		// adv_id action data
        		if (currPd.getADVERTISINGIDACTION() != null){
        			p.add(Bytes.toBytes(secondary_iot_tab_colfam),
							Bytes.toBytes("hasAdIDAction"), Bytes.toBytes(true));
        			if ( currPd.getADVERTISINGIDACTION().getAction() != null ){
	        			p.add(Bytes.toBytes(secondary_iot_tab_colfam),
								Bytes.toBytes("AdIDActionActionName"), Bytes.toBytes(currPd.getADVERTISINGIDACTION().getAction()));
        			}
        			if ( currPd.getADVERTISINGIDACTION().getAppSessionId() != null ){
	        			p.add(Bytes.toBytes(secondary_iot_tab_colfam),
								Bytes.toBytes("AdIDActionAppSessionID"), Bytes.toBytes(currPd.getADVERTISINGIDACTION().getAppSessionId()));
        			}
        			if ( currPd.getADVERTISINGIDACTION().getADVERTISINGIDKEY() != null ){
	        			p.add(Bytes.toBytes(secondary_iot_tab_colfam),
								Bytes.toBytes("AdIDActionAdIDKey"), Bytes.toBytes(currPd.getADVERTISINGIDACTION().getADVERTISINGIDKEY()));
        			}
        			if ( currPd.getADVERTISINGIDACTION().getTimestamp() != null ){
	        			p.add(Bytes.toBytes(secondary_iot_tab_colfam),
								Bytes.toBytes("AdIDActionTimestamp"), Bytes.toBytes(currPd.getADVERTISINGIDACTION().getTimestamp()));
        			}
        			if ( currPd.getADVERTISINGIDACTION().getSessionId() != null ){
	        			p.add(Bytes.toBytes(secondary_iot_tab_colfam),
								Bytes.toBytes("AdIDActionSessionID"), Bytes.toBytes(currPd.getADVERTISINGIDACTION().getSessionId()));
        			}
        			if ( currPd.getADVERTISINGIDACTION().getADVERTISINGIDOPTOUT() != null ){
	        			p.add(Bytes.toBytes(secondary_iot_tab_colfam),
								Bytes.toBytes("AdIDActionAdIDOptOut"), Bytes.toBytes(currPd.getADVERTISINGIDACTION().getADVERTISINGIDOPTOUT()));
        			}
    			}
        		// new device data
        		if (currPd.getNewDevice() != null){
        			p.add(Bytes.toBytes(secondary_iot_tab_colfam),
							Bytes.toBytes("hasNewDev"), Bytes.toBytes(true));
        			if ( currPd.getNewDevice().getAction() != null ){
	        			p.add(Bytes.toBytes(secondary_iot_tab_colfam),
								Bytes.toBytes("NewDevAction"), Bytes.toBytes(currPd.getNewDevice().getAction()));
        			}
        			if ( currPd.getNewDevice().getTimestamp() != null ){
	        			p.add(Bytes.toBytes(secondary_iot_tab_colfam),
								Bytes.toBytes("NewDevTimeStamp"), Bytes.toBytes(currPd.getNewDevice().getTimestamp()));
        			}
        			if ( currPd.getNewDevice().getContext() != null ){
        				p.add(Bytes.toBytes(secondary_iot_tab_colfam),
								Bytes.toBytes("hasNewDevCxt"), Bytes.toBytes(true));
        				// context features data
        				if ( currPd.getNewDevice().getContext().getFeatures() != null ){
        					p.add(Bytes.toBytes(secondary_iot_tab_colfam),
									Bytes.toBytes("hasNewDevCxtFeatures"), Bytes.toBytes(true));
        					if ( currPd.getNewDevice().getContext().getFeatures().getHasNFC() != null ){
        						p.add(Bytes.toBytes(secondary_iot_tab_colfam),
										Bytes.toBytes("NewDevCxtFeaturesNFC"), 
										Bytes.toBytes(currPd.getNewDevice().getContext().getFeatures().getHasNFC()));
        					}
        					if ( currPd.getNewDevice().getContext().getFeatures().getHasTelephony() != null ){
        						p.add(Bytes.toBytes(secondary_iot_tab_colfam),
										Bytes.toBytes("NewDevCxtFeaturesTelephony"), 
										Bytes.toBytes(currPd.getNewDevice().getContext().getFeatures().getHasTelephony()));
        					}
        					if ( currPd.getNewDevice().getContext().getFeatures().getHasGPS() != null ){
        						p.add(Bytes.toBytes(secondary_iot_tab_colfam),
										Bytes.toBytes("hasNewDevCxtFeaturesGPS"), 
										Bytes.toBytes(currPd.getNewDevice().getContext().getFeatures().getHasGPS()));
        					}
        					if ( currPd.getNewDevice().getContext().getFeatures().getHasAccelerometer() != null ){
        						p.add(Bytes.toBytes(secondary_iot_tab_colfam),
										Bytes.toBytes("hasNewDevCxtFeaturesAcclroMtr"), 
										Bytes.toBytes(currPd.getNewDevice().getContext().getFeatures().getHasAccelerometer()));
        					}
        					if ( currPd.getNewDevice().getContext().getFeatures().getHasBarometer() != null ){
        						p.add(Bytes.toBytes(secondary_iot_tab_colfam),
										Bytes.toBytes("hasNewDevCxtFeaturesBaromtr"), 
										Bytes.toBytes(currPd.getNewDevice().getContext().getFeatures().getHasBarometer()));
        					}
        					if ( currPd.getNewDevice().getContext().getFeatures().getHasCompass() != null ){
        						p.add(Bytes.toBytes(secondary_iot_tab_colfam),
										Bytes.toBytes("hasNewDevCxtFeaturesCompass"), 
										Bytes.toBytes(currPd.getNewDevice().getContext().getFeatures().getHasCompass()));
        					}
        					if ( currPd.getNewDevice().getContext().getFeatures().getHasGyroscope() != null ){
        						p.add(Bytes.toBytes(secondary_iot_tab_colfam),
										Bytes.toBytes("hasNewDevCxtFeaturesGyro"), 
										Bytes.toBytes(currPd.getNewDevice().getContext().getFeatures().getHasGyroscope()));
        					}
        					if ( currPd.getNewDevice().getContext().getFeatures().getHasLightsensor() != null ){
        						p.add(Bytes.toBytes(secondary_iot_tab_colfam),
										Bytes.toBytes("hasNewDevCxtFeaturesLightSensr"), 
										Bytes.toBytes(currPd.getNewDevice().getContext().getFeatures().getHasLightsensor()));
        					}
        					if ( currPd.getNewDevice().getContext().getFeatures().getHasProximity() != null ){
        						p.add(Bytes.toBytes(secondary_iot_tab_colfam),
										Bytes.toBytes("hasNewDevCxtFeaturesProxmty"), 
										Bytes.toBytes(currPd.getNewDevice().getContext().getFeatures().getHasProximity()));
        					}
        					if ( currPd.getNewDevice().getContext().getFeatures().getBluetoothVersion() != null ){
        						p.add(Bytes.toBytes(secondary_iot_tab_colfam),
										Bytes.toBytes("hasNewDevCxtFeaturesBTVrsn"), 
										Bytes.toBytes(currPd.getNewDevice().getContext().getFeatures().getBluetoothVersion()));
        					}
        					
        				}
        				// context display data
        				if ( currPd.getNewDevice().getContext().getDisplay() != null ){
        					p.add(Bytes.toBytes(secondary_iot_tab_colfam),
									Bytes.toBytes("hasNewDevCxtDisplay"), Bytes.toBytes(true));
        					if ( currPd.getNewDevice().getContext().getDisplay().getDisplayHeight() != null ){
        						p.add(Bytes.toBytes(secondary_iot_tab_colfam),
										Bytes.toBytes("NewDevCxtDisplayHeight"), 
										Bytes.toBytes(currPd.getNewDevice().getContext().getDisplay().getDisplayHeight()));
        					}
        					if ( currPd.getNewDevice().getContext().getDisplay().getDisplayWidth() != null ){
        						p.add(Bytes.toBytes(secondary_iot_tab_colfam),
										Bytes.toBytes("NewDevCxtDisplayWidth"), 
										Bytes.toBytes(currPd.getNewDevice().getContext().getDisplay().getDisplayWidth()));
        					}
        					if ( currPd.getNewDevice().getContext().getDisplay().getDisplayDensity() != null ){
        						p.add(Bytes.toBytes(secondary_iot_tab_colfam),
										Bytes.toBytes("NewDevCxtDisplayDensity"), 
										Bytes.toBytes(currPd.getNewDevice().getContext().getDisplay().getDisplayDensity()));
        					}
        				}
        				// context total mem info data
        				if ( currPd.getNewDevice().getContext().getTotalMemoryInfo() != null ){
        					p.add(Bytes.toBytes(secondary_iot_tab_colfam),
									Bytes.toBytes("hasNewDevCxtTotalMemry"), Bytes.toBytes(true));
        					if ( currPd.getNewDevice().getContext().getTotalMemoryInfo().getTotalRAM() != null ){
        						p.add(Bytes.toBytes(secondary_iot_tab_colfam),
										Bytes.toBytes("NewDevCxtTotalMemryRAM"), 
										Bytes.toBytes(currPd.getNewDevice().getContext().getTotalMemoryInfo().getTotalRAM()));
        					}
        					if ( currPd.getNewDevice().getContext().getTotalMemoryInfo().getTotalStorage() != null ){
        						p.add(Bytes.toBytes(secondary_iot_tab_colfam),
										Bytes.toBytes("NewDevCxtTotalMemryStorage"), 
										Bytes.toBytes(currPd.getNewDevice().getContext().getTotalMemoryInfo().getTotalStorage()));
        					}
        				}
        			}
        			
        		}
        		// device info data
        		if (currPd.getDeviceInfo() != null){
        			p.add(Bytes.toBytes(secondary_iot_tab_colfam),
							Bytes.toBytes("hasDevInfo"), Bytes.toBytes(true));
        			if (currPd.getDeviceInfo().getAction() != null){
        				p.add(Bytes.toBytes(secondary_iot_tab_colfam),
								Bytes.toBytes("DevInfoAction"), 
								Bytes.toBytes(currPd.getDeviceInfo().getAction()));
        			}
        			if (currPd.getDeviceInfo().getTimestamp() != null){
        				p.add(Bytes.toBytes(secondary_iot_tab_colfam),
								Bytes.toBytes("DevInfoTimestamp"), 
								Bytes.toBytes(currPd.getDeviceInfo().getTimestamp()));
        			}
        			if (currPd.getDeviceInfo().getAppSessionId() != null){
        				p.add(Bytes.toBytes(secondary_iot_tab_colfam),
								Bytes.toBytes("DevInfoAppSessionID"), 
								Bytes.toBytes(currPd.getDeviceInfo().getAppSessionId()));
        			}
        			if (currPd.getDeviceInfo().getSessionId() != null){
        				p.add(Bytes.toBytes(secondary_iot_tab_colfam),
								Bytes.toBytes("DevInfoSessionID"), 
								Bytes.toBytes(currPd.getDeviceInfo().getSessionId()));
        			}
        			if (currPd.getDeviceInfo().getContext() != null){
        				p.add(Bytes.toBytes(secondary_iot_tab_colfam),
								Bytes.toBytes("hasDevInfoCxt"), Bytes.toBytes(true));
        				if (currPd.getDeviceInfo().getContext().getAppBuild() != null){
        					p.add(Bytes.toBytes(secondary_iot_tab_colfam),
									Bytes.toBytes("hasDevInfoCxtAppBuild"), Bytes.toBytes(true));
        					if (currPd.getDeviceInfo().getContext().getAppBuild().getPackageName() != null){
        						p.add(Bytes.toBytes(secondary_iot_tab_colfam),
										Bytes.toBytes("DevInfoCxtAppBuildPackageName"),
										Bytes.toBytes(currPd.getDeviceInfo().getContext().getAppBuild().getPackageName()));
        					}
        					if (currPd.getDeviceInfo().getContext().getAppBuild().getVersionCode() != null){
        						p.add(Bytes.toBytes(secondary_iot_tab_colfam),
										Bytes.toBytes("DevInfoCxtAppBuildVrsnCode"),
										Bytes.toBytes(currPd.getDeviceInfo().getContext().getAppBuild().getVersionCode()));
        					}
        					if (currPd.getDeviceInfo().getContext().getAppBuild().getVersionName() != null){
        						p.add(Bytes.toBytes(secondary_iot_tab_colfam),
										Bytes.toBytes("DevInfoCxtAppBuildVrsnName"),
										Bytes.toBytes(currPd.getDeviceInfo().getContext().getAppBuild().getVersionName()));
        					}
        				}
        				// device info context device
        				if (currPd.getDeviceInfo().getContext().getDevice() != null){
        					p.add(Bytes.toBytes(secondary_iot_tab_colfam),
									Bytes.toBytes("hasDevInfoCxtDevice"), Bytes.toBytes(true));
        					if (currPd.getDeviceInfo().getContext().getDevice().getSdkVersion() != null){
        						p.add(Bytes.toBytes(secondary_iot_tab_colfam),
										Bytes.toBytes("DevInfoCxtDeviceSDKVrsn"),
										Bytes.toBytes(currPd.getDeviceInfo().getContext().getDevice().getSdkVersion()));
        					}
        					if (currPd.getDeviceInfo().getContext().getDevice().getReleaseVersion() != null){
        						p.add(Bytes.toBytes(secondary_iot_tab_colfam),
										Bytes.toBytes("DevInfoCxtDeviceReleaseVrsn"),
										Bytes.toBytes(currPd.getDeviceInfo().getContext().getDevice().getReleaseVersion()));
        					}
        					if (currPd.getDeviceInfo().getContext().getDevice().getDeviceBrand() != null){
        						p.add(Bytes.toBytes(secondary_iot_tab_colfam),
										Bytes.toBytes("DevInfoCxtDeviceBrand"),
										Bytes.toBytes(currPd.getDeviceInfo().getContext().getDevice().getDeviceBrand()));
        					}
        					if (currPd.getDeviceInfo().getContext().getDevice().getDeviceManufacturer() != null){
        						p.add(Bytes.toBytes(secondary_iot_tab_colfam),
										Bytes.toBytes("DevInfoCxtDeviceManfactrer"),
										Bytes.toBytes(currPd.getDeviceInfo().getContext().getDevice().getDeviceManufacturer()));
        					}
        					if (currPd.getDeviceInfo().getContext().getDevice().getDeviceModel() != null){
        						p.add(Bytes.toBytes(secondary_iot_tab_colfam),
										Bytes.toBytes("DevInfoCxtDeviceModel"),
										Bytes.toBytes(currPd.getDeviceInfo().getContext().getDevice().getDeviceModel()));
        					}
        					if (currPd.getDeviceInfo().getContext().getDevice().getDeviceBoard() != null){
        						p.add(Bytes.toBytes(secondary_iot_tab_colfam),
										Bytes.toBytes("DevInfoCxtDeviceBoard"),
										Bytes.toBytes(currPd.getDeviceInfo().getContext().getDevice().getDeviceBoard()));
        					}
        					if (currPd.getDeviceInfo().getContext().getDevice().getDeviceProduct() != null){
        						p.add(Bytes.toBytes(secondary_iot_tab_colfam),
										Bytes.toBytes("DevInfoCxtDeviceProduct"),
										Bytes.toBytes(currPd.getDeviceInfo().getContext().getDevice().getDeviceProduct()));
        					}
        				}
        				// device info context locale
        				if (currPd.getDeviceInfo().getContext().getLocale() != null){
        					p.add(Bytes.toBytes(secondary_iot_tab_colfam),
									Bytes.toBytes("hasDevInfoCxtLocale"), Bytes.toBytes(true));
        					if (currPd.getDeviceInfo().getContext().getLocale().getDeviceCountry() != null){
        						p.add(Bytes.toBytes(secondary_iot_tab_colfam),
										Bytes.toBytes("DevInfoCxtLocaleDevCountry"),
										Bytes.toBytes(currPd.getDeviceInfo().getContext().getLocale().getDeviceCountry()));
        					}
        					if (currPd.getDeviceInfo().getContext().getLocale().getDeviceLanguage() != null){
        						p.add(Bytes.toBytes(secondary_iot_tab_colfam),
										Bytes.toBytes("DevInfoCxtLocaleDevLang"),
										Bytes.toBytes(currPd.getDeviceInfo().getContext().getLocale().getDeviceLanguage()));
        					}
        				}
        				// device info context location
        				if (currPd.getDeviceInfo().getContext().getLocation() != null){
        					p.add(Bytes.toBytes(secondary_iot_tab_colfam),
									Bytes.toBytes("hasDevInfoCxtLocation"), Bytes.toBytes(true));
        					if (currPd.getDeviceInfo().getContext().getLocation().getLatitude() != null){
        						p.add(Bytes.toBytes(secondary_iot_tab_colfam),
										Bytes.toBytes("DevInfoCxtLocationLat"),
										Bytes.toBytes(currPd.getDeviceInfo().getContext().getLocation().getLatitude()));
        					}
        					if (currPd.getDeviceInfo().getContext().getLocation().getLongitude() != null){
        						p.add(Bytes.toBytes(secondary_iot_tab_colfam),
										Bytes.toBytes("DevInfoCxtLocationLong"),
										Bytes.toBytes(currPd.getDeviceInfo().getContext().getLocation().getLongitude()));
        					}
        					if (currPd.getDeviceInfo().getContext().getLocation().getSpeed() != null){
        						p.add(Bytes.toBytes(secondary_iot_tab_colfam),
										Bytes.toBytes("DevInfoCxtLocationSpeed"),
										Bytes.toBytes(currPd.getDeviceInfo().getContext().getLocation().getSpeed()));
        					}
        				}
        				// device info context telephone
        				if (currPd.getDeviceInfo().getContext().getTelephone() != null){
        					p.add(Bytes.toBytes(secondary_iot_tab_colfam),
									Bytes.toBytes("hasDevInfoCxtTelephone"), Bytes.toBytes(true));
        					if (currPd.getDeviceInfo().getContext().getTelephone().getPhoneCarrier() != null){
        						p.add(Bytes.toBytes(secondary_iot_tab_colfam),
										Bytes.toBytes("DevInfoCxtTelephonePhnCarrier"),
										Bytes.toBytes(currPd.getDeviceInfo().getContext().getTelephone().getPhoneCarrier()));
        					}
        					if (currPd.getDeviceInfo().getContext().getTelephone().getPhoneRadio() != null){
        						p.add(Bytes.toBytes(secondary_iot_tab_colfam),
										Bytes.toBytes("DevInfoCxtTelephonePhnRadio"),
										Bytes.toBytes(currPd.getDeviceInfo().getContext().getTelephone().getPhoneRadio()));
        					}
        					if (currPd.getDeviceInfo().getContext().getTelephone().getInRoaming() != null){
        						p.add(Bytes.toBytes(secondary_iot_tab_colfam),
										Bytes.toBytes("DevInfoCxtTelephoneInRoaming"),
										Bytes.toBytes(currPd.getDeviceInfo().getContext().getTelephone().getInRoaming()));
        					}
        				}
        				// device info context wifi
        				if (currPd.getDeviceInfo().getContext().getWifi() != null){
        					p.add(Bytes.toBytes(secondary_iot_tab_colfam),
									Bytes.toBytes("hasDevInfoCxtWifi"), Bytes.toBytes(true));
        					p.add(Bytes.toBytes(secondary_iot_tab_colfam),
									Bytes.toBytes("DevInfoCxtWifi"), 
									Bytes.toBytes(currPd.getDeviceInfo().getContext().getWifi().toString()));
        				}
        				// device info context bluetoothInfo
        				if (currPd.getDeviceInfo().getContext().getBluetoothInfo() != null){
        					p.add(Bytes.toBytes(secondary_iot_tab_colfam),
									Bytes.toBytes("hasDevInfoCxtBTInfo"), Bytes.toBytes(true));
        					if ( currPd.getDeviceInfo().getContext().getBluetoothInfo().getBluetoothStatus() != null ){
	        					p.add(Bytes.toBytes(secondary_iot_tab_colfam),
										Bytes.toBytes("DevInfoCxtBTInfoBTStatus"), 
										Bytes.toBytes(currPd.getDeviceInfo().getContext().getBluetoothInfo().getBluetoothStatus()));
        					}
        				}
        				// device info context availableMemoryInfo
        				if (currPd.getDeviceInfo().getContext().getAvailableMemoryInfo() != null){
        					p.add(Bytes.toBytes(secondary_iot_tab_colfam),
									Bytes.toBytes("hasDevInfoCxtAvailbleMemryInfo"), Bytes.toBytes(true));
        					if ( currPd.getDeviceInfo().getContext().getAvailableMemoryInfo().getAvailableRAM() != null ){
	        					p.add(Bytes.toBytes(secondary_iot_tab_colfam),
										Bytes.toBytes("DevInfoCxtAvailbleMemryInfoAvailRAM"), 
										Bytes.toBytes(currPd.getDeviceInfo().getContext().getAvailableMemoryInfo().getAvailableRAM()));
        					}
        					if ( currPd.getDeviceInfo().getContext().getAvailableMemoryInfo().getAvailableStorage() != null ){
	        					p.add(Bytes.toBytes(secondary_iot_tab_colfam),
										Bytes.toBytes("DevInfoCxtAvailbleMemryInfoAvailStorage"), 
										Bytes.toBytes(currPd.getDeviceInfo().getContext().getAvailableMemoryInfo().getAvailableStorage()));
        					}
        				}
        				// device info context cpuInfo
        				if (currPd.getDeviceInfo().getContext().getCpuInfo() != null){
        					p.add(Bytes.toBytes(secondary_iot_tab_colfam),
									Bytes.toBytes("hasDevInfoCxtCPUInfo"), Bytes.toBytes(true));
        					if ( currPd.getDeviceInfo().getContext().getCpuInfo().getCpuTotal() != null ){
	        					p.add(Bytes.toBytes(secondary_iot_tab_colfam),
										Bytes.toBytes("DevInfoCxtCPUInfoTotal"), 
										Bytes.toBytes(currPd.getDeviceInfo().getContext().getCpuInfo().getCpuTotal()));
        					}
        					if ( currPd.getDeviceInfo().getContext().getCpuInfo().getCpuIdle() != null ){
	        					p.add(Bytes.toBytes(secondary_iot_tab_colfam),
										Bytes.toBytes("DevInfoCxtCPUInfoIdle"), 
										Bytes.toBytes(currPd.getDeviceInfo().getContext().getCpuInfo().getCpuIdle()));
        					}
        					if ( currPd.getDeviceInfo().getContext().getCpuInfo().getCpuUsage() != null ){
	        					p.add(Bytes.toBytes(secondary_iot_tab_colfam),
										Bytes.toBytes("DevInfoCxtCPUInfoUsage"), 
										Bytes.toBytes(currPd.getDeviceInfo().getContext().getCpuInfo().getCpuUsage()));
        					}
        				}
        				// device info context USER_AGENT_ACTION
        				if (currPd.getDeviceInfo().getContext().getUSERAGENTACTION() != null){
        					p.add(Bytes.toBytes(secondary_iot_tab_colfam),
									Bytes.toBytes("hasDevInfoCxtUsrAgntActn"), Bytes.toBytes(true));
        					if ( currPd.getDeviceInfo().getContext().getUSERAGENTACTION().getUserAgent() != null ){
        						p.add(Bytes.toBytes(secondary_iot_tab_colfam),
										Bytes.toBytes("DevInfoCxtUsrAgntActnUsrAgnt"),
										Bytes.toBytes(currPd.getDeviceInfo().getContext().getUSERAGENTACTION().getUserAgent()));
        					}
        				}
        			}
        		}
        		
        	}
        	
        	// return the put object
        	return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(Bytes.toBytes(currIotRowKey)), p);
        	
		}catch (IOException e) {  
            e.printStackTrace();  
        }
		// if something fails!!
		return new Tuple2<ImmutableBytesWritable, Put>(
				new ImmutableBytesWritable(Bytes.toBytes("PUT-FAILED")), 
				new Put(Bytes.toBytes("PUT-FAILED")));
	}
	
	public void insertPTCampaignData(String appSecret, String campaignId, String campaignName, 
			long startTime, long endTime, String message, long frequency, String eventName,
			String eventProperty, String eventPropValue, String compareOp) throws IOException {  
			
			try {
				HTable hbaseTabName = new HTable(hbaseIotConf, push_trigger_campaign_tab_name);
				String currRowKey = appSecret + "__" + campaignId;
				Put p = new Put(Bytes.toBytes(currRowKey));
				
				p.add(Bytes.toBytes(push_trigger_campaign_tab_colfam),
						Bytes.toBytes("appSecret"), 
						Bytes.toBytes(appSecret));
				
				p.add(Bytes.toBytes(push_trigger_campaign_tab_colfam),
						Bytes.toBytes("campaignId"), 
						Bytes.toBytes(campaignId));
				
				p.add(Bytes.toBytes(push_trigger_campaign_tab_colfam),
						Bytes.toBytes("campaignName"), 
						Bytes.toBytes(campaignName));
				
				p.add(Bytes.toBytes(push_trigger_campaign_tab_colfam),
						Bytes.toBytes("startTime"), 
						Bytes.toBytes(startTime));
				
				p.add(Bytes.toBytes(push_trigger_campaign_tab_colfam),
						Bytes.toBytes("endTime"), 
						Bytes.toBytes(endTime));
				
				p.add(Bytes.toBytes(push_trigger_campaign_tab_colfam),
						Bytes.toBytes("message"), 
						Bytes.toBytes(message));
				
				p.add(Bytes.toBytes(push_trigger_campaign_tab_colfam),
						Bytes.toBytes("frequency"), 
						Bytes.toBytes(frequency));
				
				p.add(Bytes.toBytes(push_trigger_campaign_tab_colfam),
						Bytes.toBytes("eventName"), 
						Bytes.toBytes(eventName));
				
				p.add(Bytes.toBytes(push_trigger_campaign_tab_colfam),
						Bytes.toBytes("eventProperty"), 
						Bytes.toBytes(eventProperty));
				
				p.add(Bytes.toBytes(push_trigger_campaign_tab_colfam),
						Bytes.toBytes("eventPropValue"), 
						Bytes.toBytes(eventPropValue));
				
				p.add(Bytes.toBytes(push_trigger_campaign_tab_colfam),
						Bytes.toBytes("compareOp"), 
						Bytes.toBytes(compareOp));
				
				if ( !compareOp.equals("ge") && !compareOp.equals("le") && !compareOp.equals("equals") ){
					System.out.println("compareOp should be 'ge', 'le' or 'equals.' You gave-->" + compareOp);
					logger.info("compareOp should be 'ge', 'le' or 'equals'. You gave-->" + compareOp);
					return;
				}
				
				hbaseTabName.put(p);
				
			}catch (IOException e) {  
	            e.printStackTrace();  
	        }
	}
	
	public ArrayList<PTCampaignBean> queryPTCampaignList() {
		 
		 ArrayList<PTCampaignBean> PTCampaignList = new ArrayList<PTCampaignBean>();
		 
		 try {
			 
			 HTable hbaseTabName = new HTable(hbaseIotConf, push_trigger_campaign_tab_name);
	    	 Scan scan = new Scan();
	    	 ResultScanner scanner = hbaseTabName.getScanner(scan);
	    	 for (Result rr : scanner) {
	    		 // if we are past end date skip the result
	    		 long currendTime = Bytes.toLong(rr.getValue(
		   					Bytes.toBytes(push_trigger_campaign_tab_colfam),
		   					Bytes.toBytes("endTime")));
	    		 
	    		 String currCmpgnId = Bytes.toString(rr.getValue(
	   					Bytes.toBytes(push_trigger_campaign_tab_colfam),
	   					Bytes.toBytes("campaignId")));
	    		 
	    		 String currAppSecret = Bytes.toString(rr.getValue(
	    					Bytes.toBytes(push_trigger_campaign_tab_colfam),
	    					Bytes.toBytes("appSecret")));
	    		 
	    		if (currendTime < System.currentTimeMillis()){
	    			logger.info("Skipping Expired TP campaign--->" + currAppSecret + "__" + currCmpgnId + "__" + currendTime);
	    			continue;
	    		}
	    		logger.info("Loading Active TP campaign--->" + currAppSecret + "__" + currCmpgnId + "__" + currendTime);
	    		PTCampaignBean currPTCBean = new PTCampaignBean();
		 	    currPTCBean.setendTime(currendTime);
	    		 
	    	     
	    	     currPTCBean.setappSecret(currAppSecret);
	    	     
	    	     
	    	     currPTCBean.setcampaignId(currCmpgnId);
	    	     
	    	     String currCmpgnName = Bytes.toString(rr.getValue(
	   					Bytes.toBytes(push_trigger_campaign_tab_colfam),
	   					Bytes.toBytes("campaignName")));
	 	    	 currPTCBean.setcampaignName(currCmpgnName);
	 	    	     
	 	    	 
	 	    	long currstartTime = Bytes.toLong(rr.getValue(
	   					Bytes.toBytes(push_trigger_campaign_tab_colfam),
	   					Bytes.toBytes("startTime")));
	 	    	 currPTCBean.setendTime(currstartTime);
	 	    	 
	 	    	long frequency = Bytes.toLong(rr.getValue(
	   					Bytes.toBytes(push_trigger_campaign_tab_colfam),
	   					Bytes.toBytes("frequency")));
	 	    	 currPTCBean.setfrequency(frequency);
	 	    	 
	 	    	String eventPropValue = Bytes.toString(rr.getValue(
	   					Bytes.toBytes(push_trigger_campaign_tab_colfam),
	   					Bytes.toBytes("eventPropValue")));
	 	    	 currPTCBean.seteventPropValue(eventPropValue);
	    	     
	 	    	String curreventName = Bytes.toString(rr.getValue(
	   					Bytes.toBytes(push_trigger_campaign_tab_colfam),
	   					Bytes.toBytes("eventName")));
	 	    	 currPTCBean.seteventName(curreventName);
	 	    	 
	 	    	String curreventProperty = Bytes.toString(rr.getValue(
	   					Bytes.toBytes(push_trigger_campaign_tab_colfam),
	   					Bytes.toBytes("eventProperty")));
	 	    	 currPTCBean.seteventProperty(curreventProperty);
	 	    	 
	 	    	String currmessage = Bytes.toString(rr.getValue(
	   					Bytes.toBytes(push_trigger_campaign_tab_colfam),
	   					Bytes.toBytes("message")));
	 	    	 currPTCBean.setmessage(currmessage);
	    	     
	 	    	PTCampaignList.add( currPTCBean );
	    	 }
			 
		 }catch (Exception e) {  
	            e.printStackTrace();  
	        }
		return PTCampaignList;
		 
	 }
	
	
	public HashMap<String,List<PTCampaignBean>> queryPTCampaignMapData() {
		 
		 HashMap<String,List<PTCampaignBean>> ptCmpMap = new HashMap<String,List<PTCampaignBean>>();
		 
		 try {
			 
			 HTable hbaseTabName = new HTable(hbaseIotConf, push_trigger_campaign_tab_name);
	    	 Scan scan = new Scan();
	    	 ResultScanner scanner = hbaseTabName.getScanner(scan);
	    	 for (Result rr : scanner) {
	    		 // if we are past end date skip the result
	    		 long currendTime = Bytes.toLong(rr.getValue(
		   					Bytes.toBytes(push_trigger_campaign_tab_colfam),
		   					Bytes.toBytes("endTime")));
	    		 
	    		 String currCmpgnId = Bytes.toString(rr.getValue(
	   					Bytes.toBytes(push_trigger_campaign_tab_colfam),
	   					Bytes.toBytes("campaignId")));
	    		 
	    		 String currAppSecret = Bytes.toString(rr.getValue(
	    					Bytes.toBytes(push_trigger_campaign_tab_colfam),
	    					Bytes.toBytes("appSecret")));
	    		if ( currendTime != 0L){
		    		if (currendTime < System.currentTimeMillis()){
		    			logger.info("PT MAP Skipping Expired TP campaign--->" 
		    					+ currAppSecret + "__" + currCmpgnId + "__" + currendTime);
		    			continue;
		    		}
	    		}
	    		logger.info("PT CMPGN MAP Loading Active TP campaign--->" + currAppSecret + "__" + currCmpgnId + "__" + currendTime);
	    		
	    		PTCampaignBean currPTCBean = new PTCampaignBean();
		 	    currPTCBean.setendTime(currendTime);
	    		 
	    	     
	    	    currPTCBean.setappSecret(currAppSecret);
	    	     
	    	     
	    	    currPTCBean.setcampaignId(currCmpgnId);
	    	     
	    	     String currCmpgnName = Bytes.toString(rr.getValue(
	   					Bytes.toBytes(push_trigger_campaign_tab_colfam),
	   					Bytes.toBytes("campaignName")));
	 	    	currPTCBean.setcampaignName(currCmpgnName);
	 	    	
	 	    	String currCmpgnTitle = Bytes.toString(rr.getValue(
	   					Bytes.toBytes(push_trigger_campaign_tab_colfam),
	   					Bytes.toBytes("title")));
	 	    	currPTCBean.setCampaignTitle(currCmpgnTitle);
	 	    	     
	 	    	 
	 	    	long currstartTime = Bytes.toLong(rr.getValue(
	   					Bytes.toBytes(push_trigger_campaign_tab_colfam),
	   					Bytes.toBytes("startTime")));
	 	    	currPTCBean.setendTime(currstartTime);
	 	    	 
	 	    	long frequency = Bytes.toLong(rr.getValue(
	   					Bytes.toBytes(push_trigger_campaign_tab_colfam),
	   					Bytes.toBytes("frequency")));
	 	    	currPTCBean.setfrequency(frequency);
	 	    	 
	 	    	String eventPropValue = Bytes.toString(rr.getValue(
	   					Bytes.toBytes(push_trigger_campaign_tab_colfam),
	   					Bytes.toBytes("eventPropValue")));
	 	    	currPTCBean.seteventPropValue(eventPropValue);
	    	     
	 	    	String curreventName = Bytes.toString(rr.getValue(
	   					Bytes.toBytes(push_trigger_campaign_tab_colfam),
	   					Bytes.toBytes("eventName")));
	 	    	currPTCBean.seteventName(curreventName);
	 	    	 
	 	    	String curreventProperty = Bytes.toString(rr.getValue(
	   					Bytes.toBytes(push_trigger_campaign_tab_colfam),
	   					Bytes.toBytes("eventProperty")));
	 	    	currPTCBean.seteventProperty(curreventProperty);
	 	    	 
	 	    	String currmessage = Bytes.toString(rr.getValue(
	   					Bytes.toBytes(push_trigger_campaign_tab_colfam),
	   					Bytes.toBytes("message")));
	 	    	currPTCBean.setmessage(currmessage);
	 	    	
	 	    	String currCompareOp = Bytes.toString(rr.getValue(
	   					Bytes.toBytes(push_trigger_campaign_tab_colfam),
	   					Bytes.toBytes("compareOp")));
	 	    	currPTCBean.setcompareOp(currCompareOp);
	    	    
//	 	    	String currPutKey = currAppSecret + "__" + curreventName + "__" +
//	 	    			curreventProperty + "__" + eventPropValue + "__" + currCompareOp;
	 	    	
	 	    	String currPutKey = currAppSecret + "__" + curreventName + "__" +
	 	    			curreventProperty + "__" + currCompareOp;
	 	    	
	 	    	if ( ptCmpMap.containsKey(currPutKey) ){
	 	    		logger.info("multiple campaigns with same event, eventprop and compareOp present-->" + 
	 	    				currPutKey + "adding campaign id--> " + currCmpgnId);
	 	    		List<PTCampaignBean> currKeyCmpgnList = ptCmpMap.get(currPutKey);
	 	    		currKeyCmpgnList.add(currPTCBean);
	 	    		ptCmpMap.put(currPutKey, currKeyCmpgnList);
	 	    	}
	 	    	else{
	 	    		logger.info("creating campaign key-->" + currPutKey);
	 	    		List<PTCampaignBean> selKeyPTCmpgn = new ArrayList<PTCampaignBean>();
	 	    		selKeyPTCmpgn.add( currPTCBean );
		 	    	ptCmpMap.put(currPutKey, selKeyPTCmpgn);
	 	    	}
	 	    	
	    	 }
			 
		 }catch (Exception e) {  
	            e.printStackTrace();  
	        }
		return ptCmpMap;
		 
	 }
	
	@SuppressWarnings("deprecation")
	public Tuple2<ImmutableBytesWritable, Put> cnvrtIotPushTriggerDevInfoToPut(
			SerializableIotData iotRawPacket,PTCampaignBean ptcmpgnData) throws IOException {  
			
			try {
				HTable hbaseTabName = new HTable(hbaseIotConf, push_trigger_deviceInput_tab_name);
				// put the data
				String currKey = iotRawPacket.getAppSecret() + "__" + 
						ptcmpgnData.getcampaignId() + "__" + iotRawPacket.getDeviceId();
				// get event data
				String eventProps = "NOT-FOUND";
				String eventTime = "NOT-FOUND";
				for(PacketData currPd : iotRawPacket.getPacket()){
					  if( currPd.getEvents() != null ){
						  if ( currPd.getEvents().getEvent() != null ){
							  if (currPd.getEvents().getEvent().equals(ptcmpgnData.geteventName())){
								  eventProps = currPd.getEvents().getProperties().toString();
								  eventTime = currPd.getEvents().getTimestamp();
							  }
						  }
					  }
				}
				Put p = new Put(Bytes.toBytes(currKey));
				// add basic data to corresponding columns
				p.add(Bytes.toBytes(push_trigger_deviceInput_tab_colfam),
						Bytes.toBytes("appSecret"), Bytes.toBytes(iotRawPacket.getAppSecret()));
				p.add(Bytes.toBytes(push_trigger_deviceInput_tab_colfam),
						Bytes.toBytes("deviceId"), Bytes.toBytes(iotRawPacket.getDeviceId()));
				p.add(Bytes.toBytes(push_trigger_deviceInput_tab_colfam),
						Bytes.toBytes("serverTime"), Bytes.toBytes(iotRawPacket.getServerTime()));
				p.add(Bytes.toBytes(push_trigger_deviceInput_tab_colfam),
						Bytes.toBytes("eventName"), Bytes.toBytes(ptcmpgnData.geteventName()));
				p.add(Bytes.toBytes(push_trigger_deviceInput_tab_colfam),
						Bytes.toBytes("eventProps"), Bytes.toBytes(eventProps));
				p.add(Bytes.toBytes(push_trigger_deviceInput_tab_colfam),
						Bytes.toBytes("eventTime"), Bytes.toBytes(eventTime));

				p.add(Bytes.toBytes(push_trigger_deviceInput_tab_colfam),
						Bytes.toBytes("campaignId"), Bytes.toBytes(ptcmpgnData.getcampaignId()));
				p.add(Bytes.toBytes(push_trigger_deviceInput_tab_colfam),
						Bytes.toBytes("campaignName"), Bytes.toBytes(ptcmpgnData.getcampaignName()));
				p.add(Bytes.toBytes(push_trigger_deviceInput_tab_colfam),
						Bytes.toBytes("title"), Bytes.toBytes(ptcmpgnData.getCampaignTitle()));
				p.add(Bytes.toBytes(push_trigger_deviceInput_tab_colfam),
						Bytes.toBytes("campaignEndTime"), Bytes.toBytes(ptcmpgnData.getendTime()));
				p.add(Bytes.toBytes(push_trigger_deviceInput_tab_colfam),
						Bytes.toBytes("campaignFreq"), Bytes.toBytes(ptcmpgnData.getfrequency()));
				p.add(Bytes.toBytes(push_trigger_deviceInput_tab_colfam),
						Bytes.toBytes("campaignMessage"), Bytes.toBytes(ptcmpgnData.getmessage()));
				// make sure the system knows its a new device
				p.add(Bytes.toBytes(push_trigger_deviceInput_tab_colfam),
						Bytes.toBytes("is_new_input_dev"), Bytes.toBytes(true));
				
				// return the put object
	        	return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(Bytes.toBytes(currKey)), p);
	        	
			}catch (IOException e) {  
	            e.printStackTrace();  
	        }
			// if something fails!!
			return new Tuple2<ImmutableBytesWritable, Put>(
					new ImmutableBytesWritable(Bytes.toBytes("PUT-FAILED")), 
					new Put(Bytes.toBytes("PUT-FAILED")));
	}
	
	public static List<PTDeviceBean> queryTPNewDevs(){
		
		List<PTDeviceBean> ptDevBeans = new ArrayList<PTDeviceBean>();
		
		try {
        	
        	List<Filter> filters = new ArrayList<Filter>();
        	HTable hbaseTabName = new HTable(hbaseIotConf, push_trigger_deviceInput_tab_name);
        	
        	
        	// check for new inputs from spark streaming 
            SingleColumnValueFilter newInputData = new SingleColumnValueFilter(Bytes  
                    .toBytes(push_trigger_deviceInput_tab_colfam), Bytes  
                    .toBytes("is_new_input_dev"), CompareOp.EQUAL, Bytes  
                    .toBytes(true));  
            newInputData.setFilterIfMissing(true);
            filters.add(newInputData);
//            // number of tries to send push should be less than 3
//            SingleColumnValueFilter filterNumTries = new SingleColumnValueFilter(Bytes  
//                    .toBytes(push_trigger_deviceInput_tab_colfam), Bytes  
//                    .toBytes("num_tries_push_sent"), CompareOp.LESS_OR_EQUAL, Bytes  
//                    .toBytes(3L));  
////            filterNumTries.setFilterIfMissing(true);
//            filters.add(filterNumTries);
            
            FilterList PTDevList = new FilterList(FilterList.Operator.MUST_PASS_ALL,filters);
        	

            Scan scan = new Scan();  
            scan.setFilter(newInputData);  
            ResultScanner rs = hbaseTabName.getScanner(scan);
            String currappSecret = "NONE";
            String currdeviceId = "NONE";
            String currserverTime = "NONE";
            String curreventName = "Campaign Name";
            String curreventProps = "NONE";
            String curreventTime = "NONE";
            String currcampaignId = "NONE";
            String currcampaignName = "NONE";
            String currcampaignTitle = "NONE";
            String currcampaignMessage = "NONE";
            
            long currcampaignFreq = 0L;
            long currcampaignEndTime = 0L;
            long currnumPushSent = 0L;
            long currlastPushTime = 0L;
            // set default timestamp to current time (just in case we dont get a time)
            long pbTimeStamp = System.currentTimeMillis();
            for (Result r : rs) {  
            	if( !r.isEmpty() ){
            		PTDeviceBean ptBn = new PTDeviceBean();
            		String rowKeyStr = Bytes.toString( r.getRow() );
            		currappSecret = Bytes.toString(r.getValue(
	    					Bytes.toBytes(push_trigger_deviceInput_tab_colfam),
	    					Bytes.toBytes("appSecret")));
            		currdeviceId = Bytes.toString(r.getValue(
	    					Bytes.toBytes(push_trigger_deviceInput_tab_colfam),
	    					Bytes.toBytes("deviceId")));
            		currserverTime = Bytes.toString(r.getValue(
	    					Bytes.toBytes(push_trigger_deviceInput_tab_colfam),
	    					Bytes.toBytes("serverTime")));
            		curreventName = Bytes.toString(r.getValue(
	    					Bytes.toBytes(push_trigger_deviceInput_tab_colfam),
	    					Bytes.toBytes("eventName")));
            		curreventProps = Bytes.toString(r.getValue(
	    					Bytes.toBytes(push_trigger_deviceInput_tab_colfam),
	    					Bytes.toBytes("eventProps")));
            		curreventTime = Bytes.toString(r.getValue(
	    					Bytes.toBytes(push_trigger_deviceInput_tab_colfam),
	    					Bytes.toBytes("eventTime")));
            		
            		currcampaignId = Bytes.toString(r.getValue(
	    					Bytes.toBytes(push_trigger_deviceInput_tab_colfam),
	    					Bytes.toBytes("campaignId")));
            		currcampaignName = Bytes.toString(r.getValue(
	    					Bytes.toBytes(push_trigger_deviceInput_tab_colfam),
	    					Bytes.toBytes("campaignName")));
            		currcampaignTitle = Bytes.toString(r.getValue(
	    					Bytes.toBytes(push_trigger_deviceInput_tab_colfam),
	    					Bytes.toBytes("title")));
            		currcampaignMessage = Bytes.toString(r.getValue(
	    					Bytes.toBytes(push_trigger_deviceInput_tab_colfam),
	    					Bytes.toBytes("campaignMessage")));
            		currcampaignFreq = Bytes.toLong(r.getValue(
	    					Bytes.toBytes(push_trigger_deviceInput_tab_colfam),
	    					Bytes.toBytes("campaignFreq")));
            		currcampaignEndTime = Bytes.toLong(r.getValue(
	    					Bytes.toBytes(push_trigger_deviceInput_tab_colfam),
	    					Bytes.toBytes("campaignEndTime")));
            		
            		
            		// get num_tries postback
            		// this could be null/ column not set
            		// take care of that.
            		if ( r.containsColumn(Bytes.toBytes(push_trigger_deviceInput_tab_colfam), Bytes.toBytes("num_push_sent")) ){
            			currnumPushSent = Bytes.toLong(r.getValue(
		    					Bytes.toBytes(push_trigger_deviceInput_tab_colfam),
		    					Bytes.toBytes("num_push_sent")));
            		}
            		if ( r.containsColumn(Bytes.toBytes(push_trigger_deviceInput_tab_colfam), Bytes.toBytes("last_push_time")) ){
            			currlastPushTime = Bytes.toLong(r.getValue(
		    					Bytes.toBytes(push_trigger_deviceInput_tab_colfam),
		    					Bytes.toBytes("last_push_time")));
            		}
            		ptBn.setappSecret(currappSecret);
            		ptBn.setdevInpRowKey(rowKeyStr);
            		ptBn.setdeviceId(currdeviceId);
            		ptBn.setcampaignId(currcampaignId);
            		ptBn.setcampaignName(currcampaignName);
            		ptBn.setCampaignTitle(currcampaignTitle);
            		ptBn.setcampaignMessage(currcampaignMessage);
            		ptBn.setcampaignFreq(currcampaignFreq);
            		ptBn.seteventName(curreventName);
            		ptBn.seteventProps(curreventProps);
            		ptBn.seteventTime(curreventTime);
            		ptBn.setlastPushSentTime(currlastPushTime);
            		ptBn.setnumSentPush(currnumPushSent);
            		ptBn.setserverTime(currserverTime);
            		ptBn.setcampaignEndTime(currcampaignEndTime);
            		ptDevBeans.add(ptBn);
            		System.out.println("push trigger rowkey-->"+rowKeyStr);
            	}
            }  
            rs.close();
            return ptDevBeans;
        } catch (Exception e) {  
            e.printStackTrace();  
        }
		return ptDevBeans;
	}
	
	public static boolean setNewPTDevFalse(PTDeviceBean ptBean){
		
		try{
			HTable hbaseTabName = new HTable(hbaseIotConf, push_trigger_deviceInput_tab_name);
			Put p = new Put(Bytes.toBytes(ptBean.getdevInpRowKey()));
			p.add(Bytes.toBytes(push_trigger_deviceInput_tab_colfam),
					  Bytes.toBytes("is_new_input_dev"),
					  Bytes.toBytes(false));
			hbaseTabName.put(p);
			hbaseTabName.close();
			return true;
		} catch (Exception e){
			e.printStackTrace();
			return false;
		}
	}
	
	public static boolean updateLastPushSentTime(PTDeviceBean ptBean){
		
		try{
			HTable hbaseTabName = new HTable(hbaseIotConf, push_trigger_deviceInput_tab_name);
			Put p = new Put(Bytes.toBytes(ptBean.getdevInpRowKey()));
			p.add(Bytes.toBytes(push_trigger_deviceInput_tab_colfam),
					  Bytes.toBytes("last_push_time"),
					  Bytes.toBytes(System.currentTimeMillis()));
			
			hbaseTabName.put(p);
			hbaseTabName.close();
			return true;
		} catch (Exception e){
			e.printStackTrace();
			return false;
		}
	}
	
	public static boolean incrementNumPushSent(PTDeviceBean ptBean) {
		try {
			HTable hbaseTabName = new HTable(hbaseIotConf, push_trigger_deviceInput_tab_name);
			hbaseTabName.incrementColumnValue(
					Bytes.toBytes(ptBean.getdevInpRowKey()),
					Bytes.toBytes(push_trigger_deviceInput_tab_colfam),
					Bytes.toBytes("num_push_sent"), 1L);
			hbaseTabName.close();
			return true;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}

	}
	
	public static boolean resetNumPushCnt(PTDeviceBean ptBean){
			
			try{
				HTable hbaseTabName = new HTable(hbaseIotConf, push_trigger_deviceInput_tab_name);
				Put p = new Put(Bytes.toBytes(ptBean.getdevInpRowKey()));
				p.add(Bytes.toBytes(push_trigger_deviceInput_tab_colfam),
						  Bytes.toBytes("num_push_sent"),
						  Bytes.toBytes(0L));
				
				hbaseTabName.put(p);
				return true;
			} catch (Exception e){
				e.printStackTrace();
				return false;
			}
	}
	
	
	public void insertPTCampaignData(String appSecret, String campaignId, String campaignName, 
			long startTime, long endTime, String message, long frequency, String eventName,
			String eventProperty, String eventPropValue, String compareOp,String campaignTitle) throws IOException {  
			
			try {
				HTable hbaseTabName = new HTable(hbaseIotConf, push_trigger_campaign_tab_name);
				String currRowKey = appSecret + "__" + campaignId;
				Put p = new Put(Bytes.toBytes(currRowKey));
				
				p.add(Bytes.toBytes(push_trigger_campaign_tab_colfam),
						Bytes.toBytes("appSecret"), 
						Bytes.toBytes(appSecret));
				
				p.add(Bytes.toBytes(push_trigger_campaign_tab_colfam),
						Bytes.toBytes("campaignId"), 
						Bytes.toBytes(campaignId));
				
				p.add(Bytes.toBytes(push_trigger_campaign_tab_colfam),
						Bytes.toBytes("campaignName"), 
						Bytes.toBytes(campaignName));
				
				p.add(Bytes.toBytes(push_trigger_campaign_tab_colfam),
						Bytes.toBytes("startTime"), 
						Bytes.toBytes(startTime));
				
				p.add(Bytes.toBytes(push_trigger_campaign_tab_colfam),
						Bytes.toBytes("endTime"), 
						Bytes.toBytes(endTime));
				
				p.add(Bytes.toBytes(push_trigger_campaign_tab_colfam),
						Bytes.toBytes("message"), 
						Bytes.toBytes(message));
				
				p.add(Bytes.toBytes(push_trigger_campaign_tab_colfam),
						Bytes.toBytes("frequency"), 
						Bytes.toBytes(frequency));
				
				p.add(Bytes.toBytes(push_trigger_campaign_tab_colfam),
						Bytes.toBytes("eventName"), 
						Bytes.toBytes(eventName));
				
				p.add(Bytes.toBytes(push_trigger_campaign_tab_colfam),
						Bytes.toBytes("eventProperty"), 
						Bytes.toBytes(eventProperty));
				
				p.add(Bytes.toBytes(push_trigger_campaign_tab_colfam),
						Bytes.toBytes("eventPropValue"), 
						Bytes.toBytes(eventPropValue));
				
				p.add(Bytes.toBytes(push_trigger_campaign_tab_colfam),
						Bytes.toBytes("compareOp"), 
						Bytes.toBytes(compareOp));
				
				p.add(Bytes.toBytes(push_trigger_campaign_tab_colfam),
						Bytes.toBytes("title"), 
						Bytes.toBytes(campaignTitle));
				
				if ( !compareOp.equals("ge") && !compareOp.equals("le") && !compareOp.equals("equals") ){
					System.out.println("compareOp should be 'ge', 'le' or 'equals.' You gave-->" + compareOp);
					logger.info("compareOp should be 'ge', 'le' or 'equals'. You gave-->" + compareOp);
					return;
				}
				
				hbaseTabName.put(p);
				
			}catch (IOException e) {  
	            e.printStackTrace();  
	        }
	}
	
	
	public static void main(String[] args) throws IOException, ParseException, java.text.ParseException {
		HbaseUtils hbUtils = new HbaseUtils("production_conf.json");
//		hbUtils.insertPTCampaignData("inthings1", "test1", "coolyourself", 
//				System.currentTimeMillis(), 0L, 
//				"Hey! You just finished walking. Reward yourself with a refreshing drink", 100L, 
//				"run", "distance", "5", "ge");
	//	HashMap<String,PTCampaignBean> ptCmpgnMap = hbUtils.queryPTCampaignMapData();
	//	System.out.println("aaaa-->" + ptCmpgnMap.toString());
	//	System.exit(0);
		
		
		Long endDate=0L,startDate=0L;
		DateFormat formatter;
		formatter = new SimpleDateFormat("yyyy-mm-dd");
		
		Date date1 = (Date) formatter.parse("2016-10-5");
		startDate=date1.getTime();
		System.out.println("Time 1:"+startDate);
		
		DateFormat formatter1;
		formatter1 = new SimpleDateFormat("yyyy-mm-dd");
		
		Date date2 = (Date) formatter1.parse("2016-11-30");
		endDate=date2.getTime();
		System.out.println("Time 2:"+endDate);
		
		Long frequency=Long.parseLong("500");
		
		Long s1 = Long.parseLong("1475653834000");
		Long s2 = Long.parseLong("1480492234000");
		
		//System.out.println(System.currentTimeMillis());
		
		hbUtils.insertPTCampaignData("StepCounter", "Time To Hydrate Your Body", "Gatorade", s1, s2, "Grab a free energy drink at Starbucks!", frequency, "walking", 
										"steps", "40", "ge", "Time To Hydrate Your Body");
		
		System.out.println("Done inserting into push_trigger_campaign table!");
	 }
				
}