package com.iot.data.stream;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import kafka.serializer.DefaultDecoder;
import kafka.serializer.StringDecoder;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.iot.data.databeans.PTCampaignBean;
import com.iot.data.schema.PacketData;
import com.iot.data.schema.RcvdData;
import com.iot.data.schema.SerializableIotData;
import com.iot.data.utils.HbaseUtils;

import scala.Tuple2;

// hbase imports
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;

public class IotDataStreamer {
	
	// conf file settings
//	private static File dirNamefile;
//	private static File currDirParentPath;
	private static String confFileName;
//	private static String schemaFileName;
	private static String confTypeFile;
	private static JSONParser confParser = new JSONParser();
	private static Object confObj;
	private static JSONObject confJsonObject;
	// spark settings
	private SparkConf sparkConf;
	private static JavaStreamingContext jssc;
	private static JavaSparkContext javasc;
	private static int numThreadsTopics;
	private static HashMap<String, String> props;
	private static String iot_app_name;
	private static String raw_iot_master_val;
	private static String raw_iot_spark_serializer;
	private static String raw_iot_spark_driver_memory;
	private static Long raw_iot_streaming_batch_size;
	private static String raw_iot_streaming_block_size;
	private static String raw_iot_spark_checkpoint_dir;
	private static String raw_iot_tab_name;
	private static String raw_iot_tab_colfam;
	private static String secondary_iot_tab_name;
	private static String secondary_iot_tab_colfam;
	private static String push_trigger_deviceInput_tab_name;
	private static String push_trigger_deviceInput_tab_colfam;
	private static String push_trigger_deviceStats_tab_name;
	private static String push_trigger_deviceStats_tab_colfam;
	// kafka/zookeeper settings
	private static String raw_iot_server_ip;
	private static String raw_iot_spark_kafka_port;
	private static String raw_iot_kafka_cons_group_id;
	private static String raw_iot_kafka_cons_id;
	private static String zk_node_conn_port;
	private static String raw_iot_zk_conn_timeout_milsec;
	private static String kafka_iot_main_topic;
	// avro schema 
	private static DatumReader<RcvdData> iotRawDatumReader;
	private static Decoder iotDecoder;
//	private static String schemaString;
	// hbase settings
	private static Configuration hbaseConf;
	private static Job newAPIJobConfigIotRaw;
	private static Job newAPIJobConfigTPDevice;
	private static Job newAPIJobConfigIotSecondary;
	private static String hbase_master_ip;
	private static String hbase_master_port;
	private static String hbase_zookeeper_port;
	private static HbaseUtils hbaseUtilsObj;
	
	private static HashMap<String,List<PTCampaignBean>> ptCmpgnMap;
	
	private final static Logger logger = LoggerFactory
			.getLogger(IotDataStreamer.class);
	
	private IotDataStreamer() throws IOException, ParseException {
		// ###################### CONF FILE TYPE ######################
		// ###################### CONF FILE TYPE ######################
		// settings for production or testing (choose one)
		confTypeFile = "production_conf.json";
		// ###################### CONF FILE TYPE ######################
		// ###################### CONF FILE TYPE ######################
		// read conf file and corresponding params
//		dirNamefile = new File(System.getProperty("user.dir"));
//		currDirParentPath = new File(dirNamefile.getParent());
		confFileName = "/home/iot/fsociety/dal/conf/" + confTypeFile;
//		schemaFileName = "home/iot/conf/iot_data_schema.avsc";
		// read the json file and create a map of the parameters
		confObj = confParser.parse(new FileReader(
				confFileName));
        confJsonObject = (JSONObject) confObj;
        // read parameters from conf file
        iot_app_name = (String) confJsonObject.get("raw_iot_stream_app_name");
        raw_iot_master_val = (String) confJsonObject.get("raw_iot_master_val");
        raw_iot_spark_serializer = (String) confJsonObject.get("raw_iot_spark_serializer");
        raw_iot_spark_driver_memory = (String) confJsonObject.get("raw_iot_spark_driver_memory");
        raw_iot_streaming_batch_size = (Long) confJsonObject.get("raw_iot_streaming_batch_size");
        raw_iot_streaming_block_size = (String) confJsonObject.get("raw_iot_streaming_block_size");
        raw_iot_server_ip = (String) confJsonObject.get("server_ip");
        raw_iot_spark_kafka_port = (String) confJsonObject.get("raw_iot_spark_kafka_port");
        raw_iot_kafka_cons_id = (String) confJsonObject.get("raw_iot_kafka_cons_id_primary");
        raw_iot_kafka_cons_group_id = (String) confJsonObject.get("raw_iot_kafka_cons_group_id_primary");
        zk_node_conn_port = (String) confJsonObject.get("zk_node_conn_port");
        raw_iot_zk_conn_timeout_milsec = (String) confJsonObject.get("raw_iot_zk_conn_timeout_milsec");
        kafka_iot_main_topic = (String) confJsonObject.get("kafka_iot_main_topic");
        raw_iot_spark_checkpoint_dir = (String) confJsonObject.get("raw_iot_spark_checkpoint_dir");
        raw_iot_tab_name = (String) confJsonObject.get("hbase_table_primary");
        raw_iot_tab_colfam = (String) confJsonObject.get("hbase_raw_data_tab_colfam");
        
        secondary_iot_tab_name = (String) confJsonObject.get("hbase_table_secondary");
        secondary_iot_tab_colfam = (String) confJsonObject.get("hbase_table_secondary_colfam");

        push_trigger_deviceInput_tab_name = "push_trigger_device_input_tab";
        push_trigger_deviceInput_tab_colfam = "push_trigger_device_input_cf";
        push_trigger_deviceStats_tab_name = "push_trigger_device_stats_tab";
        push_trigger_deviceStats_tab_colfam = "push_trigger_device_stats_cf";
        
        hbase_master_ip = (String) confJsonObject.get("server_ip");
        hbase_master_port = (String) confJsonObject.get("hbase_master_port");
        hbase_zookeeper_port = (String) confJsonObject.get("hbase_zookeeper_port");
        // avro deserialization
        iotRawDatumReader = new SpecificDatumReader<RcvdData>(RcvdData.getClassSchema());
		// set spark conf
		sparkConf = new SparkConf().setAppName(iot_app_name)
				.setMaster(raw_iot_master_val)
	    		.set("spark.serializer", raw_iot_spark_serializer)
	    		.set("spark.driver.memory", raw_iot_spark_driver_memory)
	    		.set("spark.streaming.blockInterval", raw_iot_streaming_block_size)
				.set("spark.driver.allowMultipleContexts", "true")
				.set("spark.scheduler.mode", "FAIR");
	    // Create the context with the given batch size
	    jssc = new JavaStreamingContext(sparkConf, new Duration(raw_iot_streaming_batch_size));
	    javasc = new JavaSparkContext(sparkConf);	    
	    // spark streaming props
	    props = new HashMap<String, String>();
	    props.put("metadata.broker.list", raw_iot_server_ip+":"+raw_iot_spark_kafka_port);
		props.put("kafka.consumer.id", raw_iot_kafka_cons_id);
		props.put("group.id", raw_iot_kafka_cons_group_id);
		props.put("zookeeper.connect", raw_iot_server_ip + ":" +  zk_node_conn_port);
		props.put("zookeeper.connection.timeout.ms", raw_iot_zk_conn_timeout_milsec);
		// conf for reading from/ writing to hbase
 		hbaseConf = HBaseConfiguration.create();
	    hbaseConf.set("hbase.master",hbase_master_ip + ":" + hbase_master_port);
	    hbaseConf.set("hbase.zookeeper.quorum", hbase_master_ip);
	    hbaseConf.set("hbase.zookeeper.property.clientPort", hbase_zookeeper_port);
	    // settings for writing to iot raw data table
	    newAPIJobConfigIotRaw = Job.getInstance(hbaseConf);
	    newAPIJobConfigIotRaw.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, raw_iot_tab_name);
	    newAPIJobConfigIotRaw.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.class);
	    // secondary table
	    newAPIJobConfigIotSecondary = Job.getInstance(hbaseConf);
	    newAPIJobConfigIotSecondary.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, secondary_iot_tab_name);
	    newAPIJobConfigIotSecondary.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.class);
	    // tp device info table
	    newAPIJobConfigTPDevice = Job.getInstance(hbaseConf);
	    newAPIJobConfigTPDevice.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, push_trigger_deviceInput_tab_name);
	    newAPIJobConfigTPDevice.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.class);
	    hbaseUtilsObj = new HbaseUtils(confTypeFile);
	}
	
	
	@SuppressWarnings("deprecation")
	public static void getIotRawData() throws ClassNotFoundException, IOException {
		// Make a topic map and assign threads to the topics
		// Currently we have 1 topic and 1 thread assigned to it.
		try {
			int numThreads = numThreadsTopics;
			// we replaced the "createStream" method used below with "createDirectStream"
		    // method - fault tolerance is better with "createDirectStream".
			Map<String, Integer> topicMap = new HashMap<String, Integer>();
		    String[] topics = kafka_iot_main_topic.split(",");
		    for (String topic: topics) {
		    	//System.out.println("1 ********"+topic);
		        topicMap.put(topic, numThreads);
		    }
		    String topicsDirct = kafka_iot_main_topic;
		    HashSet<String> topicsSet = new HashSet<String>(Arrays.asList(topicsDirct.split(",")));
		    
		    
		    JavaPairInputDStream<String, byte[]> messages = KafkaUtils.createDirectStream(
		            jssc,
		            String.class,
		            byte[].class,
		            StringDecoder.class,
		            DefaultDecoder.class,
		            props,
		            topicsSet
		        );
		    
		    
		    // Read the Trigger Push campaign list once the application is started.
		    ptCmpgnMap = hbaseUtilsObj.queryPTCampaignMapData();

		    
		    // get the iot raw data objects, deserialize them using the (Java) Serializable
		    // interface (rather than the default avro class/interface). We do this as avro schema classes
		    // don't implement the Serializable interface of Java and are not directly compatible 
		    // with Spark.
		    JavaDStream<SerializableIotData> allIoTData = messages.map(new Function<Tuple2<String, byte[]>, SerializableIotData>() {
		        public SerializableIotData call(Tuple2<String, byte[]> tuple2) throws ClassNotFoundException, IOException {
		        	// Deserialize the data from avro format
		        	iotDecoder = DecoderFactory.get().binaryDecoder(tuple2._2, null);
		        	RcvdData avroDataPacket= iotRawDatumReader.read(null, iotDecoder);
		        	SerializableIotData iotDataPacket = new SerializableIotData(avroDataPacket);
		        	logger.info("iotDataPacket: " + avroDataPacket.getAppSecret() + "__" + avroDataPacket.getDeviceId() + "__" + avroDataPacket.getPacketId());
		            return iotDataPacket;
		        }
		      });
		    
		    // cache allIoTData RDD for future use
		    allIoTData.cache();
		    
		    
		 // check the incoming data for any push trigger related events that are active
		    JavaDStream<SerializableIotData> ptCmpgnData = allIoTData.filter(
		    		new Function<SerializableIotData, Boolean>() {
		    	        public Boolean call(SerializableIotData iotObj) throws IOException {
	    	        		boolean recExists = false;
	    	        		logger.info("PT Check Appsec -->" +  iotObj.getAppSecret());
	    	        		// loop through and find events related packets
	    	        		for(PacketData currPd : iotObj.getPacket()){
								  if( currPd.getEvents() != null ){
									  if ( currPd.getEvents().getEvent() != null ){
										  logger.info("PT Campaign events -->" +  currPd.getEvents().getEvent());
										  // check if there is the given event property and value are present
										// get properties in a hashmap and put the data
						                   if ( currPd.getEvents().getProperties() != null ){
						                       Map<String,String> currEventsMap = new HashMap<String,String>();
						                       currEventsMap = currPd.getEvents().getProperties();
						                       for (Map.Entry<String, String> entry : currEventsMap.entrySet())
						                       {
						                    	   String currEventProp = entry.getKey();
						                    	   String currEventPropVal = entry.getValue();
						                    	   logger.info("PT Campaign currEventPropVal -->" +  currEventProp + "__" + currEventPropVal);
						                    	   // create the (part of the) key to check in campaign Map
						                    	   // basically except for the compare op create the key and check
//						                    	   String currPartKey = iotObj.getAppSecret() + "__" +
//						                    			   currPd.getEvents().getEvent() +
//						                    			    "__" + currEventProp + "__" + currEventPropVal;
						                    	   String currPartKey = iotObj.getAppSecret() + "__" +
						                    			   currPd.getEvents().getEvent() +
						                    			    "__" + currEventProp;
						                    	   logger.info("PT key from dev-->" + currPartKey);
						                    	   // check for compareOp vals that are ge or le
						                    	   // if ge/le campaigns exist, check if the eventpropvals
						                    	   // are long/numeric types, otherwise there is no sense to continue.
						                    	   // If its a numeric value, check if they satisfy ge/le conditions.
						                    	   // if campaigns are equals type compare ops then strings work as well.
						                    	   // ge check
						                    	   if (ptCmpgnMap.containsKey(currPartKey + "__ge")){
						                    		   
						                    		   for (PTCampaignBean currKeyCurrPtcBean : ptCmpgnMap.get(currPartKey + "__ge")) {
						                    			   if ( StringUtils.isNumeric(currEventPropVal) ){
						                    				   long propValNum = Long.parseLong(currEventPropVal);
						                    				   if ( StringUtils.isNumeric(currKeyCurrPtcBean.geteventPropValue()) ){
						                    					   long givenPropValThreshold = Long.parseLong( currKeyCurrPtcBean.geteventPropValue() );
						                    					   if ( propValNum >= givenPropValThreshold){
						                    						   logger.info("PT Campaign key match-->" + currPartKey + "__ge" + propValNum + " >= " + givenPropValThreshold);
								                    				   return true;
						                    					   }
						                    				   }
						                    			   }
						                    		   }
						                    	   }
						                    	   // le check
						                    	   if (ptCmpgnMap.containsKey(currPartKey + "__le")){
						                    		   
						                    		   
						                    		   for (PTCampaignBean currKeyCurrPtcBean : ptCmpgnMap.get(currPartKey + "__le")) {
						                    			   long propValNum = Long.parseLong(currEventPropVal);
						                    			   if ( StringUtils.isNumeric(currKeyCurrPtcBean.geteventPropValue()) ){
						                    				   long givenPropValThreshold = Long.parseLong( currKeyCurrPtcBean.geteventPropValue() );
						                    				   if ( propValNum <= givenPropValThreshold){
							                    				   logger.info("PT Campaign key match-->" + currPartKey + "__le" + propValNum + " <= " + givenPropValThreshold);
							                    				   return true;
							                    			   }
						                    			   }
						                    			   
						                    		   }  
						                    	   }
						                    	   // equals check
						                    	   if (ptCmpgnMap.containsKey(currPartKey + "__equals")){
						                    		   
						                    		   for (PTCampaignBean currKeyCurrPtcBean : ptCmpgnMap.get(currPartKey + "__equals")) {
						                    			   String propValString = currEventPropVal;
						                    			   String givenPropValThreshold = currKeyCurrPtcBean.geteventPropValue();
						                    			   if ( propValString.equals(givenPropValThreshold)){
						                    				   logger.info("PT Campaign key match-->" + currPartKey + "__equals" + propValString);
						                    				   return true;
						                    			   }
						                    		   }
						                    	   }
						                       }
						                   }
									  }
								  }
		    	        	}
		    	        	return recExists;
		    	        }
		    	    }
		    );
		    
		    JavaPairDStream<SerializableIotData, PTCampaignBean> putTPCmpgnIotInfoData = ptCmpgnData.mapToPair(new PairFunction<SerializableIotData, SerializableIotData, PTCampaignBean>() {
				public Tuple2<SerializableIotData, PTCampaignBean> call(SerializableIotData iotObj)
						throws Exception {
					for(PacketData currPd : iotObj.getPacket()){
						  if( currPd.getEvents() != null ){
							  if ( currPd.getEvents().getEvent() != null ){
								  logger.info("PT Campaign events 2-->" +  currPd.getEvents().getEvent());
								  // check if there is the given event property and value are present
								// get properties in a hashmap and put the data
				                   if ( currPd.getEvents().getProperties() != null ){
				                       Map<String,String> currEventsMap = new HashMap<String,String>();
				                       currEventsMap = currPd.getEvents().getProperties();
				                       for (Map.Entry<String, String> entry : currEventsMap.entrySet())
				                       {
				                    	   String currEventProp = entry.getKey();
				                    	   String currEventPropVal = entry.getValue();
				                    	   logger.info("PT Campaign currEventPropVal 2-->" +  currEventProp + "__" + currEventPropVal);
				                    	   // create the (part of the) key to check in campaign Map
				                    	   // basically except for the compare op create the key and check
//				                    	   String currPartKey = iotObj.getAppSecret() + "__" +
//				                    			   currPd.getEvents().getEvent() +
//				                    			    "__" + currEventProp + "__" + currEventPropVal;
				                    	   String currPartKey = iotObj.getAppSecret() + "__" +
				                    			   currPd.getEvents().getEvent() +
				                    			    "__" + currEventProp;
				                    	   // check for compareOp vals that are ge or le
				                    	   // if ge/le campaigns exist, check if the eventpropvals
				                    	   // are long/numeric types, otherwise there is no sense to continue.
				                    	   // If its a numeric value, check if they satisfy ge/le conditions.
				                    	   // if campaigns are equals type compare ops then strings work as well.
				                    	   // ge check
				                    	   if (ptCmpgnMap.containsKey(currPartKey + "__ge")){
				                    		   
				                    		   Long selPropValNum = null;
				                    		   PTCampaignBean selCampgnBean = null;
				                    		   for (PTCampaignBean currKeyCurrPtcBean : ptCmpgnMap.get(currPartKey + "__ge")) {
				                    			   if ( StringUtils.isNumeric(currEventPropVal) ){
					                    			   long propValNum = Long.parseLong(currEventPropVal);
					                    			   if ( StringUtils.isNumeric(currKeyCurrPtcBean.geteventPropValue()) ){
					                    				   long givenPropValThreshold = Long.parseLong( currKeyCurrPtcBean.geteventPropValue() );
					                    				   if ( propValNum >= givenPropValThreshold){
					                    					   logger.info("PT Campaign key condition match 2-->" + currPartKey +
					                    							   "__ge" + propValNum + " >= " + givenPropValThreshold);
					                    					   if (selPropValNum == null){
					                    						   selPropValNum = givenPropValThreshold;
					                    						   selCampgnBean = currKeyCurrPtcBean;
					                    					   }else{
					                    						   if (selPropValNum < givenPropValThreshold){
					                    							   logger.info("PT Campaign key match 2 ge UPDATED!");
					                    							   selPropValNum = givenPropValThreshold;
						                    						   selCampgnBean = currKeyCurrPtcBean;
					                    						   }
					                    					   }
				                    					   }
					                    				   
					                    			   }
				                    			   }
				                    		   }
				                    		   if (selCampgnBean != null){
				                    			   logger.info("PT Campaign key match 2-->" + currPartKey + "__ge" +
				                    					   selPropValNum + " >= " + selCampgnBean.geteventPropValue());
				                    			   return new Tuple2<SerializableIotData, PTCampaignBean>(iotObj, selCampgnBean);
				                    		   }
				                    		   else{
				                    			   logger.info("SOMETHING WRONG GE COMPARE OP!! CHECK CODE/LOGS BRO!");
				                    		   }
				                    		   
				                    	   }
				                    	   // le check
				                    	   if (ptCmpgnMap.containsKey(currPartKey + "__le")){
				                    		   
				                    		   
				                    		   Long selPropValNum = null;
				                    		   PTCampaignBean selCampgnBean = null;
				                    		   for (PTCampaignBean currKeyCurrPtcBean : ptCmpgnMap.get(currPartKey + "__le")) {
				                    			   if ( StringUtils.isNumeric(currEventPropVal) ){
					                    			   long propValNum = Long.parseLong(currEventPropVal);
					                    			   if ( StringUtils.isNumeric(currKeyCurrPtcBean.geteventPropValue()) ){
					                    				   long givenPropValThreshold = Long.parseLong( currKeyCurrPtcBean.geteventPropValue() );
					                    				   if ( propValNum <= givenPropValThreshold){
					                    					   logger.info("PT Campaign key match 2-->" + currPartKey + "__le" +
					                    							   propValNum + " <= " + givenPropValThreshold);
					                    					   if (selPropValNum == null){
					                    						   selPropValNum = givenPropValThreshold;
					                    						   selCampgnBean = currKeyCurrPtcBean;
					                    					   }else{
					                    						   if (selPropValNum > givenPropValThreshold){
					                    							   logger.info("PT Campaign key match 2 le UPDATED!");
					                    							   selPropValNum = givenPropValThreshold;
						                    						   selCampgnBean = currKeyCurrPtcBean;
					                    						   }
					                    					   }
					                    				   }
					                    				   
					                    			   }
				                    			   }
				                    		   }
				                    		   if (selCampgnBean != null){
				                    			   logger.info("PT Campaign key match 2-->" + currPartKey + "__le" +
				                    					   selPropValNum + " <= " + selCampgnBean.geteventPropValue());
				                    			   return new Tuple2<SerializableIotData, PTCampaignBean>(iotObj, selCampgnBean);
				                    		   }
				                    		   else{
				                    			   logger.info("SOMETHING WRONG LE COMPARE OP!! CHECK CODE/LOGS BRO!");
				                    		   }
				                    	   }
				                    	   // equals check
				                    	   if (ptCmpgnMap.containsKey(currPartKey + "__equals")){
				                    			   String propValString = currEventPropVal;
				                    			   for (PTCampaignBean currKeyCurrPtcBean : ptCmpgnMap.get(currPartKey + "__equals")) {
				                    				   if ( propValString.equals(currKeyCurrPtcBean.geteventPropValue())){
				                    					   logger.info("PT Campaign key match 2-->" + currPartKey + "__equals" + propValString);
				                    					   logger.info("PT Campaign key match 2-->" + currPartKey + "__equals" + propValString);
					                    				   return new Tuple2<SerializableIotData, PTCampaignBean>(iotObj, currKeyCurrPtcBean);
				                    				   }
				                    			   }
				                    	   }
				                       }
				                   }
							  }
						  }
  	        	}
					return null; 
				}
			});
		    
		    putTPCmpgnIotInfoData.foreachRDD(new Function<JavaPairRDD<SerializableIotData, PTCampaignBean>, Void>(){ 
		    	public Void call(JavaPairRDD<SerializableIotData, PTCampaignBean> ptIotCmpgnInfoList) throws IOException {
		    		if (ptIotCmpgnInfoList.count() > 0) {
		    			JavaPairRDD<ImmutableBytesWritable, Put> putIotCmpgnDataPacket = ptIotCmpgnInfoList.mapToPair(new PairFunction<Tuple2<SerializableIotData, PTCampaignBean>, ImmutableBytesWritable, Put>() {
							public Tuple2<ImmutableBytesWritable, Put> call(Tuple2<SerializableIotData, PTCampaignBean> iotPTCmpgnDevObj)
									throws Exception {
								logger.info("ptData device info to hbase--->" + iotPTCmpgnDevObj._1.getDeviceId() +
										"__" + iotPTCmpgnDevObj._1.getAppSecret() + "__" + iotPTCmpgnDevObj._2.getcampaignId()
										+ "__" + iotPTCmpgnDevObj._2.geteventName());
								logger.info("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ CAMPAIGN TITLE IS: " + iotPTCmpgnDevObj._2.getCampaignTitle());
								return hbaseUtilsObj.cnvrtIotPushTriggerDevInfoToPut(iotPTCmpgnDevObj._1, iotPTCmpgnDevObj._2);
							}
						});
		    			putIotCmpgnDataPacket.saveAsNewAPIHadoopDataset(newAPIJobConfigTPDevice.getConfiguration());
		    		}
					return null;
		      }});
		    
		    
		    // put all the raw data in hbase primary, secondary and query for new PT campaign
		    allIoTData.foreachRDD(new Function<JavaRDD<SerializableIotData>, Void>(){ 
		    	public Void call(JavaRDD<SerializableIotData> iotRawList) throws IOException {
		    		if (iotRawList.count() > 0) {
		    			// store data in raw iot table table
		    			JavaPairRDD<ImmutableBytesWritable, Put> putIotRawData = iotRawList.mapToPair(new PairFunction<SerializableIotData, ImmutableBytesWritable, Put>() {
							public Tuple2<ImmutableBytesWritable, Put> call(SerializableIotData iotRawDataObj)
									throws Exception {
								logger.info("iotDataPacket primary to hbase--->" + iotRawDataObj.getAppSecret() + "__" + iotRawDataObj.getDeviceId() + "__" + iotRawDataObj.getPacketId());
								
								return hbaseUtilsObj.cnvrtIotRawDataToPut(iotRawDataObj); 
							}
						});
		    			putIotRawData.saveAsNewAPIHadoopDataset(newAPIJobConfigIotRaw.getConfiguration());
		    			// store data in iot secondary table
		    			JavaPairRDD<ImmutableBytesWritable, Put> putIotSecondaryData = iotRawList.mapToPair(new PairFunction<SerializableIotData, ImmutableBytesWritable, Put>() {
							public Tuple2<ImmutableBytesWritable, Put> call(SerializableIotData iotRawDataObj)
									throws Exception {
								logger.info("iotDataPacket Secondary to hbase--->" + iotRawDataObj.getAppSecret() + "__" + iotRawDataObj.getDeviceId() + "__" + iotRawDataObj.getPacketId());
								
								return hbaseUtilsObj.cnvrtIotSecondaryDataToPut(iotRawDataObj); 
							}
						});
		    			putIotSecondaryData.saveAsNewAPIHadoopDataset(newAPIJobConfigIotSecondary.getConfiguration());
		    		}
		    		// also query Trigger Push campaign list once the application is started.
		    		ptCmpgnMap = hbaseUtilsObj.queryPTCampaignMapData();
					return null;
		      }});
		    
		    
		    
		    
		    
		} catch (Exception e) {
	        e.printStackTrace();  
	    }  
		
		
	    
	    System.out.println("Spark Streaming started....");
	    jssc.checkpoint(raw_iot_spark_checkpoint_dir);
	    jssc.start();
	    jssc.awaitTermination();
	    System.out.println("Stopped Spark Streaming");
	    
		
	}

	
	public static void main(String[] args) throws IOException, ClassNotFoundException, ParseException {
		
		IotDataStreamer iotStrmObj = new IotDataStreamer();
		iotStrmObj.getIotRawData();
	}

}
