package com.iot.data.processes;

import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.iot.data.schema.PacketData;
import com.iot.data.schema.RcvdData;
import com.iot.data.schema.SerializableIotData;
import com.iot.data.utils.HbaseUtils;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import scala.Tuple2;

@SuppressWarnings("deprecation")
public class HbaseTableEvents {
	//misc variables
	private static ObjectMapper mapper;
	
	//conf file variables
	private static String confFileName;
	private static String confTypeFile;
	private static JSONParser confParser = new JSONParser();
	private static Object confObj;
	private static JSONObject confJsonObject;
	
	//kafka variables
	private static Properties props;
	private static String kafka_consumer_id_events;
	private static String kafka_consumer_group_id_events;
	private static String kafka_iot_main_topic;
	private static String raw_iot_server_ip;
	private static String raw_iot_spark_kafka_port;
	private static String zk_node_conn_port;
	private static String raw_iot_zk_conn_timeout_milsec;
	private static int NO_OF_THREADS;
	
	//avro variables
	private static DatumReader<RcvdData> iotRawDatumReader;
	private static Decoder iotDecoder;
	
	//hbase variables
	private static String hbaseTableEvents1;
	private static String hbaseTableEvents1_colfam;
	private static Configuration hbaseIotConf;
	private static Put p;
	private static String hbase_master_port;
	private static String hbase_zookeeper_port;
	private static HTable hbaseTabEvents;
	
	private static HbaseUtils hbaseUtilsObj;
	private final static Logger logger = LoggerFactory.getLogger(HbaseTableEvents.class);
	
	static {
		try {
			//json related
			mapper = new ObjectMapper();
			
			//conf file settings
			confTypeFile = "production_conf.json";
			confFileName = "/home/iot/fsociety/dal/conf/" + confTypeFile;
			//confFileName = "/Users/sahil/Desktop/random/conf/" + confTypeFile;
			confObj = confParser.parse(new FileReader(confFileName));
	        confJsonObject = (JSONObject) confObj;
	        raw_iot_server_ip = (String) confJsonObject.get("server_ip");
	        raw_iot_spark_kafka_port = (String) confJsonObject.get("raw_iot_spark_kafka_port");
	        zk_node_conn_port = (String) confJsonObject.get("zk_node_conn_port");
	        raw_iot_zk_conn_timeout_milsec = (String) confJsonObject.get("raw_iot_zk_conn_timeout_milsec");
	        hbase_master_port = (String) confJsonObject.get("hbase_master_port");
			hbase_zookeeper_port = (String) confJsonObject.get("hbase_zookeeper_port");
	       
	        //hbase settings
	        hbaseTableEvents1 = (String) confJsonObject.get("hbase_table_events1");
	        hbaseTableEvents1_colfam = (String) confJsonObject.get("hbase_table_events1_colfam");
	        
	        //avro deserialization
	        iotRawDatumReader = new SpecificDatumReader<RcvdData>(RcvdData.getClassSchema());
	        
	        hbaseUtilsObj = new HbaseUtils(confTypeFile);
	        
	        //kafka and zookeeper settings
	        kafka_iot_main_topic = (String) confJsonObject.get("kafka_iot_main_topic");
	        kafka_consumer_id_events = (String)confJsonObject.get("raw_iot_kafka_cons_id_events");
			kafka_consumer_group_id_events = (String)confJsonObject.get("raw_iot_kafka_cons_group_id_events");
	        props = new Properties();
			props.put("metadata.broker.list", raw_iot_server_ip + ":" + raw_iot_spark_kafka_port);
			props.put("kafka.consumer.id", kafka_consumer_id_events);
			props.put("group.id", kafka_consumer_group_id_events);
			props.put("zookeeper.connect", raw_iot_server_ip + ":" +  zk_node_conn_port);
			props.put("zookeeper.connection.timeout.ms", raw_iot_zk_conn_timeout_milsec);
			NO_OF_THREADS = 1;
	        
			//hbase settings
			hbaseIotConf = HBaseConfiguration.create();
		    hbaseIotConf.set("hbase.master", raw_iot_server_ip + ":" + hbase_master_port);
		    hbaseIotConf.set("hbase.zookeeper.quorum", raw_iot_server_ip);
		    hbaseIotConf.set("hbase.zookeeper.property.clientPort", hbase_zookeeper_port);
		    
		    hbaseTabEvents = new HTable(hbaseIotConf, hbaseTableEvents1);
		} catch(IOException e) {
			e.printStackTrace();
		} catch(ParseException p) {
			p.printStackTrace();
		}
	}
	
	private void setupEventsConsumer() {
		try {
			while(true) {
				System.out.println("Started Kafka Consumer to listen on topic " + kafka_iot_main_topic + "...");
				ConsumerConfig consumerConf = new ConsumerConfig(props);
				ConsumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(consumerConf);
	 	
				Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
				topicCountMap.put(kafka_iot_main_topic, new Integer(NO_OF_THREADS));
	 		
				Map<String, List<KafkaStream<byte[], byte[]>>> iotStreamMap = consumer.createMessageStreams(topicCountMap);
				List<KafkaStream<byte[], byte[]>> iotStreams = iotStreamMap.get(kafka_iot_main_topic);
				KafkaStream<byte[], byte[]> iotRawStream = iotStreams.get(0);
	 	
				ConsumerIterator<byte[], byte[]> iterator = iotRawStream.iterator();
	 		
				while(iterator.hasNext()) {
					byte[] avroObject = iterator.next().message();
					iotDecoder = DecoderFactory.get().binaryDecoder(avroObject, null);
					RcvdData avroDataPacket = iotRawDatumReader.read(null, iotDecoder);
					SerializableIotData iotDataPacket = new SerializableIotData(avroDataPacket);
	 				logger.info("iot data packet received in EVENTS =======> " + iotDataPacket.getAppSecret() + "__" + iotDataPacket.getPacketId());
	 				
	 				//dump data in Events table
	 				dumpInEventsHbaseTable(iotDataPacket);
				}
			}
		} catch(IOException e) {
			e.printStackTrace();
		}	
	}
	
	@SuppressWarnings("deprecation")
	private static void dumpInEventsHbaseTable(SerializableIotData iotDataPacket) {
		try {
			int ctr = 1;
			String currentRowKey, eventPropJSON;
		
			for(PacketData currPacket : iotDataPacket.getPacket()) {
				//events data
				if(currPacket.getEvents() != null) {
					currentRowKey = iotDataPacket.getPacketId() + "__" + ctr;
					ctr++;
					//put the data
					p = new Put(Bytes.toBytes(currentRowKey));
					//add corresponding events data to events preprocess table
					p.add(Bytes.toBytes(hbaseTableEvents1_colfam), Bytes.toBytes("packet_id"), Bytes.toBytes(iotDataPacket.getPacketId()));
					p.add(Bytes.toBytes(hbaseTableEvents1_colfam), Bytes.toBytes("app_secret"), Bytes.toBytes(iotDataPacket.getAppSecret()));
					p.add(Bytes.toBytes(hbaseTableEvents1_colfam), Bytes.toBytes("device_id"), Bytes.toBytes(iotDataPacket.getDeviceId()));
					p.add(Bytes.toBytes(hbaseTableEvents1_colfam), Bytes.toBytes("server_time"), Bytes.toBytes(iotDataPacket.getServerTime()));
					
					if(currPacket.getEvents() != null) {
						if(currPacket.getEvents().getEvent() != null) {
							p.add(Bytes.toBytes(hbaseTableEvents1_colfam), Bytes.toBytes("event_name"), Bytes.toBytes(currPacket.getEvents().getEvent()));
						}
					}
			
					if(currPacket.getEvents().getProperties() != null) {
						Map<String, String> eventProps = new HashMap<String, String>();
						eventProps = currPacket.getEvents().getProperties();
						eventPropJSON = mapper.writeValueAsString(eventProps);
						if(eventPropJSON != null) {
							p.add(Bytes.toBytes(hbaseTableEvents1_colfam), Bytes.toBytes("event_properties"), Bytes.toBytes(eventPropJSON));
						}
					}
					
					hbaseTabEvents.put(p);
				}
			}
			
		} catch(IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		
		HbaseTableEvents events = new HbaseTableEvents();	
		events.setupEventsConsumer();
		
	}
	
}