package com.rajvansia.kafka;
import java.util.Properties;
import java.util.Random;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

public class VitalsProducer {

	
	 
		  public static void main(String[] args) {
			  
			    final String USER_SCHEMA = "{"
			            + "\"type\":\"record\","
			            + "\"name\":\"myrecord\","
			            + "\"fields\":["
			            + "  { \"name\":\"str1\", \"type\":\"string\" },"
			            + "  { \"name\":\"str2\", \"type\":\"string\" },"
			            + "  { \"name\":\"int1\", \"type\":\"int\" }"
			            + "]}";
			    
			 
			    
		    Properties props = new Properties();
		    props.put("bootstrap.servers", "127.0.0.1:9092");
		    props.put("acks", "all");
		    props.put("retries", 0);
		    props.put("batch.size", 16384);
		    props.put("linger.ms", 1);
		    props.put("buffer.memory", 33554432);
		    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		    
		    Properties propsInt = new Properties();
		    propsInt.put("bootstrap.servers", "127.0.0.1:9092");
		    propsInt.put("acks", "all");
		    propsInt.put("retries", 0);
		    propsInt.put("batch.size", 16384);
		    propsInt.put("linger.ms", 1);
		    propsInt.put("buffer.memory", 33554432);
		    propsInt.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
		    propsInt.put("value.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
		    
		    Properties propsAvro = new Properties();
		    propsAvro.put("bootstrap.servers", "127.0.0.1:9092");
		    propsAvro.put("acks", "all");
		    propsAvro.put("retries", 0);
		    propsAvro.put("batch.size", 16384);
		    propsAvro.put("linger.ms", 1);
		    propsAvro.put("buffer.memory", 33554432);
		    propsAvro.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		    propsAvro.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
		    
		    Producer<String, String> producer = null;
		    Producer<String, Integer> producerInt = null;
		    Producer<String, byte[]> producerAvro = null;
		    
		    Schema.Parser parser = new Schema.Parser();
	        Schema schema = parser.parse(USER_SCHEMA);
	        Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);
		    
		    Random rand = new Random();
		    try {  
		      producer = new KafkaProducer<String,String>(props);
		      producerInt = new KafkaProducer<String,Integer>(propsInt);
		      producerAvro = new KafkaProducer<String, byte[]>(propsAvro);
		     

		    
		      for (int i = 0; i < 100; i++) {
		    	int SpO2 = rand.nextInt(100 - 80) + 80;
		    	String ip = "989898";
		        String msg = "SpO2 " + Integer.toString(SpO2);
		        GenericData.Record avroRecord = new GenericData.Record(schema);
		        avroRecord.put("str1", "Str 1-" + ip);
	            avroRecord.put("str2", "Str 2-" + ip);
	            avroRecord.put("int1", SpO2);
	            
		        producer.send(new ProducerRecord<String, String>("vitals", msg));
		        producerInt.send(new ProducerRecord<String, Integer>("vitals", i));
		        byte[] bytes = recordInjection.apply(avroRecord);

	            producerAvro.send(new ProducerRecord<String, byte[]>("vitals", bytes));

		      
		        System.out.println("Sent:" + msg);
		        Thread.sleep(rand.nextInt(300 - 100) + 100);
		      }
		      for (int i = 0; i < 500; i++) {
		    	    int RR = rand.nextInt(100 - 20) + 20;
			        String msg = "RR " + Integer.toString(RR);
			        producer.send(new ProducerRecord<String, String>("vitals", msg));
			        producerInt.send(new ProducerRecord<String, Integer>("vitals", i));
			        System.out.println("SentRR:" + (RR));
			        Thread.sleep(rand.nextInt(30 - 10) + 10);
			      }
		    } catch (Exception e) {
		      e.printStackTrace();
		 
		    } finally {
		      producer.close();
		    }
		 
		  }
		 
		}

