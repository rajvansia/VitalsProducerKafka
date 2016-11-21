package com.rajvansia.kafka;
import java.util.Properties;
import java.util.Random;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
public class VitalsProducer {

		 
		  public static void main(String[] args) {
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
		    
		    Producer<String, String> producer = null;
		    Producer<String, Integer> producerInt = null;
		    
		    Random rand = new Random();
		    try {
		      producer = new KafkaProducer<String,String>(props);
		      producerInt = new KafkaProducer<String,Integer>(propsInt);
		      for (int i = 0; i < 500; i++) {
		    	int SpO2 = rand.nextInt(100 - 20) + 20;
		        String msg = "SpO2 " + Integer.toString(SpO2);
		        producer.send(new ProducerRecord<String, String>("vitals", msg));
		        producerInt.send(new ProducerRecord<String, Integer>("vitals", i));
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

