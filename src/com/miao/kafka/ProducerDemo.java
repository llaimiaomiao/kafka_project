package com.miao.kafka;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;



public class ProducerDemo {
	public static void main(String[] args) throws Exception {
		Properties prop = new Properties();
		prop.put("zk.connect", "MiaoMiao01:2181,MiaoMiao02:2181,MiaoMiao03:2181");
		prop.put("metadata.broker.list", "MiaoMiao01:9092,MiaoMiao02:9092,MiaoMiao03:9092");
		prop.put("serializer.class", "kafka.serializer.StringEncoder");
		ProducerConfig config = new ProducerConfig(prop);
		Producer<String, String> producer = new Producer<String, String>(config);
		for (int i = 0; i < 1000; i++) {
			Thread.sleep(500);
			
			producer.send(new KeyedMessage<String, String>("myBaby2", "I say that I love you "+i+" times"));
			
		}
	}

}
