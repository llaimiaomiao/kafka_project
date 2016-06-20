package com.miao.kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

public class ConsumeDemo {
	private static final String topic ="myBaby2";
	public static void main(String[] args) {
		Properties prop = new Properties();
		prop.put("zookeeper.connect", "MiaoMiao01:2181,MiaoMiao02:2181,MiaoMiao03:2181");
		prop.put("group.id", "1111");
		prop.put("auto.offset.reset", "smallest");
		ConsumerConfig config = new ConsumerConfig(prop);
		ConsumerConnector consumer = Consumer.createJavaConsumerConnector(config);
		Map<String, Integer> topicMap = new HashMap<String,Integer>();
		
		topicMap.put(topic, 1);
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicMap);
		List<KafkaStream<byte[], byte[]>> streamList = consumerMap.get(topic);
		for (final KafkaStream<byte[], byte[]> kafkaStream :streamList){
			new Thread(){
				public void run() {
					for (MessageAndMetadata<byte[], byte[]> mm : kafkaStream) {
						String msg = new String(mm.message());
						System.out.println(msg);
						
					}
				};
			}.start();
		};
	}

}
