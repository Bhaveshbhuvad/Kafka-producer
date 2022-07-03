package io.classpath.kafka.examples.producer;

import java.util.Properties;
import java.util.stream.IntStream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import io.classpath.kafka.examples.config.AppConfig;


public class ProducerDemo {
	
	public static void main(String[] args) {
		Properties producerProperties = new Properties();
		producerProperties.put(ProducerConfig.CLIENT_ID_CONFIG, AppConfig.CLIENT_ID);
		producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfig.BOOTSTRAP_SERVERS);
		producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
		producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		
		
		KafkaProducer<Integer, String> kafkaProducer = new KafkaProducer(producerProperties);
		
		IntStream.range(0, 100).forEach(index -> {
			ProducerRecord<Integer, String> record = new ProducerRecord<>(AppConfig.TOPIC, index, "Message-"+index);
			kafkaProducer.send(record);
		});
		
		kafkaProducer.close();
	}

}
