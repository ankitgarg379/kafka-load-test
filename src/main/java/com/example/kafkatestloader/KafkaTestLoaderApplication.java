package com.example.kafkatestloader;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.core.KafkaTemplate;

@SpringBootApplication
@Slf4j
public class KafkaTestLoaderApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaTestLoaderApplication.class, args);
	}

//	public void producerRecords (KafkaTemplate<String, String> kafkaTemplate){
//		log.error("Inside produce records");
//		String commandTopic = "local_shipment_service_topic";
//		String message = "{\"command_name\" : \"Create\",  \"tracking_number\": \"kjkdjs\", \"order_number\": \"skdjskdj\", \"service_type\": \"EXPRESS\", \"shipment_type\": \"EXPRESS\", \"payment_mode\": \"COD\", \"origin_address\" : { \"address_line1\": \"alsjkjsd\", \"address_line2\": \"sddds\" }}";
//		kafkaTemplate.send(new ProducerRecord<>(commandTopic,null,message ));
//
//	}



}
