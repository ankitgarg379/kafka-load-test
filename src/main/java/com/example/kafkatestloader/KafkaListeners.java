package com.example.kafkatestloader;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.fasterxml.jackson.databind.introspect.AnnotatedClass;
import com.fasterxml.jackson.databind.introspect.JacksonAnnotationIntrospector;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

@Slf4j
@Component
public class KafkaListeners {

    @KafkaListener(topics = "local_shipment_service_reply",  groupId = "group1")
    public void listenEvents(ConsumerRecord<String, String> consumerRecord) throws IllegalAccessException, InstantiationException, JsonProcessingException {
         try {
             log.error("Event listener: {} \n Time: {}" , consumerRecord.value(), convertTime(consumerRecord.timestamp()));

        } catch (Exception exception) {
            log.error("",exception);
        }
    }

    ObjectWriter objectWriter;
    ObjectReader objectReader;

    @KafkaListener(topics = "local_shipment_service_events", groupId = "group1", concurrency = "10")
    public void listenReply(@Payload String message,
                            @Header(name = KafkaHeaders.RECEIVED_MESSAGE_KEY, required = false) String key,
                            @Header(name = KafkaHeaders.RECEIVED_PARTITION_ID) int partition_id,
                            @Header(KafkaHeaders.OFFSET) Long offset,
                            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                            @Header("time1") String  time1,
                            @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long ts) throws IllegalAccessException, InstantiationException, JsonProcessingException {
        try {
            //x - sent time
            //y - db commit time
            //z - listener time
            Long timex = Long.valueOf(time1.substring(1,time1.length()-1));
            Long timez = ts;
//            Long timey = Long.valueOf(time2.substring(1,time2.length()-1));
//            Long timez = System.currentTimeMillis();

            Command.last_time = System.currentTimeMillis();
//            Command.db_latencies.add(timey-timex);
            Command.round_trip_latencies.add(timez-timex);
            log.error("Event listener Round trip time:{} \n Message: {}",  timez - timex);

//            log.error("Reply listener: \n DB time: {} ,Roundtrip time:{} \n Message: {}", timey - timex, timez - timex, message);

        } catch (Exception exception) {
            log.error("", exception);
        }
    }

    public String convertTime(long time) {
        Date date = new Date(time);
        Format format = new SimpleDateFormat("yyyy MM dd HH:mm:ss");
        return format.format(date);
    }
}
