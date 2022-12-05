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
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@RestController
@Slf4j
public class Command {

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    ObjectReader objectReader;
    ObjectWriter objectWriter;

    static public List<Long> db_latencies = new ArrayList<>();
    static public List<Long> round_trip_latencies = new ArrayList<>();

    static public long first_time = 0;
    static public long last_time = 0;

    String msg1 = "";


    private static final String COMMAND_TOPIC = "local_shipment_service_command";

    @GetMapping("/publish/{message}")
    public String publishMessage(@PathVariable("message") final String message) {
        // Sending the message
        kafkaTemplate.send(COMMAND_TOPIC, message);
        return "Published Successfully";
    }

    @GetMapping("/clear")
    public String clear() {
        // Sending the message
        Collections.sort(db_latencies);
        Collections.sort(round_trip_latencies);
        String reply = "Total Events Received: " + round_trip_latencies.size();
        log.error("Calculating latencies of db_latencies");
//        reply = reply + percentile(db_latencies, 50) + "    " + percentile(db_latencies, 90) + "," + percentile(db_latencies, 99);
        reply = reply + " with p50 (" + percentile(round_trip_latencies, 50) + "), p90 (" + percentile(round_trip_latencies, 90) + "), p99 (" + percentile(round_trip_latencies, 99)+") ";
//        log.error("50th percentile- {}", percentile(db_latencies, 50));
//        log.error("90th percentile- {}", percentile(db_latencies, 90));
//        log.error("99th percentile- {}", percentile(db_latencies, 99));
        log.error("Calculating latencies of round_trip_latencies");
        log.error("50th percentile- {}", percentile(round_trip_latencies, 50));
        log.error("90th percentile- {}", percentile(round_trip_latencies, 90));
        log.error("99th percentile- {}", percentile(round_trip_latencies, 99));
        reply = reply + "   and  Total time: " + (Command.last_time - Command.first_time);
//                + " first time: " + Command.last_time + " last time: " + Command.first_time;
        db_latencies.clear();
        round_trip_latencies.clear();
        return msg1 + " ||     " + reply + "    Latencies cleared";
    }

    @GetMapping("/test/{itr}")
    public String test(@PathVariable("itr") final int itr) throws JsonProcessingException, InterruptedException {
        ObjectMapper mapper = new ObjectMapper();
        mapper.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.setAnnotationIntrospector(new JacksonAnnotationIntrospector() {
            @Override
            public JsonPOJOBuilder.Value findPOJOBuilderConfig(AnnotatedClass ac) {
                if (ac.hasAnnotation(JsonPOJOBuilder.class)) {//If no annotation present use default as empty prefix
                    return super.findPOJOBuilderConfig(ac);
                }
                return new JsonPOJOBuilder.Value("build", "");
            }
        });
        this.objectReader = mapper.reader();
        this.objectWriter = mapper.writer();
        log.error("Inside produce records");
        String createMessage = "{\"command_name\" : \"Create\",  \"tracking_number\": \"kjjnjnjkdjs\", \"order_number\": \"skdjskdj\", \"service_type\": \"EXPRESS\", \"shipment_type\": \"EXPRESS\", \"payment_mode\": \"COD\", \"origin_address\" : { \"address_line1\": \"alsjkjsd\", \"address_line2\": \"sddds\" }}";
        String key = "1047048178622271488";
        String updateMessage = "{\"command_name\" : \"Update\", \"payment_mode\": \"COD\"}";
        long a = System.currentTimeMillis();
        int jloop = 4;
        int interval = 3;
        Command.first_time = System.currentTimeMillis();
        for (int i = 0; i < itr; i++) {
            for (int j = 0; j < jloop; j++) {
//                kafkaTemplate.send(COMMAND_TOPIC,key, updateMessage);
                kafkaTemplate.send(COMMAND_TOPIC, createMessage);
            }
            Thread.sleep(interval);
        }

//        log.error("Time to send {} requests: {} with batch {} and interval {}", itr * jloop, System.currentTimeMillis() - a, jloop, interval);
        msg1 = "Time to send " + (itr * jloop) + " requests: " + (System.currentTimeMillis() - a) + " with batch " + jloop + " and interval " + interval;
        return msg1;

    }

    public static long percentile(List<Long> latencies, double percentile) {
        int index = (int) Math.ceil(percentile / 100.0 * latencies.size());
        return latencies.get(index - 1);
    }
}