package com.example.kafkatestloader;


import com.fareye.shipment.entity.service.common.commands.UpsertCommand;
import com.fareye.shipment.entity.service.common.shipmentDTOs.*;
import com.fareye.shipment.entity.service.common.valueobjects.*;
import com.fareye.shipment.entity.service.common.valueobjects.Exception;
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

import java.time.LocalDateTime;
import java.util.*;

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


    private static final String COMMAND_TOPIC = "aw-or-dv-shipment-service-fe-command-topic";

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
        reply = reply + " with p50 (" + percentile(round_trip_latencies, 50) + "), p90 (" + percentile(round_trip_latencies, 90) + "), p99 (" + percentile(round_trip_latencies, 99) + ") ";
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


    @GetMapping("/test/{itr}/{update}")
    public String test(@PathVariable("itr") final int itr, @PathVariable("update") final boolean isUpdate) throws JsonProcessingException, InterruptedException {
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
        Long comp = 1L;
        //String createMessage = "{\"command_name\" : \"Create\", \"user_id\":10,  \"company_id\":" + comp + ", \"tracking_number\": \"kjkdjs\", \"order_number\": \"skdjskdj\", \"service_type\": \"EXPRESS\", \"shipment_type\": \"EXPRESS\", \"payment_mode\": \"COD\", \"origin_address\" : { \"address_line1\": \"alsjkjsd\", \"address_line2\": \"sddds\" }}";
        String exampleUpsertMessage = "{\"command_name\" : \"Upsert\",\"command_type\": \"update\",  \"user_id\":10,  \"company_id\":" + comp +
                ",\"order_number\": \"skdjskdj\", \"service_type\": \"EXPRESS\", \"shipment_type\": \"EXPRESS\", \"payment_mode\": \"COD\"," +
                " \"origin_address\" : { \"address_line1\": \"alsjkjsd\", \"address_line2\": \"sddds\" }}";


        UpsertCommand newUpsertCommandCreate = UpsertCommand.builder()
                .commandName("Upsert")
                .commandType("create")
                .companyId(3L)
                .userId(10L)
                .shipmentBasicDTO(ShipmentBasicDTO.builder()
                        .shipmentDetails(ShipmentDetails.builder()
                                .orderNumber("skdjskdj")
                                .serviceType("EXPRESS")
                                .itemDetails(getItemDetails())
                                .qualityCheckDetails(getQCs())
                                .build())
                        .origin(Origin.builder()
                                .originAddress(Address.builder()
                                        .addressLine1("34 east block")
                                        .addressLine2("rohini-86")
                                        .city("delhi")
                                        .code("DEL")
                                        .build())
                                .originPaymentDetails(Payment.builder()
                                        .amountToBeCollected(2.3)
                                        .paymentMode("Cash")
                                        .build())
                                .build())
                        .destination(Destination.builder()
                                .destinationAddress(Address.builder()
                                        .addressLine1("34 east block")
                                        .addressLine2("rohini-86")
                                        .city("delhi")
                                        .code("DEL")
                                        .build())
//                                .destinationEndTime(LocalDateTime.now())
                                .build())
                        .build())
                .shipmentMilestonesInfoDTO(ShipmentMilestonesInfoDTO.builder()
                        .merchantCode("mcode")
                        .merchantName("mname")
                        .carrierPlanningOnTimeFlag(true)
                        .carrierPlanningOnTimeFlag(false)
                        .exceptions(getException())
                        .build())
                .build();

        UpsertCommand newUpsertCommandUpdate = UpsertCommand.builder()
                .commandName("Upsert")
                .commandType("update")
                .companyId(2L)
                .userId(10L)
                .shipmentBasicDTO(ShipmentBasicDTO.builder()
                        .shipmentDetails(ShipmentDetails.builder()
                                .orderNumber("skdjskdj")
                                .serviceType("EXPRESS")
                                .itemDetails(getItemDetails())
                                .qualityCheckDetails(getQCs())
                                .build())
                        .origin(Origin.builder()
                                .originAddress(Address.builder()
                                        .addressLine1("34 east block")
                                        .addressLine2("rohini-86")
                                        .city("delhi")
                                        .code("DEL")
                                        .build())
                                .originPaymentDetails(Payment.builder()
                                        .amountToBeCollected(2.3)
                                        .paymentMode("Cash")
                                        .build())
                                .build())
                        .destination(Destination.builder()
                                .destinationAddress(Address.builder()
                                        .addressLine1("34 east block")
                                        .addressLine2("rohini-86")
                                        .city("delhi")
                                        .code("DEL")
                                        .build())
//                                .destinationEndTime(LocalDateTime.now())
                                .build())
                        .build())
                .shipmentMilestonesInfoDTO(ShipmentMilestonesInfoDTO.builder()
                        .merchantCode("mcode")
                        .merchantName("mname")
                        .carrierPlanningOnTimeFlag(true)
                        .carrierPlanningOnTimeFlag(false)
                        .exceptions(getException())
                        .build())
                .build();


//        long currTime = System.currentTimeMillis();
//        String key = "111111" + Long.toString(currTime);
        String updateMessage = "{\"command_name\" : \"Update\", \"payment_mode\": \"COD\"}";
        long a = System.currentTimeMillis();
        int jloop = 1;
        int interval = 3;
        Command.first_time = System.currentTimeMillis();
        for (int i = 0; i < itr; i++) {
            for (int j = 0; j < jloop; j++) {
//                kafkaTemplate.send(COMMAND_TOPIC,key, updateMessage);
//                kafkaTemplate.send(COMMAND_TOPIC, createMessage);
                String keyInsert = "111111" + System.currentTimeMillis();
                String keyUpdate = "1111111678685992240";
                if (isUpdate)
                    kafkaTemplate.send(COMMAND_TOPIC, keyUpdate, objectWriter.writeValueAsString(newUpsertCommandUpdate));
                else
                    kafkaTemplate.send(COMMAND_TOPIC, keyInsert, objectWriter.writeValueAsString(newUpsertCommandCreate));

            }
            Thread.sleep(interval);
        }

//        log.error("Time to send {} requests: {} with batch {} and interval {}", itr * jloop, System.currentTimeMillis() - a, jloop, interval);
        msg1 = "Time to send " + (itr * jloop) + " requests: " + (System.currentTimeMillis() - a) + " with batch " + jloop + " and interval " + interval;
        return msg1;

    }

    @GetMapping("/example/{itr}/{update}")
    public String example_test(@PathVariable("itr") final int itr, @PathVariable("update") final boolean isUpdate) throws JsonProcessingException, InterruptedException {
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
        Long comp = 10L;




        String exampleUpsertMessage = "{\"command_name\" : \"Upsert\",\"command_type\": \"update\",  \"user_id\":10,  \"company_id\":" + comp +
                ",\"order_number\": \"skdjskdj\", \"service_type\": \"EXPRESS\", \"shipment_type\": \"EXPRESS\", \"payment_mode\": \"COD\"," +
                " \"origin_address\" : { \"address_line1\": \"alsjkjsd\", \"address_line2\": \"sddds\" }}";



//        long currTime = System.currentTimeMillis();
//        String key = "111111" + Long.toString(currTime);
        String updateMessage = "{\"command_name\" : \"Update\", \"payment_mode\": \"COD\"}";
        long a = System.currentTimeMillis();
        int jloop = 1;
        int interval = 3;
        Command.first_time = System.currentTimeMillis();
        for (int i = 0; i < itr; i++) {
            for (int j = 0; j < jloop; j++) {
//                kafkaTemplate.send(COMMAND_TOPIC,key, updateMessage);
//                kafkaTemplate.send(COMMAND_TOPIC, createMessage);
                String keyInsert = "111111" + System.currentTimeMillis();
                String keyUpdate = "1111111678685992240";
                String exampleInsertMessage = "{\"command_name\" : \"Create\",\"tracking_number\": " +keyInsert+ ",  \"user_id\":10,  \"company_id\":" + comp +
                        ",\"order_number\": \"skdjskdj\", \"service_type\": \"EXPRESS\", \"shipment_type\": \"EXPRESS\", \"payment_mode\": \"COD\"," +
                        " \"origin_address\" : { \"address_line1\": \"alsjkjsd\", \"address_line2\": \"sddds\" }}";
                if (isUpdate)
                    kafkaTemplate.send(COMMAND_TOPIC, keyUpdate, exampleUpsertMessage);
                else
                    kafkaTemplate.send(COMMAND_TOPIC, keyInsert, exampleInsertMessage);

            }
//            Thread.sleep(interval);
        }

//        log.error("Time to send {} requests: {} with batch {} and interval {}", itr * jloop, System.currentTimeMillis() - a, jloop, interval);
        msg1 = "Time to send " + (itr * jloop) + " requests: " + (System.currentTimeMillis() - a) + " with batch " + jloop + " and interval " + interval;
        return msg1;

    }

    public static long percentile(List<Long> latencies, double percentile) {
        int index = (int) Math.ceil(percentile / 100.0 * latencies.size());
        return latencies.get(index - 1);
    }

    Set<Exception> getException() {

        Set<Exception> exceptions = new HashSet<>();
        exceptions.add(Exception.builder()
                .createdBy(1234L)
                .type("order create")
                .exception("This is exception message at" + System.currentTimeMillis())
                .build());
        exceptions.add(Exception.builder()
                .createdBy(1234L)
                .type("order create")
                .exception(" exception message 2  at " + System.currentTimeMillis())
                .build());
        return exceptions;
    }

    List<Item> getItemDetails() {
        List<Item> items = new ArrayList<>();
        items.add(Item.builder()
                .code("laptop")
                .hsnName("gds")
                .imageUrl("qer.wer")
                .name("laptop")
                .build());
        items.add(Item.builder()
                .code("mobile")
                .hsnName("qqq")
                .imageUrl("qer.qqq")
                .name("mobile")
                .build());
        return items;
    }

    List<QualityCheck> getQCs() {
        List<QualityCheck> qcs = new ArrayList<>();
        qcs.add(QualityCheck.builder()
                .checkpoint("hub")
                .image("ad.we")
                .label("ok")
                .value(5.5)
                .build());
        qcs.add(QualityCheck.builder()
                .checkpoint("hub2")
                .image("ad.we")
                .label("ok")
                .value(52.5)
                .build());
        qcs.add(QualityCheck.builder()
                .checkpoint("hub3")
                .image("ad.we")
                .label("ok")
                .value(53.5)
                .build());
        return qcs;
    }
}


//in config
//ProducerRecord<String, String> eventRecord = new ProducerRecord<>(kafkaTopicConfig.getEventsTopicName(), objectWriter.writeValueAsString(event));
//            eventRecord.headers().add("time1", objectWriter.writeValueAsBytes(String.valueOf(ts)));
////            if (System.currentTimeMillis() % 2 == 0) throw new RuntimeException("manual exception");
//                    kafkaTemplate.send(eventRecord);