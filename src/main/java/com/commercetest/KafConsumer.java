package com.commercetest;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import java.util.Properties;
import java.util.List;
import java.util.Collections;
import static java.util.Arrays.asList;

public class KafConsumer {
    
    public static void main (String[] args){
        Integer switchEvery = 2000;
        String currentTarget = "kafka-source-0";
        Long lastReceivedOffset = null;
        Integer lastReceivedPartition = null;
        Integer prevVal = null;
        Long consumerPosition = null;

        KafkaConsumer<String, String> consumer = makeConsumer(currentTarget, null, null, asList("joe_topic"));
        // Thread logThread = makeLogThread(consumer);
        // logThread.start();

        long lastExecution = System.currentTimeMillis();
        while(true) {
            if((System.currentTimeMillis() - lastExecution) >= switchEvery) {
                lastExecution = System.currentTimeMillis();
                
                if(currentTarget == "kafka-vm-0") {
                    currentTarget = "kafka-vm-1";
                } else {
                    currentTarget = "kafka-vm-0";
                }

                if(lastReceivedPartition != null){
                    TopicPartition partition = new TopicPartition("joe_topic", lastReceivedPartition);
                    Long newPos = consumer.position(partition);
                    if(consumerPosition == null || newPos >= consumerPosition) {
                        consumerPosition = newPos;
                    }
                    consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(consumerPosition)));
                }

                System.out.println("Now listening to [" + currentTarget + "] at offset ["+consumerPosition+"]");


                // logThread.interrupt();
                consumer.close();
                if(consumerPosition != null && lastReceivedPartition != null) {
                consumer = makeConsumer(currentTarget, lastReceivedPartition, consumerPosition, asList("joe_topic"));
                } else {
                    consumer = makeConsumer(currentTarget, null, null, asList("joe_topic"));
                }
                // logThread = makeLogThread(consumer);
                // logThread.start();
            }

            ConsumerRecords<String, String> records = consumer.poll(100);
            ConsumerRecord<String, String> lastRecord = null;
            for (ConsumerRecord<String, String> record : records) {
                if(lastRecord == null || record.offset() > lastRecord.offset()) {
                    lastRecord = record;
                }
                if(consumerPosition == null || record.offset() >= consumerPosition) {                    
                    String value = record.value();
                    Integer val = null;
                    try {
                        val = Integer.parseInt(value);
                    } catch(Exception err) {
                        System.err.println("Failed to parse ["+value+"]: " + err.getMessage());
                        val = null;
                    }
                    
                    if(prevVal != null && val != null && val != (prevVal+1)) {
                        System.err.println("Invalid messge value/series. Previous message was ["+prevVal+"], this message was ["+val+"], expected ["+(prevVal+1)+"]");
                    }
                    prevVal = val;

                    if(true || val != null && (val % 10000) == 0) {
                        System.out.printf("offset = %d, key = %s, value = %s, ts = %d%n", record.offset(), record.key(), record.value(), record.timestamp());
                    }
                }
            }
            if(lastRecord != null && lastReceivedPartition == null) {
                lastReceivedPartition = lastRecord.partition();
            }
        }
    }

    private static Properties getProperties(String target) {
        Properties props = new Properties();
        props.put("group.id", "kafka_switch");
        props.put("bootstrap.servers", target+":9092"); //target = kafka-destination-0
        props.put("enable.auto.commit", "false");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }

    private static KafkaConsumer makeConsumer(String target, Integer lastReceivedPartition, Long resumeOffset, List<String> topics){
        Properties props = getProperties(target);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(topics);
        if(resumeOffset != null && lastReceivedPartition != null) {
            consumer.poll(0); // Subscribe is lazy
            consumer.seek(new TopicPartition("joe_topic", lastReceivedPartition), resumeOffset);
        }
        
        return consumer;
    }

    // private static Thread makeLogThread(final KafkaConsumer consumer){
    //     Thread logMessages = new Thread() {
    //         public void run() {
    //             while (true) {
    //                 ConsumerRecords<String, String> records = consumer.poll(100);
    //                 for (ConsumerRecord<String, String> record : records)
    //                     System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
    //             }
    //         }  
    //     };
    //     return logMessages;
    // }
}