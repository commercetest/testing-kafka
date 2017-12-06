package com.commercetest;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;
import java.io.Console;

public class KafProducer {

    private Integer indexVal = 0;

    public static void main (String[] args){
        if(args.length != 3) {
            throw new IllegalArgumentException("Expecting 3 arguments: [String:messageBody Long:repeatCount Long:repeatMs]");
        }
        String messageBody = args[0];
        Long repeatCount = Long.parseLong(args[1]);
        Long repeatMs = Long.parseLong(args[2]);

        KafProducer kafProducer = new KafProducer();
        kafProducer.run(messageBody, repeatCount, repeatMs);
    }

    public void run(String messageBody, Long repeatCount, Long repeatMs){
        Producer producer = getKafkaProducer();
        
        long lastExecution = 0;
        while(true) {
            if((System.currentTimeMillis() - lastExecution) >= repeatMs) {
                lastExecution = System.currentTimeMillis();
                
                ProducerRecord<String, String> message;
                for(Long i = repeatCount; i > 0; --i) {
                    message = new ProducerRecord<>("joe_topic", "" + (++indexVal));
                    producer.send(message);
                }
                producer.flush();

                System.out.println("Sent batch of messages @ "+lastExecution + " Now at index [" + indexVal + "]");
            }
        }
    }

    private  Producer<String, String> getKafkaProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "kafka-vm-0:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        return new KafkaProducer<>(props);
    }
}