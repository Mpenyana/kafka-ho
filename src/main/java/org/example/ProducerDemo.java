package org.example;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.UUID;

public class ProducerDemo {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());
    public static void main(String[] args) {
        log.info("Hello, World!");

        //prep Producer's properties;
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());


        // Kafka Producer;
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //sending data;
            // Create Record/Event;
        ProducerRecord<String, String> record = new ProducerRecord<>("java-test", UUID.randomUUID().toString(),
                "Key'd up");

        producer.send(record, (recordMetadata, e) -> {
            if(e != null) {
                log.error("Error producing record: ", e);
            } else {
                log.info(String.format("Record stored with offset %s, in partition: %s", recordMetadata.offset(),
                        recordMetadata.partition()));
            }
        });


        producer.flush();
        producer.close();

    }
}
