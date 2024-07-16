package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Consumer {


    private static final Logger log = LoggerFactory.getLogger(Consumer.class);

    public static void main(String[] args) {
        String groupId = "group-A1";
        String topic = "java-test";

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest");

        //Consumer;
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        //Graceful shutdown;
            //reference to the thread running the program;
        final Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown detected, call consumer.wakeup() ?");
            consumer.wakeup();

            try {
                mainThread.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }));

        try {
            //Sub to the topic;
            consumer.subscribe(Arrays.asList(topic));

            while(true) {
                log.info("polling");
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for(ConsumerRecord<String, String> record: records) {
                    log.info(String.format("Key: %s, Value: %s", record.key(), record.value()));
                    log.info(String.format("Partition: %s, Offset: %s", record.partition(), record.offset()));
                }
            }
        } catch (WakeupException e) {
            log.info("Consumer is starting to shutdown");
        } catch (Exception e) {
            log.error("Unexpected exception: ", e);
        } finally {
            consumer.close();
            log.info("Consumer gracefully shutdown");
        }
    }
}
