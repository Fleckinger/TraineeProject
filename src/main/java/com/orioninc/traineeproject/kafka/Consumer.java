package com.orioninc.traineeproject.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class Consumer {

    private final Properties properties;
    private final String topic;

    public Consumer(Properties properties, String topic) {
        this.properties = properties;
        this.topic = topic;
    }

    public void read() {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singleton(topic));

        AtomicBoolean flag = new AtomicBoolean();
        flag.set(true);

        while (flag.get()) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
            consumerRecords.forEach(record -> System.out.println(record.value()));

            if (consumerRecords.isEmpty()) {
                flag.set(false);
            }
        }
        consumer.close();
    }
}
