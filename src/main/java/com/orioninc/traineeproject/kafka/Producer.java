package com.orioninc.traineeproject.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class Producer {

    private final Properties properties;
    private final String topic;


    public Producer(Properties properties, String topic) {
        this.properties = properties;
        this.topic = topic;
    }

    public void send(String valueToSend) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, valueToSend);

        producer.send(record);
        producer.flush();
        producer.close();
    }
}
