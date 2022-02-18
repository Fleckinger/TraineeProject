package com.orioninc.traineeproject;

import com.orioninc.traineeproject.kafka.Consumer;
import com.orioninc.traineeproject.kafka.Producer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Collections;
import java.util.Properties;
import java.util.Scanner;

public class EchoServer {

    private final Properties producerProperties;
    private final Properties consumerProperties;

    public EchoServer(String server) {
        this.producerProperties = new Properties();
        producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        producerProperties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        producerProperties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");
        producerProperties.setProperty(ProducerConfig.RETRIES_CONFIG, "0");
        producerProperties.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, "1048576");
        producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        this.consumerProperties = new Properties();
        consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "firstGroup");
        consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    }

    public void start() {
        Scanner scanner = new Scanner(System.in);
        System.out.println("Enter topic name: ");
        String topicName = scanner.nextLine();

        AdminClient admin = AdminClient.create(producerProperties);
        NewTopic topic = new NewTopic(topicName, 1, (short) 3);
        admin.createTopics(Collections.singleton(topic));

        Producer producer = new Producer(producerProperties, topicName);
        Consumer consumer = new Consumer(consumerProperties, topicName);

        System.out.println("Enter message for sending: ");
        String messageForSending = scanner.nextLine();

        while (!messageForSending.equalsIgnoreCase("exit")) {
            producer.send(messageForSending);
            consumer.read();

            System.out.println("Enter message for sending: ");
            messageForSending = scanner.nextLine();
        }
    }

}
