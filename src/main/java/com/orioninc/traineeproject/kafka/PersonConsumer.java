package com.orioninc.traineeproject.kafka;

import com.orioninc.traineeproject.entity.Person;
import com.orioninc.traineeproject.service.JsonMessagesService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
public class PersonConsumer {

    private final JsonMessagesService jsonMessagesService;

    @Autowired
    public PersonConsumer(JsonMessagesService jsonMessagesService) {
        this.jsonMessagesService = jsonMessagesService;
    }

    @KafkaListener(topics = "topic1", groupId = "firstGroup", containerFactory = "kafkaListenerContainerFactory")
    public void consume(@Payload Person person) {
        jsonMessagesService.sendWithCurrentTimestamp(person);
    }
}
