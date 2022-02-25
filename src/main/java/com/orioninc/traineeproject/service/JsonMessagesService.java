package com.orioninc.traineeproject.service;

import com.orioninc.traineeproject.entity.Person;
import com.orioninc.traineeproject.kafka.PersonProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Date;

@Service
public class JsonMessagesService {

    private final PersonProducer producer;

    @Autowired
    public JsonMessagesService(PersonProducer producer) {
        this.producer = producer;
    }

    public void sendWithCurrentTimestamp(Person person) {
        person.setTimestamp(new Date());
        producer.send(person, "topic2");
    }

}
