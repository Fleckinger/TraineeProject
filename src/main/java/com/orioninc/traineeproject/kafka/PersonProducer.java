package com.orioninc.traineeproject.kafka;

import com.orioninc.traineeproject.entity.Person;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class PersonProducer {

    private final KafkaTemplate<String, Person> personKafkaTemplate;

    @Autowired
    public PersonProducer(KafkaTemplate<String, Person> personKafkaTemplate) {
        this.personKafkaTemplate = personKafkaTemplate;
    }

    public void send(Person person, String topic) {
        personKafkaTemplate.send(topic, person);
        personKafkaTemplate.flush();
    }
}
