package com.orioninc.traineeproject.entity;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Date;

public class Person {
    private String firstName;
    private String lastName;
    private int age;
    private Date timestamp;


    public Person(@JsonProperty("firstName") String firstName,
                  @JsonProperty("lastName") String lastName,
                  @JsonProperty("age") int age) {
        this.firstName = firstName;
        this.lastName = lastName;
        this.age = age;
        timestamp = new Date();
        System.out.println(timestamp);
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Person{" +
                "firstName='" + firstName + '\'' +
                ", lastName='" + lastName + '\'' +
                ", age=" + age +
                ", timestamp=" + timestamp +
                '}';
    }
}
