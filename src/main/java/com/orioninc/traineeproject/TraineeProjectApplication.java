package com.orioninc.traineeproject;

public class TraineeProjectApplication {

    public static void main(String[] args) {
        EchoServer echoServer = new EchoServer("localhost:9092");
        echoServer.start();
    }

}
