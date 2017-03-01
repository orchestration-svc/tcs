package com.task.coordinator.producer;

public interface TcsProducer {
    public void sendMessage(String exchange, String routingKey, Object message);
}
