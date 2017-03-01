package com.task.coordinator.producer;

import org.springframework.amqp.core.AmqpTemplate;

public class TcsProducerImpl implements TcsProducer {

    private final AmqpTemplate amqpTemplate;

    public TcsProducerImpl(AmqpTemplate amqpTemplate) {
        this.amqpTemplate = amqpTemplate;
    }

    @Override
    public void sendMessage(String exchange, String routingKey, Object message) {
        amqpTemplate.convertAndSend(exchange, routingKey, message);
    }
}
