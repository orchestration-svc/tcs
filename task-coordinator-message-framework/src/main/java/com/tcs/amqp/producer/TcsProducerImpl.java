package com.tcs.amqp.producer;

import org.springframework.amqp.core.AmqpTemplate;

import com.task.coordinator.base.message.TcsCtrlMessage;
import com.task.coordinator.producer.TcsProducer;

public class TcsProducerImpl implements TcsProducer {

    private final AmqpTemplate amqpTemplate;

    public TcsProducerImpl(AmqpTemplate amqpTemplate){
        this.amqpTemplate = amqpTemplate;
    }

    @Override
    public <R extends TcsCtrlMessage> void sendMessage(String exchange, String routingKey, R message) {
        amqpTemplate.convertAndSend(exchange, routingKey, message);
    }
}
