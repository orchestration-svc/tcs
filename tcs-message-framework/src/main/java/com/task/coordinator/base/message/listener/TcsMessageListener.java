package com.task.coordinator.base.message.listener;

import org.springframework.amqp.rabbit.core.ChannelAwareMessageListener;
import org.springframework.amqp.support.converter.MessageConverter;

import com.task.coordinator.producer.TcsProducer;

public abstract class TcsMessageListener implements ChannelAwareMessageListener {

    protected final MessageConverter messageConverter;

    protected final TcsProducer producer;

    public TcsMessageListener(MessageConverter messageConverter, TcsProducer producer) {
        this.messageConverter = messageConverter;
        this.producer = producer;
    }
}
