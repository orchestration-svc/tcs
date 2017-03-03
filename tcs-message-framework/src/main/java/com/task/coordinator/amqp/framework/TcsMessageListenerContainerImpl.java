package com.task.coordinator.amqp.framework;

import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;

import com.task.coordinator.base.message.listener.TcsMessageListener;
import com.task.coordinator.base.message.listener.TcsMessageListenerContainer;

public class TcsMessageListenerContainerImpl extends SimpleMessageListenerContainer implements TcsMessageListenerContainer {

    public TcsMessageListenerContainerImpl(ConnectionFactory connectionFactory) {
        super(connectionFactory);
    }
    public void start(int concurrentConsumers) {
        super.setConcurrentConsumers(concurrentConsumers);
        super.start();
    }

    @Override
    public TcsMessageListener getMessageListener(){
        return (TcsMessageListener) super.getMessageListener();
    }
}
