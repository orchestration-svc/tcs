package com.task.coordinator.amqp.framework;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;

import com.task.coordinator.base.message.listener.TcsMessageListener;
import com.task.coordinator.base.message.listener.TcsMessageListenerContainer;

/*
 * Used to bind listers to queues for a .
 * */
public class TcsListenerContainerFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(TcsListenerContainerFactory.class);

    private ConnectionFactory connectionFactory;

    public ConnectionFactory getConnectionFactory() {
        return connectionFactory;
    }

    public void setConnectionFactory(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    public TcsMessageListenerContainer createListenerContainer(TcsMessageListener listener, List<String> queueName){
        LOGGER.info("Requesting Container for queues : {}", queueName.toString());
        final TcsMessageListenerContainerImpl container = new TcsMessageListenerContainerImpl(connectionFactory);
        container.setMessageListener(listener);
        container.setQueueNames(queueName.toArray(new String[0]));
        return container;
    }
}
