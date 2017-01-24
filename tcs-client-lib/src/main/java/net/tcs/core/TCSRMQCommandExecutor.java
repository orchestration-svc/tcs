package net.tcs.core;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.task.coordinator.endpoint.TcsTaskExecutionEndpoint;

import net.tcs.exceptions.TCSRuntimeException;
import net.tcs.messaging.RMQConnectionFactory;

public class TCSRMQCommandExecutor {

    private static final Logger LOGGER = LoggerFactory.getLogger(TCSRMQCommandExecutor.class);

    private Connection connection;
    private Channel channel;

    public TCSRMQCommandExecutor() {
    }

    public void initialize(String brokerAddress) {
        final ConnectionFactory factory = RMQConnectionFactory.getConnectionFactory(brokerAddress);

        try {
            connection = factory.newConnection();
        } catch (final IOException | TimeoutException ex) {
            ex.printStackTrace();
            LOGGER.error("Failed to create RMQ channel to Broker: {}", brokerAddress, ex);
            throw new TCSRuntimeException(ex);
        }

        try {
            channel = connection.createChannel();
        } catch (final IOException ex) {
            ex.printStackTrace();
            LOGGER.error("Failed to create RMQ channel to Broker: {}", brokerAddress, ex);
            throw new TCSRuntimeException(ex);
        }
    }

    public String bindWithPrivateQueue(TcsTaskExecutionEndpoint endpointAddress) {
        final String queueName = UUID.randomUUID().toString();
        try {
            channel.queueDeclare(queueName, false, false, true, null);
            channel.queueBind(queueName, endpointAddress.getExchangeName(), endpointAddress.getRoutingKey());
            return queueName;
        } catch (final IOException ex) {
            ex.printStackTrace();
            LOGGER.error("Failed to create/bind queue with URI {}", endpointAddress.toEndpointURI(), ex);
            throw new TCSRuntimeException(ex);
        }
    }

    public String bindWithPrivateQueue(String queueName, TcsTaskExecutionEndpoint endpointAddress) {
        try {
            channel.queueDeclare(queueName, false, false, true, null);
            channel.queueBind(queueName, endpointAddress.getExchangeName(), endpointAddress.getRoutingKey());
            return queueName;
        } catch (final IOException ex) {
            ex.printStackTrace();
            LOGGER.error("Failed to create/bind queue with URI {}", endpointAddress.toEndpointURI(), ex);
            throw new TCSRuntimeException(ex);
        }
    }

    public void close() {
        try {
            if (channel != null) {
                channel.close();
            }

            if (connection != null) {
                connection.close();
            }
        } catch (final IOException e) {
            e.printStackTrace();
        } catch (final TimeoutException e) {
            e.printStackTrace();
        }
    }
}

