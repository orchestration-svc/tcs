package net.tcs.messaging;

import com.rabbitmq.client.ConnectionFactory;

public class RMQConnectionFactory {

    private static volatile ConnectionFactory rmqFactory = null;

    public static ConnectionFactory getConnectionFactory(String rmqBrokerAddress) {
        if (rmqFactory == null) {
            synchronized (RMQConnectionFactory.class) {
                if (rmqFactory == null) {
                    final ConnectionFactory factory = new ConnectionFactory();
                    factory.setAutomaticRecoveryEnabled(true);
                    factory.setHost(rmqBrokerAddress);
                    rmqFactory = factory;
                }
            }
        }
        return rmqFactory;
    }
}
