package net.tcs.messaging;

import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.support.RetryTemplate;

public class SpringRmqConnectionFactory {
    private final CachingConnectionFactory rmqFactory;
    private final RabbitAdmin admin;

    public SpringRmqConnectionFactory(CachingConnectionFactory rmqFactory) {
        this.rmqFactory = rmqFactory;
        admin = new RabbitAdmin(rmqFactory);
    }

    public CachingConnectionFactory getRmqConnectionFactory() {
        return rmqFactory;
    }

    public static SpringRmqConnectionFactory createConnectionFactory(String rmqBrokerAddress) {
        final CachingConnectionFactory factory = new CachingConnectionFactory(rmqBrokerAddress);
        factory.setConnectionCacheSize(32);
        return new SpringRmqConnectionFactory(factory);
    }

    public AmqpTemplate createRabbitTemplate() {
        final RabbitTemplate template = new RabbitTemplate(rmqFactory);
        template.setMessageConverter(new Jackson2JsonMessageConverter());
        final RetryTemplate retryTemplate = new RetryTemplate();
        final ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
        backOffPolicy.setInitialInterval(500);
        backOffPolicy.setMultiplier(10.0);
        backOffPolicy.setMaxInterval(10000);
        retryTemplate.setBackOffPolicy(backOffPolicy);
        template.setRetryTemplate(retryTemplate);

        return template;
    }

    public RabbitAdmin getAdmin() {
        return admin;
    }

    public void cleanup() {
        rmqFactory.destroy();
    }
}
