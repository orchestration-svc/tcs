package net.tcs.drivers;

import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory.CacheMode;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.support.RetryTemplate;

import com.task.coordinator.amqp.framework.TcsListenerContainerFactory;
import com.task.coordinator.producer.TcsProducer;
import com.task.coordinator.producer.TcsProducerImpl;

@Configuration
public class TCSRmqConfiguration {
    @Bean(name = "defaultConnectionFactory")
    public ConnectionFactory connectionFactory() {
        final CachingConnectionFactory factory = new CachingConnectionFactory(
                TCSDriver.getConfig().getRabbitConfig().getBrokerAddress());
        factory.setCacheMode(CacheMode.CONNECTION);
        factory.setConnectionCacheSize(32);
        return factory;

    }

    @Bean
    public AmqpAdmin amqpAdmin() {
        return new RabbitAdmin(connectionFactory());
    }

    @Bean(name = "defaultMessageConverter")
    public MessageConverter messageConverter() {
        return new Jackson2JsonMessageConverter();
    }

    @Bean(name = "rabbitTemplate")
    public RabbitTemplate rabbitTemplate() {
        final RabbitTemplate template = new RabbitTemplate(connectionFactory());
        template.setMessageConverter(messageConverter());
        final RetryTemplate retryTemplate = new RetryTemplate();
        final ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
        backOffPolicy.setInitialInterval(500);
        backOffPolicy.setMultiplier(10.0);
        backOffPolicy.setMaxInterval(10000);
        retryTemplate.setBackOffPolicy(backOffPolicy);
        template.setRetryTemplate(retryTemplate);
        return template;
    }

    @Bean(name = "defaultProducer")
    public TcsProducer producer() {
        return new TcsProducerImpl(rabbitTemplate());
    }

    @Bean(name = "TcsListenerContainerFactory")
    public TcsListenerContainerFactory listenerFactory() {
        final TcsListenerContainerFactory listenerFactory = new TcsListenerContainerFactory();
        listenerFactory.setConnectionFactory(connectionFactory());
        return listenerFactory;
    }
}
