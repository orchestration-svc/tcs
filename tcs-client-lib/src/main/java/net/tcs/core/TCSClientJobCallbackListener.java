package net.tcs.core;

import java.util.Arrays;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.support.converter.MessageConverter;

import com.rabbitmq.client.Channel;
import com.task.coordinator.amqp.framework.TcsListenerContainerFactory;
import com.task.coordinator.base.message.listener.TcsMessageListener;
import com.task.coordinator.base.message.listener.TcsMessageListenerContainer;
import com.task.coordinator.endpoint.TcsTaskExecutionEndpoint;
import com.task.coordinator.producer.TcsProducer;
import com.task.coordinator.request.message.JobCompleteMessage;
import com.task.coordinator.request.message.JobFailedMessage;
import com.task.coordinator.request.message.JobRollbackCompleteMessage;

import net.tcs.api.TCSJobHandler;

public class TCSClientJobCallbackListener extends TcsMessageListener {

    private TCSJobHandler callback;
    private final String queueName;
    private TcsMessageListenerContainer container;
    private TcsTaskExecutionEndpoint endpoint;

    public TcsTaskExecutionEndpoint getEndpoint() {
        return endpoint;
    }

    public void registerJobCallback(TCSJobHandler callback) {
        this.callback = callback;
    }

    public TCSClientJobCallbackListener(String queueName, TcsProducer producer, MessageConverter converter) {
        super(converter, producer);
        this.queueName = queueName;

    }

    public void initialize(TcsListenerContainerFactory factory, TcsTaskExecutionEndpoint endpoint) {
        this.endpoint = endpoint;
        container = factory.createListenerContainer(this, Arrays.asList(queueName));
        container.start(1);
    }

    public void close() {
        container.destroy();
    }

    @Override
    public void onMessage(Message message, Channel arg1) throws Exception {
        try {
            final Object object = messageConverter.fromMessage(message);
            if (object instanceof JobCompleteMessage) {
                final JobCompleteMessage job = (JobCompleteMessage) object;
                callback.jobComplete(job.getJobId());
            } else if (object instanceof JobFailedMessage) {
                final JobFailedMessage job = (JobFailedMessage) object;
                callback.jobFailed(job.getJobId());
            } else if (object instanceof JobRollbackCompleteMessage) {
                final JobRollbackCompleteMessage job = (JobRollbackCompleteMessage) object;
                callback.jobRollbackComplete(job.getJobId());
            } else {
                System.out.println("Error unsupported message type");
            }

        } catch (final Exception ex) {
        }
    }
}
