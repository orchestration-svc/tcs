package net.tcs.core;

import java.util.Arrays;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.support.converter.MessageConverter;

import com.rabbitmq.client.Channel;
import com.task.coordinator.amqp.framework.TcsListenerContainerFactory;
import com.task.coordinator.base.message.listener.TcsMessageListener;
import com.task.coordinator.base.message.listener.TcsMessageListenerContainer;
import com.task.coordinator.producer.TcsProducer;
import com.task.coordinator.request.message.BeginTaskMessage;
import com.task.coordinator.request.message.BeginTaskRollbackMessage;

import net.tcs.api.TCSCallback;
import net.tcs.api.TCSTaskContext;

public class TCSClientTaskCallbackListener extends TcsMessageListener {

    private TCSCallback callback;
    private final String queueName;
    private TcsMessageListenerContainer container;

    public void registerTCSCallback(TCSCallback callback) {
        this.callback = callback;
    }

    public TCSClientTaskCallbackListener(String queueName, TcsProducer producer, MessageConverter converter) {
        super(converter, producer);
        this.queueName = queueName;

    }

    public void initialize(TcsListenerContainerFactory factory) {
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
            if (object instanceof BeginTaskMessage) {
                final BeginTaskMessage taskMsg = (BeginTaskMessage) object;
                final TCSTaskContext taskContext = new TaskContextImpl(taskMsg.getTaskName(),
                        taskMsg.getTaskParallelIndex(), taskMsg.getTaskId(),
                        taskMsg.getJobName(), taskMsg.getJobId(), taskMsg.getShardId(), taskMsg.getRetryCount());

                callback.startTask(taskContext,
                        (taskMsg.getTaskInput() != null) ? taskMsg.getTaskInput().getBytes() : null,
                                taskMsg.getParentTaskOutput(), taskMsg.getJobContext());
            } else if (object instanceof BeginTaskRollbackMessage) {
                final BeginTaskRollbackMessage taskMsg = (BeginTaskRollbackMessage) object;
                /*
                 * REVISIT TODO fix BeginTaskRollbackMessage to pass
                 * taskParallelExecutionIndex
                 */
                final TCSTaskContext taskContext = new TaskContextImpl(taskMsg.getTaskName(), 0, taskMsg.getTaskId(),
                        taskMsg.getJobName(), taskMsg.getJobId(), taskMsg.getShardId(), taskMsg.getRetryCount());

                callback.rollbackTask(taskContext, taskMsg.getTaskOutput().getBytes());
            } else {
                System.out.println("Error unsupported message type");
            }
        } catch (final Exception ex) {
        }
    }
}
