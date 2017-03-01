package net.tcs.messagehandlers;

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.support.converter.MessageConverter;

import com.rabbitmq.client.Channel;
import com.task.coordinator.amqp.framework.TcsListenerContainerFactory;
import com.task.coordinator.base.message.listener.TcsMessageListener;
import com.task.coordinator.base.message.listener.TcsMessageListenerContainer;
import com.task.coordinator.message.utils.TCSMessageUtils;
import com.task.coordinator.producer.TcsProducer;

import net.tcs.core.TCSCommandType;
import net.tcs.core.TCSDispatcher;
import net.tcs.core.TCSRollbackDispatcher;
import net.tcs.core.TaskBoard;
import net.tcs.db.JobInstanceDAO;
import net.tcs.db.adapter.JobInstanceDBAdapter;
import net.tcs.db.adapter.TaskInstanceDBAdapter;
import net.tcs.messages.BeginJobMessage;
import net.tcs.messages.RollbackJobMessage;

public class TcsJobExecBeginListener extends TcsMessageListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(TcsJobExecBeginListener.class);

    private final String shardId;
    private final JobInstanceDBAdapter jobInstanceDBAdapter = new JobInstanceDBAdapter();
    protected final TaskInstanceDBAdapter taskDBAdapter = new TaskInstanceDBAdapter();
    private final TaskBoard taskBoard;
    private TcsMessageListenerContainer listenerContainer;
    private final TCSRollbackDispatcher rollbackDispatcher;

    public TcsJobExecBeginListener(String shardId, MessageConverter messageConverter, TcsProducer producer,
            TaskBoard taskBoard, TCSRollbackDispatcher rollbackDispatcher) {
        super(messageConverter, producer);
        this.shardId = shardId;
        this.taskBoard = taskBoard;
        this.rollbackDispatcher = rollbackDispatcher;
    }

    public void initialize(TcsListenerContainerFactory factory) {
        final String jobSubmitQueueName = TCSMessageUtils.getQueueNameForProcessingJobsOnShard(shardId);
        listenerContainer = factory.createListenerContainer(this, Arrays.asList(jobSubmitQueueName));
        listenerContainer.start(1);
    }

    public void cleanup() {
        if (listenerContainer != null) {
            listenerContainer.destroy();
        }
    }

    @Override
    public void onMessage(Message message, Channel channel) {
        try {
            final Object resultMessage = messageConverter.fromMessage(message);
            if (resultMessage instanceof BeginJobMessage) {
                handleBeginJob((BeginJobMessage) resultMessage);
            } else if (resultMessage instanceof RollbackJobMessage) {
                handleRollbackJob((RollbackJobMessage) resultMessage);
            }
        } catch (final Exception e) {
            LOGGER.error("Exception in TcsJobExecSubmitListener.onMessage()", e);
        }
    }

    /**
     * On receipt of a SubmitJob event, (1) Read the Job from DB, (b) change
     * state from INIT to INPROGRESS, and (3) submit the Job to TaskBoard for
     * routing to a TCSDisptcher for execution.
     *
     * @param channel
     * @param objectMapper
     * @param properties
     * @param body
     */
    void handleBeginJob(BeginJobMessage message) {

        final JobInstanceDAO jobDAO = jobInstanceDBAdapter.saveInProgressJob(message.getJobId());
        final TCSDispatcher taskDispatcher = taskBoard.registerJobForExecution(jobDAO.getInstanceId());
        taskDispatcher.enqueueCommand(TCSCommandType.COMMAND_BEGIN_JOB, jobDAO);
    }

    private void handleRollbackJob(RollbackJobMessage message) {
        final JobInstanceDAO jobDAO = jobInstanceDBAdapter.getJobInstanceFromDB(message.getJobId());
        rollbackDispatcher.enqueueCommand(TCSCommandType.COMMAND_ROLLBACK_JOB, jobDAO);
    }
}
