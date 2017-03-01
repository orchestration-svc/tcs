package net.tcs.messagehandlers;

import java.util.Arrays;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.support.converter.MessageConverter;

import com.fasterxml.jackson.databind.ObjectMapper;
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
import net.tcs.db.TaskInstanceDAO;
import net.tcs.db.adapter.TaskInstanceDBAdapter;
import net.tcs.exceptions.TaskRollbackStateException;
import net.tcs.exceptions.TaskStateException;
import net.tcs.messages.TaskCompleteMessage;
import net.tcs.messages.TaskFailedMessage;
import net.tcs.messages.TaskInProgressMessage;
import net.tcs.messages.TaskRollbackCompleteMessage;

public class TcsTaskExecEventListener extends TcsMessageListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(TcsTaskExecEventListener.class);

    private final String shardId;
    private final ObjectMapper mapper = new ObjectMapper();
    private final TaskInstanceDBAdapter taskDBAdapter = new TaskInstanceDBAdapter();
    private final TaskBoard taskBoard;
    private TcsMessageListenerContainer listenerContainer;
    private final TCSRollbackDispatcher rollbackDispatcher;

    public TcsTaskExecEventListener(String shardId, MessageConverter messageConverter, TcsProducer producer,
            TaskBoard taskBoard, TCSRollbackDispatcher rollbackDispatcher) {
        super(messageConverter, producer);
        this.taskBoard = taskBoard;
        this.shardId = shardId;
        this.rollbackDispatcher = rollbackDispatcher;
    }

    public void initialize(TcsListenerContainerFactory factory) {
        final String taskQueueName = TCSMessageUtils.getQueueNameForProcessingTasksOnShard(shardId);
        listenerContainer = factory.createListenerContainer(this, Arrays.asList(taskQueueName));
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
            final Object requestMessage = messageConverter.fromMessage(message);
            if ( requestMessage instanceof TaskCompleteMessage ) {
                handleTaskComplete((TaskCompleteMessage) requestMessage);
            } else if (requestMessage instanceof TaskFailedMessage) {
                handleTaskFailed((TaskFailedMessage) requestMessage);
            } else if (requestMessage instanceof TaskInProgressMessage) {
                handleTaskUpdate((TaskInProgressMessage) requestMessage);
            } else if (requestMessage instanceof TaskRollbackCompleteMessage) {
                handleTaskRollbackComplete((TaskRollbackCompleteMessage) requestMessage);
            } else {
                LOGGER.error("UnSupported message");
            }
        } catch (final Exception e) {
            LOGGER.error("Exception in TcsTaskExecEventListener.onMessage()", e);
        }
    }

    void handleTaskComplete(TaskCompleteMessage msg) {

        final Map<String, String> taskContextOutput = msg.getTaskContextOutput();

        final TaskInstanceDAO taskDAO;
        try {
            if (taskContextOutput.isEmpty()) {
                taskDAO = taskDBAdapter.saveCompletedTask(msg.getTaskId(), msg.getTaskOutput());
            } else {
                taskDAO = taskDBAdapter.saveCompletedTaskWithContext(msg.getTaskId(), msg.getJobId(),
                        msg.getTaskOutput(), msg.getTaskContextOutput(), mapper);
            }
        } catch (final TaskStateException ex) {
            LOGGER.error("Exception in TcsTaskExecSubmitListener.handleTaskComplete()", ex);
            final String err = String.format(
                    "TaskStateException while saving completed taskId: {%s}, Expected state: {%s}, Actual state: {%s}",
                    msg.getTaskId(), ex.getExpectedState().value(), ex.getActualState().value());
            LOGGER.error(err);
            return;
        }

        if (taskDAO == null) {
            LOGGER.error("Task {} does not exist", msg.getTaskId());
            return;
        }
        final TCSDispatcher taskDispatcher = taskBoard.getDispatcherForJob(taskDAO.getJobInstanceId());
        if (taskDispatcher != null) {
            taskDispatcher.enqueueCommand(TCSCommandType.COMMAND_TASK_COMPLETE, taskDAO);
        } else {
            LOGGER.error("Task completion for task {} cannot be processed, since Job {} is not in progress anymore",
                    taskDAO.getName(), taskDAO.getJobInstanceId());
        }
    }

    private void handleTaskRollbackComplete(TaskRollbackCompleteMessage msg) {
        final TaskInstanceDAO taskDAO;
        try {
            taskDAO = taskDBAdapter.saveRollbackCompletedTask(msg.getTaskId());
        } catch (final TaskRollbackStateException ex) {
            LOGGER.error("Exception in TcsTaskExecSubmitListener.handleTaskRollbackComplete()", ex);
            final String err = String.format(
                    "TaskRollbackStateException while saving rolled-back taskId: {%s}, Expected state: {%s}, Actual state: {%s}",
                    msg.getTaskId(), ex.getExpectedState().value(), ex.getActualState().value());
            LOGGER.error(err);
            return;
        }

        if (taskDAO == null) {
            LOGGER.error("Task {} does not exist", msg.getTaskId());
            return;
        }

        rollbackDispatcher.enqueueCommand(TCSCommandType.COMMAND_TASK_ROLLBACK_COMPLETE, taskDAO);
    }

    void handleTaskFailed(TaskFailedMessage msg) {
        final TaskInstanceDAO taskDAO;
        try {
            taskDAO = taskDBAdapter.saveFailedTask(msg.getTaskId(), msg.getError());
        } catch (final TaskStateException ex) {

            final String err = String.format(
                    "TaskStateException while saving failed task; taskId: {%s}, Expected state: {%s}, Actual state: {%s}",
                    msg.getTaskId(), ex.getExpectedState().value(), ex.getActualState().value());

            LOGGER.error(err);
            return;
        }
        if (taskDAO == null) {
            LOGGER.error("Task {} does not exist", msg.getTaskId());
            return;
        }

        final TCSDispatcher taskDispatcher = taskBoard.getDispatcherForJob(taskDAO.getJobInstanceId());
        if (taskDispatcher != null) {
            taskDispatcher.enqueueCommand(TCSCommandType.COMMAND_TASK_FAILED, taskDAO);
        } else {
            LOGGER.error("Task failure for task {} cannot be processed, since Job {} is not in progress",
                    taskDAO.getName(), taskDAO.getJobInstanceId());
        }
    }

    private void handleTaskUpdate(TaskInProgressMessage msg) {
        try {
            taskDBAdapter.saveUpdatedTask(msg.getTaskId());
        } catch (final TaskStateException ex) {

            final String err = String.format(
                    "TaskStateException while saving updated task; taskId: {%s}, Expected state: {%s}, Actual state: {%s}",
                    msg.getTaskId(), ex.getExpectedState().value(), ex.getActualState().value());

            LOGGER.error(err);
        }
    }
}
