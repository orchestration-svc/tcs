package net.tcs.messagebox;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.task.coordinator.message.utils.TCSConstants;
import com.task.coordinator.message.utils.TCSMessageUtils;
import com.task.coordinator.request.message.TaskCompleteMessage;
import com.task.coordinator.request.message.TaskFailedMessage;
import com.task.coordinator.request.message.TaskInProgressMessage;
import com.task.coordinator.request.message.TaskRollbackCompleteMessage;

import net.messaging.clusterbox.ClusterBoxDropBox;
import net.messaging.clusterbox.ClusterBoxMessageBox;
import net.messaging.clusterbox.ClusterBoxMessageHandler;
import net.messaging.clusterbox.message.RequestMessage;
import net.tcs.core.TCSCommandType;
import net.tcs.core.TCSDispatcher;
import net.tcs.core.TCSRollbackDispatcher;
import net.tcs.core.TaskBoard;
import net.tcs.db.TaskInstanceDAO;
import net.tcs.db.adapter.TaskInstanceDBAdapter;
import net.tcs.exceptions.TaskRollbackStateException;
import net.tcs.exceptions.TaskStateException;

public class TcsTaskExecEventMessageBox implements ClusterBoxMessageBox, ClusterBoxMessageHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(TcsTaskExecEventMessageBox.class);

    private final String shardId;
    private final ObjectMapper mapper = new ObjectMapper();
    private final TaskInstanceDBAdapter taskDBAdapter = new TaskInstanceDBAdapter();
    private final TaskBoard taskBoard;
    private final TCSRollbackDispatcher rollbackDispatcher;
    private final String taskNotificationQueueName;
    private final String taskNotificationRoutingKey;

    public TcsTaskExecEventMessageBox(String shardId, TaskBoard taskBoard, TCSRollbackDispatcher rollbackDispatcher) {
        this.taskBoard = taskBoard;
        this.shardId = shardId;
        this.rollbackDispatcher = rollbackDispatcher;
        this.taskNotificationQueueName = TCSMessageUtils.getQueueNameForProcessingTasksOnShard(shardId);
        this.taskNotificationRoutingKey = String.format("%s.%s", TCSConstants.TCS_TASK_NOTIF_RKEY, shardId);
    }

    public void cleanup() {

    }

    @Override
    public void handleMessage(RequestMessage message, ClusterBoxDropBox dropBox) {

        try {
            final Object requestMessage = message.getPayload();
            if (requestMessage instanceof TaskCompleteMessage) {
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

    public void handleTaskComplete(TaskCompleteMessage msg) {

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

    public void handleTaskFailed(TaskFailedMessage msg) {
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

    @Override
    public String getRequestName() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ClusterBoxMessageHandler<?> getMessageHandler() {
        return this;
    }

    @Override
    public String getMessageBoxId() {
        return taskNotificationQueueName;
    }

    @Override
    public String getMessageBoxName() {
        return taskNotificationRoutingKey;
    }
}
