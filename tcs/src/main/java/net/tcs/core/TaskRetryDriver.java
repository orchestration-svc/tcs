package net.tcs.core;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.task.coordinator.message.utils.TCSConstants;

import net.tcs.db.TaskInstanceDAO;
import net.tcs.db.adapter.TaskInstanceDBAdapter;

/**
 * Responsible for reading all the in-progress timed-out tasks in the shard from
 * DB, and sending them to TCSDispatcher for Task retry.
 *
 * Scheduled to execute periodically.
 */
public class TaskRetryDriver implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskRetryDriver.class);

    public static final int TASK_RETRY_THREAD_INTERVAL_SECS = 5;

    private int timeout = TCSConstants.TASK_TIMEOUT_RETRY_INTERVAL_SECS;

    private int retryInterval = TASK_RETRY_THREAD_INTERVAL_SECS;

    private final String shardId;

    private final TaskBoard taskBoard;

    private final TaskInstanceDBAdapter taskDBAdapter = new TaskInstanceDBAdapter();

    public TaskRetryDriver(String shardId, TaskBoard taskBoard) {
        super();
        this.shardId = shardId;
        this.taskBoard = taskBoard;
    }

    public int getRetryInterval() {
        return retryInterval;
    }

    @Override
    public void run() {
        try {
            final List<TaskInstanceDAO> expiredTasks = taskDBAdapter.getAllInprogressTasksForShard(shardId, timeout);
            if (expiredTasks != null && !expiredTasks.isEmpty()) {
                for (final TaskInstanceDAO task : expiredTasks) {
                    final TCSDispatcher dispatcher = taskBoard.getDispatcherForJob(task.getJobInstanceId());
                    if (dispatcher != null) {
                        LOGGER.trace("Retrying Task: {}. JobId: {}", task.getName(), task.getJobInstanceId());
                        dispatcher.enqueueCommand(TCSCommandType.COMMAND_TASK_RETRY, task);
                    } else {
                        LOGGER.warn("Task {} cannot be retried, since Job {} is not in progress anymore.",
                                task.getName(), task.getJobInstanceId());
                    }
                }
            }
        } catch (final Exception ex) {
            LOGGER.error("Exception in TaskRetryDriver.run()", ex);
        }
    }

    @VisibleForTesting
    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    @VisibleForTesting
    public void setRetryInterval(int retryInterval) {
        this.retryInterval = retryInterval;
    }
}
