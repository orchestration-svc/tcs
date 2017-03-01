package net.tcs.core;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.task.coordinator.endpoint.TcsTaskExecutionEndpoint;
import com.task.coordinator.producer.TcsProducer;

import net.tcs.db.JobInstanceDAO;
import net.tcs.db.TaskInstanceDAO;
import net.tcs.exceptions.JobStateException;
import net.tcs.messages.BeginTaskRollbackMessage;
import net.tcs.messages.JobRollbackCompleteMessage;
import net.tcs.messaging.AddressParser;
import net.tcs.task.JobDefinition;

/**
 * This class is responsible for maintaining an in-memory view of Job instances
 * being rolled-back, keeping track of the tasks, and dispatching
 * ready-to-be-rolled-back tasks.
 *
 */
public class TCSRollbackDispatcher extends TCSAbstractDispatcher {
    private static final Logger LOGGER = LoggerFactory.getLogger(TCSRollbackDispatcher.class);

    public TCSRollbackDispatcher(TcsProducer producer) {
        super(producer);
    }

    @Override
    public void processCommand(TCSCommand command) {
        switch (command.commandType) {

        case COMMAND_ROLLBACK_JOB:
            handleRollbackJobCommand(command);
            break;

        case COMMAND_TASK_ROLLBACK_COMPLETE:
            handleTaskRollbackCompleteCommand(command);
            break;

        case COMMAND_TASK_ROLLBACK_FAILED:
            // REVISIT TODO
            break;

        default:
            LOGGER.warn("Unknown command: {}", command.commandType);
            break;
        }
    }

    /**
     * Handles TaskRollbackComplete event. If there are no downstream tasks to
     * be executed, then marks the JobInstance as ROLLBACK_COMPLETE. If there
     * are downstream tasks ready to rollback, then it fires off those tasks.
     *
     * @param command
     */
    private void handleTaskRollbackCompleteCommand(final TCSCommand command) {
        final TaskInstanceDAO completedTaskDAO = (TaskInstanceDAO) command.body;

        final String jobId = completedTaskDAO.getJobInstanceId();

        final JobStateMachineContextBase jobContext = inProgressJobsMap.get(jobId);
        if (jobContext == null) {
            LOGGER.warn("JobId: {} not found in InProgressJob map", jobId);
            return;
        }

        final List<TaskStateMachineContextBase> tasksReadyToExecute = jobContext
                .taskComplete(completedTaskDAO.getName(), completedTaskDAO.getParallelExecutionIndex());

        if (tasksReadyToExecute.isEmpty()) {
            if (jobContext.areAllTasksComplete()) {
                sendJobRollbackCompleteNotification(jobId, jobContext.getName(), jobContext.getRegistrant());
                try {
                    jobInstanceDBAdapter.saveRolledbackJob(jobId);
                } catch (final JobStateException ex) {
                    LOGGER.error("JobStateException while saving rolled back job", ex);
                    return;
                } finally {
                    inProgressJobsMap.remove(jobId);
                }
            }
            return;
        }

        LOGGER.trace("Tasks ready to rollback, for Rollback-InProgress Job Id: {}", jobContext.getInstanceId());
        /*
         * Since all the tasks that are ready to execute are non-root tasks, we
         * do not need the JobInput, so OK to pass null here.
         */
        final JobInstanceDAO jobDAO = null;
        beginRollbackTasks(jobContext, jobDAO, tasksReadyToExecute);
    }

    /**
     * Handles BeginJob event. Fires off the root task(s).
     *
     * @param command
     */
    private void handleRollbackJobCommand(final TCSCommand command) {
        final JobInstanceDAO job = (JobInstanceDAO) command.body;

        final JobDefinition jobDef = jobDefDBAdapter.getJobDefinition(job.getName());
        if (jobDef == null) {
            return;
        }

        final List<TaskInstanceDAO> completedTaskDAOs = taskDBAdapter.getAllCompletedTasksForJobId(job.getInstanceId());
        if (completedTaskDAOs == null || completedTaskDAOs.isEmpty()) {
            LOGGER.warn("Nothing to rollback, since no completed tasks for Job: {}, Id: {}", job.getName(),
                    job.getInstanceId());
            sendJobRollbackCompleteNotification(job.getInstanceId(), job.getName(), job.getJobNotificationUri());
            return;
        }

        final JobRollbackStateMachineContext jobContext = new JobRollbackStateMachineContext(job.getName(),
                job.getInstanceId(), job.getShardId(), jobDef.hasSteps(), job.getJobNotificationUri());

        jobContext.initializeFromJobDefinition(jobDef, completedTaskDAOs);
        jobContext.updateGraph(completedTaskDAOs);

        inProgressJobsMap.put(job.getInstanceId(), jobContext);
        final List<TaskStateMachineContextBase> tasks = jobContext.getRootTasks();

        beginRollbackTasks(jobContext, job, tasks);
    }

    private void beginRollbackTasks(final JobStateMachineContextBase jobContext, JobInstanceDAO jobDAO,
            final List<TaskStateMachineContextBase> tasks) {
        for (final TaskStateMachineContextBase task : tasks) {
            LOGGER.trace("Task {} is ready to rollback, for Job Id: {}", task.getName(), jobContext.getInstanceId());

            task.markInProgress();
            try {
                taskDBAdapter.updateTaskRollbackStateToInProgress(task.getInstanceId());
            } catch (final Exception ex) {
                task.revertMarkInProgress();
                LOGGER.error("Failed to update task-state to Rollback-InProgress in DB: Task {}; Job Id: {}",
                        task.getName(), jobContext.getInstanceId());
                continue;
            }

            final String tcsTaskOutput = taskDBAdapter.getOutputForTask(task.getInstanceId());

            final int retryCount = 0;
            final BeginTaskRollbackMessage beginTaskMessage = new BeginTaskRollbackMessage(task.getName(),
                    task.getInstanceId(),
                    jobContext.getName(), jobContext.getInstanceId(), jobContext.getShardId(), retryCount,
                    tcsTaskOutput);

            final String taskExecutionTarget = task.getTaskExecutionTarget();
            sendBeginRollbackTaskNotification(taskExecutionTarget, beginTaskMessage);
        }
    }

    private void sendJobRollbackCompleteNotification(final String jobId, final String jobName,
            final String registrantURI) {

        final JobRollbackCompleteMessage jobCompleteMessage = new JobRollbackCompleteMessage(jobName,
                jobId);

        if (registrantURI != null) {
            final TcsTaskExecutionEndpoint targetAddress = AddressParser.parseAddress(registrantURI);
            producer.sendMessage(targetAddress.getExchangeName(), targetAddress.getRoutingKey(), jobCompleteMessage);
        }
    }

    private void sendBeginRollbackTaskNotification(final String taskExecutionTarget,
            final BeginTaskRollbackMessage beginTaskMessage) {
        final TcsTaskExecutionEndpoint targetAddress = AddressParser.parseAddress(taskExecutionTarget);
        producer.sendMessage(targetAddress.getExchangeName(), targetAddress.getRoutingKey(), beginTaskMessage);
    }
}
