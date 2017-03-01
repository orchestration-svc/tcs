package net.tcs.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.task.coordinator.endpoint.TcsTaskExecutionEndpoint;
import com.task.coordinator.producer.TcsProducer;

import net.tcs.db.JobInstanceDAO;
import net.tcs.db.TaskInstanceDAO;
import net.tcs.db.TaskInstanceDataDAO;
import net.tcs.exceptions.JobStateException;
import net.tcs.exceptions.TaskRetryException;
import net.tcs.exceptions.TaskStateException;
import net.tcs.messages.BeginTaskMessage;
import net.tcs.messages.JobCompleteMessage;
import net.tcs.messages.JobFailedMessage;
import net.tcs.messaging.AddressParser;
import net.tcs.shard.TCSShardRecoveryManager.JobRecoverContext;
import net.tcs.state.JobState;
import net.tcs.state.TaskState;
import net.tcs.task.JobDefinition;
import net.tcs.task.PredecessorTaskOutputImpl;
import net.tcs.task.TCSTaskInput;

/**
 * This class is responsible for maintaining an in-memory view of the running
 * Job instances, keeping track of the tasks, and dispatching ready-to-execute
 * tasks.
 *
 * A TCSDispatcher can process multiple JobInstances, but at any point of time,
 * a JobInstance is owned by a TCSDispatcher instance. This is ensured by
 * {@link #taskBoard}
 */
public class TCSDispatcher extends TCSAbstractDispatcher {
    private static final Logger LOGGER = LoggerFactory.getLogger(TCSDispatcher.class);

    private final String dispatcherName = "TCSDispatcher-" + UUID.randomUUID().toString();

    private final TaskBoard taskBoard;

    private final ObjectMapper mapper = new ObjectMapper();

    private final TCSStepParallelizer stepParallelizer;

    public TCSDispatcher(TaskBoard taskBoard, TcsProducer producer) {
        super(producer);
        this.taskBoard = taskBoard;
        stepParallelizer = new TCSStepParallelizer(taskDBAdapter, jobInstanceDBAdapter);
        taskBoard.registerTaskDispatcher(getTaskDispatcherId(), this);
    }

    public String getTaskDispatcherId() {
        return dispatcherName;
    }

    @Override
    public void processCommand(TCSCommand command) {

        switch (command.commandType) {

        case COMMAND_BEGIN_JOB:
            handleBeginJobCommand(command);
            break;

        case COMMAND_TASK_COMPLETE:
            handleTaskCompleteCommand(command);
            break;

        case COMMAND_TASK_FAILED:
            handleTaskFailedCommand(command);
            break;

        case COMMAND_TASK_RETRY:
            handleTaskRetryCommand(command);
            break;

        case COMMAND_RECOVER_JOB:
            handleRecoverJobCommand(command);
            break;

        default:
            LOGGER.warn("Unknown command: {}", command.commandType);
            break;
        }
    }

    /**
     * Handles TaskComplete event. If there are no downstream tasks to be
     * executed, then marks the JobInstance as COMPLETE. If there are downstream
     * tasks ready to execute, then it fires off those tasks.
     *
     * @param command
     */
    private void handleTaskCompleteCommand(final TCSCommand command) {
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
                handleJobComplete(jobId, jobContext);
            }
            return;
        }

        LOGGER.trace("Tasks ready to execute, for InProgress Job Id: {}", jobContext.getInstanceId());
        final JobInstanceDAO jobDAO = jobInstanceDBAdapter.getJobInstanceFromDB(jobId);
        if (jobDAO == null) {
            LOGGER.error("JobId: {} not found in DB", jobId);
            return;
        }

        beginTasks(jobContext, jobDAO, tasksReadyToExecute);
    }

    private void handleJobComplete(final String jobId, final JobStateMachineContextBase jobContext) {
        sendJobCompleteNotification(jobId, jobContext);
        try {
            jobInstanceDBAdapter.saveCompletedJob(jobId);
        } catch (final JobStateException ex) {
            LOGGER.error("JobStateException while saving completed job", ex);
        } finally {
            taskBoard.jobComplete(jobId);
            inProgressJobsMap.remove(jobId);
        }
    }

    /**
     * Handles TaskRetry event, by updating the retry-count, and firing off the
     * Task. If it has reached max-retry limit, then it marks the Task as
     * FAILED.
     *
     * @param command
     */
    private void handleTaskRetryCommand(final TCSCommand command) {

        TaskInstanceDAO taskDAO = (TaskInstanceDAO) command.body;
        final String taskId = taskDAO.getInstanceId();
        final String jobId = taskDAO.getJobInstanceId();

        try {
            taskDAO = taskDBAdapter.updateTaskRetry(taskId);
        } catch (final TaskStateException ex) {

            final String err = String.format(
                    "TaskStateException during TaskRetry DB update. taskId: {%s}, Expected state: {%s}, Actual state: {%s}",
                    taskId, ex.getExpectedState().value(), ex.getActualState().value());

            LOGGER.warn(err);
            return;
        } catch (final TaskRetryException ex) {
            LOGGER.error("TaskRetryException during TaskRetry DB update", ex);
            handleTaskFailed(jobId);
            return;
        }

        final JobStateMachineContextBase jobStateMachineContext = inProgressJobsMap.get(taskDAO.getJobInstanceId());
        if (jobStateMachineContext == null) {
            LOGGER.warn("JobId: {} not found in InProgressJob map", taskDAO.getJobInstanceId());
            return;
        }

        final TaskStateMachineContextBase taskStateMachineContext = jobStateMachineContext
                .getTaskExecutionContext(taskDAO.getName(), taskDAO.getParallelExecutionIndex());

        final JobInstanceDAO jobDAO = jobInstanceDBAdapter.getJobInstanceFromDB(jobId);
        if (jobDAO == null) {
            return;
        }

        final TCSTaskInput tcsTaskInput = jobStateMachineContext.getTaskInputForExecution(taskStateMachineContext,
                taskDBAdapter);

        final PredecessorTaskOutputImpl parentTaskOutput = new PredecessorTaskOutputImpl(tcsTaskInput.getParentTaskOutput());

        final BeginTaskMessage beginTaskMessage = new BeginTaskMessage(taskDAO.getName(),
                taskDAO.getParallelExecutionIndex(), taskDAO.getInstanceId(),
                jobStateMachineContext.getName(), taskDAO.getJobInstanceId(), taskDAO.getShardId(), taskDAO.getRetryCount(),
                tcsTaskInput.getTaskInput(), parentTaskOutput);
        beginTaskMessage.setJobContext(getJobContext(jobDAO));

        final String taskExecutionTarget = taskStateMachineContext.getTaskExecutionTarget();
        sendBeginTaskNotification(taskExecutionTarget, beginTaskMessage);
    }

    @SuppressWarnings("unchecked")
    private Map<String, String> getJobContext(JobInstanceDAO jobDAO) {
        try {
            return StringUtils.isEmpty(jobDAO.getJobContext()) ? new HashMap<>()
                    : mapper.readValue(jobDAO.getJobContext(), Map.class);
        } catch (final IOException e) {
            return new HashMap<>();
        }

    }

    private void handleTaskFailedCommand(final TCSCommand command) {
        final TaskInstanceDAO taskDAO = (TaskInstanceDAO) command.body;
        handleTaskFailed(taskDAO.getJobInstanceId());
    }

    /**
     * Handles BeginJob event. Fires off the root task(s).
     *
     * @param command
     */
    private void handleBeginJobCommand(final TCSCommand command) {

        final JobInstanceDAO job = (JobInstanceDAO) command.body;

        final JobDefinition jobDef = jobDefDBAdapter.getJobDefinition(job.getName());
        if (jobDef == null) {
            return;
        }

        final List<TaskInstanceDAO> taskDAOs = taskDBAdapter.getAllTasksForJobId(job.getInstanceId());

        final JobStateMachineContext jobContext = createJobStateMachineContext(job, jobDef.hasSteps());
        jobContext.initializeFromJobDefinition(jobDef);
        if (taskDAOs != null && !taskDAOs.isEmpty()) {
            jobContext.updateGraph(taskDAOs);
        }

        inProgressJobsMap.put(job.getInstanceId(), jobContext);
        final List<TaskStateMachineContextBase> tasks = jobContext.getRootTasks();

        beginTasks(jobContext, job, tasks);
    }

    private void beginRecoveredJob(JobRecoverContext recoverContext, JobDefinition jobDef) {
        LOGGER.trace("Job {} is still in INIT state; Job Id: {}", jobDef.getJobName(),
                recoverContext.getJobDAO().getInstanceId());

        final JobInstanceDAO job = jobInstanceDBAdapter.saveInProgressJob(recoverContext.getJobDAO().getInstanceId());

        final List<TaskInstanceDAO> taskDAOs = taskDBAdapter.getAllTasksForJobId(job.getInstanceId());

        final JobStateMachineContext jobContext = createJobStateMachineContext(job, jobDef.hasSteps());
        jobContext.initializeFromJobDefinition(jobDef);
        if (taskDAOs != null && !taskDAOs.isEmpty()) {
            jobContext.updateGraph(taskDAOs);
        }

        inProgressJobsMap.put(job.getInstanceId(), jobContext);
        final List<TaskStateMachineContextBase> tasks = jobContext.getRootTasks();

        recoverContext.notifyJobRecovered();
        beginTasks(jobContext, job, tasks);
    }

    private JobStateMachineContext createJobStateMachineContext(JobInstanceDAO job, boolean hasSteps) {
        if (hasSteps) {
            return new StepJobStateMachineContext(job.getName(), job.getInstanceId(), job.getShardId(),
                    job.getJobNotificationUri());
        } else {
            return new JobStateMachineContext(job.getName(), job.getInstanceId(), job.getShardId(),
                    job.getJobNotificationUri());
        }
    }

    /**
     * Handles RecoverJob command.
     *
     * @param command
     */
    private void handleRecoverJobCommand(final TCSCommand command) {
        final JobRecoverContext recoverContext = (JobRecoverContext) command.body;

        final JobInstanceDAO job = recoverContext.getJobDAO();

        final JobDefinition jobDef = jobDefDBAdapter.getJobDefinition(job.getName());

        if (jobDef == null) {
            taskBoard.jobComplete(job.getInstanceId());
            recoverContext.notifyJobRecovered();
            return;
        }

        /*
         * Here are the cases that we need to handle:
         *
         *
         * 1. Job is in INIT state.
         *
         * 2. Some tasks are in COMPLETE state. No tasks are in INPROGRESS
         * state.
         *
         * 3. Some tasks are in COMPLETE state. Some tasks are in INPROGRESS
         * state.
         *
         * 4. All tasks are in COMPLETE state.
         *
         * 5. At-least one task is in FAILED state.
         *
         * We do not have to do anything here to recover in-progress tasks. They
         * will be recovered by the TaskRetryDriver.
         */
        if (JobState.INIT == JobState.get(job.getState())) {
            /*
             * case 1
             */
            beginRecoveredJob(recoverContext, jobDef);
            return;
        }

        final List<TaskInstanceDAO> taskDAOs = taskDBAdapter.getAllTasksForJobId(job.getInstanceId());

        final JobStateMachineContext jobContext = createJobStateMachineContext(job, jobDef.hasSteps());

        jobContext.initializeFromJobDefinition(jobDef);
        inProgressJobsMap.put(job.getInstanceId(), jobContext);

        if (anyFailedTasks(taskDAOs)) {
            recoverContext.notifyJobRecovered();

            /*
             * case 5
             */
            LOGGER.info("Shard recovery: One or more failed tasks for Job name: {}, Job Id: {}", job.getName(),
                    job.getInstanceId());
            handleTaskFailed(job.getInstanceId());
            return;
        }

        final List<TaskStateMachineContextBase> tasksReadyToExecute;

        /*
         * Only get the tasks that are ready to execute, (in READY state).
         * In-progress tasks will be picked up for execution by the
         * TaskRetryDriver.
         */
        if (taskDAOs == null || taskDAOs.isEmpty()) {
            recoverContext.notifyJobRecovered();
            LOGGER.trace("No tasks have started executing for recovered InProgress Job name: {}, Job Id: {}",
                    job.getName(), job.getInstanceId());
            tasksReadyToExecute = jobContext.getRootTasks();
        } else {
            /*
             * case 2 or 3
             */

            if (jobDef.hasSteps()) {
                stepParallelizer.injectParallelTasksIntoGraphDuringRecovery(jobDef, taskDAOs, jobContext);
            }

            jobContext.updateGraph(taskDAOs);

            recoverContext.notifyJobRecovered();

            tasksReadyToExecute = jobContext.getTasksReadyToExecute();
        }

        if (tasksReadyToExecute.isEmpty()) {
            if (jobContext.areAllTasksComplete()) {
                LOGGER.trace("All tasks are complete for recovered InProgress Job name: {}, Job Id: {}", job.getName(),
                        job.getInstanceId());
                /*
                 * case 4
                 */
                final String jobId = job.getInstanceId();
                handleJobComplete(jobId, jobContext);
            } else {
                /*
                 * Some tasks are in-progress state.
                 */
                LOGGER.trace(
                        "No tasks are ready to execute, but some tasks are InProgress, for recovered InProgress Job name: {}, Job Id: {}",
                        job.getName(), job.getInstanceId());
            }
            return;
        }

        LOGGER.trace("Tasks ready to execute, for recovered InProgress Job Id: {}", job.getInstanceId());
        beginTasks(jobContext, job, tasksReadyToExecute);
    }

    private void beginTasks(final JobStateMachineContextBase jobStateMachineContext, JobInstanceDAO jobDAO,
            final List<TaskStateMachineContextBase> tasks) {

        final Map<String, String> context = getJobContext(jobDAO);

        final List<TaskStateMachineContextBase> tasksList;
        if (stepParallelizer.needParallelization(jobStateMachineContext, context, tasks)) {
            final JobDefinition jobDef = jobDefDBAdapter.getJobDefinition(jobDAO.getName());

            tasksList = new ArrayList<>();

            /*
             * For each (first) task (of step) that needs parallelization,
             * create parallel tasks, save in DB, and inject in in-memory DAG.
             */
            for (final TaskStateMachineContextBase task : tasks) {
                tasksList.addAll(
                        stepParallelizer.createParallelTasks(task, context, jobStateMachineContext, jobDAO, jobDef));
            }
        } else {
            tasksList = tasks;
        }

        for (final TaskStateMachineContextBase task : tasksList) {
            beginTask(jobStateMachineContext, task, context);
        }
    }

    private void beginTask(final JobStateMachineContextBase jobStateMachineContext, TaskStateMachineContextBase task,
            final Map<String, String> context) {
        LOGGER.trace("Task {} is ready to execute, for Job Id: {}", task.getName(),
                jobStateMachineContext.getInstanceId());

        task.markInProgress();
        try {
            taskDBAdapter.updateTaskStateToInProgress(task.getInstanceId());
        } catch (final Exception ex) {
            task.revertMarkInProgress();
            LOGGER.error("Failed to update task-state to InProgress in DB: Task {}; Job Id: {}", task.getName(),
                    jobStateMachineContext.getInstanceId());
            LOGGER.error("updateTaskStateToInProgress failed", ex);
            return;
        }

        final TCSTaskInput tcsTaskInput = jobStateMachineContext.getTaskInputForExecution(task, taskDBAdapter);

        final PredecessorTaskOutputImpl parentTaskOutput = new PredecessorTaskOutputImpl(
                tcsTaskInput.getParentTaskOutput());

        final int retryCount = 0;
        final BeginTaskMessage beginTaskMessage = new BeginTaskMessage(task.getName(), task.getParallelExecutionIndex(),
                task.getInstanceId(),
                jobStateMachineContext.getName(), jobStateMachineContext.getInstanceId(),
                jobStateMachineContext.getShardId(), retryCount, tcsTaskInput.getTaskInput(), parentTaskOutput);
        beginTaskMessage.setJobContext(context);

        final String taskExecutionTarget = task.getTaskExecutionTarget();
        sendBeginTaskNotification(taskExecutionTarget, beginTaskMessage);
    }

    private void handleTaskFailed(String jobId) {
        final JobStateMachineContextBase jobContext = inProgressJobsMap.get(jobId);
        if (jobContext == null) {
            LOGGER.warn("JobId: {} not found in InProgressJob map", jobId);
            return;
        }

        sendJobFailedNotification(jobId, jobContext);
        try {
            jobInstanceDBAdapter.saveFailedJob(jobId);
        } catch (final JobStateException ex) {
            LOGGER.error("JobStateException while saving completed job", ex);
        } finally {
            taskBoard.jobComplete(jobId);
            inProgressJobsMap.remove(jobId);
        }
    }

    private void sendJobCompleteNotification(final String jobId, final JobStateMachineContextBase jobContext) {

        final JobCompleteMessage jobCompleteMessage = new JobCompleteMessage(jobContext.getName(), jobId);
        final List<TaskStateMachineContextBase> taskContexts = jobContext.getAllCompletedTasks();
        final Map<String, String> contextToResultMap = new HashMap<>();
        for (final TaskStateMachineContextBase context : taskContexts) {
            final TaskInstanceDataDAO taskInstance = taskDBAdapter.getTaskFromDB(context.getInstanceId());

            if (!StringUtils.isEmpty(taskInstance.getTaskInput())
                    && !StringUtils.isEmpty(taskInstance.getTaskOutput())) {
                contextToResultMap.put(taskInstance.getTaskInput(), taskInstance.getTaskOutput());
            }
        }
        jobCompleteMessage.setTaskContextToResult(contextToResultMap);

        if (jobContext.getRegistrant() != null) {
            final TcsTaskExecutionEndpoint targetAddress = AddressParser.parseAddress(jobContext.getRegistrant());
            producer.sendMessage(targetAddress.getExchangeName(), targetAddress.getRoutingKey(), jobCompleteMessage);
        }
    }

    private void sendJobFailedNotification(final String jobId, final JobStateMachineContextBase jobContext) {
        final JobFailedMessage jobFailedMessage = new JobFailedMessage(jobContext.getName(), jobId);

        if (jobContext.getRegistrant() != null) {
            final TcsTaskExecutionEndpoint targetAddress = AddressParser.parseAddress(jobContext.getRegistrant());
            producer.sendMessage(targetAddress.getExchangeName(), targetAddress.getRoutingKey(), jobFailedMessage);
        }
    }

    private void sendBeginTaskNotification(final String taskExecutionTarget, final BeginTaskMessage beginTaskMessage) {
        final TcsTaskExecutionEndpoint targetAddress = AddressParser.parseAddress(taskExecutionTarget);
        producer.sendMessage(targetAddress.getExchangeName(), targetAddress.getRoutingKey(), beginTaskMessage);
    }

    private static boolean anyFailedTasks(List<TaskInstanceDAO> taskDAOs) {
        for (final TaskInstanceDAO taskDAO : taskDAOs) {
            if (TaskState.FAILED == TaskState.get(taskDAO.getState())) {
                return true;
            }
        }
        return false;
    }
}
