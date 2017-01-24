package net.tcs.api;

import java.util.Map;

import net.tcs.task.JobSpec;

/**
 * Client interface to interact with TCS service.
 */
public interface TCSClient {
    /**
     * Must be called before calling any other APIs.
     */
    public void initialize();

    /**
     * Register a JobSpec with TCS
     *
     * @param job
     */
    public void registerJob(JobSpec job);


    /**
     * Query a JobSpec that has been registered with TCS
     *
     * @param jobName
     * @return
     */
    public JobSpec queryRegisteredJob(String jobName);

    /**
     * Prepare to execute a subset of tasks for a JobSpec. This will set up
     * message listeners, so that the job and task notifications can be
     * received.
     *
     * @param jobName
     * @param taskHandlers
     *            Map of {TaskName => TCSCallback handler}
     */
    public void prepareToExecute(String jobName, TCSJobHandler jobHandler, Map<String, TCSCallback> taskHandlers);

    /**
     * Start a Job
     *
     * @param jobName
     *            Name of JobSpec
     * @param jobInputForParentTasks
     *            Initial input for the Job execution
     * @return jobInstanceId
     */
    public String startJob(String jobName, byte[] jobInputForParentTasks);

    /**
     * Start a Job
     *
     * @param jobName
     *            Name of JobSpec
     * @param jobInputForParentTasks
     *            Initial input for the Job execution
     * @param jobContext
     * @return jobInstanceId
     */
    public String startJob(String jobName, byte[] jobInputForParentTasks, Map<String, String> jobContext);

    /**
     * Start a Job
     *
     * @param jobName
     *            Name of JobSpec
     * @param jobInput
     *            Initial input for the Job execution
     * @return jobInstanceId
     */
    public String startJob(String jobName, Map<String, byte[]> jobInput);

    /**
     * Start a Job
     *
     * @param jobName
     *            Name of JobSpec
     * @param jobInput
     *            Initial input for the Job execution
     * @param jobContext
     * @return jobInstanceId
     */
    public String startJob(String jobName, Map<String, byte[]> jobInput, Map<String, String> jobContext);

    /**
     * Notify TCS that a task is complete.
     *
     * @param taskExecutionContext
     *            Task context
     * @param taskOutput
     *            Output from the Task execution
     */
    public void taskComplete(TCSTaskContext taskExecutionContext, byte[] taskOutput);

    /**
     * Notify TCS that a task is complete.
     *
     * @param taskExecutionContext
     *            Task context
     * @param taskOutput
     *            Output from the Task execution
     * @param taskContextOutput
     *            Output context map from the Task execution
     */
    public void taskComplete(TCSTaskContext taskExecutionContext, byte[] taskOutput, Map<String, String> taskContextOutput);

    /**
     * Notify TCS that a task is still in progress, and therefore, should not be
     * retried. For a long-running task, the TCS client must send this message
     * as a way of pinging TCS, so that TCS does not attempt to retry the task.
     *
     * @param taskContext
     *            Task context
     */
    public void taskInProgress(TCSTaskContext taskContext);

    /**
     * Notify TCS that a task has failed.
     *
     * @param taskContext
     *            Task context
     * @param error
     *            Output from the Task execution
     */
    public void taskFailed(TCSTaskContext taskContext, byte[] error);

    public void rollbackJob(String jobName, String jobInstanceId);

    public void taskRollbackComplete(TCSTaskContext taskContext);

    public void taskRollbackNotSupported();

    public void taskRollbackFailed();

    /**
     * Cleanup TCSClient runtime.
     */
    public void cleanup();
}
