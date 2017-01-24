package net.tcs.core;

import net.tcs.api.TCSTaskContext;

public class TaskContextImpl implements TCSTaskContext {
    public TaskContextImpl(String taskName, int taskParallelExecutionIndex, String taskId, String jobName, String jobId,
            String shardId,
            int retryCount) {
        super();
        this.taskName = taskName;
        this.taskParallelExecutionIndex = taskParallelExecutionIndex;
        this.taskId = taskId;
        this.jobName = jobName;
        this.jobId = jobId;
        this.shardId = shardId;
        this.retryCount = retryCount;
    }

    private final String taskName;
    private final int taskParallelExecutionIndex;
    private final String taskId;
    private final String jobName;
    private final String jobId;
    private final String shardId;
    private final int retryCount;

    @Override
    public String getTaskName() {
        return taskName;
    }

    @Override
    public int getTaskRetryCount() {
        return retryCount;
    }

    @Override
    public String getTaskId() {
        return taskId;
    }

    @Override
    public String getJobName() {
        return jobName;
    }

    @Override
    public String getJobId() {
        return jobId;
    }

    @Override
    public String getShardId() {
        return shardId;
    }

    @Override
    public int getTaskParallelExecutionIndex() {
        return taskParallelExecutionIndex;
    }
}
