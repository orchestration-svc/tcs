package net.tcs.api;

public interface TCSTaskContext {
    public String getTaskName();

    public int getTaskParallelExecutionIndex();

    public int getTaskRetryCount();

    public String getTaskId();

    public String getJobName();

    public String getJobId();

    public String getShardId();
}
