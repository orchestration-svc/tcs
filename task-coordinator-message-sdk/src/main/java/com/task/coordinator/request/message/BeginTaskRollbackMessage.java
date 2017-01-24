package com.task.coordinator.request.message;

import com.task.coordinator.base.message.TcsAsyncCtrlMessage;

public class BeginTaskRollbackMessage extends TcsAsyncCtrlMessage {

    public BeginTaskRollbackMessage(String taskName, String taskId, String jobName, String jobId, String shardId,
            int retryCount, String taskOutput) {
        this.taskName = taskName;
        this.taskId = taskId;
        this.jobName = jobName;
        this.jobId = jobId;
        this.shardId = shardId;
        this.retryCount = retryCount;
        this.taskOutput = taskOutput;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    public BeginTaskRollbackMessage() {
    }

    public String getTaskName() {
        return taskName;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public String getShardId() {
        return shardId;
    }

    public void setShardId(String shardId) {
        this.shardId = shardId;
    }

    public String getTaskOutput() {
        return taskOutput;
    }

    public void setTaskOutput(String taskOutput) {
        this.taskOutput = taskOutput;
    }

    private String taskName;
    private String taskId;
    private String jobName;
    private String jobId;
    private String shardId;
    private int retryCount;
    private String taskOutput;

    @Override
    public String toString() {
        return "BeginTaskRollbackMessage [taskName=" + taskName + ", taskId=" + taskId + ", jobName=" + jobName
                + ", jobId=" + jobId + ", shardId=" + shardId + ", retryCount=" + retryCount + ", taskOutput="
                + taskOutput + "]";
    }
}
