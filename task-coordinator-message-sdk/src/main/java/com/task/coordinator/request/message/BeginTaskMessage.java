package com.task.coordinator.request.message;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.task.coordinator.base.message.TcsAsyncCtrlMessage;

import net.tcs.task.ParentTaskOutput;
import net.tcs.task.PredecessorTaskOutputImpl;

public class BeginTaskMessage extends TcsAsyncCtrlMessage {

    public BeginTaskMessage(String taskName, int taskParallelIndex, String taskId, String jobName, String jobId,
            String shardId,
            int retryCount, String taskInput, ParentTaskOutput parentTaskOutput) {
        this.taskName = taskName;
        this.taskParallelIndex = taskParallelIndex;
        this.taskId = taskId;
        this.jobName = jobName;
        this.jobId = jobId;
        this.shardId = shardId;
        this.retryCount = retryCount;
        this.taskInput = taskInput;
        this.parentTaskOutput = parentTaskOutput;
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

    public BeginTaskMessage() {
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

    public ParentTaskOutput getParentTaskOutput() {
        return parentTaskOutput;
    }

    public void setParentTaskOutput(ParentTaskOutput parentTaskOutput) {
        this.parentTaskOutput = parentTaskOutput;
    }

    public String getShardId() {
        return shardId;
    }

    public void setShardId(String shardId) {
        this.shardId = shardId;
    }

    public String getTaskInput() {
        return taskInput;
    }

    public void setTaskInput(String taskInput) {
        this.taskInput = taskInput;
    }

    public Map<String, String> getJobContext() {
        final Map<String, String> jc = new HashMap<>(jobContext);
        return jc;
    }

    public void setJobContext(Map<String, String> jobContext) {
        this.jobContext = new HashMap<>(jobContext);
    }

    public int getTaskParallelIndex() {
        return taskParallelIndex;
    }

    public void setTaskParallelIndex(int taskParallelIndex) {
        this.taskParallelIndex = taskParallelIndex;
    }

    private String taskName;
    private int taskParallelIndex;
    private String taskId;
    private String jobName;
    private String jobId;
    private String shardId;
    private int retryCount;
    private Map<String, String> jobContext;
    private String taskInput;
    @JsonDeserialize(as = PredecessorTaskOutputImpl.class)
    private ParentTaskOutput parentTaskOutput;

    @Override
    public String toString() {
        return "BeginTaskMessage [taskName=" + taskName + ", taskParallelIndex=" + taskParallelIndex + ", taskId="
                + taskId + ", jobName=" + jobName + ", jobId="
                + jobId + ", shardId=" + shardId + ", retryCount=" + retryCount + ", taskInput=" + taskInput
                + ", parentTaskOutput=" + parentTaskOutput + "]";
    }

}
