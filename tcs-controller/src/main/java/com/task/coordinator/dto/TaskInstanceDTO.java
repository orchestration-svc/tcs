package com.task.coordinator.dto;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.task.coordinator.controller.TaskInstanceDAO;
import com.task.coordinator.controller.TaskInstanceDataDAO;

@JsonInclude(Include.NON_NULL)
@JsonPropertyOrder({ "name", "instanceId", "parallelExecutionIndex", "jobName", "jobInstanceId", "shardId", "state",
        "retryCount", "startTime", "updateTime", "completionTime", "rollbackState", "rollbackStartTime",
        "rollbackCompletionTime" })
public class TaskInstanceDTO {
    public String getStartTime() {
        return startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    public String getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(String updateTime) {
        this.updateTime = updateTime;
    }

    public String getCompletionTime() {
        return completionTime;
    }

    public void setCompletionTime(String completionTime) {
        this.completionTime = completionTime;
    }

    public String getRollbackStartTime() {
        return rollbackStartTime;
    }

    public void setRollbackStartTime(String rollbackStartTime) {
        this.rollbackStartTime = rollbackStartTime;
    }

    public String getRollbackCompletionTime() {
        return rollbackCompletionTime;
    }

    public void setRollbackCompletionTime(String rollbackCompletionTime) {
        this.rollbackCompletionTime = rollbackCompletionTime;
    }

    public String getInstanceId() {
        return instanceId;
    }

    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }

    public int getParallelExecutionIndex() {
        return parallelExecutionIndex;
    }

    public void setParallelExecutionIndex(int parallelExecutionIndex) {
        this.parallelExecutionIndex = parallelExecutionIndex;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public String getJobInstanceId() {
        return jobInstanceId;
    }

    public void setJobInstanceId(String jobInstanceId) {
        this.jobInstanceId = jobInstanceId;
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

    public String getTaskOutput() {
        return taskOutput;
    }

    public void setTaskOutput(String taskOutput) {
        this.taskOutput = taskOutput;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getRollbackState() {
        return rollbackState;
    }

    public void setRollbackState(String rollbackState) {
        this.rollbackState = rollbackState;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    public TaskInstanceDTO() {
    }

    public TaskInstanceDTO(String jobName, TaskInstanceDAO taskInstance, TaskInstanceDataDAO taskInstanceData) {
        this.jobName = jobName;
        this.instanceId = taskInstance.getInstanceId();
        this.parallelExecutionIndex = taskInstance.getParallelExecutionIndex();
        this.name = taskInstance.getName();
        this.jobInstanceId = taskInstance.getJobInstanceId();
        this.shardId = taskInstance.getShardId();
        this.taskInput = taskInstanceData.getTaskInput();
        this.taskOutput = taskInstanceData.getTaskOutput();
        this.state = taskInstance.getState();
        this.rollbackState = taskInstance.getRollbackState();
        this.retryCount = taskInstance.getRetryCount();
        this.startTime = getTimeAsString(taskInstance.getStartTime());
        this.updateTime = getTimeAsString(taskInstance.getUpdateTime());
        this.completionTime = getTimeAsString(taskInstance.getCompletionTime());
        this.rollbackStartTime = getTimeAsString(taskInstance.getRollbackStartTime());
        this.rollbackCompletionTime = getTimeAsString(taskInstance.getRollbackCompletionTime());
    }

    private static String getTimeAsString(Date time) {
        if (time != null) {
            return time.toString();
        } else {
            return null;
        }
    }
    private String instanceId;

    private String name;

    private int parallelExecutionIndex;

    private String jobName;

    private String jobInstanceId;

    private String shardId;

    private String taskInput;

    private String taskOutput;

    private String state;

    private String rollbackState;

    private int retryCount;

    private String startTime;

    private String updateTime;

    private String completionTime;

    private String rollbackStartTime;

    private String rollbackCompletionTime;
}
