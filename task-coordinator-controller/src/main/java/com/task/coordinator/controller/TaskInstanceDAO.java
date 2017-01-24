package com.task.coordinator.controller;

import java.util.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

@Entity
@Table(name = "TaskInstance")
public class TaskInstanceDAO {

    public TaskInstanceDAO() {
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

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public Date getStartTime() {
        return startTime;
    }

    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }

    public Date getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Date updateTime) {
        this.updateTime = updateTime;
    }

    public Date getCompletionTime() {
        return completionTime;
    }

    public void setCompletionTime(Date completionTime) {
        this.completionTime = completionTime;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
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

    public int getRetryCount() {
        return retryCount;
    }

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    public String getRollbackState() {
        return rollbackState;
    }

    public void setRollbackState(String rollbackState) {
        this.rollbackState = rollbackState;
    }

    public Date getRollbackStartTime() {
        return rollbackStartTime;
    }

    public void setRollbackStartTime(Date rollbackStartTime) {
        this.rollbackStartTime = rollbackStartTime;
    }

    public Date getRollbackCompletionTime() {
        return rollbackCompletionTime;
    }

    public void setRollbackCompletionTime(Date rollbackCompletionTime) {
        this.rollbackCompletionTime = rollbackCompletionTime;
    }

    @Id
    @Column(name = "instanceId")
    private String instanceId;

    @Column(name="name")
    private String name;

    @Column(name = "parallelExecutionIndex")
    private int parallelExecutionIndex;

    @Column(name = "jobInstanceId")
    private String jobInstanceId;

    @Column(name = "shardId")
    private String shardId;

    @Column(name="state")
    private String state;

    @Column(name = "rollbackState")
    private String rollbackState;

    @Column(name = "retryCount")
    private int retryCount;

    @Column(name = "startTime", columnDefinition = "DATETIME")
    @Temporal(TemporalType.TIMESTAMP)
    private Date startTime;

    @Column(name = "updateTime", columnDefinition = "DATETIME")
    @Temporal(TemporalType.TIMESTAMP)
    private Date updateTime;

    @Column(name = "completionTime", columnDefinition = "DATETIME")
    @Temporal(TemporalType.TIMESTAMP)
    private Date completionTime;

    @Column(name = "rollbackStartTime", columnDefinition = "DATETIME")
    @Temporal(TemporalType.TIMESTAMP)
    private Date rollbackStartTime;

    @Column(name = "rollbackCompletionTime", columnDefinition = "DATETIME")
    @Temporal(TemporalType.TIMESTAMP)
    private Date rollbackCompletionTime;
}
