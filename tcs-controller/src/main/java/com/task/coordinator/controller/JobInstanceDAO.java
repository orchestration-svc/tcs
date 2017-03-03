package com.task.coordinator.controller;

import java.util.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

@Entity
@Table(name = "JobInstance")
public class JobInstanceDAO {

    public JobInstanceDAO() {
    }

    public String getInstanceId() {
        return instanceId;
    }

    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }

    public String getShardId() {
        return shardId;
    }

    public void setShardId(String shardId) {
        this.shardId = shardId;
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

    public String getJobNotificationUri() {
        return jobNotificationUri;
    }

    public void setJobNotificationUri(String jobNotificationUri) {
        this.jobNotificationUri = jobNotificationUri;
    }

    public String getJobContext() {
        return jobContext;
    }

    public void setJobContext(String jobContext) {
        this.jobContext = jobContext;
    }

    @Id
    @Column(name = "instanceId")
    private String instanceId;

    @Column(name="name")
    private String name;

    @Column(name = "shardId")
    private String shardId;

    @Column(name="state")
    private String state;

    @Column(name = "startTime", columnDefinition = "DATETIME")
    @Temporal(TemporalType.TIMESTAMP)
    private Date startTime;

    @Column(name = "completionTime", columnDefinition = "DATETIME")
    @Temporal(TemporalType.TIMESTAMP)
    private Date completionTime;

    @Column(name = "jobNotificationUri")
    private String jobNotificationUri;

    @Column(name = "jobContext")
    private String jobContext;
}
