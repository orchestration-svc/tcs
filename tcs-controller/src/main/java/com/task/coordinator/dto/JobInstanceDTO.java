package com.task.coordinator.dto;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.task.coordinator.controller.JobInstanceDAO;

@JsonInclude(Include.NON_NULL)
@JsonPropertyOrder({ "name", "instanceId", "shardId", "state", "startTime", "completionTime", "jobNotificationUri",
        "jobContext" })
public class JobInstanceDTO {

    public String getInstanceId() {
        return instanceId;
    }

    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
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

    public String getStartTime() {
        return startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    public String getCompletionTime() {
        return completionTime;
    }

    public void setCompletionTime(String completionTime) {
        this.completionTime = completionTime;
    }

    public String getJobNotificationUri() {
        return jobNotificationUri;
    }

    public void setJobNotificationUri(String jobNotificationUri) {
        this.jobNotificationUri = jobNotificationUri;
    }

    public Map<String, String> getJobContext() {
        return new HashMap<>(jobContext);
    }

    public void setJobContext(Map<String, String> ctx) {
        jobContext.clear();
        jobContext.putAll(ctx);
    }

    public JobInstanceDTO() {
    }

    public JobInstanceDTO(JobInstanceDAO jobDAO) {
        instanceId = jobDAO.getInstanceId();
        name = jobDAO.getName();
        shardId = jobDAO.getShardId();
        state = jobDAO.getState();
        startTime = getTimeAsString(jobDAO.getStartTime());
        completionTime = getTimeAsString(jobDAO.getCompletionTime());
        jobNotificationUri = jobDAO.getJobNotificationUri();
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

    private String shardId;

    private String state;

    private String startTime;

    private String completionTime;

    private String jobNotificationUri;

    private final Map<String, String> jobContext = new HashMap<>();
}
