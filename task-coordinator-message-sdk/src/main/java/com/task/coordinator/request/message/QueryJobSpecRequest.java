package com.task.coordinator.request.message;

public class QueryJobSpecRequest {

    public QueryJobSpecRequest() {
    }

    public QueryJobSpecRequest(String jobName) {
        this.jobName = jobName;
    }

    private String jobName;

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

}
