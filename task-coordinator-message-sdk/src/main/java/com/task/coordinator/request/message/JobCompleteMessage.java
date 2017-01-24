package com.task.coordinator.request.message;

import java.util.Map;

import com.task.coordinator.base.message.TcsAsyncCtrlMessage;

public class JobCompleteMessage extends TcsAsyncCtrlMessage{

    private String jobName;
    private String jobId;
    private Map<String, String> taskContextToResult;

    public JobCompleteMessage(String jobName, String jobId) {
        this.jobName = jobName;
        this.jobId = jobId;
    }

    public JobCompleteMessage() {
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public Map<String, String> getTaskContextToResult() {
        return taskContextToResult;
    }

    public void setTaskContextToResult(Map<String, String> taskContextToResult) {
        this.taskContextToResult = taskContextToResult;
    }

    @Override
    public String toString() {
        return "JobCompleteMessage [jobName=" + jobName + ", jobId=" + jobId + "]";
    }
}
