package net.tcs.messages;

public class JobRollbackResponse {
    public JobRollbackResponse(String jobName, String jobId) {
        this.jobName = jobName;
        this.jobId = jobId;
    }

    public JobRollbackResponse(String jobName, String jobId, String errorCode) {
        this.jobName = jobName;
        this.jobId = jobId;
        this.errorMessage = errorCode;
    }

    public JobRollbackResponse() {
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    private String jobName;

    private String jobId;

    private String errorMessage;
    // TODO job start time
}
