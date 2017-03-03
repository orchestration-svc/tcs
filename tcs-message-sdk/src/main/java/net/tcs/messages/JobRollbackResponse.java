package net.tcs.messages;

public class JobRollbackResponse {
    public JobRollbackResponse(String jobName, String jobId) {
        this.jobName = jobName;
        this.jobId = jobId;
        this.status = "OK";
    }

    public JobRollbackResponse(String jobName, String status, String error) {
        this.jobName = jobName;
        this.errorDetails = error;
        this.status = status;
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

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getErrorDetails() {
        return errorDetails;
    }

    public void setErrorDetails(String errorDetails) {
        this.errorDetails = errorDetails;
    }

    private String jobName;

    private String jobId;

    private String status;

    private String errorDetails;
}
