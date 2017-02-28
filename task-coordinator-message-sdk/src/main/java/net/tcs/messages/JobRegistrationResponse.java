package net.tcs.messages;

public class JobRegistrationResponse {

    public JobRegistrationResponse() {
    }

    public JobRegistrationResponse(String jobName, String status) {
        this.jobName = jobName;
        this.status = status;
    }

    public JobRegistrationResponse(String jobName, String status, String errorDetails) {
        this.jobName = jobName;
        this.status = status;
        this.errorDetails = errorDetails;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
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
    private String status;
    private String errorDetails;
}
