package net.tcs.messages;

public class QueryJobSpecResponse {

    public QueryJobSpecResponse() {
    }

    public QueryJobSpecResponse(String jobSpec, String status) {
        this.jobSpec = jobSpec;
        this.status = status;
    }

    public QueryJobSpecResponse(String jobSpec, String status, String errorDetails) {
        this.jobSpec = jobSpec;
        this.status = status;
        this.errorDetails = errorDetails;
    }

    public String getJobSpec() {
        return jobSpec;
    }

    public void setJobSpec(String jobSpec) {
        this.jobSpec = jobSpec;
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

    private String jobSpec;
    private String status;
    private String errorDetails;
}
