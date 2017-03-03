package net.tcs.messages;

public class JobSubmitResponse {
    public JobSubmitResponse(String jobName, String jobId, String shardId) {
        this.jobName = jobName;
        this.jobId = jobId;
        this.shardId = shardId;
        this.status = "OK";
    }

    public JobSubmitResponse(String jobName, String status) {
        this.jobName = jobName;
        this.status = status;
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

    public String getShardId() {
        return shardId;
    }

    public void setShardId(String shardId) {
        this.shardId = shardId;
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

    public JobSubmitResponse() {
    }

    private String jobName;

    private String jobId;

    private String shardId;

    private String status;

    private String errorDetails;
    // TODO job start time
}
