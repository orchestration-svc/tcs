package net.tcs.messages;

public class JobSubmitResponse {
    public JobSubmitResponse(String jobName, String jobId, String shardId) {
        this.jobName = jobName;
        this.jobId = jobId;
        this.shardId = shardId;
    }

    public JobSubmitResponse(String jobName, String errorCode) {
        this.jobName = jobName;
        this.errorMessage = errorCode;
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

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public JobSubmitResponse() {
    }

    private String jobName;

    private String jobId;

    private String shardId;

    private String errorMessage;
    // TODO job start time
}
