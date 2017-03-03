package net.tcs.messages;

public class JobRollbackRequest {
    private String jobName;

    private String jobId;

    private String jobNotificationUri;

    public JobRollbackRequest(String jobName, String jobId, String jobNotificationUri) {
        this.jobName = jobName;
        this.jobId = jobId;
        this.jobNotificationUri = jobNotificationUri;
    }

    public JobRollbackRequest() {
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

    public String getJobNotificationUri() {
        return jobNotificationUri;
    }

    public void setJobNotificationUri(String jobNotificationUri) {
        this.jobNotificationUri = jobNotificationUri;
    }
}
