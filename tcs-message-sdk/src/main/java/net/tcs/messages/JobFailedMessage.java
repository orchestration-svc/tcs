package net.tcs.messages;

public class JobFailedMessage {

    private String jobName;
    private String jobId;

    public JobFailedMessage(String jobName, String jobId) {
        this.jobName = jobName;
        this.jobId = jobId;
    }

    public JobFailedMessage() {
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

    @Override
    public String toString() {
        return "JobFailedMessage [jobName=" + jobName + ", jobId=" + jobId + "]";
    }
}
