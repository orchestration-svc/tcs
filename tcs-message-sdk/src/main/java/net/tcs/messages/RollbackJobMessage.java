package net.tcs.messages;

public class RollbackJobMessage {

    public RollbackJobMessage() {
        super();
    }

    public RollbackJobMessage(String jobId) {
        super();
        this.jobId = jobId;
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    private String jobId;
}
