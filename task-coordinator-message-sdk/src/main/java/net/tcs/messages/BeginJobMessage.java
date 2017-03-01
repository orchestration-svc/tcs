package net.tcs.messages;

public class BeginJobMessage {

    public BeginJobMessage() {
        super();
    }

    public BeginJobMessage(String jobId) {
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
