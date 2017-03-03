package net.tcs.messages;

import java.util.HashMap;
import java.util.Map;

public class JobSubmitRequest {

    private String jobName;

    private String jobId;

    private String jobNotificationUri;

    private Map<String, String> input;

    private Map<String, String> jobContext = new HashMap<>();

    public JobSubmitRequest(String jobName, String jobId, String jobNotificationUri, Map<String, String> input) {
        this.jobName = jobName;
        this.jobId = jobId;
        this.jobNotificationUri = jobNotificationUri;
        this.input = input;
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

    public Map<String, String> getInput() {
        return input;
    }

    public void setInput(Map<String, String> input) {
        this.input = input;
    }

    public Map<String, String> getJobContext() {
        final Map<String, String> jc = new HashMap<>(jobContext);
        return jc;
    }

    public void setJobContext(Map<String, String> jobContext) {
        this.jobContext = new HashMap<>(jobContext);
    }

    public JobSubmitRequest() {
    }
}
