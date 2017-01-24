package net.tcs.exceptions;

public class JobInstanceNotFoundException extends RuntimeException {
    public String getJobId() {
        return jobId;
    }

    public JobInstanceNotFoundException(String jobId) {
        super();
        this.jobId = jobId;
    }

    private static final long serialVersionUID = 1L;

    private final String jobId;
}
