package net.tcs.api;

public interface TCSJobHandler {
    /**
     * Called by Task Coordination Service (TCS) upon Job completion.
     *
     * @param jobId
     *            Job UUID
     */
    public void jobComplete(String jobId);

    /**
     * Called by Task Coordination Service (TCS) upon Job failure, as a result
     * of one or more task failures.
     *
     * @param jobId
     *            Job UUID
     */
    public void jobFailed(String jobId);

    public void jobRollbackComplete(String jobId);

    public void jobRollbackFailed(String jobId);
}
