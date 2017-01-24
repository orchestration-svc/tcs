package net.tcs.exceptions;

import net.tcs.state.JobState;

public class JobStateException extends IllegalStateException {

    public JobState getExpectedState() {
        return expectedState;
    }

    public JobState getActualState() {
        return actualState;
    }

    public JobStateException(JobState expectedState, JobState actualState) {
        super();
        this.expectedState = expectedState;
        this.actualState = actualState;
    }

    private static final long serialVersionUID = 1L;

    private final JobState expectedState;

    private final JobState actualState;
}
