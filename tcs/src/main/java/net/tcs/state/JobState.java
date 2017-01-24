package net.tcs.state;

public enum JobState {
    READY("READY"),
    INPROGRESS("INPROGRESS"),
    COMPLETE("COMPLETE"),
    FAILED("FAILED"),
    ROLLBACK_INPROGRESS("ROLLBACK_INPROGRESS"),
    ROLLBACK_COMPLETE("ROLLBACK_COMPLETE"),
    ROLLBACK_FAILED("ROLLBACK_FAILED");

    private final String state;

    private JobState(final String state) {
        this.state = state;
    }

    public String value() {
        return state;
    }

    public static JobState get(String state) {
        switch (state) {
        case "READY":
            return JobState.READY;
        case "INPROGRESS":
            return JobState.INPROGRESS;
        case "COMPLETE":
            return JobState.COMPLETE;
        case "FAILED":
            return JobState.FAILED;
        case "ROLLBACK_INPROGRESS":
            return JobState.ROLLBACK_INPROGRESS;
        case "ROLLBACK_COMPLETE":
            return JobState.ROLLBACK_COMPLETE;
        case "ROLLBACK_FAILED":
            return JobState.ROLLBACK_FAILED;
        default:
            throw new IllegalArgumentException(state);
        }
    }
}
