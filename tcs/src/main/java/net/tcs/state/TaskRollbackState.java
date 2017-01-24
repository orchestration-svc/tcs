package net.tcs.state;

public enum TaskRollbackState {
    ROLLBACK_READY("ROLLBACK_READY"), ROLLBACK_NOT_READY("ROLLBACK_NOT_READY"), ROLLBACK_INPROGRESS(
            "ROLLBACK_INPROGRESS"), ROLLBACK_COMPLETE("ROLLBACK_COMPLETE"), ROLLBACK_FAILED(
                    "ROLLBACK_FAILED"), ROLLBACK_DISALLOWED("ROLLBACK_DISALLOWED");

    private final String state;

    private TaskRollbackState(final String state) {
        this.state = state;
    }

    public String value() {
        return state;
    }

    public static TaskRollbackState get(String state) {
        switch (state) {
        case "ROLLBACK_READY":
            return TaskRollbackState.ROLLBACK_READY;
        case "ROLLBACK_NOT_READY":
            return TaskRollbackState.ROLLBACK_NOT_READY;
        case "ROLLBACK_INPROGRESS":
            return TaskRollbackState.ROLLBACK_INPROGRESS;
        case "ROLLBACK_COMPLETE":
            return TaskRollbackState.ROLLBACK_COMPLETE;
        case "ROLLBACK_FAILED":
            return TaskRollbackState.ROLLBACK_FAILED;
        case "ROLLBACK_DISALLOWED":
            return TaskRollbackState.ROLLBACK_DISALLOWED;
        default:
            throw new IllegalArgumentException(state);
        }
    }
}
