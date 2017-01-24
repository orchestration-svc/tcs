package net.tcs.exceptions;

public class JobRollbackIllegalStateException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public JobRollbackIllegalStateException(String message) {
        super(message);
    }

    public JobRollbackIllegalStateException(String message, Throwable cause) {
        super(message, cause);
    }
}
