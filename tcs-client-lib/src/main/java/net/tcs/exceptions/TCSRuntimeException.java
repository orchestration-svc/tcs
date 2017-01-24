package net.tcs.exceptions;

public class TCSRuntimeException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public TCSRuntimeException(Throwable ex) {
        super(ex);
    }
}
