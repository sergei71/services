package ru.gex.mdm_proxy.elk_consumer.domain.exception;

public class InternalServiceException extends Exception {
    public InternalServiceException() {
        super();
    }

    public InternalServiceException(String message) {
        super(message);
    }

    public InternalServiceException(String message, Throwable cause) {
        super(message, cause);
    }

    public InternalServiceException(Throwable cause) {
        super(cause);
    }

    protected InternalServiceException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
