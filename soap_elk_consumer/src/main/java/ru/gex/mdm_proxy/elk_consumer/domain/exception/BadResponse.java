package ru.gex.mdm_proxy.elk_consumer.domain.exception;

public class BadResponse extends Exception {
    public BadResponse() {
        super();
    }

    public BadResponse(String message) {
        super(message);
    }

    public BadResponse(String message, Throwable cause) {
        super(message, cause);
    }

    public BadResponse(Throwable cause) {
        super(cause);
    }

    protected BadResponse(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
