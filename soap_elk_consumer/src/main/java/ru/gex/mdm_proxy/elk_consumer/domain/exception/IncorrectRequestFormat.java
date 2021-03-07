package ru.gex.mdm_proxy.elk_consumer.domain.exception;

public class IncorrectRequestFormat extends Exception {
    public IncorrectRequestFormat() {
        super();
    }

    public IncorrectRequestFormat(String message) {
        super(message);
    }

    public IncorrectRequestFormat(String message, Throwable cause) {
        super(message, cause);
    }

    public IncorrectRequestFormat(Throwable cause) {
        super(cause);
    }

    protected IncorrectRequestFormat(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
