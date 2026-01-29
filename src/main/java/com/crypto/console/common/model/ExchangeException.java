package com.crypto.console.common.model;

public class ExchangeException extends RuntimeException {
    private final String userMessage;

    public ExchangeException(String userMessage) {
        super(userMessage);
        this.userMessage = userMessage;
    }

    public ExchangeException(String userMessage, Throwable cause) {
        super(userMessage, cause);
        this.userMessage = userMessage;
    }

    public String getUserMessage() {
        return userMessage;
    }
}



