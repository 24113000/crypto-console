package com.crypto.console.common.model;

public class OrderResult {
    public final String orderId;
    public final String status;
    public final String message;

    public OrderResult(String orderId, String status, String message) {
        this.orderId = orderId;
        this.status = status;
        this.message = message;
    }
}



