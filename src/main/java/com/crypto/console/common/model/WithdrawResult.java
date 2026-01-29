package com.crypto.console.common.model;

public class WithdrawResult {
    public final String withdrawalId;
    public final String status;
    public final String message;

    public WithdrawResult(String withdrawalId, String status, String message) {
        this.withdrawalId = withdrawalId;
        this.status = status;
        this.message = message;
    }
}



