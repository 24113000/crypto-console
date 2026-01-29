package com.crypto.console.common.model;

public class CommandResult {
    public final boolean success;
    public final String message;

    public CommandResult(boolean success, String message) {
        this.success = success;
        this.message = message;
    }

    public static CommandResult success(String message) {
        return new CommandResult(true, message);
    }

    public static CommandResult failure(String message) {
        return new CommandResult(false, message);
    }
}



